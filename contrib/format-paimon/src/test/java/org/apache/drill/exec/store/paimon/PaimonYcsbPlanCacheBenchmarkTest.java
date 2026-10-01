/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.paimon;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.Hashing;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Read-only YCSB-style workloads over a generated Paimon primary-key table. */
public class PaimonYcsbPlanCacheBenchmarkTest extends ClusterTest {
  private static final int ROWS = 10_000;
  private static final String TABLE = "dfs.tmp.`ycsb_paimon/default.db/usertable`";

  @BeforeClass
  public static void setUp() throws Exception {
    boolean enabled = Boolean.parseBoolean(System.getProperty("drill.ycsb.cache.enabled", "false"));
    startCluster(ClusterFixture.builder(dirTestWatcher)
      .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, enabled).saveProfiles());

    StoragePluginRegistry registry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig original = (FileSystemConfig) registry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(original.getFormats());
    formats.put("paimon", PaimonFormatPluginConfig.builder().build());
    FileSystemConfig updated = new FileSystemConfig(original.getConnection(), original.getConfig(),
      original.getWorkspaces(), formats, PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    updated.setEnabled(original.isEnabled());
    registry.put(DFS_PLUGIN_NAME, updated);

    Path warehouse = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath())
      .resolve("ycsb_paimon");
    Options options = new Options();
    options.set("warehouse", warehouse.toUri().toString());
    options.set("metastore", "filesystem");
    try (Catalog catalog = CatalogFactory.createCatalog(
        CatalogContext.create(options, new Configuration()))) {
      catalog.createDatabase("default", true);
      Schema.Builder schema = Schema.newBuilder().column("ycsb_key", DataTypes.STRING());
      for (int i = 0; i < 10; i++) {
        schema.column("field" + i, DataTypes.STRING());
      }
      schema.option(CoreOptions.BUCKET.key(), "1").primaryKey("ycsb_key");
      Identifier id = Identifier.create("default", "usertable");
      catalog.createTable(id, schema.build(), false);
      Table table = catalog.getTable(id);
      BatchWriteBuilder writer = table.newBatchWriteBuilder();
      List<CommitMessage> messages;
      try (BatchTableWrite write = writer.newWrite()) {
        for (int row = 0; row < ROWS; row++) {
          Object[] fields = new Object[11];
          fields[0] = BinaryString.fromString(key(row));
          for (int col = 0; col < 10; col++) {
            fields[col + 1] = BinaryString.fromString(payload(row, col));
          }
          write.write(GenericRow.of(fields));
        }
        messages = write.prepareCommit();
      }
      try (BatchTableCommit commit = writer.newCommit()) {
        commit.commit(messages);
      }
    }
    System.out.printf("YCSB_LOAD rows=%d fields=10 primaryKey=ycsb_key%n", ROWS);
  }

  private static String key(int id) {
    return String.format("user%08d", id);
  }

  private static String payload(int row, int field) {
    char[] value = new char[100];
    Arrays.fill(value, (char) ('a' + (row + field) % 26));
    return new String(value);
  }

  private static String quoted(String value) {
    return '"' + value + '"';
  }

  @Test
  public void benchmarkReadWorkloads() throws Exception {
    int samples = Integer.getInteger("drill.ycsb.samples", 9);
    assertTrue(samples > 0);
    String table = Boolean.getBoolean("drill.ycsb.unqualified.table")
      ? "`ycsb_paimon/default.db/usertable`" : TABLE;
    benchmark("point_read", "select field0, field9 from " + table
        + " where ycsb_key = '%s'", new String[][]{
          {key(101)}, {key(2500)}, {key(9000)}}, 1, samples);
    benchmark("range_scan", "select ycsb_key, field0 from " + table
        + " where ycsb_key >= '%s' and ycsb_key < '%s'", new String[][]{
          {key(100), key(150)}, {key(2500), key(2550)}, {key(9000), key(9050)}},
        50, samples);
    benchmark("ordered_scan", "select ycsb_key, field0 from " + table
        + " where ycsb_key >= '%s' order by ycsb_key limit 10", new String[][]{
          {key(100)}, {key(2500)}, {key(9000)}}, 10, samples);
  }

  private static void benchmark(String name, String template, String[][] variants,
      int expectedRows, int samples) throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    boolean enabled = Boolean.parseBoolean(System.getProperty("drill.ycsb.cache.enabled", "false"));
    List<Long> plans = new ArrayList<>();
    List<Long> elapsed = new ArrayList<>();
    int hits = 0;
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      if (Boolean.getBoolean("drill.ycsb.use.default.schema")) {
        assertTrue(first.queryBuilder().sql("USE dfs.tmp").run().succeeded());
        assertTrue(second.queryBuilder().sql("USE dfs.tmp").run().succeeded());
      }
      for (int i = 0; i <= samples; i++) {
        int variant = i % variants.length;
        String sql = String.format(template, (Object[]) variants[variant]);
        ClientFixture client = i % 2 == 0 ? first : second;
        long before = cluster.drillbit().getContext().getPlanCache().getHitCount();
        QuerySummary result = client.queryBuilder().sql(sql).run();
        assertTrue(name + " variant=" + variant + " id=" + result.queryIdString(),
          result.succeeded());
        assertEquals(name + " variant=" + variant, expectedRows, result.recordCount());
        JsonNode profile = mapper.readTree(new File(cluster.getProfileDir(),
          result.queryIdString() + ".sys.drill"));
        long planMs = profile.path("planEnd").asLong() - profile.path("start").asLong();
        long hit = cluster.drillbit().getContext().getPlanCache().getHitCount() - before;
        if (i == 0) {
          cluster.drillbit().getContext().getPlanCache().awaitWrites();
        } else {
          plans.add(planMs);
          elapsed.add(result.runTimeMs());
          hits += hit;
        }
        System.out.printf("YCSB_SAMPLE cache=%s workload=%s sample=%d variant=%d"
            + " planMs=%d elapsedMs=%d hits=%d rows=%d%n",
          enabled, name, i, variant, planMs, result.runTimeMs(), hit, result.recordCount());
      }
      for (int i = 0; i < variants.length; i++) {
        String sql = String.format(template, (Object[]) variants[i]);
        DirectRowSet rows = first.queryBuilder().sql(sql).rowSet();
        List<String> values = new ArrayList<>();
        try {
          RowSetReader reader = rows.reader();
          while (reader.next()) {
            StringBuilder value = new StringBuilder();
            for (int column = 0; column < reader.columnCount(); column++) {
              value.append(reader.column(column).getAsString()).append('|');
            }
            values.add(value.toString());
          }
        } finally {
          rows.clear();
        }
        assertEquals(name + " fingerprint rows variant=" + i, expectedRows, values.size());
        Collections.sort(values);
        List<String> expected = new ArrayList<>();
        int start = Integer.parseInt(variants[i][0].substring("user".length()));
        if ("point_read".equals(name)) {
          expected.add(quoted(payload(start, 0)) + '|' + quoted(payload(start, 9)) + '|');
        } else {
          int end = "range_scan".equals(name) ? start + 50 : start + 10;
          for (int row = start; row < end; row++) {
            expected.add(quoted(key(row)) + '|' + quoted(payload(row, 0)) + '|');
          }
        }
        assertEquals(name + " content variant=" + i, expected, values);
        String fingerprint = Hashing.sha256().hashString(String.join("\n", values),
          StandardCharsets.UTF_8).toString();
        System.out.printf("YCSB_FINGERPRINT cache=%s workload=%s variant=%d sha256=%s%n",
          enabled, name, i, fingerprint);
      }
    }
    Collections.sort(plans);
    Collections.sort(elapsed);
    System.out.printf("YCSB_SUMMARY cache=%s workload=%s samples=%d hits=%d"
        + " planMedianMs=%d elapsedMedianMs=%d%n", enabled, name, samples, hits,
      plans.get(samples / 2), elapsed.get(samples / 2));
  }
}
