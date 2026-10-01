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
package org.apache.drill.exec.store.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.iceberg.format.IcebergFormatPlugin;
import org.apache.drill.exec.store.iceberg.format.IcebergFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IcebergPlanCacheTest extends ClusterTest {
  private static final String MAIN = "dfs.tmp.iceberg_cache_main";
  private static final String MUTABLE = "dfs.tmp.iceberg_cache_mutable";
  private static Table mutableTable;
  private static final ObjectMapper JSON = new ObjectMapper();

  @BeforeClass
  public static void setUp() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
      .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true)
      .saveProfiles());

    StoragePluginRegistry registry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig original = (FileSystemConfig) registry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(original.getFormats());
    formats.put("iceberg", IcebergFormatPluginConfig.builder().build());
    FileSystemConfig updated = new FileSystemConfig(original.getConnection(), original.getConfig(),
      original.getWorkspaces(), formats, null, PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    updated.setEnabled(original.isEnabled());
    registry.put(DFS_PLUGIN_NAME, updated);

    Configuration config = new Configuration();
    config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    HadoopTables tables = new HadoopTables(config);
    Schema schema = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "category", Types.StringType.get()),
      Types.NestedField.optional(3, "amount", Types.DecimalType.of(8, 2)));
    String root = dirTestWatcher.getDfsTestTmpDir().toURI().getPath();
    Table main = tables.create(schema, Paths.get(root, "iceberg_cache_main").toString());
    List<DataFile> files = new ArrayList<>();
    for (int file = 0; file < 8; file++) {
      files.add(writeRows(main, "part-" + file, file * 125, 125));
    }
    org.apache.iceberg.AppendFiles append = main.newAppend();
    files.forEach(append::appendFile);
    append.commit();

    mutableTable = tables.create(schema, Paths.get(root, "iceberg_cache_mutable").toString());
    mutableTable.newAppend().appendFile(writeRows(mutableTable, "initial", 1, 2)).commit();
  }

  private static DataFile writeRows(Table table, String name, int first, int count) throws Exception {
    OutputFile output = table.io().newOutputFile(
      new Path(table.location(), FileFormat.PARQUET.addExtension(name)).toUri().getPath());
    FileAppender<Record> appender = new GenericAppenderFactory(table.schema())
      .newAppender(output, FileFormat.PARQUET);
    try {
      for (int id = first; id < first + count; id++) {
        Record row = GenericRecord.create(table.schema());
        row.setField("id", id);
        row.setField("category", id == 0 ? null : "category-" + id % 10);
        row.setField("amount", new BigDecimal(id + ".50"));
        appender.add(row);
      }
    } finally {
      appender.close();
    }
    return DataFiles.builder(table.spec())
      .withInputFile(output.toInputFile())
      .withMetrics(appender.metrics())
      .build();
  }

  private static long hits() {
    return cluster.drillbit().getContext().getPlanCache().getHitCount();
  }

  @Test
  public void rebindsPredicatesAcrossConnections() throws Exception {
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(101, first.queryBuilder().sql("SELECT id FROM " + MAIN + " WHERE id = 101")
        .singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long before = hits();
      assertEquals(751, second.queryBuilder().sql("SELECT id FROM " + MAIN + " WHERE id = 751")
        .singletonInt());
      assertTrue(hits() > before);
      before = hits();
      assertEquals(999, first.queryBuilder().sql("SELECT id FROM " + MAIN + " WHERE id = 999")
        .singletonInt());
      assertTrue(hits() > before);
    }
  }

  @Test
  public void rebindsPredicateToEmptyScan() throws Exception {
    String query = "SELECT id FROM " + MAIN + " WHERE id > ";
    assertEquals(99, queryBuilder().sql(query + "900").run().recordCount());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    long before = hits();
    assertEquals(0, queryBuilder().sql(query + "999").run().recordCount());
    assertTrue(hits() > before);
  }

  @Test
  public void rebindsStringAndDecimalPredicates() throws Exception {
    String stringQuery = "SELECT COUNT(*) FROM " + MAIN + " WHERE category = '%s'";
    assertEquals(100, queryBuilder().sql(String.format(stringQuery, "category-1")).singletonLong());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    long before = hits();
    assertEquals(100, queryBuilder().sql(String.format(stringQuery, "category-2")).singletonLong());
    assertTrue(hits() > before);

    String decimalQuery = "SELECT COUNT(*) FROM " + MAIN + " WHERE amount >= %s";
    assertEquals(100, queryBuilder().sql(String.format(decimalQuery, "900.50")).singletonLong());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    before = hits();
    assertEquals(50, queryBuilder().sql(String.format(decimalQuery, "950.50")).singletonLong());
    assertTrue(hits() > before);
  }

  @Test
  public void currentSnapshotIsReplannedAndSchemaChangeInvalidates() throws Exception {
    String sql = "SELECT id FROM " + MUTABLE + " WHERE id >= 1";
    assertEquals(2, queryBuilder().sql(sql).run().recordCount());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    mutableTable.newAppend().appendFile(writeRows(mutableTable, "appended", 3, 1)).commit();
    long before = hits();
    assertEquals(3, queryBuilder().sql(sql).run().recordCount());
    assertTrue("data-only snapshot change should retain the cached plan", hits() > before);

    mutableTable.updateSpec().addField("category").commit();
    before = hits();
    assertEquals(3, queryBuilder().sql(sql).run().recordCount());
    assertTrue("partition spec change should replan tasks from the cached scan", hits() > before);

    mutableTable.updateSchema().addColumn("new_col", Types.StringType.get()).commit();
    before = hits();
    assertEquals(3, queryBuilder().sql(sql).run().recordCount());
    assertEquals("schema change must invalidate the cached plan", before, hits());
  }

  @Test
  public void metadataTablesDoNotEnterCache() throws Exception {
    String sql = "SELECT COUNT(*) FROM dfs.tmp.`iceberg_cache_main#snapshots`";
    assertEquals(1, queryBuilder().sql(sql).singletonLong());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    long before = hits();
    assertEquals(1, queryBuilder().sql(sql).singletonLong());
    assertEquals(before, hits());
  }

  @Test
  public void recreatedTableHasDifferentCacheVersion() throws Exception {
    Configuration config = new Configuration();
    config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
    HadoopTables tables = new HadoopTables(config);
    String location = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath(),
      "iceberg_cache_recreated").toString();
    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    tables.create(schema, location);
    FileSystemPlugin dfs = (FileSystemPlugin) cluster.drillbit().getContext().getStorage()
      .getPlugin(DFS_PLUGIN_NAME);
    IcebergFormatPlugin iceberg = (IcebergFormatPlugin) dfs.getFormatPlugin("iceberg");
    String firstVersion = iceberg.planCacheTableVersion(new Path(location));
    assertTrue(tables.dropTable(location, true));
    tables.create(schema, location);
    String secondVersion = iceberg.planCacheTableVersion(new Path(location));
    assertFalse(firstVersion.equals(secondVersion));
  }

  @Test
  public void benchmarkPlanningWithAndWithoutCache() throws Exception {
    List<Long> cachedPlans = new ArrayList<>();
    List<Long> uncachedPlans = new ArrayList<>();
    List<Long> cachedElapsed = new ArrayList<>();
    List<Long> uncachedElapsed = new ArrayList<>();
    try (ClientFixture cached = cluster.addClientFixture(new Properties());
         ClientFixture uncached = cluster.addClientFixture(new Properties())) {
      uncached.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      measure(cached, "SELECT id FROM " + MAIN + " WHERE id = 101", false);
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      measure(uncached, "SELECT id FROM " + MAIN + " WHERE id = 101", false);
      for (int i = 0; i < 20; i++) {
        int id = 100 + i * 37;
        String sql = "SELECT id FROM " + MAIN + " WHERE id = " + id;
        Sample withoutCache;
        Sample withCache;
        if (i % 2 == 0) {
          withoutCache = measure(uncached, sql, false);
          withCache = measure(cached, sql, true);
        } else {
          withCache = measure(cached, sql, true);
          withoutCache = measure(uncached, sql, false);
        }
        uncachedPlans.add(withoutCache.planMs);
        uncachedElapsed.add(withoutCache.elapsedMs);
        cachedPlans.add(withCache.planMs);
        cachedElapsed.add(withCache.elapsedMs);
      }
    }
    System.out.printf("ICEBERG_PLAN_CACHE_BENCHMARK samples=20 files=8 rows=1000"
        + " planMedianMs cached=%d uncached=%d"
        + " elapsedMedianMs cached=%d uncached=%d%n",
      median(cachedPlans), median(uncachedPlans),
      median(cachedElapsed), median(uncachedElapsed));
  }

  private static Sample measure(ClientFixture client, String sql, boolean expectHit) throws Exception {
    QuerySummary result = client.queryBuilder().sql(sql).run();
    assertTrue(sql, result.succeeded());
    assertEquals(sql, 1, result.recordCount());
    JsonNode profile = JSON.readTree(new File(cluster.getProfileDir(),
      result.queryIdString() + ".sys.drill"));
    assertEquals(sql, expectHit, profile.path("planCacheHit").asBoolean());
    return new Sample(profile.path("planEnd").asLong() - profile.path("start").asLong(),
      result.runTimeMs());
  }

  private static long median(List<Long> samples) {
    Collections.sort(samples);
    return samples.get(samples.size() / 2);
  }

  private static class Sample {
    private final long planMs;
    private final long elapsedMs;

    private Sample(long planMs, long elapsedMs) {
      this.planMs = planMs;
      this.elapsedMs = elapsedMs;
    }
  }
}
