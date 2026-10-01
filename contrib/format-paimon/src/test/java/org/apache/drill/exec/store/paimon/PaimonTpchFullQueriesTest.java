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
import com.google.common.io.ByteStreams;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

/** Runs the repository's complete 22-query TPC-H set over full SF0.01 Paimon tables. */
public class PaimonTpchFullQueriesTest extends ClusterTest {
  private static final String[] TABLES = {"customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier"};
  private static final int[] EXPECTED_ROWS = {4, 2, 10, 5, 5, 1, 4, 2, 175, 20, 297,
    2, 33, 1, 1, 288, 1, 2, 1, 6, 2, 7};
  private static final String Q4_FINGERPRINT =
    "5763fa39dae29489a07d7bb411263f0db57bf584c8ecb370176fd1c7542bccc9";
  private static final String Q15_FINGERPRINT =
    "da92ed556bf7448c7386a7c33511eee7bfea8e82830c92f89e1df223350d9587";
  private static final String TABLE_PREFIX = "dfs.tmp.`tpch_sf001_paimon/default.db/";

  @BeforeClass
  public static void setUp() throws Exception {
    assumeTrue("Set drill.tpch.full.csv.dir", System.getProperty("drill.tpch.full.csv.dir") != null);
    boolean enabled = Boolean.parseBoolean(System.getProperty("drill.tpch.full.cache.enabled", "false"));
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
      .resolve("tpch_sf001_paimon");
    Options options = new Options();
    options.set("warehouse", warehouse.toUri().toString());
    options.set("metastore", "filesystem");
    Path csvDir = Paths.get(System.getProperty("drill.tpch.full.csv.dir"));
    try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options, new Configuration()))) {
      catalog.createDatabase("default", true);
      for (String name : TABLES) {
        loadTable(catalog, name, csvDir.resolve(name + ".tsv"));
      }
    }
  }

  private static DataType[] types(String table) {
    DataType i = DataTypes.INT();
    DataType d = DataTypes.DOUBLE();
    DataType s = DataTypes.STRING();
    DataType date = DataTypes.DATE();
    switch (table) {
      case "customer": return new DataType[]{i, s, s, i, s, d, s, s};
      case "lineitem": return new DataType[]{i, i, i, i, d, d, d, d, s, s,
        date, date, date, s, s, s};
      case "nation": return new DataType[]{i, s, i, s};
      case "orders": return new DataType[]{i, i, s, d, date, s, s, i, s};
      case "part": return new DataType[]{i, s, s, s, s, i, s, d, s};
      case "partsupp": return new DataType[]{i, i, i, d, s};
      case "region": return new DataType[]{i, s, s};
      case "supplier": return new DataType[]{i, s, s, i, s, d, s};
      default: throw new IllegalArgumentException(table);
    }
  }

  private static void loadTable(Catalog catalog, String name, Path input) throws Exception {
    DataType[] types = types(name);
    String[] names = columnNames(name);
    Schema.Builder schema = Schema.newBuilder();
    for (int i = 0; i < names.length; i++) {
      schema.column(names[i], types[i]);
    }
    Identifier id = Identifier.create("default", name);
    catalog.createTable(id, schema.build(), false);
    Table table = catalog.getTable(id);
    BatchWriteBuilder writer = table.newBatchWriteBuilder();
    List<CommitMessage> messages;
    int rows = 0;
    try (BatchTableWrite write = writer.newWrite();
         BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split("\t", -1);
        assertEquals(name + " row=" + rows, types.length, values.length);
        Object[] fields = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
          if ("\\N".equals(values[i])) {
            fields[i] = null;
          } else if (types[i].equals(DataTypes.INT())) {
            fields[i] = Integer.parseInt(values[i]);
          } else if (types[i].equals(DataTypes.DATE())) {
            fields[i] = (int) LocalDate.parse(values[i]).toEpochDay();
          } else if (types[i].equals(DataTypes.DOUBLE())) {
            fields[i] = Double.parseDouble(values[i]);
          } else {
            fields[i] = BinaryString.fromString(values[i]);
          }
        }
        write.write(GenericRow.of(fields));
        rows++;
      }
      messages = write.prepareCommit();
    }
    try (BatchTableCommit commit = writer.newCommit()) {
      commit.commit(messages);
    }
    System.out.printf("TPCH_FULL_LOAD table=%s rows=%d%n", name, rows);
  }

  private static String[] columnNames(String table) {
    String[] cols;
    switch (table) {
      case "customer": cols = new String[]{"custkey", "name", "address", "nationkey", "phone",
        "acctbal", "mktsegment", "comment"}; break;
      case "lineitem": cols = new String[]{"orderkey", "partkey", "suppkey", "linenumber",
        "quantity", "extendedprice", "discount", "tax", "returnflag", "linestatus", "shipdate",
        "commitdate", "receiptdate", "shipinstruct", "shipmode", "comment"}; break;
      case "nation": cols = new String[]{"nationkey", "name", "regionkey", "comment"}; break;
      case "orders": cols = new String[]{"orderkey", "custkey", "orderstatus", "totalprice",
        "orderdate", "orderpriority", "clerk", "shippriority", "comment"}; break;
      case "part": cols = new String[]{"partkey", "name", "mfgr", "brand", "type", "size",
        "container", "retailprice", "comment"}; break;
      case "partsupp": cols = new String[]{"partkey", "suppkey", "availqty", "supplycost", "comment"}; break;
      case "region": cols = new String[]{"regionkey", "name", "comment"}; break;
      case "supplier": cols = new String[]{"suppkey", "name", "address", "nationkey", "phone",
        "acctbal", "comment"}; break;
      default: throw new IllegalArgumentException(table);
    }
    String prefix = table.equals("partsupp") ? "ps_" : table.substring(0, 1) + "_";
    for (int i = 0; i < cols.length; i++) {
      cols[i] = prefix + cols[i];
    }
    return cols;
  }

  private static String sql(int number) throws Exception {
    String input;
    if (number == 15) {
      try (InputStream stream = PaimonTpchFullQueriesTest.class.getResourceAsStream(
          "/queries/tpch/15-subquery.sql")) {
        if (stream == null) {
          throw new IllegalStateException("Missing Q15 subquery resource");
        }
        input = new String(ByteStreams.toByteArray(stream), StandardCharsets.UTF_8);
      }
    } else {
      Path file = Paths.get(System.getProperty("drill.tpch.full.query.dir"),
        String.format("%02d.sql", number));
      input = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
    }
    input = input.replace("\r\n", "\n");
    input = input.replaceAll("(?m)--[^\\r\\n]*", "");
    for (String name : TABLES) {
      input = input.replace("cp.`tpch/" + name + ".parquet`", TABLE_PREFIX + name + "`");
    }
    if (number == 19) {
      // The three OR branches contain the same equijoin. Expose it as a join
      // condition so Drill can use a hash join rather than a Cartesian join.
      input = input.replace(TABLE_PREFIX + "lineitem` l,\n  " + TABLE_PREFIX + "part` p",
        TABLE_PREFIX + "lineitem` l\n  join " + TABLE_PREFIX
          + "part` p on p.p_partkey = l.l_partkey");
      input = input.replace("p.p_partkey = l.l_partkey\n    and ", "");
    }
    return input.trim();
  }

  @Test
  public void runAllQueries() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    int failed = 0;
    int first = Integer.getInteger("drill.tpch.full.first", 1);
    int last = Integer.getInteger("drill.tpch.full.last", 22);
    boolean enabled = Boolean.parseBoolean(System.getProperty("drill.tpch.full.cache.enabled", "false"));
    try (ClientFixture client = cluster.addClientFixture(new Properties())) {
      for (int query = first; query <= last; query++) {
        String source = sql(query);
        try {
          long expectedRows = -1;
          source = source.replaceFirst(";\\s*$", "");
          for (int pass = 1; pass <= 2; pass++) {
            long before = cluster.drillbit().getContext().getPlanCache().getHitCount();
            String id;
            long rows;
            boolean success = true;
            QuerySummary result = client.queryBuilder().sql(source).run();
            id = result.queryIdString();
            rows = result.recordCount();
            success = result.succeeded();
            JsonNode profile = mapper.readTree(new File(cluster.getProfileDir(), id + ".sys.drill"));
            if (Boolean.getBoolean("drill.tpch.full.print.plan")) {
              System.out.printf("TPCH_FULL_PLAN cache=%s q=%02d pass=%d %s%n",
                enabled, query, pass, profile.path("plan").asText().replace('\n', '|'));
            }
            long planMs = profile.path("planEnd").asLong() - profile.path("start").asLong();
            long hits = cluster.drillbit().getContext().getPlanCache().getHitCount() - before;
            System.out.printf("TPCH_FULL_RESULT cache=%s q=%02d pass=%d success=%s rows=%d"
                + " planMs=%d elapsedMs=%d hits=%d id=%s%n",
              enabled, query, pass, success, rows, planMs,
              result.runTimeMs(), hits, id);
            if (!success) {
              failed++;
              break;
            }
            assertEquals("DuckDB SF0.01 expected row count for Q" + query,
              EXPECTED_ROWS[query - 1], rows);
            if (enabled && query == 15 && pass == 2) {
              assertEquals("Q15 subquery should reuse its plan", 1, hits);
            }
            expectedRows = rows;
            cluster.drillbit().getContext().getPlanCache().awaitWrites();
          }
          List<String> values = new ArrayList<>();
          DirectRowSet result = client.queryBuilder().sql(source).rowSet();
          try {
            if (result != null) {
              RowSetReader batch = result.reader();
              while (batch.next()) {
                StringBuilder row = new StringBuilder();
                for (int col = 0; col < batch.columnCount(); col++) {
                  String value = batch.column(col).getAsString();
                  row.append(value == null ? -1 : value.length()).append(':').append(value).append('|');
                }
                values.add(row.toString());
              }
            }
          } finally {
            if (result != null) {
              result.clear();
            }
          }
          Collections.sort(values);
          assertEquals("first result batch must contain all Q" + query + " rows",
            expectedRows, values.size());
          if (Boolean.getBoolean("drill.tpch.full.print.rows")) {
            for (String row : values) {
              System.out.printf("TPCH_FULL_ROW cache=%s q=%02d %s%n", enabled, query, row);
            }
          }
          String fingerprint = Hashing.sha256().hashString(String.join("\n", values),
            StandardCharsets.UTF_8).toString();
          if (query == 4) {
            assertEquals("Q4 must preserve both pushed predicates", Q4_FINGERPRINT, fingerprint);
          } else if (query == 15) {
            assertEquals("Q15 subquery must match the original view result", Q15_FINGERPRINT, fingerprint);
          }
          System.out.printf("TPCH_FULL_FINGERPRINT cache=%s q=%02d rows=%d sha256=%s%n",
            enabled, query, values.size(), fingerprint);
        } catch (Exception e) {
          failed++;
          System.out.printf("TPCH_FULL_FAILURE cache=%s q=%02d error=%s%n",
            enabled, query, e.toString().replace('\n', ' '));
          if (Boolean.getBoolean("drill.tpch.full.verbose.errors") && e instanceof UserException) {
            System.out.printf("TPCH_FULL_STACK q=%02d %s%n", query,
              ((UserException) e).getVerboseMessage());
          }
        }
      }
    }
    System.out.printf("TPCH_FULL_SUMMARY cache=%s queries=%d failures=%d%n",
      enabled, last - first + 1, failed);
    assertEquals("Failed TPC-H queries", 0, failed);
  }
}
