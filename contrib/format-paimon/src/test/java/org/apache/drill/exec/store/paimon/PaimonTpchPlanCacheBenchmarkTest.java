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
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/** Reproducible local benchmark using the bundled TPC-H SF0.01 sample. */
public class PaimonTpchPlanCacheBenchmarkTest extends ClusterTest {
  private static final String[] TABLES = {"customer", "orders", "lineitem"};
  private static String tablePrefix;

  @BeforeClass
  public static void setUp() throws Exception {
    assumeTrue("Set drill.tpch.csv.dir to the exported SF0.01 TSV directory",
      System.getProperty("drill.tpch.csv.dir") != null);
    boolean enabled = Boolean.parseBoolean(System.getProperty("drill.plan.cache.benchmark.enabled", "false"));
    startCluster(ClusterFixture.builder(dirTestWatcher)
      .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, enabled)
      .saveProfiles());

    StoragePluginRegistry registry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig original = (FileSystemConfig) registry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(original.getFormats());
    formats.put("paimon", PaimonFormatPluginConfig.builder().build());
    FileSystemConfig updated = new FileSystemConfig(original.getConnection(), original.getConfig(),
      original.getWorkspaces(), formats, PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    updated.setEnabled(original.isEnabled());
    registry.put(DFS_PLUGIN_NAME, updated);

    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouse = dfsRoot.resolve("tpch_sf001_paimon");
    tablePrefix = "dfs.tmp.`tpch_sf001_paimon/default.db/";
    Options options = new Options();
    options.set("warehouse", warehouse.toUri().toString());
    options.set("metastore", "filesystem");
    Path csvDir = Paths.get(System.getProperty("drill.tpch.csv.dir"));
    try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options, new Configuration()))) {
      catalog.createDatabase("default", true);
      for (String name : TABLES) {
        loadTable(catalog, name, csvDir.resolve(name + ".tsv"));
      }
    }
  }

  private static void loadTable(Catalog catalog, String name, Path input) throws Exception {
    String[] columns;
    DataType[] types;
    switch (name) {
      case "customer":
        columns = new String[]{"c_custkey", "c_mktsegment"};
        types = new DataType[]{DataTypes.INT(), DataTypes.STRING()};
        break;
      case "orders":
        columns = new String[]{"o_orderkey", "o_custkey", "o_orderdate", "o_shippriority"};
        types = new DataType[]{DataTypes.INT(), DataTypes.INT(), DataTypes.DATE(), DataTypes.INT()};
        break;
      default:
        columns = new String[]{"l_orderkey", "l_shipdate", "l_quantity", "l_extendedprice",
          "l_discount", "l_returnflag", "l_linestatus"};
        types = new DataType[]{DataTypes.INT(), DataTypes.DATE(), DataTypes.DOUBLE(),
          DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.STRING(), DataTypes.STRING()};
    }
    Schema.Builder schema = Schema.newBuilder();
    for (int i = 0; i < columns.length; i++) {
      schema.column(columns[i], types[i]);
    }
    Identifier id = Identifier.create("default", name);
    catalog.createTable(id, schema.build(), false);
    Table table = catalog.getTable(id);
    BatchWriteBuilder writer = table.newBatchWriteBuilder();
    List<CommitMessage> messages;
    long rows = 0;
    try (BatchTableWrite write = writer.newWrite();
         BufferedReader reader = Files.newBufferedReader(input, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] values = line.split("\t", -1);
        assertEquals(name + " row " + rows, types.length, values.length);
        Object[] fields = new Object[values.length];
        for (int i = 0; i < values.length; i++) {
          if (values[i].isEmpty()) {
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
    System.out.printf("TPCH_Paimon_LOAD table=%s rows=%d%n", name, rows);
  }

  private static String table(String name) {
    return tablePrefix + name + "`";
  }

  @Test
  public void benchmarkTpchQueries() throws Exception {
    final int samples = Integer.getInteger("drill.tpch.benchmark.samples", 10);
    assertTrue(samples > 0);
    String lineitem = table("lineitem");
    String orders = table("orders");
    String customer = table("customer");
    benchmark("q1_filter_count", samples,
      "select count(*) from " + lineitem + " where l_shipdate <= date '%s'",
      new String[][]{{"1998-09-02"}, {"1998-08-02"}, {"1998-07-02"}},
      new long[]{57600, 56772, 56013});
    benchmark("customer_segment_count", samples,
      "select count(*) from " + customer + " where c_mktsegment = '%s'",
      new String[][]{{"BUILDING"}, {"AUTOMOBILE"}, {"FURNITURE"}},
      new long[]{337, 302, 279});
    benchmark("orders_date_count", samples,
      "select count(*) from " + orders + " where o_orderdate < date '%s'",
      new String[][]{{"1995-03-15"}, {"1995-04-15"}, {"1995-05-15"}},
      new long[]{6858, 7022, 7203});
    if (Boolean.getBoolean("drill.tpch.benchmark.include.q6")) {
      benchmark("q6_filter_count", samples,
        "select count(*) from " + lineitem
          + " where l_shipdate >= date '%s' and l_shipdate < date '%s'"
          + " and l_discount between %s and %s and l_quantity < %s",
        new String[][]{{"1994-01-01", "1995-01-01", "0.05", "0.07", "24.0"},
          {"1995-01-01", "1996-01-01", "0.04", "0.06", "25.0"},
          {"1996-01-01", "1997-01-01", "0.03", "0.05", "26.0"}},
        new long[]{1214, 1141, 1260});
    }
  }

  private static void benchmark(String name, int samples, String template, String[][] parameters,
      long[] expected)
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    List<Long> plan = new ArrayList<>();
    List<Long> elapsed = new ArrayList<>();
    long before = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture benchmarkClient = cluster.addClientFixture(new Properties())) {
      for (int i = 0; i < 3; i++) {
        String warmup = "select count(*) from " + table(TABLES[i]);
        assertTrue(benchmarkClient.queryBuilder().sql(warmup).run().succeeded());
      }
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsAfterWarmup = cluster.drillbit().getContext().getPlanCache().getHitCount();
      for (int i = 0; i <= samples; i++) {
        int variant = i % parameters.length;
        String sql = String.format(template, (Object[]) parameters[variant]);
        org.apache.drill.test.QueryBuilder.QuerySummary summary = benchmarkClient.queryBuilder()
          .sql(sql).run();
        assertTrue("query=" + name + ", variant=" + variant + ", id=" + summary.queryIdString(),
          summary.succeeded());
        JsonNode profile = mapper.readTree(new File(cluster.getProfileDir(),
          summary.queryIdString() + ".sys.drill"));
        long planning = profile.path("planEnd").asLong() - profile.path("start").asLong();
        if (i == 0) {
          System.out.printf("TPCH_PLAN_CACHE_COLD enabled=%s query=%s planMs=%d elapsedMs=%d rows=%d%n",
            System.getProperty("drill.plan.cache.benchmark.enabled"), name, planning,
            summary.runTimeMs(), summary.recordCount());
          cluster.drillbit().getContext().getPlanCache().awaitWrites();
        } else {
          plan.add(planning);
          elapsed.add(summary.runTimeMs());
          System.out.printf("TPCH_PLAN_CACHE_SAMPLE enabled=%s query=%s variant=%d planMs=%d"
              + " elapsedMs=%d rows=%d id=%s%n",
            System.getProperty("drill.plan.cache.benchmark.enabled"), name, variant,
            planning, summary.runTimeMs(), summary.recordCount(), summary.queryIdString());
        }
      }
      long hits = cluster.drillbit().getContext().getPlanCache().getHitCount() - hitsAfterWarmup;
      plan.sort(Long::compareTo);
      elapsed.sort(Long::compareTo);
      System.out.printf("TPCH_PLAN_CACHE_SUMMARY enabled=%s query=%s planMedianMs=%d planP90Ms=%d"
          + " elapsedMedianMs=%d elapsedP90Ms=%d hits=%d samples=%d totalHits=%d%n",
        System.getProperty("drill.plan.cache.benchmark.enabled"), name,
        plan.get(samples / 2), plan.get(samples * 9 / 10),
        elapsed.get(samples / 2), elapsed.get(samples * 9 / 10), hits, samples,
        cluster.drillbit().getContext().getPlanCache().getHitCount() - before);
      for (int variant = 0; variant < parameters.length; variant++) {
        String sql = String.format(template, (Object[]) parameters[variant]);
        assertEquals(name + " variant=" + variant, expected[variant],
          benchmarkClient.queryBuilder().sql(sql).singletonLong());
      }
    }
  }

  @Test
  public void diagnoseQ6() throws Exception {
    assumeTrue(Boolean.getBoolean("drill.tpch.q6.diagnostic"));
    String sql = "select count(*) from " + table("lineitem")
      + " where l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'"
      + " and l_discount between 0.05 and 0.07 and l_quantity < 24.0";
    try (ClientFixture diagnosticClient = cluster.addClientFixture(new Properties())) {
      for (int i = 0; i < 3; i++) {
        long before = cluster.drillbit().getContext().getPlanCache().getHitCount();
        long result = diagnosticClient.queryBuilder().sql(sql).singletonLong();
        assertEquals("Q6 iteration=" + i, 1214, result);
        cluster.drillbit().getContext().getPlanCache().awaitWrites();
        System.out.printf("TPCH_Q6_DIAGNOSTIC enabled=%s iteration=%d result=%d hits=%d%n",
          System.getProperty("drill.plan.cache.benchmark.enabled"), i, result,
          cluster.drillbit().getContext().getPlanCache().getHitCount() - before);
      }
    }
    ObjectMapper mapper = new ObjectMapper();
    File[] profiles = cluster.getProfileDir().listFiles((directory, name) -> name.endsWith(".sys.drill"));
    if (profiles != null) {
      for (File profileFile : profiles) {
        JsonNode profile = mapper.readTree(profileFile);
        if (sql.equals(profile.path("query").asText())) {
          System.out.printf("TPCH_Q6_PLAN enabled=%s plan=%s%n",
            System.getProperty("drill.plan.cache.benchmark.enabled"),
            profile.path("plan").asText().replace('\n', '|'));
          break;
        }
      }
    }
  }

  @Test
  public void diagnoseQ6Components() throws Exception {
    assumeTrue(Boolean.getBoolean("drill.tpch.q6.components"));
    String source = "select count(*) from " + table("lineitem") + " where ";
    String[] predicates = {
      "l_shipdate >= date '1994-01-01'",
      "l_shipdate < date '1995-01-01'",
      "l_discount >= 0.05",
      "l_discount <= 0.07",
      "l_quantity < 24.0",
      "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'",
      "l_discount >= 0.05 and l_discount <= 0.07",
      "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'"
        + " and l_discount >= 0.05 and l_discount <= 0.07",
      "l_shipdate >= date '1994-01-01' and l_shipdate < date '1995-01-01'"
        + " and l_discount >= 0.05 and l_discount <= 0.07 and l_quantity < 24.0"
    };
    try (ClientFixture diagnosticClient = cluster.addClientFixture(new Properties())) {
      for (int i = 0; i < predicates.length; i++) {
        long result = diagnosticClient.queryBuilder().sql(source + predicates[i]).singletonLong();
        System.out.printf("TPCH_Q6_COMPONENT enabled=%s index=%d result=%d%n",
          System.getProperty("drill.plan.cache.benchmark.enabled"), i, result);
      }
    }
  }

  @Test
  public void diagnoseJoin() throws Exception {
    assumeTrue(Boolean.getBoolean("drill.tpch.join.diagnostic"));
    String source = "select count(*) from " + table("customer") + " c join "
      + table("orders") + " o on c.c_custkey = o.o_custkey";
    String[] predicates = {
      "",
      " where c.c_mktsegment = 'BUILDING'",
      " where o.o_orderdate < date '1995-03-15'",
      " where c.c_mktsegment = 'BUILDING' and o.o_orderdate < date '1995-03-15'"
    };
    long[] expected = {15000, 3706, 6858, 1693};
    try (ClientFixture diagnosticClient = cluster.addClientFixture(new Properties())) {
      for (int i = 0; i < predicates.length; i++) {
        String sql = source + predicates[i];
        long result = diagnosticClient.queryBuilder().sql(sql).singletonLong();
        assertEquals("join index=" + i, expected[i], result);
        System.out.printf("TPCH_JOIN_DIAGNOSTIC enabled=%s index=%d result=%d%n",
          System.getProperty("drill.plan.cache.benchmark.enabled"), i, result);
        if (i > 0) {
          ObjectMapper mapper = new ObjectMapper();
          File[] profiles = cluster.getProfileDir().listFiles((directory, name) -> name.endsWith(".sys.drill"));
          if (profiles != null) {
            for (File profileFile : profiles) {
              JsonNode profile = mapper.readTree(profileFile);
              if (sql.equals(profile.path("query").asText())) {
                System.out.printf("TPCH_JOIN_PLAN index=%d plan=%s%n", i,
                  profile.path("plan").asText().replace('\n', '|'));
                break;
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void diagnoseComplexQueries() throws Exception {
    assumeTrue(Boolean.getBoolean("drill.tpch.complex.diagnostic"));
    String q1 = "select l_returnflag, l_linestatus, count(*), sum(l_quantity),"
      + " sum(l_extendedprice) from " + table("lineitem")
      + " where l_shipdate <= date '1998-09-02' group by l_returnflag, l_linestatus"
      + " order by l_returnflag, l_linestatus";
    String q3 = "select o.o_orderkey, sum(l.l_extendedprice * (1 - l.l_discount)) as revenue"
      + " from " + table("customer") + " c join " + table("orders")
      + " o on c.c_custkey = o.o_custkey join " + table("lineitem")
      + " l on l.l_orderkey = o.o_orderkey"
      + " where c.c_mktsegment = 'BUILDING' and o.o_orderdate < date '1995-03-15'"
      + " and l.l_shipdate > date '1995-03-15' group by o.o_orderkey"
      + " order by revenue desc limit 10";
    try (ClientFixture diagnosticClient = cluster.addClientFixture(new Properties())) {
      long q1Rows = diagnosticClient.queryBuilder().sql(q1).run().recordCount();
      long q3Rows = diagnosticClient.queryBuilder().sql(q3).run().recordCount();
      System.out.printf("TPCH_COMPLEX_DIAGNOSTIC enabled=%s q1Rows=%d q3Rows=%d%n",
        System.getProperty("drill.plan.cache.benchmark.enabled"), q1Rows, q3Rows);
      assertEquals(4, q1Rows);
      assertEquals(10, q3Rows);
      long q1Count = diagnosticClient.queryBuilder().sql("select sum(cnt) from "
        + "(select count(*) as cnt from " + table("lineitem")
        + " where l_shipdate <= date '1998-09-02'"
        + " group by l_returnflag, l_linestatus) q").singletonLong();
      long q3Keys = diagnosticClient.queryBuilder().sql(
        "select sum(o_orderkey) from (" + q3 + ") q").singletonLong();
      System.out.printf("TPCH_COMPLEX_VALUES enabled=%s q1Count=%d q3OrderKeys=%d%n",
        System.getProperty("drill.plan.cache.benchmark.enabled"), q1Count, q3Keys);
      assertEquals(57600, q1Count);
      assertEquals(306743, q3Keys);
    }
  }
}
