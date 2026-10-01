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

import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.security.PlainCredentialsProvider;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.rowSet.RowSet;
import org.apache.drill.exec.physical.rowSet.RowSetBuilder;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.rowSet.RowSetComparison;
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
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;
import static org.apache.drill.exec.util.StoragePluginTestUtils.DFS_PLUGIN_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class PaimonQueriesTest extends ClusterTest {

  private static final String DB_NAME = "default";
  private static final String TABLE_NAME = "append_table";
  private static final String PK_TABLE_NAME = "pk_table";
  private static final String PAIMON_SCAN_PATTERN = "(PAIMON_GROUP_SCAN|PaimonGroupScan)";
  private static String tableRelativePath;
  private static String pkTableRelativePath;
  private static String fractionalTableRelativePath;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ClusterFixtureBuilder clusterBuilder = ClusterFixture.builder(dirTestWatcher)
      .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION,
        Boolean.parseBoolean(System.getProperty("drill.plan.cache.benchmark.enabled", "true")))
      .saveProfiles();
    startCluster(clusterBuilder);

    StoragePluginRegistry pluginRegistry = cluster.drillbit().getContext().getStorage();
    FileSystemConfig pluginConfig = (FileSystemConfig) pluginRegistry.getPlugin(DFS_PLUGIN_NAME).getConfig();
    Map<String, FormatPluginConfig> formats = new HashMap<>(pluginConfig.getFormats());
    formats.put("paimon", PaimonFormatPluginConfig.builder().build());
    FileSystemConfig newPluginConfig = new FileSystemConfig(
      pluginConfig.getConnection(),
      pluginConfig.getConfig(),
      pluginConfig.getWorkspaces(),
      formats,
      PlainCredentialsProvider.EMPTY_CREDENTIALS_PROVIDER);
    newPluginConfig.setEnabled(pluginConfig.isEnabled());
    pluginRegistry.put(DFS_PLUGIN_NAME, newPluginConfig);

    tableRelativePath = createAppendTable();
    pkTableRelativePath = createPrimaryKeyTable();
    fractionalTableRelativePath = createFractionalTable();
  }

  @Test
  public void testReadAppendTable() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", tableRelativePath);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(2, "bob")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testPlanCacheRebuildsSplitsForNewPredicate() throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "select id from dfs.tmp.`%s` where id = %d";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder()
        .sql(String.format(sql, tableRelativePath, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder()
        .sql(String.format(sql, tableRelativePath, 2)).singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheReusesJoinWithSortAndLimit() throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "select a.id from dfs.tmp.`%1$s` a join dfs.tmp.`%2$s` b "
        + "on a.id = b.id where a.id = %3$d order by a.id limit 1";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder()
          .sql(String.format(sql, tableRelativePath, pkTableRelativePath, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder()
          .sql(String.format(sql, tableRelativePath, pkTableRelativePath, 2)).singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheReusesCte() throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "with t as (select id from dfs.tmp.`%s` where id = %d) "
        + "select id from t";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder()
          .sql(String.format(sql, tableRelativePath, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder()
          .sql(String.format(sql, tableRelativePath, 2)).singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheBindsIntegerInProjectAndFilter() throws Exception {
    String sql = "select id + cast(%2$d as integer) from dfs.tmp.`%1$s` "
        + "where id = cast(%3$d as integer)";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, 4, 1), 5,
        String.format(sql, tableRelativePath, 7, 2), 9);
  }

  @Test
  public void testPlanCacheBindsStringPredicate() throws Exception {
    String sql = "select id from dfs.tmp.`%s` where upper(name) = '%s'";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, "ALICE"), 1,
        String.format(sql, tableRelativePath, "CAROL"), 3);
  }

  @Test
  public void testPlanCacheBindsStringProject() throws Exception {
    String sql = "select concat(name, cast('%2$s' as varchar(1))) from dfs.tmp.`%1$s` "
        + "where upper(name) = '%3$s'";
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals("alice!", first.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "!", "ALICE")).singletonString());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals("carol?", second.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "?", "CAROL")).singletonString());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheBindsBooleanLiteral() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` "
        + "where case when id = 1 then cast(%s as boolean) "
        + "else cast(%s as boolean) end";
    assertPlanCacheReusesLong(
        String.format(sql, tableRelativePath, "TRUE", "FALSE"), 1,
        String.format(sql, tableRelativePath, "FALSE", "TRUE"), 2);
  }

  @Test
  public void testPlanCacheBindsFractionalLiteral() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` "
        + "where cast(id as double) < cast(%s as double)";
    assertPlanCacheReusesLong(
        String.format(sql, tableRelativePath, "1.5"), 1,
        String.format(sql, tableRelativePath, "2.5"), 2);
  }

  @Test
  public void testPlanCacheBindsFractionalLiteralWithoutCast() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` "
        + "where discount >= %s and discount <= %s";
    assertPlanCacheReusesLong(
        String.format(sql, fractionalTableRelativePath, "0.05", "0.07"), 1,
        String.format(sql, fractionalTableRelativePath, "0.04", "0.06"), 2);
  }

  @Test
  public void testPaimonFilterOnUnprojectedColumn() throws Exception {
    String sql = "select id from dfs.tmp.`%s` where name = '%s'";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, "alice"), 1,
        String.format(sql, tableRelativePath, "carol"), 3);
  }

  @Test
  public void testPlanCacheBindsBigintLiteral() throws Exception {
    String sql = "select id from dfs.tmp.`%s` "
        + "where cast(id as bigint) = cast(%d as bigint)";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, 1), 1,
        String.format(sql, tableRelativePath, 2), 2);
  }

  @Test
  public void testPlanCacheKeepsDateLiteralInTemplate() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` "
        + "where extract(year from date '%s') = %d and id = 1";
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "2020-01-01", 2020)).singletonLong());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(1, second.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "2021-01-01", 2021)).singletonLong());
    }
    assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
  }

  @Test
  public void testPlanCacheBindsDecimalLiteral() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` "
        + "where cast(id as decimal(9,2)) < cast(%s as decimal(9,2))";
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      first.alterSession("planner.enable_decimal_data_type", true);
      second.alterSession("planner.enable_decimal_data_type", true);
      assertEquals(1, first.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "1.50")).singletonLong());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder()
          .sql(String.format(sql, tableRelativePath, "2.50")).singletonLong());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheReusesFixedNullLiteral() throws Exception {
    String sql = "select id from dfs.tmp.`%s` "
        + "where id = coalesce(cast(null as integer), cast(%d as integer))";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, 1), 1,
        String.format(sql, tableRelativePath, 2), 2);
  }

  @Test
  public void testPlanCacheReusesUntypedNullExactly() throws Exception {
    String sql = String.format("select id from dfs.tmp.`%s` where id = coalesce(null, 1)",
        tableRelativePath);
    assertPlanCacheReusesInt(sql, 1, sql, 1);
  }

  @Test
  public void testPlanCacheReusesAggregateWithHaving() throws Exception {
    String sql = "select count(*) from dfs.tmp.`%s` where id > %d having count(*) > %d";
    assertPlanCacheReusesLong(
        String.format(sql, tableRelativePath, 1, 1), 2,
        String.format(sql, tableRelativePath, 2, 0), 1);
  }

  @Test
  public void testPlanCacheReusesNestedSubquery() throws Exception {
    String sql = "select id from dfs.tmp.`%1$s` where id in "
        + "(select id from dfs.tmp.`%1$s` where upper(name) = '%2$s')";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, "ALICE"), 1,
        String.format(sql, tableRelativePath, "CAROL"), 3);
  }

  @Test
  public void testPlanCacheReusesUnionOfPaimonScans() throws Exception {
    String sql = "select id from dfs.tmp.`%1$s` where id = %2$d "
        + "union select id from dfs.tmp.`%1$s` where id = %2$d";
    assertPlanCacheReusesInt(
        String.format(sql, tableRelativePath, 1), 1,
        String.format(sql, tableRelativePath, 2), 2);
  }

  @Test
  public void testPlanCacheReusesWindowOverPaimon() throws Exception {
    String sql = "select id + row_number() over (order by id) from dfs.tmp.`%s` "
        + "where id = %d";
    assertPlanCacheReusesLong(
        String.format(sql, tableRelativePath, 1), 2,
        String.format(sql, tableRelativePath, 2), 3);
  }

  private void assertPlanCacheReusesInt(String firstSql, int firstExpected,
      String secondSql, int secondExpected) throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(firstExpected, first.queryBuilder().sql(firstSql).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(secondExpected, second.queryBuilder().sql(secondSql).singletonInt());
    }
    assertTrue("Expected a cache hit for " + secondSql,
        cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  private void assertPlanCacheReusesLong(String firstSql, long firstExpected,
      String secondSql, long secondExpected) throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(firstExpected, first.queryBuilder().sql(firstSql).singletonLong());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(secondExpected, second.queryBuilder().sql(secondSql).singletonLong());
    }
    assertTrue("Expected a cache hit for " + secondSql,
        cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  private void assertPlanCacheDoesNotReuseLong(String firstSql, long firstExpected,
      String secondSql, long secondExpected) throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(firstExpected, first.queryBuilder().sql(firstSql).singletonLong());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(secondExpected, second.queryBuilder().sql(secondSql).singletonLong());
    }
    assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
  }

  @Test
  public void testPlanCacheDoesNotCacheViewOverPaimon() throws Exception {
    String view = "dfs.tmp.`plan_cache_paimon_view`";
    queryBuilder().sql("create view " + view + " as select id from dfs.tmp.`"
        + tableRelativePath + "`").run();
    try {
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(1, queryBuilder().sql("select id from " + view + " where id = 1").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, queryBuilder().sql("select id from " + view + " where id = 2").singletonInt());
      assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
    } finally {
      queryBuilder().sql("drop view " + view).run();
    }
  }

  /** Run alone with -Ddrill.plan.cache.benchmark.enabled=true and false. */
  @Test
  public void benchmarkPlanCachePlanningTime() throws Exception {
    assumeTrue(System.getProperty("drill.plan.cache.benchmark.enabled") != null);
    final int samples = Integer.getInteger("drill.plan.cache.benchmark.samples", 20);
    final String workload = System.getProperty("drill.plan.cache.benchmark.workload", "paimon");
    final boolean paimon = "paimon".equals(workload);
    assertTrue("Unknown benchmark workload: " + workload, paimon || "parquet".equals(workload));
    final ObjectMapper mapper = new ObjectMapper();
    final long[] planningMs = new long[samples];
    final long[] elapsedMs = new long[samples];
    final String parquetTable = "plan_cache_bench_" + Long.toHexString(System.nanoTime());
    final String parquetPath = "dfs.tmp.`" + parquetTable + "`";
    final String warmupSql = paimon
      ? "select id from dfs.tmp.`" + tableRelativePath + "` where id > %d"
      : "select n_nationkey from " + parquetPath + " where n_nationkey > %d";
    final String measuredSql = paimon
      ? "select id from dfs.tmp.`" + tableRelativePath + "` where id = %d"
      : "select n_nationkey from " + parquetPath + " where n_nationkey = %d";
    try (ClientFixture benchmarkClient = cluster.addClientFixture(new Properties())) {
      if (!paimon) {
        benchmarkClient.queryBuilder().sql("create table " + parquetPath
          + " as select cast(n as integer) as n_nationkey "
          + "from (values (1), (2), (3)) as t(n)").run();
      }
      for (int i = 0; i < 5; i++) {
        benchmarkClient.queryBuilder().sql(String.format(warmupSql, i % 3 + 1)).run();
      }
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      long coldPlanningMs = -1;
      for (int i = 0; i <= samples; i++) {
        org.apache.drill.test.QueryBuilder.QuerySummary summary = benchmarkClient.queryBuilder()
          .sql(String.format(measuredSql, i % 3 + 1)).run();
        assertTrue(summary.succeeded());
        assertEquals("sample=" + i + ", query=" + String.format(measuredSql, i % 3 + 1)
          + ", queryId=" + summary.queryIdString(), 1, summary.recordCount());
        File profileFile = new File(cluster.getProfileDir(), summary.queryIdString() + ".sys.drill");
        JsonNode profile = mapper.readTree(profileFile);
        long planMs = profile.path("planEnd").asLong() - profile.path("start").asLong();
        if (i == 0) {
          coldPlanningMs = planMs;
          cluster.drillbit().getContext().getPlanCache().awaitWrites();
        } else {
          planningMs[i - 1] = planMs;
          elapsedMs[i - 1] = summary.runTimeMs();
        }
      }
      Arrays.sort(planningMs);
      Arrays.sort(elapsedMs);
      long hits = cluster.drillbit().getContext().getPlanCache().getHitCount() - hitsBefore;
      System.out.printf("PLAN_CACHE_BENCH enabled=%s workload=%s coldPlanMs=%d planMedianMs=%d "
          + "planP90Ms=%d elapsedMedianMs=%d elapsedP90Ms=%d hits=%d samples=%d%n",
        System.getProperty("drill.plan.cache.benchmark.enabled"), workload,
        coldPlanningMs, planningMs[samples / 2], planningMs[samples * 9 / 10],
        elapsedMs[samples / 2], elapsedMs[samples * 9 / 10], hits, samples);
      if (paimon && Boolean.parseBoolean(System.getProperty("drill.plan.cache.benchmark.enabled"))) {
        assertEquals(samples, hits);
      } else {
        assertEquals(0, hits);
      }
    }
  }

  @Test
  public void testPlanCacheRefreshesSnapshotAndInvalidatesSchema() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");
    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");
    CatalogContext catalogContext = CatalogContext.create(options, new Configuration());
    Identifier identifier = Identifier.create(DB_NAME, "cache_evolution_table");
    try (Catalog catalog = CatalogFactory.createCatalog(catalogContext)) {
      catalog.createTable(identifier, Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .build(), false);
      writeRows(catalog.getTable(identifier), Arrays.asList(
        GenericRow.of(1, BinaryString.fromString("first"))));

      String path = dfsRoot.relativize(warehouseDir.resolve(DB_NAME + ".db")
        .resolve("cache_evolution_table")).toString().replace('\\', '/');
      String sql = "select id from dfs.tmp.`" + path + "` where id = %d";
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(1, queryBuilder().sql(String.format(sql, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();

      writeRows(catalog.getTable(identifier), Arrays.asList(
        GenericRow.of(2, BinaryString.fromString("second"))));
      assertEquals(2, queryBuilder().sql(String.format(sql, 2)).singletonInt());
      long hitsAfterAppend = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertTrue(hitsAfterAppend > hitsBefore);

      catalog.alterTable(identifier,
        SchemaChange.addColumn("extra", DataTypes.STRING()), false);
      assertEquals(1, queryBuilder().sql(String.format(sql, 1)).singletonInt());
      assertEquals(hitsAfterAppend,
        cluster.drillbit().getContext().getPlanCache().getHitCount());
    }
  }

  @Test
  public void testPlanCacheInvalidatesRecreatedTableAtSamePath() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");
    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");
    Identifier identifier = Identifier.create(DB_NAME, "cache_recreated_table");
    try (Catalog catalog = CatalogFactory.createCatalog(
        CatalogContext.create(options, new Configuration()))) {
      catalog.createTable(identifier, Schema.newBuilder()
          .column("id", DataTypes.INT()).build(), false);
      writeRows(catalog.getTable(identifier), Arrays.asList(GenericRow.of(1)));

      String path = dfsRoot.relativize(warehouseDir.resolve(DB_NAME + ".db")
          .resolve("cache_recreated_table")).toString().replace('\\', '/');
      String sql = "select id from dfs.tmp.`" + path + "` where id = %d";
      assertEquals(1, queryBuilder().sql(String.format(sql, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();

      catalog.dropTable(identifier, false);
      // Paimon's path-based UUID uses the creation time of schema-0; keep the
      // two incarnations distinct on filesystems with one-second timestamps.
      Thread.sleep(1100);
      catalog.createTable(identifier, Schema.newBuilder()
          .column("id", DataTypes.INT())
          .column("extra", DataTypes.STRING()).build(), false);
      writeRows(catalog.getTable(identifier), Arrays.asList(
          GenericRow.of(2, BinaryString.fromString("new"))));

      assertEquals(2, queryBuilder().sql(String.format(sql, 2)).singletonInt());
      assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
    }
  }

  @Test
  public void testPlanCacheTextPlanShowsPaimonPredicateSlot() throws Exception {
    String sql = "select id from dfs.tmp.`" + tableRelativePath + "` where id = %d";
    org.apache.drill.test.QueryBuilder.QuerySummary first = queryBuilder()
        .sql(String.format(sql, 1)).run();
    String firstPlan = new ObjectMapper().readTree(new File(cluster.getProfileDir(),
        first.queryIdString() + ".sys.drill")).path("plan").asText();
    assertTrue(firstPlan, firstPlan.matches("(?s).*" + PAIMON_SCAN_PATTERN + ".*"));
    assertTrue(firstPlan.contains("?0"));
    assertFalse(firstPlan.contains("bound_dynamic_param("));
    assertTrue(firstPlan.endsWith("Parameters: ?0 = 1"));
    cluster.drillbit().getContext().getPlanCache().awaitWrites();

    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    org.apache.drill.test.QueryBuilder.QuerySummary second = queryBuilder()
        .sql(String.format(sql, 2)).run();
    String secondPlan = new ObjectMapper().readTree(new File(cluster.getProfileDir(),
        second.queryIdString() + ".sys.drill")).path("plan").asText();
    assertTrue(secondPlan, secondPlan.matches("(?s).*" + PAIMON_SCAN_PATTERN + ".*"));
    assertTrue(secondPlan.contains("?0"));
    assertFalse(secondPlan.contains("bound_dynamic_param("));
    assertTrue(secondPlan.endsWith("Parameters: ?0 = 2"));
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void testPlanCacheInvalidatesJoinWhenOneTableVersionChanges() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");
    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");
    Identifier identifier = Identifier.create(DB_NAME, "cache_join_version_table");
    try (Catalog catalog = CatalogFactory.createCatalog(
        CatalogContext.create(options, new Configuration()))) {
      catalog.createTable(identifier, Schema.newBuilder()
          .column("id", DataTypes.INT())
          .column("name", DataTypes.STRING())
          .build(), false);
      writeRows(catalog.getTable(identifier), Arrays.asList(
          GenericRow.of(1, BinaryString.fromString("first")),
          GenericRow.of(2, BinaryString.fromString("second"))));

      String secondPath = dfsRoot.relativize(warehouseDir.resolve(DB_NAME + ".db")
          .resolve("cache_join_version_table")).toString().replace('\\', '/');
      String sql = "select a.id from dfs.tmp.`" + tableRelativePath
          + "` a join dfs.tmp.`" + secondPath
          + "` b on a.id = b.id where a.id = %d";
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(1, queryBuilder().sql(String.format(sql, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, queryBuilder().sql(String.format(sql, 2)).singletonInt());
      long hitsAfterReuse = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertTrue(hitsAfterReuse > hitsBefore);

      catalog.alterTable(identifier, SchemaChange.addColumn("extra", DataTypes.STRING()), false);
      assertEquals(1, queryBuilder().sql(String.format(sql, 1)).singletonInt());
      assertEquals(hitsAfterReuse,
          cluster.drillbit().getContext().getPlanCache().getHitCount());
    }
  }

  @Test
  public void testReadPrimaryKeyTable() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", pkTableRelativePath);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "dave")
      .addRow(2, "erin")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testProjectionPushdown() throws Exception {
    String query = String.format("select name from dfs.tmp.`%s`", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*columns=\\[.*name.*\\]")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow("alice")
      .addRow("bob")
      .addRow("carol")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testMultiColumnProjection() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s`", tableRelativePath);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(2, "bob")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdown() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id = 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*2")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2, "bob")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownGT() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*1")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2, "bob")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownLT() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id < 3", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*3")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(2, "bob")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownGE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id >= 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*2")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2, "bob")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownLE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id <= 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*2")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(2, "bob")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownNE() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id <> 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*2")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownAnd() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1 and id < 3", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*booleanAnd")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2, "bob")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownOr() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id = 1 or id = 3", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*booleanOr")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testFilterPushdownNot() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where not (id = 2)", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*2")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testLimitPushdown() throws Exception {
    String query = String.format("select id from dfs.tmp.`%s` limit 2", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*maxRecords=2")
      .match(true);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(2, results.rowCount());
    results.clear();
  }

  @Test
  public void testCombinedPushdownFilterProjectionLimit() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` where id > 1 limit 1", tableRelativePath);

    queryBuilder()
      .sql(query)
      .planMatcher()
      .include(PAIMON_SCAN_PATTERN + ".*condition=.*id.*1")
      .include(PAIMON_SCAN_PATTERN + ".*maxRecords=1")
      .match(true);

    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(2, "bob")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSelectWildcard() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s`", tableRelativePath);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(1, "alice")
      .addRow(2, "bob")
      .addRow(3, "carol")
      .build();
    new RowSetComparison(expected).unorderedVerifyAndClearAll(results);
  }

  @Test
  public void testSelectWithOrderBy() throws Exception {
    String query = String.format("select id, name from dfs.tmp.`%s` order by id desc", tableRelativePath);
    RowSet results = queryBuilder().sql(query).rowSet();
    TupleMetadata actualSchema = results.schema();
    assertEquals(TypeProtos.MinorType.INT, actualSchema.metadata("id").type());
    assertEquals(TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").type());

    TupleMetadata expectedSchema = new SchemaBuilder()
      .add("id", TypeProtos.MinorType.INT, actualSchema.metadata("id").mode())
      .add("name", TypeProtos.MinorType.VARCHAR, actualSchema.metadata("name").mode())
      .buildSchema();
    RowSet expected = new RowSetBuilder(client.allocator(), expectedSchema)
      .addRow(3, "carol")
      .addRow(2, "bob")
      .addRow(1, "alice")
      .build();
    new RowSetComparison(expected).verifyAndClearAll(results);
  }

  @Test
  public void testSelectWithCount() throws Exception {
    String query = String.format("select count(*) from dfs.tmp.`%s`", tableRelativePath);

    assertEquals(3, queryBuilder().sql(query).singletonLong());
  }

  @Test
  public void testSerDe() throws Exception {
    String snapshotQuery = String.format(
      "select snapshot_id from dfs.tmp.`%s#snapshots` order by commit_time limit 1", tableRelativePath);

    long snapshotId = queryBuilder().sql(snapshotQuery).singletonLong();
    String sql = String.format(
      "select count(*) as cnt from table(dfs.tmp.`%s`(type => 'paimon', snapshotId => %d))",
      tableRelativePath, snapshotId);
    String plan = queryBuilder().sql(sql).explainJson();
    long count = queryBuilder().physical(plan).singletonLong();
    assertEquals(2, count);
  }

  @Test
  public void testInvalidColumnName() throws Exception {
    String query = String.format("select id, invalid_column from dfs.tmp.`%s`", tableRelativePath);
    try {
      queryBuilder().sql(query).run();
      fail("Expected UserRemoteException for invalid column name");
    } catch (UserRemoteException e) {
      assertThat(e.getVerboseMessage(), containsString("invalid_column"));
    }
  }

  @Test
  public void testSelectWithSnapshotId() throws Exception {
    String snapshotQuery = String.format(
      "select snapshot_id from dfs.tmp.`%s#snapshots` order by commit_time limit 1", tableRelativePath);

    long snapshotId = queryBuilder().sql(snapshotQuery).singletonLong();
    String query = String.format(
      "select id, name from table(dfs.tmp.`%s`(type => 'paimon', snapshotId => %d))",
      tableRelativePath, snapshotId);
    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectWithSnapshotAsOfTime() throws Exception {
    String snapshotQuery = String.format(
      "select commit_time from dfs.tmp.`%s#snapshots` order by commit_time limit 1", tableRelativePath);

    long snapshotTime = queryBuilder().sql(snapshotQuery).singletonLong();
    String query = String.format(
      "select id, name from table(dfs.tmp.`%s`(type => 'paimon', snapshotAsOfTime => %d))",
      tableRelativePath, snapshotTime);
    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectWithSnapshotIdAndSnapshotAsOfTime() throws Exception {
    String query = String.format(
      "select * from table(dfs.tmp.`%s`(type => 'paimon', snapshotId => %d, snapshotAsOfTime => %d))",
      tableRelativePath, 123, 456);
    try {
      queryBuilder().sql(query).run();
      fail();
    } catch (UserRemoteException e) {
      assertThat(e.getVerboseMessage(),
        containsString("Both 'snapshotId' and 'snapshotAsOfTime' cannot be specified"));
    }
  }

  @Test
  public void testSelectSnapshotsMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#snapshots`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectSchemasMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#schemas`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(1, count);
  }

  @Test
  public void testSelectFilesMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#files`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  @Test
  public void testSelectManifestsMetadata() throws Exception {
    String query = String.format("select * from dfs.tmp.`%s#manifests`", tableRelativePath);

    long count = queryBuilder().sql(query).run().recordCount();
    assertEquals(2, count);
  }

  private static String createAppendTable() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");

    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");

    CatalogContext context = CatalogContext.create(options, new Configuration());
    try (Catalog catalog = CatalogFactory.createCatalog(context)) {
      catalog.createDatabase(DB_NAME, true);

      Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .build();
      Identifier identifier = Identifier.create(DB_NAME, TABLE_NAME);
      catalog.createTable(identifier, schema, false);

      Table table = catalog.getTable(identifier);
      writeRows(table, Arrays.asList(
        GenericRow.of(1, BinaryString.fromString("alice")),
        GenericRow.of(2, BinaryString.fromString("bob"))
      ));
      writeRows(table, Arrays.asList(
        GenericRow.of(3, BinaryString.fromString("carol"))
      ));
    }

    Path tablePath = warehouseDir.resolve(DB_NAME + ".db").resolve(TABLE_NAME);
    Path relativePath = dfsRoot.relativize(tablePath);
    return relativePath.toString().replace('\\', '/');
  }

  private static String createPrimaryKeyTable() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");

    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");

    CatalogContext context = CatalogContext.create(options, new Configuration());
    try (Catalog catalog = CatalogFactory.createCatalog(context)) {
      catalog.createDatabase(DB_NAME, true);

      Schema schema = Schema.newBuilder()
        .column("id", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .option(CoreOptions.BUCKET.key(), "1")
        .primaryKey("id")
        .build();
      Identifier identifier = Identifier.create(DB_NAME, PK_TABLE_NAME);
      catalog.createTable(identifier, schema, false);

      Table table = catalog.getTable(identifier);
      writeRows(table, Arrays.asList(
        GenericRow.of(1, BinaryString.fromString("dave")),
        GenericRow.of(2, BinaryString.fromString("erin"))
      ));
    }

    Path tablePath = warehouseDir.resolve(DB_NAME + ".db").resolve(PK_TABLE_NAME);
    Path relativePath = dfsRoot.relativize(tablePath);
    return relativePath.toString().replace('\\', '/');
  }

  private static String createFractionalTable() throws Exception {
    Path dfsRoot = Paths.get(dirTestWatcher.getDfsTestTmpDir().toURI().getPath());
    Path warehouseDir = dfsRoot.resolve("paimon_warehouse");
    Options options = new Options();
    options.set("warehouse", warehouseDir.toUri().toString());
    options.set("metastore", "filesystem");
    try (Catalog catalog = CatalogFactory.createCatalog(
        CatalogContext.create(options, new Configuration()))) {
      Identifier identifier = Identifier.create(DB_NAME, "fractional_table");
      catalog.createTable(identifier, Schema.newBuilder()
          .column("id", DataTypes.INT())
          .column("discount", DataTypes.DOUBLE())
          .build(), false);
      writeRows(catalog.getTable(identifier), Arrays.asList(
          GenericRow.of(1, 0.04d),
          GenericRow.of(2, 0.05d),
          GenericRow.of(3, 0.08d)));
    }
    return dfsRoot.relativize(warehouseDir.resolve(DB_NAME + ".db")
        .resolve("fractional_table")).toString().replace('\\', '/');
  }

  private static void writeRows(Table table, List<GenericRow> rows) throws Exception {
    BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();
    List<CommitMessage> messages;
    try (BatchTableWrite write = writeBuilder.newWrite()) {
      for (GenericRow row : rows) {
        write.write(row);
      }
      messages = write.prepareCommit();
    }
    try (BatchTableCommit commit = writeBuilder.newCommit()) {
      commit.commit(messages);
    }
  }

}
