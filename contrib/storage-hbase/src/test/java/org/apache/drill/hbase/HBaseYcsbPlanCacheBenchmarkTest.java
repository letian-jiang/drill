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
package org.apache.drill.hbase;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.exec.store.hbase.HBaseStoragePluginConfig;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBuilder.QuerySummary;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** YCSB-style HBase reads: compare planning and end-to-end cost in one cluster. */
public class HBaseYcsbPlanCacheBenchmarkTest extends ClusterTest {
  private static final TableName TABLE_NAME = TableName.valueOf("drill_ycsb_plan_cache");
  private static final String SQL_TABLE = "hbase.`drill_ycsb_plan_cache`";
  private static final int ROWS = 1_000;

  @BeforeClass
  public static void setUp() throws Exception {
    HBaseTestsSuite.configure(true, false);
    HBaseTestsSuite.initCluster();
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true).saveProfiles());
    HBaseStoragePluginConfig config = new HBaseStoragePluginConfig(null, false);
    config.setEnabled(true);
    config.setZookeeperPort(HBaseTestsSuite.getZookeeperPort());
    cluster.drillbit().getContext().getStorage().put("hbase", config);

    HTableDescriptor descriptor = new HTableDescriptor(TABLE_NAME);
    descriptor.addFamily(new HColumnDescriptor("f"));
    HBaseTestsSuite.getAdmin().createTable(descriptor);
    try (Table table = HBaseTestsSuite.getConnection().getTable(TABLE_NAME)) {
      List<Put> puts = new ArrayList<>();
      for (int row = 0; row < ROWS; row++) {
        Put put = new Put(key(row).getBytes(StandardCharsets.UTF_8));
        for (int field = 0; field < 10; field++) {
          put.addColumn("f".getBytes(StandardCharsets.UTF_8),
              ("field" + field).getBytes(StandardCharsets.UTF_8),
              ("value" + row + "_" + field).getBytes(StandardCharsets.UTF_8));
        }
        puts.add(put);
      }
      table.put(puts);
    }
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    if (HBaseTestsSuite.getAdmin() != null && HBaseTestsSuite.getAdmin().tableExists(TABLE_NAME)) {
      HBaseTestsSuite.getAdmin().disableTable(TABLE_NAME);
      HBaseTestsSuite.getAdmin().deleteTable(TABLE_NAME);
    }
    HBaseTestsSuite.tearDownCluster();
  }

  private static String key(int row) {
    return String.format("user%08d", row);
  }

  @Test
  public void benchmarkReadWorkloads() throws Exception {
    int samples = Integer.getInteger("drill.ycsb.samples", 7);
    benchmark("point_read", "select row_key from " + SQL_TABLE +
        " where row_key = '%s'", new String[][] {{key(101)}, {key(250)}, {key(900)}}, 1, samples);
    benchmark("range_scan", "select row_key from " + SQL_TABLE +
        " where row_key >= '%s' and row_key < '%s'",
        new String[][] {{key(100), key(150)}, {key(250), key(300)}, {key(900), key(950)}},
        50, samples);
    benchmark("column_filter", "select row_key from " + SQL_TABLE +
        " t where t.f.field0 = '%s'",
        new String[][] {{"value101_0"}, {"value250_0"}, {"value900_0"}}, 1, samples);
  }

  private static void benchmark(String name, String template, String[][] variants,
      int expectedRows, int samples) throws Exception {
    assertTrue("samples must be positive", samples > 0);
    ObjectMapper mapper = new ObjectMapper();
    List<Long> offPlans = new ArrayList<>();
    List<Long> offElapsed = new ArrayList<>();
    List<Long> onPlans = new ArrayList<>();
    List<Long> onElapsed = new ArrayList<>();
    try (ClientFixture offFirst = cluster.addClientFixture(new Properties());
         ClientFixture offSecond = cluster.addClientFixture(new Properties());
         ClientFixture onFirst = cluster.addClientFixture(new Properties());
         ClientFixture onSecond = cluster.addClientFixture(new Properties())) {
      offFirst.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      offSecond.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      onFirst.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      onSecond.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true);
      String warmupSql = String.format(template, (Object[]) variants[0]);
      runQuery(mapper, name, warmupSql, offFirst, expectedRows, false, 0);
      runQuery(mapper, name, warmupSql, onFirst, expectedRows, true, 0);
      cluster.drillbit().getContext().getPlanCache().awaitWrites();

      for (int i = 1; i <= samples; i++) {
        String sql = String.format(template, (Object[]) variants[i % variants.length]);
        ClientFixture off = i % 2 == 0 ? offFirst : offSecond;
        ClientFixture on = i % 2 == 0 ? onFirst : onSecond;
        // Alternate order so transient load does not systematically favor one mode.
        Sample baseline;
        Sample cached;
        if (i % 2 == 0) {
          cached = runQuery(mapper, name, sql, on, expectedRows, true, i);
          baseline = runQuery(mapper, name, sql, off, expectedRows, false, i);
        } else {
          baseline = runQuery(mapper, name, sql, off, expectedRows, false, i);
          cached = runQuery(mapper, name, sql, on, expectedRows, true, i);
        }
        assertEquals("baseline cache hits", 0, baseline.hits);
        assertEquals("cached query hits", 1, cached.hits);
        offPlans.add(baseline.planMs);
        offElapsed.add(baseline.elapsedMs);
        onPlans.add(cached.planMs);
        onElapsed.add(cached.elapsedMs);
      }
      for (String[] variant : variants) {
        String sql = String.format(template, (Object[]) variant);
        List<String> baseline = readKeys(offFirst, sql);
        List<String> cached = readKeys(onSecond, sql);
        assertEquals(name + " cache result", baseline, cached);
        assertEquals(expectedRows, cached.size());
        if (expectedRows == 1) {
          int row = Integer.parseInt(variant[0].replace("value", "").replace("_0", "")
              .replace("user", ""));
          assertEquals(key(row), cached.get(0));
        } else {
          List<String> expected = new ArrayList<>();
          int start = Integer.parseInt(variant[0].substring("user".length()));
          for (int row = start; row < start + expectedRows; row++) {
            expected.add(key(row));
          }
          assertEquals(expected, cached);
        }
      }
    }
    long offPlan = median(offPlans);
    long offE2e = median(offElapsed);
    long onPlan = median(onPlans);
    long onE2e = median(onElapsed);
    System.out.printf("HBASE_YCSB_COMPARE workload=%s samples=%d offPlanMs=%d onPlanMs=%d "
            + "planSpeedup=%.2f offE2eMs=%d onE2eMs=%d e2eSpeedup=%.2f%n",
        name, samples, offPlan, onPlan, (double) offPlan / onPlan,
        offE2e, onE2e, (double) offE2e / onE2e);
  }

  private static Sample runQuery(ObjectMapper mapper, String name, String sql,
      ClientFixture client, int expectedRows, boolean enabled, int sample) throws Exception {
    long before = cluster.drillbit().getContext().getPlanCache().getHitCount();
    QuerySummary result = client.queryBuilder().sql(sql).run();
    assertTrue(name + " " + result.queryIdString(), result.succeeded());
    assertEquals(name + " sample=" + sample, expectedRows, result.recordCount());
    JsonNode profile = mapper.readTree(new File(cluster.getProfileDir(),
        result.queryIdString() + ".sys.drill"));
    Sample measurement = new Sample(profile.path("planEnd").asLong()
        - profile.path("start").asLong(), result.runTimeMs(),
        cluster.drillbit().getContext().getPlanCache().getHitCount() - before);
    System.out.printf("HBASE_YCSB_SAMPLE cache=%s workload=%s sample=%d planMs=%d e2eMs=%d hits=%d rows=%d%n",
        enabled, name, sample, measurement.planMs, measurement.elapsedMs,
        measurement.hits, result.recordCount());
    return measurement;
  }

  private static List<String> readKeys(ClientFixture client, String sql) throws Exception {
    DirectRowSet rows = client.queryBuilder().sql(sql).rowSet();
    try {
      RowSetReader reader = rows.reader();
      List<String> keys = new ArrayList<>();
      while (reader.next()) {
        keys.add(new String(reader.scalar(0).getBytes(), StandardCharsets.UTF_8));
      }
      Collections.sort(keys);
      return keys;
    } finally {
      rows.clear();
    }
  }

  private static long median(List<Long> values) {
    Collections.sort(values);
    return values.get(values.size() / 2);
  }

  private static final class Sample {
    private final long planMs;
    private final long elapsedMs;
    private final long hits;

    private Sample(long planMs, long elapsedMs, long hits) {
      this.planMs = planMs;
      this.elapsedMs = elapsedMs;
      this.hits = hits;
    }
  }
}
