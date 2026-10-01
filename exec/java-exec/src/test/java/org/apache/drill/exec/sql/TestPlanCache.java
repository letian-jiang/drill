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
package org.apache.drill.exec.sql;

import java.io.File;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.drill.exec.planner.physical.PlannerSettings;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestPlanCache extends ClusterTest {
  @BeforeClass
  public static void setup() throws Exception {
    startCluster(ClusterFixture.builder(dirTestWatcher)
        .setOptionDefault(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, true)
        .saveProfiles());
  }

  @Test
  public void reusesParameterizedPlanAcrossConnections() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql("SELECT CAST(1 AS INTEGER) AS v").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder().sql("SELECT CAST(2 AS INTEGER) AS v").singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void sessionOptionControlsPlanCache() throws Exception {
    String sql = "SELECT CAST(456 AS INTEGER) AS v";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(456, first.queryBuilder().sql(sql).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      second.alterSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION, false);
      assertEquals(456, second.queryBuilder().sql(sql).singletonInt());
      assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
      second.resetSession(PlannerSettings.ENABLE_PLAN_CACHE_OPTION);
      assertEquals(456, second.queryBuilder().sql(sql).singletonInt());
      assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
    }
  }

  @Test
  public void reusesPlanWithoutParameterSlots() throws Exception {
    String sql = "SELECT v FROM (VALUES (1)) AS t(v)";
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    assertEquals(1, queryBuilder().sql(sql).singletonInt());
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    assertEquals(1, queryBuilder().sql(sql).singletonInt());
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
  }

  @Test
  public void reusesPlanWhenProjectionEliminatesParameter() throws Exception {
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(
          "SELECT x FROM (SELECT x, CAST(5 AS INTEGER) AS unused "
              + "FROM (VALUES (1)) AS t(x))").run().recordCount());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(1, second.queryBuilder().sql(
          "SELECT x FROM (SELECT x, CAST(6 AS INTEGER) AS unused "
              + "FROM (VALUES (1)) AS t(x))").run().recordCount());
      assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
    }
  }

  @Test
  public void textPlanShowsTemplateSlotsAndCurrentValues() throws Exception {
    long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      org.apache.drill.test.QueryBuilder.QuerySummary firstQuery = first.queryBuilder()
          .sql("SELECT CAST(731 AS INTEGER) + CAST(41 AS INTEGER) AS v").run();
      JsonNode firstProfile = new ObjectMapper().readTree(new File(cluster.getProfileDir(),
          firstQuery.queryIdString() + ".sys.drill"));
      String firstPlan = firstProfile.path("plan").asText();
      assertFalse(firstProfile.path("planCacheHit").asBoolean());
      assertFalse(firstPlan.contains("bound_dynamic_param("));
      assertTrue(firstPlan.contains("?0"));
      assertTrue(firstPlan.contains("?1"));
      assertTrue(firstPlan.endsWith("Parameters: ?0 = 731, ?1 = 41"));
      cluster.drillbit().getContext().getPlanCache().awaitWrites();

      org.apache.drill.test.QueryBuilder.QuerySummary secondQuery = second.queryBuilder()
          .sql("SELECT CAST(732 AS INTEGER) + CAST(42 AS INTEGER) AS v").run();
      JsonNode secondProfile = new ObjectMapper().readTree(new File(cluster.getProfileDir(),
          secondQuery.queryIdString() + ".sys.drill"));
      String secondPlan = secondProfile.path("plan").asText();
      assertTrue(secondProfile.path("planCacheHit").asBoolean());
      assertFalse(secondPlan.contains("bound_dynamic_param("));
      assertTrue(secondPlan.contains("?0"));
      assertTrue(secondPlan.contains("?1"));
      assertTrue(secondPlan.endsWith("Parameters: ?0 = 732, ?1 = 42"));
      assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > hitsBefore);
    }
  }

  @Test
  public void systemOptionChangeDoesNotReuseOldPlan() throws Exception {
    String option = "planner.enable_hashagg";
    boolean original = cluster.drillbit().getContext().getOptionManager().getBoolean(option);
    String sql = "SELECT CAST(123 AS INTEGER) AS v";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(123, first.queryBuilder().sql(sql).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      long hitsBefore = cluster.drillbit().getContext().getPlanCache().getHitCount();
      try {
        first.alterSystem(option, !original);
        assertEquals(123, second.queryBuilder().sql(sql).singletonInt());
        assertEquals(hitsBefore, cluster.drillbit().getContext().getPlanCache().getHitCount());
      } finally {
        first.resetSystem(option);
      }
    }
  }

  @Test
  public void bindsProjectAndFilterExpressions() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(11, first.queryBuilder().sql("SELECT x + CAST(1 AS INTEGER) AS v "
          + "FROM (VALUES(10)) AS t(x) WHERE x < CAST(20 AS INTEGER)").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(12, second.queryBuilder().sql("SELECT x + CAST(2 AS INTEGER) AS v "
          + "FROM (VALUES(10)) AS t(x) WHERE x < CAST(30 AS INTEGER)").singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesOrderedLimitedQuery() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(
          "SELECT x AS v FROM (VALUES(1), (2)) AS t(x) "
              + "WHERE x = CAST(1 AS INTEGER) ORDER BY v LIMIT 1").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder().sql(
          "SELECT x AS v FROM (VALUES(1), (2)) AS t(x) "
              + "WHERE x = CAST(2 AS INTEGER) ORDER BY v LIMIT 1").singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesGroupedQuery() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(
          "SELECT x AS v FROM (VALUES(1), (2)) AS t(x) "
              + "WHERE x = CAST(1 AS INTEGER) GROUP BY x").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder().sql(
          "SELECT x AS v FROM (VALUES(1), (2)) AS t(x) "
              + "WHERE x = CAST(2 AS INTEGER) GROUP BY x").singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesExactQueryWithoutParameterSlots() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "SELECT x FROM (VALUES(1)) AS t(x) ORDER BY x LIMIT 1";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(sql).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(1, second.queryBuilder().sql(sql).singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesUnionQuery() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "SELECT CAST(%d AS INTEGER) AS v UNION SELECT CAST(%d AS INTEGER) AS v";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(String.format(sql, 1, 1)).singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder().sql(String.format(sql, 2, 2)).singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesDeterministicOtherFunction() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql(
          "SELECT ABS(CAST(-1 AS INTEGER)) AS v").singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(2, second.queryBuilder().sql(
          "SELECT ABS(CAST(-2 AS INTEGER)) AS v").singletonInt());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesWindowQuery() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    String sql = "SELECT x + ROW_NUMBER() OVER (ORDER BY x) AS v "
        + "FROM (VALUES(1), (2)) AS t(x) WHERE x = CAST(%d AS INTEGER)";
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(2L, first.queryBuilder().sql(String.format(sql, 1)).singletonLong());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(3L, second.queryBuilder().sql(String.format(sql, 2)).singletonLong());
    }
    assertTrue(cluster.drillbit().getContext().getPlanCache().getHitCount() > previousHits);
  }

  @Test
  public void reusesTypedParametersInComplexExpressions() throws Exception {
    try (ClientFixture client = cluster.addClientFixture(new Properties())) {
      long hits = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(5, client.queryBuilder().sql(
          "SELECT x FROM (VALUES (5)) AS t(x) WHERE x BETWEEN 2 - 1 AND 8 + 1")
          .singletonInt());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(5, client.queryBuilder().sql(
          "SELECT x FROM (VALUES (5)) AS t(x) WHERE x BETWEEN 3 - 1 AND 9 + 1")
          .singletonInt());
      assertTrue("BETWEEN arithmetic should reuse its typed parameters",
          cluster.drillbit().getContext().getPlanCache().getHitCount() > hits);

      hits = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals(9.0, client.queryBuilder().sql(
          "SELECT CAST(CASE WHEN x > 0 THEN x * (1 - 0.1) ELSE 0 END AS DOUBLE) "
              + "FROM (VALUES (10)) AS t(x)").singletonDouble(), 0.0001);
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals(8.0, client.queryBuilder().sql(
          "SELECT CAST(CASE WHEN x > 0 THEN x * (1 - 0.2) ELSE 0 END AS DOUBLE) "
              + "FROM (VALUES (10)) AS t(x)").singletonDouble(), 0.0001);
      assertTrue("CASE arithmetic should reuse its typed parameters",
          cluster.drillbit().getContext().getPlanCache().getHitCount() > hits);

      hits = cluster.drillbit().getContext().getPlanCache().getHitCount();
      assertEquals("ab", client.queryBuilder().sql(
          "SELECT SUBSTRING('abcde' FROM 1 FOR 2)").singletonString());
      cluster.drillbit().getContext().getPlanCache().awaitWrites();
      assertEquals("wxy", client.queryBuilder().sql(
          "SELECT SUBSTRING('vwxyz' FROM 2 FOR 3)").singletonString());
      assertTrue("SUBSTRING positions should reuse their typed parameters",
          cluster.drillbit().getContext().getPlanCache().getHitCount() > hits);
    }
  }

  @Test
  public void parquetScanFallsBackToOrdinaryPlanning() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    try (ClientFixture first = cluster.addClientFixture(new Properties());
         ClientFixture second = cluster.addClientFixture(new Properties())) {
      assertEquals(1, first.queryBuilder().sql("SELECT n_nationkey FROM cp.`tpch/nation.parquet` "
          + "WHERE n_nationkey = CAST(1 AS INTEGER)").singletonInt());
      assertEquals(2, second.queryBuilder().sql("SELECT n_nationkey FROM cp.`tpch/nation.parquet` "
          + "WHERE n_nationkey = CAST(2 AS INTEGER)").singletonInt());
    }
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
  }

  @Test
  public void dynamicFunctionDoesNotEnterCache() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    queryBuilder().sql("SELECT random() + 1").run();
    queryBuilder().sql("SELECT random() + 2").run();
    queryBuilder().sql("SELECT rand() + 1").run();
    queryBuilder().sql("SELECT rand() + 2").run();
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
  }

  @Test
  public void queryTimeFunctionsDoNotEnterCache() throws Exception {
    long previousHits = cluster.drillbit().getContext().getPlanCache().getHitCount();
    queryBuilder().sql("SELECT now(), CAST(1 AS INTEGER)").run();
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    queryBuilder().sql("SELECT now(), CAST(2 AS INTEGER)").run();
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
    queryBuilder().sql("SELECT CURRENT_TIMESTAMP, CAST(1 AS INTEGER)").run();
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    queryBuilder().sql("SELECT CURRENT_TIMESTAMP, CAST(2 AS INTEGER)").run();
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
    queryBuilder().sql("SELECT CURRENT_DATE, CAST(1 AS INTEGER)").run();
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    queryBuilder().sql("SELECT CURRENT_DATE, CAST(2 AS INTEGER)").run();
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
    queryBuilder().sql("SELECT unix_timestamp(), CAST(1 AS INTEGER)").run();
    cluster.drillbit().getContext().getPlanCache().awaitWrites();
    queryBuilder().sql("SELECT unix_timestamp(), CAST(2 AS INTEGER)").run();
    assertEquals(previousHits, cluster.drillbit().getContext().getPlanCache().getHitCount());
  }
}
