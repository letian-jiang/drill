/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.drill.exec.physical.impl.velox;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.test.BufferingQueryEventListener;
import org.apache.drill.test.QueryBatchIterator;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.rowSet.RowSetReader;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration suite: homogeneous Java Drillbits with Java or native execution.
 */
public abstract class PureNativeBenchmark extends ClusterTest {

  protected abstract void defineScan(String dataset) throws Exception;

  private static Object tupleValue(org.apache.drill.exec.vector.accessor.ObjectReader column) {
    if (column.isNull()) {
      return null;
    }
    if (column.type() == org.apache.drill.exec.vector.accessor.ObjectType.ARRAY) {
      var array = column.array();
      var result = new java.util.ArrayList<Object>();
      array.rewind();
      while (array.next()) {
        result.add(tupleValue(array.entry()));
      }
      return result;
    }
    if (column.type() != org.apache.drill.exec.vector.accessor.ObjectType.TUPLE) {
      return column.getObject();
    }
    var tuple = column.tuple();
    var result = new java.util.TreeMap<String, Object>();
    for (int i = 0; i < tuple.columnCount(); ++i) {
      result.put(tuple.column(i).schema().name(), tupleValue(tuple.column(i)));
    }
    return result;
  }

  @Test
  public void nativeFragments() throws Exception {
    String dataset = System.getProperty("drill.native.dataset");
    Assume.assumeNotNull(dataset);
    int workerCount = Integer.getInteger("drill.native.worker_count", 1);
    boolean singleNode = "single".equals(System.getProperty("drill.native.deployment"));
    assertTrue("Benchmark worker count must match deployment", singleNode ? workerCount == 0 : workerCount > 0);
    Path output = Path.of(System.getProperty("drill.native.results_dir"));
    Files.createDirectories(output);
    boolean foremanParticipates = Boolean.getBoolean("drill.native.foreman_participates");
    startCluster(ClusterFixture.builder(dirTestWatcher).withLocalZk().saveProfiles().maxParallelization(Integer.getInteger("drill.native.max_parallelization", 1)).configProperty(ExecConstants.HTTP_ENABLE, false).configProperty(ExecConstants.SERVICE_NAME, "native-benchmark-" + java.util.UUID.randomUUID()).configProperty(ExecConstants.INITIAL_USER_PORT, Integer.getInteger("drill.native.foreman.user_port", 31010)).configProperty(ExecConstants.INITIAL_BIT_PORT, Integer.getInteger("drill.native.foreman.control_port", 31011)).configProperty(ExecConstants.INITIAL_DATA_PORT, Integer.getInteger("drill.native.foreman.data_port", 31012)).sessionOption("planner.slice_target", 1L).sessionOption("planner.enable_mux_exchange", false).sessionOption("planner.enable_demux_exchange", false));
    var context = cluster.drillbit().getContext();
    boolean nativeMode = !"java".equals(System.getProperty("drill.native.worker_mode"));
    var mapper = new ObjectMapper();
    Files.writeString(output.resolve("foreman-ready.json"), mapper.writeValueAsString(Map.of("zk", context.getConfig().getString(ExecConstants.ZK_CONNECTION), "zk_root", context.getConfig().getString(ExecConstants.ZK_ROOT), "cluster_id", context.getConfig().getString(ExecConstants.SERVICE_NAME), "foreman_endpoint", context.getEndpoint().toString(), "classpath", System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")))));
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
    while (context.getBits().stream().filter(endpoint -> !org.apache.drill.exec.planner.fragment.FragmentEndpointPolicy.sameNode(endpoint, context.getEndpoint()) && (nativeMode ? NativeExecutionNode.isRpcEngine(endpoint) : endpoint.getRoles().getJavaExecutor())).count() != workerCount) {
      if (System.nanoTime() > deadline) {
        throw new IllegalStateException(workerCount + " benchmark workers did not register");
      }
      Thread.sleep(100);
    }
    var endpoints = new java.util.ArrayList<Map<String, Object>>();
    for (var node : context.getBits()) {
      var service = node.getNativeExecution();
      endpoints.add(Map.of("address", node.getAddress(), "user_port", node.getUserPort(), "control_port", node.getControlPort(), "data_port", node.getDataPort(), "java_executor", node.getRoles().getJavaExecutor(), "sql_query", node.getRoles().getSqlQuery(), "native_address", service.getAddress(), "native_control_port", service.getControlPort(), "native_data_port", service.getDataPort(), "native_protocol", service.getProtocolVersion()));
    }
    var coordinator = (org.apache.drill.exec.coord.zk.ZKClusterCoordinator) context.getClusterCoordinator();
    var records = coordinator.getCurator().getChildren().forPath("/" + context.getConfig().getString(ExecConstants.SERVICE_NAME));
    assertEquals(workerCount + 1, records.size());
    Files.writeString(output.resolve("cluster-endpoints.json"), mapper.writeValueAsString(Map.of("nodes", endpoints, "registration_ids", records, "foreman_control_port", context.getEndpoint().getControlPort())));
    defineScan(dataset);
    client.alterSession(ExecConstants.STORE_TABLE_USE_SCHEMA_FILE, true);
    Path workspace = Path.of(dataset).getParent().getParent();
    Path schemaFile = Path.of(dataset).resolve("schema.sql");
    if (!Files.exists(schemaFile)) {
      schemaFile = workspace.resolve("benchmark/schema.sql");
    }
    for (String ddl : Files.readString(schemaFile).split(";")) {
      if (!ddl.isBlank()) {
        client.queryBuilder().sql(ddl).run();
      }
    }
    client.queryBuilder().sql("USE dfs.tpch").run();
    client.alterSession("exec.foreman.root_only", !singleNode && !foremanParticipates);
    client.alterSession("exec.native_fragment.enabled", nativeMode);
    client.alterSession("exec.native_scan.enabled", !Boolean.getBoolean("drill.native.force_jni_scan"));
    client.alterSession("exec.native_fragment.strict", nativeMode);
    List<String> failures = new ArrayList<>();
    int warmups = Integer.getInteger("drill.native.warmups", 0);
    int iterations = Integer.getInteger("drill.native.iterations", 1);
    for (String query : System.getProperty("drill.native.queries", "6").split(",")) {
      int number = Integer.parseInt(query);
      for (int cycle = 0; cycle < warmups + iterations; ++cycle) {
        boolean warmup = cycle < warmups;
        String base = String.format("q%02d", number);
        String name = warmups == 0 && iterations == 1 ? base : base + (warmup ? ".warmup" : ".iteration") + String.format("%02d", warmup ? cycle + 1 : cycle - warmups + 1);
        AtomicBoolean timedOut = new AtomicBoolean();
        long queryStart = System.nanoTime();
        try {
          Path queries = Files.isDirectory(Path.of(dataset).resolve("queries")) ? Path.of(dataset).resolve("queries") : workspace.resolve("benchmark/queries");
          queries = Path.of(System.getProperty("drill.native.queries_dir", queries.toString()));
          String sql = Files.readString(queries.resolve(String.format("q%02d.sql", number)));
          sql = sql.trim();
          if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
          }
          AtomicReference<QueryId> submittedId = new AtomicReference<>();
          var listener = new BufferingQueryEventListener() {

            @Override
            public void queryIdArrived(QueryId id) {
              submittedId.set(id);
              super.queryIdArrived(id);
            }
          };
          var iterator = new QueryBatchIterator(client.allocator(), listener);
          var timer = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "benchmark-query-timeout");
            thread.setDaemon(true);
            return thread;
          });
          long start = System.nanoTime();
          int timeoutSeconds = Integer.getInteger("drill.native.query_timeout_seconds", 120);
          timer.scheduleWithFixedDelay(() -> {
            if (System.nanoTime() - start > TimeUnit.SECONDS.toNanos(timeoutSeconds) && submittedId.get() != null && timedOut.compareAndSet(false, true)) {
              System.out.println("Benchmark Q" + number + " exceeded " + timeoutSeconds + " seconds; cancelling");
              try {
                client.client().cancelQuery(submittedId.get()).checkedGet();
              } catch (Exception error) {
                error.printStackTrace();
              }
            }
          }, 1, 1, TimeUnit.SECONDS);
          List<List<String>> rows = new ArrayList<>();
          try {
            client.queryBuilder().sql(sql).withListener(listener);
            while (iterator.next()) {
              // Empty schema batches have no result values. Original Java senders
              // may omit DICT key/value children in this batch; do not ask the row
              // set metadata inference to validate a value which is never read.
              if (iterator.batch().getRecordCount() == 0) {
                iterator.release();
                continue;
              }
              var batch = org.apache.drill.exec.physical.rowSet.DirectRowSet.fromContainer(iterator.batch());
              try {
                RowSetReader reader = batch.reader();
                while (reader.next()) {
                  List<String> row = new ArrayList<>();
                  for (int i = 0; i < batch.schema().size(); ++i) {
                    var column = reader.column(i);
                    Object value = tupleValue(column);
                    row.add(value == null ? null : value instanceof BigDecimal ? ((BigDecimal) value).stripTrailingZeros().toPlainString() : value instanceof byte[] ? java.util.Base64.getEncoder().encodeToString((byte[]) value) : value instanceof Map<?, ?> || value instanceof Object[] || value instanceof List<?> ? mapper.writeValueAsString(value) : value.toString());
                  }
                  rows.add(row);
                }
              } finally {
                batch.clear();
              }
            }
            if (timedOut.get()) {
              throw new TimeoutException("Q" + number + " exceeded " + timeoutSeconds + " seconds");
            }
          } finally {
            iterator.release();
            timer.shutdownNow();
          }
          double ms = (System.nanoTime() - start) / 1_000_000.0;
          org.apache.drill.exec.proto.UserBitShared.QueryProfile profile = null;
          for (int attempt = 0; attempt < 200 && profile == null; ++attempt) {
            profile = context.getProfileStoreContext().getCompletedProfileStore().get(iterator.queryIdString());
            if (profile == null) {
              Thread.sleep(10);
            }
          }
          assertNotNull(profile);
          byte[] profileJson = context.getProfileStoreContext().getProfileStoreConfig().getSerializer().serialize(profile);
          Files.write(output.resolve(name + ".profile.json"), profileJson);
          List<Map<String, Object>> placements = new ArrayList<>();
          for (var major : profile.getFragmentProfileList()) {
            for (var minor : major.getMinorFragmentProfileList()) {
              boolean root = major.getMajorFragmentId() == 0;
              if (!foremanParticipates || root) {
                assertEquals(singleNode || root, org.apache.drill.exec.planner.fragment.FragmentEndpointPolicy.sameNode(minor.getEndpoint(), context.getEndpoint()));
              }
              if (!root) {
                assertEquals(nativeMode, NativeExecutionNode.isRpcEngine(minor.getEndpoint()));
              }
              placements.add(Map.of("major", major.getMajorFragmentId(), "minor", minor.getMinorFragmentId(), "control_port", minor.getEndpoint().getControlPort(), "state", minor.getState().name()));
            }
          }
          String resultJson = mapper.writeValueAsString(Map.of("query", number, "wall_ms", ms, "query_id", iterator.queryIdString(), "rows", rows, "placements", placements));
          Files.writeString(output.resolve(name + ".json"), resultJson);
          if (!warmup && !name.equals(base)) {
            Files.writeString(output.resolve(base + ".json"), resultJson);
            Files.write(output.resolve(base + ".profile.json"), profileJson);
          }
          System.out.println("Benchmark " + name + " completed in " + ms + " ms (" + rows.size() + " rows)");
        } catch (Exception error) {
          failures.add("Q" + number + ": " + error);
          var trace = new java.io.StringWriter();
          error.printStackTrace(new java.io.PrintWriter(trace));
          Files.writeString(output.resolve(name + ".error.stacktrace.txt"), trace.toString());
          Files.writeString(output.resolve(name + ".error.json"), mapper.writeValueAsString(Map.of("query", number, "error", error.toString(), "timed_out", timedOut.get(), "wall_ms", (System.nanoTime() - queryStart) / 1_000_000.0)));
        }
      }
    }
    Files.writeString(output.resolve("queries-complete.json"), mapper.writeValueAsString(Map.of("failures", failures)));
    if (workerCount > 0) {
      long shutdownDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!Files.exists(output.resolve("workers-stopped.json"))) {
        if (System.nanoTime() > shutdownDeadline) {
          throw new IllegalStateException("Workers did not stop before test ZooKeeper shutdown");
        }
        Thread.sleep(50);
      }
    }
    assertTrue(failures.toString(), failures.isEmpty());
  }
}
