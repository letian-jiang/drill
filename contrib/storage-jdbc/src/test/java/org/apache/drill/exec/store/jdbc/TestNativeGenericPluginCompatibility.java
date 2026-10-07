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
package org.apache.drill.exec.store.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.drill.common.logical.StoragePluginConfig.AuthMode;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.nativeexecution.scan.ScanHost;
import org.apache.drill.exec.physical.impl.velox.NativeExecutionNode;
import org.apache.drill.exec.physical.rowSet.DirectRowSet;
import org.apache.drill.exec.proto.UserBitShared.QueryProfile;
import org.apache.drill.exec.proto.UserBitShared.FragmentState;
import org.apache.drill.exec.store.easy.text.TextFormatConfig;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.test.BufferingQueryEventListener;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.QueryBatchIterator;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Real SQL -> native RPC -> generic JNI -> original plugin, with no plugin adapter.
 */
public class TestNativeGenericPluginCompatibility extends ClusterTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static Path output;

  private static final String JDBC = "compat_jdbc.COMPAT_JNI.PUBLIC.ITEMS";

  private static final String IDS = "(select cast(id as int) id from dfs.root.`compat_json`)";

  @BeforeClass
  public static void setup() throws Exception {
    assertNotNull("Native engine is required for this explicit integration test", System.getProperty("drill.native.engine.library"));
    output = Path.of(System.getProperty("drill.native.plugin_compat.output"));
    Files.createDirectories(output);
    startCluster(ClusterFixture.builder(dirTestWatcher).withLocalZk().saveProfiles().configProperty(ExecConstants.HTTP_ENABLE, false).maxParallelization(1).sessionOption("planner.slice_target", 1L));
    assertTrue(NativeExecutionNode.isRpcEngine(cluster.drillbit().getContext().getEndpoint()));
    var input = dirTestWatcher.getRootDir().toPath();
    Files.createDirectories(input.resolve("compat_csv"));
    Files.createDirectories(input.resolve("compat_json"));
    cluster.defineFormat("dfs", "csv", new TextFormatConfig(List.of("csv"), "\n", ",", "\"", "\"", "#", false, true));
    for (int part = 0; part < 2; part++) {
      var csv = new StringBuilder("id,label\n");
      var json = new StringBuilder();
      for (int row = 0; row < 22003; row++) {
        int id = part * 22003 + row;
        String label = "列🚀," + id;
        csv.append(id).append(",\"").append(label).append("\"\n");
        var value = new LinkedHashMap<String, Object>();
        value.put("id", id);
        value.put("label", id % 11 == 0 ? null : label);
        value.put("flag", id % 2 == 0);
        value.put("amount", id * 1.25);
        value.put("meta", Map.of("x", id % 7, "text", label));
        value.put("items", List.of(id, id + 1));
        json.append(JSON.writeValueAsString(value)).append('\n');
      }
      Files.writeString(input.resolve("compat_csv/part" + part + ".csv"), csv);
      Files.writeString(input.resolve("compat_json/part" + part + ".json"), json);
    }
    client.alterSession("exec.native_fragment.enabled", false);
    client.alterSession("store.format", "parquet");
    client.queryBuilder().sql("create table dfs.root.`compat_parquet` as select cast(id as int) id, label, " + "cast(amount as decimal(12,2)) amount, flag, cast('2020-01-02' as date) dt, " + "cast('03:04:05' as time) tm, cast('2020-01-02 03:04:05' as timestamp) ts, " + "convert_to(label, 'UTF8') bin from dfs.root.`compat_json`").run();
    Class.forName("org.h2.Driver");
    String url = "jdbc:h2:mem:compat_jni;DB_CLOSE_DELAY=-1";
    try (var connection = DriverManager.getConnection(url, "sa", "");
      var statement = connection.createStatement()) {
      statement.execute("create table items(id integer primary key, label varchar(100), amount decimal(12,2), " + "flag boolean, dt date, tm time, ts timestamp, bin varbinary(100))");
      statement.execute("insert into items select x, case when mod(x,11)=0 then null else '列🚀,'||x end, " + "case when mod(x,13)=0 then null else x*1.25 end, " + "case when mod(x,17)=0 then null else mod(x,2)=0 end, " + "case when mod(x,19)=0 then null else date '2020-01-02' end, " + "case when mod(x,19)=0 then null else time '03:04:05' end, " + "case when mod(x,19)=0 then null else timestamp '2020-01-02 03:04:05' end, " + "case when mod(x,23)=0 then null else X'AB0001FF' end from system_range(0,33006)");
    }
    var config = new JdbcStorageConfig("org.h2.Driver", url, "sa", "", false, false, Map.of("maximumPoolSize", 2), null, AuthMode.SHARED_USER.name(), 10000);
    config.setEnabled(true);
    cluster.defineStoragePlugin("compat_jdbc", config);
  }

  private static Object value(ObjectReader column) {
    if (column.isNull()) {
      return null;
    }
    if (column.type() == ObjectType.ARRAY) {
      var array = column.array();
      var result = new ArrayList<Object>();
      array.rewind();
      while (array.next()) {
        result.add(value(array.entry()));
      }
      return result;
    }
    if (column.type() == ObjectType.TUPLE) {
      var tuple = column.tuple();
      var result = new TreeMap<String, Object>();
      for (int i = 0; i < tuple.columnCount(); i++) {
        result.put(tuple.column(i).schema().name(), value(tuple.column(i)));
      }
      return result;
    }
    Object result = column.getObject();
    if (result instanceof BigDecimal) {
      return ((BigDecimal) result).stripTrailingZeros().toPlainString();
    }
    if (result instanceof byte[]) {
      return Base64.getEncoder().encodeToString((byte[]) result);
    }
    return result.toString();
  }

  private record Result(List<List<Object>> rows, QueryProfile profile, int batches, List<String> types, List<String> valueTypes, List<String> majorTypes) {
  }

  private static String fieldType(org.apache.drill.exec.record.MaterializedField field) {
    var type = field.getType();
    var children = new ArrayList<String>();
    for (var child : field.getChildren()) {
      children.add(fieldType(child));
    }
    return field.getName() + ":" + type.getMinorType() + ":" + type.getPrecision() + ":" + type.getScale() + ":" + type.getMode() + children;
  }

  private static String majorType(org.apache.drill.exec.record.MaterializedField field) {
    var children = new ArrayList<String>();
    for (var child : field.getChildren()) {
      children.add(majorType(child));
    }
    return field.getName() + ":" + Base64.getEncoder().encodeToString(field.getType().toByteArray()) + children;
  }

  private static String valueType(org.apache.drill.exec.record.MaterializedField field) {
    var type = field.getType();
    var children = new ArrayList<String>();
    for (var child : field.getChildren()) {
      children.add(valueType(child));
    }
    boolean decimal = type.getMinorType() == org.apache.drill.common.types.TypeProtos.MinorType.VARDECIMAL;
    boolean repeated = type.getMode() == org.apache.drill.common.types.TypeProtos.DataMode.REPEATED;
    return field.getName() + ":" + type.getMinorType() + ":" + repeated + (decimal ? ":" + type.getPrecision() + ":" + type.getScale() : "") + children;
  }

  private static Result execute(String name, String sql, boolean nativeMode) throws Exception {
    client.alterSession("exec.native_fragment.enabled", nativeMode);
    client.alterSession("exec.native_fragment.strict", nativeMode);
    client.alterSession("exec.native_scan.enabled", false);
    var listener = new BufferingQueryEventListener();
    var iterator = new QueryBatchIterator(client.allocator(), listener);
    List<List<Object>> rows = new ArrayList<>();
    int batches = 0;
    List<String> types = List.of(), valueTypes = List.of(), majorTypes = List.of();
    try {
      client.queryBuilder().sql(sql).withListener(listener);
      while (iterator.next()) {
        if (iterator.batch().getRecordCount() == 0) {
          iterator.release();
          continue;
        }
        var batch = DirectRowSet.fromContainer(iterator.batch());
        batches++;
        try {
          var currentTypes = new ArrayList<String>();
          var currentValueTypes = new ArrayList<String>();
          var currentMajorTypes = new ArrayList<String>();
          for (int i = 0; i < batch.schema().size(); i++) {
            var field = batch.schema().metadata(i).schema();
            currentTypes.add(fieldType(field));
            currentValueTypes.add(valueType(field));
            currentMajorTypes.add(majorType(field));
          }
          types = currentTypes;
          valueTypes = currentValueTypes;
          majorTypes = currentMajorTypes;
          var reader = batch.reader();
          while (reader.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 0; i < batch.schema().size(); i++) {
              row.add(value(reader.column(i)));
            }
            rows.add(row);
          }
        } finally {
          batch.clear();
        }
      }
    } catch (Exception error) {
      Files.writeString(output.resolve(name + (nativeMode ? ".native" : ".java") + ".error.json"), JSON.writeValueAsString(Map.of("sql", sql, "query_id", iterator.queryIdString(), "error", error.toString(), "partial_rows", rows.size())));
      throw error;
    } finally {
      iterator.release();
    }
    var node = cluster.drillbit().getContext();
    QueryProfile profile = null;
    for (int i = 0; i < 200 && profile == null; i++) {
      profile = node.getProfileStoreContext().getCompletedProfileStore().get(iterator.queryIdString());
      if (profile == null) {
        Thread.sleep(10);
      }
    }
    assertNotNull(profile);
    String mode = nativeMode ? "native" : "java";
    Files.write(output.resolve(name + "." + mode + ".profile.json"), node.getProfileStoreContext().getProfileStoreConfig().getSerializer().serialize(profile));
    Files.writeString(output.resolve(name + "." + mode + ".json"), JSON.writeValueAsString(Map.of("sql", sql, "query_id", iterator.queryIdString(), "rows", rows, "batches", batches, "types", types, "value_types", valueTypes, "major_types", majorTypes)));
    return new Result(rows, profile, batches, types, valueTypes, majorTypes);
  }

  private static void compare(String name, String sql, int expectedRows, String... scanTypes) throws Exception {
    Result baseline = execute(name, sql, false);
    assertEquals(expectedRows, baseline.rows().size());
    Result nativeResult = execute(name, sql, true);
    assertEquals("Complete ordered values: " + name, baseline.rows(), nativeResult.rows());
    assertEquals("Value types, decimal precision/scale and nested fields: " + name, baseline.valueTypes(), nativeResult.valueTypes());
    assertEquals("Exact output schema (mode, precision/scale, nested fields): " + name, baseline.types(), nativeResult.types());
    assertEquals("Complete MajorType attributes and proto2 presence: " + name, baseline.majorTypes(), nativeResult.majorTypes());
    boolean exactSchema = true;
    if (expectedRows > 10000) {
      assertTrue("Java fixture did not span batches", baseline.batches() > 1);
      assertTrue("Native fixture did not span batches", nativeResult.batches() > 1);
    }
    var profile = nativeResult.profile();
    int minors = 0, sources = 0;
    List<String> types = new ArrayList<>();
    var key = org.apache.drill.exec.proto.helper.QueryIdHelper.getQueryId(profile.getId()).replace("-", "");
    for (var major : profile.getFragmentProfileList()) {
      for (var minor : major.getMinorFragmentProfileList()) {
        assertEquals(FragmentState.FINISHED, minor.getState());
        if (major.getMajorFragmentId() == 0) {
          for (var op : minor.getOperatorProfileList()) {
            assertFalse(op.getOperatorTypeName().startsWith("VELOX_"));
          }
          continue;
        }
        minors++;
        assertTrue(NativeExecutionNode.isRpcEngine(minor.getEndpoint()));
        String file = key + "." + major.getMajorFragmentId() + "." + minor.getMinorFragmentId() + ".json";
        var stats = JSON.readTree(output.resolve("stats").resolve(file).toFile());
        assertTrue(stats.path("native_task_created").asBoolean());
        assertEquals("native-rpc", stats.path("execution_transport").asText());
        assertEquals(0, stats.path("host").path("jni_engine_calls").asInt(-1));
        sources += stats.path("jni_scans").size();
        collectScanTypes(JSON.readTree(output.resolve("plans").resolve(file).toFile()), types);
      }
    }
    assertTrue("Query remained in Java root: " + name, minors > 0);
    assertTrue("No actual JNI sources: " + name, sources > 0);
    for (String type : scanTypes) {
      assertTrue("Missing original scan type " + type + " in " + types, types.contains(type));
    }
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (ScanHost.activeScans() != 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals("Leaked original readers", 0, ScanHost.activeScans());
    Files.writeString(output.resolve(name + ".validation.json"), JSON.writeValueAsString(Map.of("equal_ordered_values", true, "equal_value_types", true, "exact_output_schema_equal", exactSchema, "empty_result_schema_compared", false, "rows", expectedRows, "native_minors", minors, "jni_sources", sources, "generic_scan_types", types, "active_readers_after", ScanHost.activeScans())));
  }

  private static void collectScanTypes(com.fasterxml.jackson.databind.JsonNode node, List<String> types) {
    if (node.has("jniScan")) {
      var descriptor = node.get("jniScan");
      assertEquals("Specialized plugin adapter used", "drill-java-subscan", descriptor.path("provider").asText());
      assertFalse(node.has("nativeScan"));
      types.add(descriptor.path("scan").path("pop").asText());
    }
    if (node.isObject()) {
      var fields = node.fields();
      while (fields.hasNext()) {
        var field = fields.next();
        if (!field.getKey().equals("jniScan")) {
          collectScanTypes(field.getValue(), types);
        }
      }
    } else if (node.isArray()) {
      for (var item : node) {
        collectScanTypes(item, types);
      }
    }
  }

  @Test(timeout = 120000)
  public void csvMultipleFilesAndBatches() throws Exception {
    compare("csv_full", "select cast(id as int) id, label from dfs.root.`compat_csv` order by id", 44006, "fs-sub-scan");
  }

  @Test(timeout = 120000)
  public void csvProjectionFilterAggregate() throws Exception {
    compare("csv_aggregate", "select case when cast(id as int)<22003 then 0 else 1 end k, count(*) n from dfs.root.`compat_csv` " + "where cast(id as int)>100 group by case when cast(id as int)<22003 then 0 else 1 end order by k", 2, "fs-sub-scan");
  }

  @Test(timeout = 120000)
  public void jsonNullableUnicodeMultipleFiles() throws Exception {
    compare("json_full", "select id, label, flag, amount from dfs.root.`compat_json` order by id", 44006, "fs-sub-scan");
  }

  @Test(timeout = 120000)
  public void jsonNestedMapsAndArrays() throws Exception {
    compare("json_nested", "select id, meta, items from dfs.root.`compat_json` order by id", 44006, "fs-sub-scan");
  }

  @Test(timeout = 120000)
  public void parquetDecimalTemporalBinaryAndNull() throws Exception {
    compare("parquet_full", "select id, label, amount, flag, dt, tm, ts, bin from dfs.root.`compat_parquet` order by id", 44006, "parquet-row-group-scan");
  }

  @Test(timeout = 120000)
  public void parquetProjectedAggregation() throws Exception {
    compare("parquet_aggregate", "select case when id<22003 then 0 else 1 end k, sum(amount) a from dfs.root.`compat_parquet` " + "where id>100 group by case when id<22003 then 0 else 1 end order by k", 2, "parquet-row-group-scan");
  }

  @Test(timeout = 120000)
  public void jdbcOriginalReaderCrossSourceNativeJoin() throws Exception {
    compare("jdbc_full", "select j.id,j.label,j.amount,j.flag,j.dt,j.tm,j.ts,j.bin from " + JDBC + " j join " + IDS + " d on j.id=d.id order by j.id", 33007, "jdbc-sub-scan");
  }

  @Test(timeout = 120000)
  public void jdbcEmptyResult() throws Exception {
    compare("jdbc_empty", "select j.id,j.label from " + JDBC + " j join " + IDS + " d on j.id=d.id where j.id<0 order by j.id", 0, "jdbc-sub-scan");
  }

  @Test(timeout = 120000)
  public void jdbcProjectedFilterAggregate() throws Exception {
    compare("jdbc_aggregate", "select case when j.id<22003 then 0 else 1 end k,sum(j.amount) a from " + JDBC + " j join " + IDS + " d on j.id=d.id where j.id>100 group by case when j.id<22003 then 0 else 1 end order by k", 2, "jdbc-sub-scan");
  }

  @Test(timeout = 120000)
  public void systemDrillbitsOriginalReader() throws Exception {
    compare("sys_drillbits", "select s.hostname,s.user_port from sys.drillbits s join " + IDS + " d on d.id=s.user_port order by s.hostname,s.user_port", 1, "sys");
  }

  @Test(timeout = 120000)
  public void informationSchemaOriginalReader() throws Exception {
    compare("ischema_tables", "select s.TABLE_SCHEMA,s.TABLE_NAME from `INFORMATION_SCHEMA`.`TABLES` s join " + IDS + " d on d.id=length(s.TABLE_NAME) where s.TABLE_NAME='ITEMS' and s.TABLE_SCHEMA like 'compat_jdbc%' order by s.TABLE_SCHEMA,s.TABLE_NAME", 1, "InfoSchemaSubScan");
  }

  @Test(timeout = 120000)
  public void dynamicJsonSchemaIsRejectedWithoutComputeFallback() throws Exception {
    var file = dirTestWatcher.getRootDir().toPath().resolve("compat_dynamic.json");
    var content = new StringBuilder();
    for (int i = 0; i < 18000; i++) {
      content.append("{\"id\":").append(i).append("}\n");
    }
    content.append("{\"id\":18000,\"extra\":\"late column\"}\n");
    Files.writeString(file, content);
    client.alterSession("exec.enable_union_type", true);
    try {
      String sql = "select * from dfs.root.`compat_dynamic.json` order by id";
      Result baseline = execute("json_dynamic", sql, false);
      assertEquals(18001, baseline.rows().size());
      Exception error = assertThrows(Exception.class, () -> execute("json_dynamic", sql, true));
      assertTrue(error.toString(), error.toString().contains("schema changed during a native Task"));
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
      while (ScanHost.activeScans() != 0 && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertEquals(0, ScanHost.activeScans());
      Files.writeString(output.resolve("json_dynamic.boundary.json"), JSON.writeValueAsString(Map.of("java_rows", 18001, "native_schema_change_rejected", true, "java_union_type_enabled", true, "active_readers_after", 0)));
    } finally {
      client.alterSession("exec.enable_union_type", false);
    }
  }
}
