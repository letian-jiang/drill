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
package org.apache.drill.exec.nativeexecution.scan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Base64;
import org.apache.drill.exec.physical.impl.velox.NativeMinorPlanSerializer;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitControl.QueryContextInformation;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Real, unmodified plugin creators, including legacy ScanBatch and managed ScanOperatorExec.
 */
public class TestGenericPluginScan extends NativeScanTestBase {

  private static final ObjectMapper JSON = new ObjectMapper();

  static ObjectNode descriptor(boolean extended, int rows) throws Exception {
    ObjectNode descriptor = (ObjectNode) JSON.readTree("""
      {"provider":"drill-java-subscan","scan":{"pop":"mock-sub-scan","@id":7,
      "extended":%s,"entries":[{"records":%d,"extended":%s,"types":[
      {"name":"v","type":"INT","mode":"REQUIRED"}]}]}}
      """.formatted(extended, rows, extended));
    PlanFragment context = PlanFragment.newBuilder().setHandle(FragmentHandle.newBuilder().setQueryId(QueryId.newBuilder().setPart1(19).setPart2(23)).setMajorFragmentId(2).setMinorFragmentId(3)).setAssignment(cluster.drillbit().getContext().getEndpoint().toBuilder().clearNativeExecution()).setCredentials(UserCredentials.newBuilder().setUserName("scan-test-user")).setContext(QueryContextInformation.newBuilder().setQueryStartTime(1234567).setDefaultSchemaName("dfs.tmp")).setOptionsJson("[]").build();
    descriptor.put("fragmentContext", Base64.getEncoder().encodeToString(context.toByteArray()));
    return descriptor;
  }

  private void readOriginal(boolean extended, int expected) throws Exception {
    ObjectNode descriptor = descriptor(extended, expected);
    try (var services = new ScanServices("generic-plugin-test", descriptor);
      var reader = PluginScanReader.open(descriptor, services)) {
      var context = services.getFragmentContext();
      assertEquals("scan-test-user", context.getQueryUserName());
      assertEquals(2, context.getHandle().getMajorFragmentId());
      assertEquals(3, context.getHandle().getMinorFragmentId());
      assertEquals(1234567, context.getContextInformation().getQueryStartTime());
      assertEquals("dfs.tmp", context.getContextInformation().getCurrentDefaultSchema());
      assertTrue(services.getOperatorDefn() instanceof MockSubScanPOP);
      assertEquals("v", reader.schema().getColumn(0).getName());
      new ScanBatchLayout(reader.schema());
      int count = reader.rows();
      while (reader.next()) {
        count += reader.rows();
        new ScanBatchLayout(reader.schema());
      }
      assertEquals(expected, count);
      try {
        ((ScanContext) context).getDataTunnel(DrillbitEndpoint.getDefaultInstance());
        fail("Java exchange allowed");
      } catch (UnsupportedOperationException expectedError) {
      }
    }
  }

  @Test
  public void legacyScanBatchWithoutPluginAdapter() throws Exception {
    readOriginal(false, 130003);
  }

  @Test
  public void managedScanWithoutPluginAdapter() throws Exception {
    readOriginal(true, 130003);
  }

  @Test
  public void emptyLegacyScan() throws Exception {
    readOriginal(false, 0);
  }

  @Test
  public void emptyManagedScan() throws Exception {
    readOriginal(true, 0);
  }

  @Test
  public void serializerAddsGenericBridgeAutomatically() throws Exception {
    var descriptor = descriptor(false, 3);
    try (var services = new ScanServices("serialize-original-plugin", descriptor)) {
      var scan = (MockSubScanPOP) services.planReader().readFragmentLeaf(descriptor.get("scan").toString());
      var plan = JSON.readTree(NativeMinorPlanSerializer.serialize(services.planReader(), scan, false));
      assertEquals("drill-java-subscan", plan.path("jniScan").path("provider").asText());
      assertFalse(plan.path("jniScan").path("scan").has("jniScan"));
      assertEquals(3, plan.path("jniScan").path("scan").path("entries").get(0).path("records").asInt());
    }
  }

  @Test
  public void scanHostOpenAndCloseKeepsOriginalReaderOnly() throws Exception {
    int before = ScanHost.activeScans();
    long handle = ScanHost.open(descriptor(false, 3).toString());
    try {
      assertEquals(before + 1, ScanHost.activeScans());
      assertEquals("v", JSON.readTree(ScanHost.schema(handle)).get(0).path("name").asText());
      ScanHost.cancel(handle);
      // Cancel does not enter the native importer.
      assertEquals(0, ScanHost.readBulk(handle, 0));
    } finally {
      ScanHost.close(handle);
      ScanHost.close(handle);
    }
    assertEquals(before, ScanHost.activeScans());
  }

  @Test
  public void utf8EntryPointsPreserveOriginalPluginColumnNames() throws Exception {
    int before = ScanHost.activeScans();
    for (boolean managed : new boolean[] { false, true }) {
      var descriptor = descriptor(managed, 3);
      ((ObjectNode) descriptor.path("scan").path("entries").get(0).path("types").get(0)).put("name", "列🚀");
      long handle = ScanHost.openUtf8(JSON.writeValueAsBytes(descriptor));
      try {
        assertEquals("列🚀", JSON.readTree(ScanHost.schemaUtf8(handle)).get(0).path("name").asText());
        ScanHost.cancel(handle);
        assertEquals(0, ScanHost.readBulk(handle, 0));
      } finally {
        ScanHost.close(handle);
      }
    }
    assertEquals(before, ScanHost.activeScans());
  }
}
