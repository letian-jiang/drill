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
import java.nio.file.Files;
import java.util.Base64;
import java.util.List;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.velox.NativeMinorPlanSerializer;
import org.apache.drill.exec.physical.impl.velox.NativeExecutionNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.UserCredentials;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.store.easy.text.TextFormatConfig;
import org.apache.drill.exec.store.schedule.CompleteFileWork.FileWorkImpl;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class TestBorrowedGenericPluginScan extends ClusterTest {

  @Test(timeout = 90000)
  public void originalCsvReaderBorrowsNodeRegistryAndPreservesQueryOptions() throws Exception {
    try (var fixture = ClusterFixture.builder(dirTestWatcher).configProperty(ExecConstants.HTTP_ENABLE, false).build()) {
      var node = fixture.drillbit().getContext();
      // A native-enabled Drillbit owns this registration for its entire service lifetime.
      try (AutoCloseable registration = NativeExecutionNode.isRpcEngine(node.getEndpoint()) ? () -> {
      } : ScanHost.registerHost(ScanHostServices.borrow(node))) {
        var csv = new TextFormatConfig(List.of("csv"), "\n", ",", "\"", "\"", "#", false, true);
        fixture.defineFormat("dfs", "generic_csv", csv);
        var file = dirTestWatcher.getRootDir().toPath().resolve("generic.csv");
        Files.writeString(file, "v,w\nfirst,one\nsecond,two\nthird,three\n");
        var plugin = node.getStorage().resolveFormat(node.getStorage().resolve(node.getStorage().getDefinedConfig("dfs"), FileSystemPlugin.class).getConfig(), csv, EasyFormatPlugin.class);
        var path = new org.apache.hadoop.fs.Path(file.toUri());
        var scan = new EasySubScan("scan-test-user", List.of(new FileWorkImpl(0, Files.size(file), path)), plugin, List.of(SchemaPath.getSimplePath("v"), SchemaPath.getSimplePath("w")), path.getParent(), 0, null, -1);
        scan.setOperatorId(7);
        ObjectNode plan = (ObjectNode) new ObjectMapper().readTree(NativeMinorPlanSerializer.serialize(node.getPlanReader(), scan, false));
        ObjectNode descriptor = (ObjectNode) plan.get("jniScan");
        var context = PlanFragment.newBuilder().setAssignment(node.getEndpoint()).setForeman(node.getEndpoint()).setHandle(FragmentHandle.newBuilder().setMajorFragmentId(1).setMinorFragmentId(0).setQueryId(QueryId.newBuilder().setPart1(31).setPart2(37))).setCredentials(UserCredentials.newBuilder().setUserName("scan-test-user")).setOptionsJson("""
          [{"name":"exec.native_scan.batch_records","kind":"LONG","scope":"SESSION","num_val":16384}]
          """).build();
        descriptor.put("fragmentContext", Base64.getEncoder().encodeToString(context.toByteArray()));
        long before = node.getAllocator().getAllocatedMemory();
        try (var services = new ScanServices("original-csv", descriptor);
          var reader = PluginScanReader.open(descriptor, services)) {
          assertSame(node, services.getFragmentContext().getDrillbitContext());
          assertSame(node.getScanExecutor(), services.getScanExecutor());
          assertEquals(16384, services.options().getLong("exec.native_scan.batch_records"));
          assertEquals(3, reader.rows());
          var vectors = reader.vectors().iterator();
          assertEquals("first", vectors.next().getValueVector().getAccessor().getObject(0).toString());
          assertEquals("one", vectors.next().getValueVector().getAccessor().getObject(0).toString());
          assertFalse(reader.next());
        }
        assertEquals("Reader leaked host allocator memory", before, node.getAllocator().getAllocatedMemory());
        // A reader close must leave the node's options, allocator and plugin registry alive.
        node.getOptionManager().getLong("exec.native_scan.batch_records");
        try (var buffer = node.getAllocator().buffer(8)) {
          buffer.setLong(0, 123);
          assertEquals(123, buffer.getLong(0));
        }
        assertNotNull(node.getStorage().getPlugin("dfs"));
      }
    }
  }
}
