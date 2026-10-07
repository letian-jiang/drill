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
import java.nio.file.Path;
import java.util.Base64;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class TestStandalonePluginScan {

  @Rule
  public TemporaryFolder files = new TemporaryFolder();

  @Test(timeout = 60000)
  public void originalFilePluginResolvesWithoutJavaDrillbitAndReusesNodeResources() throws Exception {
    Path file = files.newFile("native-host.csv").toPath();
    Files.writeString(file, "v,w\nfirst,one\nsecond,two\nthird,three\n");
    var endpoint = DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(64311).setDataPort(64312).build();
    var descriptor = TestGenericPluginScan.descriptor(false, 3);
    var context = PlanFragment.parseFrom(Base64.getDecoder().decode(descriptor.path("fragmentContext").asText()));
    descriptor.put("fragmentContext", Base64.getEncoder().encodeToString(context.toBuilder().setAssignment(endpoint).build().toByteArray()));
    ObjectMapper json = new ObjectMapper();
    ObjectNode scan = (ObjectNode) json.readTree("""
      {"pop":"fs-sub-scan","@id":7,"userName":"scan-test-user",
       "files":[],"storage":{"type":"file","connection":"file:///","enabled":true,
       "formats":{"csv":{"type":"text","extensions":["csv"],"extractHeader":true}}},
       "format":{"type":"text","extensions":["csv"],"extractHeader":true},
       "columns":["`v`","`w`"],"partitionDepth":0,"limit":-1}
      """);
    var work = scan.putArray("files").addObject();
    work.put("path", file.toString()).put("start", 0).put("length", Files.size(file));
    scan.put("selectionRoot", file.getParent().toString());
    descriptor.set("scan", scan);
    try (var host = new StandaloneScanHostServices(endpoint);
      var registration = ScanHost.registerHost(host)) {
      assertFalse(host.pluginContext().isForeman(endpoint));
      for (var api : java.util.List.of((Runnable) () -> host.pluginContext().getController(), () -> host.pluginContext().getDataConnectionsPool(), () -> host.pluginContext().getWorkBus(), () -> host.pluginContext().getOperatorTable())) {
        try {
          api.run();
          fail("Java node service was created");
        } catch (UnsupportedOperationException expected) {
        }
      }
      assertNotSame(host.executor(), host.scanExecutor());
      assertNotSame(host.scanExecutor(), host.decodeExecutor());
      for (int round = 0; round < 3; ++round) {
        try (var resources = new ScanServices("csv-original-creator", descriptor);
          var reader = PluginScanReader.open(descriptor, resources)) {
          assertSame(host.allocator(), ((ScanContext) resources.getFragmentContext()).getRootAllocator());
          assertSame(host.pluginContext(), resources.getFragmentContext().getDrillbitContext());
          assertEquals(3, reader.rows());
          assertEquals("first", reader.vectors().iterator().next().getValueVector().getAccessor().getObject(0).toString());
          assertFalse(reader.next());
        }
        assertEquals(0, host.allocator().getAllocatedMemory());
      }
      assertNotNull(host.pluginContext().getStorage());
    }
  }

  @Test(timeout = 60000)
  public void automaticStandaloneHostSurvivesReaderCloseAndCanBeReopened() throws Exception {
    var descriptor = TestGenericPluginScan.descriptor(false, 0);
    Object first;
    try (var one = new ScanServices("first", descriptor)) {
      first = one.hostServices();
    }
    try (var two = new ScanServices("second", descriptor)) {
      assertSame(first, two.hostServices());
      try {
        ScanHost.closeStandaloneHosts();
        fail("Host closed with a live reader resource lease");
      } catch (IllegalStateException expected) {
      }
      two.getManagedBuffer(8).setLong(0, 1234);
    }
    ScanHost.closeStandaloneHosts();
    try (var reopened = new ScanServices("reopened", descriptor)) {
      assertNotSame(first, reopened.hostServices());
      assertNotNull(reopened.planReader());
    }
    ScanHost.closeStandaloneHosts();
  }
}
