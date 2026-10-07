/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.fragment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import java.util.List;
import java.util.Map;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.impl.velox.NativeExecutionNode;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.CoordinationProtos.NativeExecutionEndpoint;
import org.apache.drill.exec.proto.SchemaBitControl;
import org.apache.drill.exec.proto.SchemaCoordinationProtos;
import org.junit.Test;

/**
 * Discovery must survive registration/control serialization and worker restart.
 */
public class TestNativeEndpointDiscovery {

  private static DrillbitEndpoint endpoint(int controlPort, int nativePort) {
    var builder = DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(controlPort);
    if (nativePort != 0) {
      builder.setNativeExecution(NativeExecutionEndpoint.newBuilder().setAddress("127.0.0.1").setPort(nativePort).setProtocolVersion(1));
    }
    return builder.build();
  }

  private static DrillbitEndpoint rpc(int control) {
    return endpoint(control, 0).toBuilder().setDataPort(control + 1).setNativeExecution(NativeExecutionEndpoint.newBuilder().setAddress("127.0.0.1").setProtocolVersion(2).setControlPort(control + 100).setDataPort(control + 101)).build();
  }

  @Test
  public void homogeneousCandidatesIncludeForemanAndEveryCapableNode() throws Exception {
    var foreman = rpc(10);
    var worker = rpc(20);
    var ordinary = endpoint(30, 0);
    assertTrue(NativeExecutionNode.isRpcEngine(foreman));
    assertEquals(List.of(foreman, worker), FragmentEndpointPolicy.nativeCandidates(List.of(foreman, ordinary, worker), foreman));
    assertEquals(List.of(foreman), FragmentEndpointPolicy.nativeCandidates(List.of(foreman), foreman));
    assertEquals(List.of(worker), FragmentEndpointPolicy.candidates(List.of(foreman, worker), foreman, false));
    assertEquals(List.of(foreman), FragmentEndpointPolicy.candidates(List.of(foreman, worker), foreman, true));
  }

  @Test
  public void capabilityNeedsBothListeningServicesAndDoesNotUseLegacyPort() {
    var node = rpc(10);
    var service = node.getNativeExecution();
    for (var invalid : List.of(service.toBuilder().clearDataPort().build(), service.toBuilder().clearControlPort().build(), service.toBuilder().setDataPort(65536).build(), service.toBuilder().setProtocolVersion(1).build(), service.toBuilder().clearAddress().build())) {
      assertFalse(NativeExecutionNode.isRpcEngine(node.toBuilder().setNativeExecution(invalid).build()));
    }
    assertTrue(NativeExecutionNode.isRpcEngine(node.toBuilder().setNativeExecution(service.toBuilder().setPort(65536)).build()));
  }

  @Test
  public void legacyIndependentAndPortZeroEnginesAreNotHomogeneousCandidates() {
    var legacy = endpoint(3, 31003);
    var embedded = rpc(10).toBuilder().setNativeExecution(NativeExecutionEndpoint.newBuilder().setAddress("127.0.0.1").setPort(0).setProtocolVersion(1)).build();
    assertThrows(PhysicalOperatorSetupException.class, () -> FragmentEndpointPolicy.nativeCandidates(List.of(legacy, embedded), embedded));
  }

  @Test
  public void mandatoryScanAffinityRejectsInsteadOfComputeFallback() throws Exception {
    var capable = rpc(10);
    var ordinary = endpoint(30, 0);
    var candidates = FragmentEndpointPolicy.nativeCandidates(List.of(ordinary, capable), ordinary);
    assertThrows(PhysicalOperatorSetupException.class, () -> FragmentEndpointPolicy.validateAffinities(candidates, Map.of(ordinary, new EndpointAffinity(ordinary, 1, true, 1))));
    FragmentEndpointPolicy.validateAffinities(candidates, Map.of(capable, new EndpointAffinity(capable, 1, true, 1)));
  }

  @Test
  public void frozenRoutesPreservePlansAndRoundTripBothSchemas() throws Exception {
    var query = org.apache.drill.exec.proto.UserBitShared.QueryId.newBuilder().setPart1(7).setPart2(9).build();
    var rootHandle = org.apache.drill.exec.proto.ExecProtos.FragmentHandle.newBuilder().setQueryId(query).setMajorFragmentId(0).setMinorFragmentId(0).build();
    var foreman = rpc(10);
    var worker = rpc(20);
    var root = PlanFragment.newBuilder().setHandle(rootHandle).setAssignment(foreman).build();
    var nonRoot = PlanFragment.newBuilder().setHandle(rootHandle.toBuilder().setMajorFragmentId(1)).setAssignment(worker).setFragmentJson("unchanged original plan").build();
    var routes = org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.freeze(root, List.of(nonRoot));
    assertEquals(foreman.getControlPort(), routes.getRoute(0).getControlPort());
    assertEquals(worker.getNativeExecution().getControlPort(), routes.getRoute(1).getControlPort());
    assertEquals(worker, routes.getRoute(1).getAssignment());
    assertEquals("unchanged original plan", nonRoot.getFragmentJson());
    var request = org.apache.drill.exec.proto.BitControl.InitializeFragments.newBuilder().addFragment(nonRoot).setNativeExecution(routes).build();
    assertEquals(request, org.apache.drill.exec.proto.BitControl.InitializeFragments.parseFrom(request.toByteArray()));
    var builder = org.apache.drill.exec.proto.BitControl.InitializeFragments.newBuilder();
    ProtobufIOUtil.mergeFrom(request.toByteArray(), builder, SchemaBitControl.InitializeFragments.MERGE);
    assertEquals(request, builder.build());
    assertEquals(request, org.apache.drill.exec.proto.BitControl.InitializeFragments.parseFrom(ProtobufIOUtil.toByteArray(request, SchemaBitControl.InitializeFragments.WRITE, LinkedBuffer.allocate())));
    builder = org.apache.drill.exec.proto.BitControl.InitializeFragments.newBuilder();
    ProtostuffIOUtil.mergeFrom(ProtostuffIOUtil.toByteArray(request, SchemaBitControl.InitializeFragments.WRITE, LinkedBuffer.allocate()), builder, SchemaBitControl.InitializeFragments.MERGE);
    assertEquals(request, builder.build());
    assertThrows(IllegalArgumentException.class, () -> org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.freeze(root, List.of(nonRoot, nonRoot)));
    var noCapability = nonRoot.toBuilder().setAssignment(endpoint(30, 0)).build();
    assertThrows(IllegalStateException.class, () -> org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.freeze(root, List.of(noCapability)));
  }

  @Test
  public void restartUsesNewAdvertisedPortWithoutAProcessCache() {
    var old = rpc(10);
    var restarted = old.toBuilder().setNativeExecution(old.getNativeExecution().toBuilder().setControlPort(32003).setDataPort(32004)).build();
    assertTrue(FragmentEndpointPolicy.sameNode(old, restarted));
    assertEquals(110, org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.control(old).getControlPort());
    assertEquals(32003, org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.control(restarted).getControlPort());
    assertEquals(32004, org.apache.drill.exec.nativeexecution.NativeExecutionRoutes.control(restarted).getDataPort());
  }

  @Test
  public void protobufAndBothProtostuffSchemasPreserveCapability() throws Exception {
    var endpoint = rpc(3);
    assertEquals(endpoint, DrillbitEndpoint.parseFrom(endpoint.toByteArray()));
    var builder = DrillbitEndpoint.newBuilder();
    ProtobufIOUtil.mergeFrom(endpoint.toByteArray(), builder, SchemaCoordinationProtos.DrillbitEndpoint.MERGE);
    assertEquals(endpoint, builder.build());
    byte[] wire = ProtobufIOUtil.toByteArray(endpoint, SchemaCoordinationProtos.DrillbitEndpoint.WRITE, LinkedBuffer.allocate());
    assertEquals(endpoint, DrillbitEndpoint.parseFrom(wire));
    wire = ProtostuffIOUtil.toByteArray(endpoint, SchemaCoordinationProtos.DrillbitEndpoint.WRITE, LinkedBuffer.allocate());
    builder = DrillbitEndpoint.newBuilder();
    ProtostuffIOUtil.mergeFrom(wire, builder, SchemaCoordinationProtos.DrillbitEndpoint.MERGE);
    assertEquals(endpoint, builder.build());
  }

  @Test
  public void controlPlanAssignmentKeepsCapability() {
    var fragment = PlanFragment.newBuilder().setAssignment(rpc(3)).setForeman(endpoint(1, 0)).build();
    byte[] wire = ProtobufIOUtil.toByteArray(fragment, SchemaBitControl.PlanFragment.WRITE, LinkedBuffer.allocate());
    var builder = PlanFragment.newBuilder();
    ProtobufIOUtil.mergeFrom(wire, builder, SchemaBitControl.PlanFragment.MERGE);
    assertEquals(fragment, builder.build());
  }

  @Test
  public void oldEndpointSchemaReadsExistingFieldsAndRetainsNewUnknownField() throws Exception {
    var file = CoordinationProtos.getDescriptor().toProto().toBuilder();
    int index = 0;
    while (!file.getMessageType(index).getName().equals("DrillbitEndpoint")) {
      index++;
    }
    var message = file.getMessageType(index).toBuilder().clearField();
    file.getMessageType(index).getFieldList().stream().filter(field -> field.getNumber() != 9).forEach(message::addField);
    file.setMessageType(index, message);
    var oldDescriptor = Descriptors.FileDescriptor.buildFrom(file.build(), new Descriptors.FileDescriptor[0]).findMessageTypeByName("DrillbitEndpoint");
    var endpoint = rpc(3);
    var old = DynamicMessage.parseFrom(oldDescriptor, endpoint.toByteArray());
    assertEquals(endpoint.getAddress(), old.getField(oldDescriptor.findFieldByName("address")));
    assertEquals(endpoint.getControlPort(), old.getField(oldDescriptor.findFieldByName("control_port")));
    assertTrue(old.getUnknownFields().hasField(9));
    assertEquals(endpoint, DrillbitEndpoint.parseFrom(old.toByteArray()));
    assertFalse(DrillbitEndpoint.parseFrom(endpoint(3, 0).toByteArray()).hasNativeExecution());
  }
}
