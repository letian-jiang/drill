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
package org.apache.drill.exec.nativeexecution;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.DrillBuf;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.proto.UserBitShared.RecordBatchDef;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Original OPTIONAL MapVector -> JNI ingress -> native project -> original Java root.
 */
public class TestNativeOptionalMaps extends SubOperatorTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test(timeout = 60000)
  public void optionalMapModeAndChildrenSurviveActualNativeEngine() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue(library != null && Files.exists(Path.of(library)));
    var inner = new SchemaBuilder().addNullable("text", MinorType.VARCHAR).addArray("tags", MinorType.BIGINT).buildSchema();
    var members = new SchemaBuilder().addNullable("n", MinorType.BIGINT).add(new org.apache.drill.exec.record.metadata.MapColumnMetadata("inner", org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, (org.apache.drill.exec.record.metadata.TupleSchema) inner)).buildSchema();
    var schema = new SchemaBuilder().add("id", MinorType.INT).add(new org.apache.drill.exec.record.metadata.MapColumnMetadata("m", org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, (org.apache.drill.exec.record.metadata.TupleSchema) members)).add(new org.apache.drill.exec.record.metadata.MapColumnMetadata("empty", org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, new org.apache.drill.exec.record.metadata.TupleSchema())).buildSchema();
    var input = fixture.rowSetBuilder(schema).addRow(0, new Object[] { 11L, new Object[] { "long-long-long-雪🚀\u0000", new long[] { 1, 2 } } }, new Object[0]).addRow(1, new Object[] { null, new Object[] { null, new long[0] } }, new Object[0]).addRow(2, new Object[] { null, new Object[] { null, new long[0] } }, new Object[0]).addRow(3, new Object[] { 33L, new Object[] { "tail", new long[] { 9 } } }, new Object[0]).build();
    try {
      var originalMap = (org.apache.drill.exec.vector.complex.MapVector) input.container().getValueAccessorById(org.apache.drill.exec.vector.complex.MapVector.class, 1).getValueVector();
      assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, originalMap.getField().getType().getMode());
      var descriptors = org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge.fields(input.container().getSchema());
      assertEquals(Boolean.TRUE, descriptors.get(1).get("optional"));
      assertEquals(originalMap.getBuffers(false).length, org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge.bufferCount(originalMap.getField()));
      originalMap.getReader().setPosition(1);
      assertTrue(originalMap.getReader().isSet());
      Map<Integer, List<JsonNode>> expected = new java.util.HashMap<>();
      for (int row = 0; row < 4; ++row) {
        List<JsonNode> values = new ArrayList<>();
        for (var wrapper : input.container()) {
          values.add(JSON.valueToTree(wrapper.getValueVector().getAccessor().getObject(row)));
        }
        values.add(JSON.valueToTree(originalMap.getChild("n").getAccessor().getObject(row)));
        expected.put(row, values);
      }
      var endpoint = DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(64321).setDataPort(64322).build();
      var query = QueryId.newBuilder().setPart1(161).setPart2(163).build();
      var failure = new AtomicReference<Throwable>();
      var engineRef = new AtomicReference<NativeEngine>();
      var terminal = new CountDownLatch(1);
      Map<Integer, List<JsonNode>> actual = new ConcurrentHashMap<>();
      var callbacks = new NativeEngine.Callbacks() {

        public void onStatus(byte[] bytes) {
          try {
            var status = FragmentStatus.parseFrom(bytes);
            if (status.getProfile().getState().getNumber() >= 3) {
              if (status.getProfile().getState().getNumber() != 3) {
                failure.compareAndSet(null, new AssertionError(status.toString()));
              }
              terminal.countDown();
            }
          } catch (Throwable error) {
            failure.set(error);
            terminal.countDown();
          }
        }

        public boolean onRootBatch(long token, byte[] bytes, ByteBuffer payload) {
          int result = 0;
          var loader = new RecordBatchLoader(fixture.allocator());
          try {
            var batch = FragmentRecordBatch.parseFrom(bytes);
            if (batch.getDef().getRecordCount() > 0) {
              assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, batch.getDef().getField(1).getMajorType().getMode());
              assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, batch.getDef().getField(2).getMajorType().getMode());
              try (DrillBuf data = fixture.allocator().buffer(payload.remaining())) {
                int length = payload.remaining();
                data.setBytes(0, payload);
                data.writerIndex(length);
                loader.load(batch.getDef(), data);
              }
              for (int row = 0; row < loader.getRecordCount(); ++row) {
                List<JsonNode> values = new ArrayList<>();
                for (var wrapper : loader) {
                  values.add(JSON.valueToTree(wrapper.getValueVector().getAccessor().getObject(row)));
                }
                assertNull("Duplicate native row", actual.putIfAbsent(values.get(0).asInt(), values));
              }
            }
          } catch (Throwable error) {
            failure.set(error);
            result = 2;
          } finally {
            loader.clear();
          }
          engineRef.get().completeRootBatch(token, result);
          return true;
        }
      };
      try (var engine = new NativeEngine(endpoint.toByteArray(), 4, callbacks)) {
        engineRef.set(engine);
        String node = Base64.getEncoder().encodeToString(endpoint.toByteArray());
        String plan = """
          {"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
           "destination":"%s","child":{"pop":"project","@id":1,"exprs":[
           {"ref":"`id`","expr":"`id`"},{"ref":"`m`","expr":"`m`"},
           {"ref":"`empty`","expr":"`empty`"},{"ref":"`n`","expr":"`m`.`n`"}],
           "child":{"pop":"unordered-receiver","@id":2,"sender-major-fragment":2,
           "senders":[{"minorFragmentId":0,"endpoint":"%s"}]}}}
          """.formatted(node, node);
        var handle = FragmentHandle.newBuilder().setQueryId(query).setMajorFragmentId(1).setMinorFragmentId(0);
        var fragment = PlanFragment.newBuilder().setHandle(handle).setAssignment(endpoint).setForeman(endpoint).setFragmentJson(plan).build();
        engine.submitFragments(InitializeFragments.newBuilder().addFragment(fragment).build().toByteArray());
        var definition = RecordBatchDef.newBuilder().setRecordCount(4);
        ByteBuffer payload = ByteBuffer.allocateDirect(1024 * 1024);
        for (var wrapper : input.container()) {
          var vector = wrapper.getValueVector();
          definition.addField(vector.getMetadata());
          for (DrillBuf buffer : vector.getBuffers(false)) {
            payload.put(buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes()));
          }
        }
        payload.flip();
        var header = FragmentRecordBatch.newBuilder().setQueryId(query).setReceivingMajorFragmentId(1).addReceivingMinorFragmentId(0).setSendingMajorFragmentId(2).setSendingMinorFragmentId(0).setDef(definition).setIsLastBatch(true);
        engine.acceptRecordBatch(header.build().toByteArray(), payload);
        input.clear();
        assertTrue("No native terminal status", terminal.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
          throw new AssertionError("Native OPTIONAL MAP execution failed", failure.get());
        }
        assertEquals(expected, actual);
      }
    } finally {
      input.clear();
    }
  }
}
