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
 * Original LIST/REPEATED vectors -> JNI ingress -> native -> original Java root.
 */
public class TestNativeArrays extends SubOperatorTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test(timeout = 60000)
  public void nullableListsAndRepeatedTreesSurviveNativeProjectionAndRoot() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue(library != null && Files.exists(Path.of(library)));
    var schema = new SchemaBuilder().add("id", MinorType.INT).addArray("n", MinorType.INT).addMapArray("maps").addNullable("v", MinorType.INT).addArray("tags", MinorType.VARCHAR).resumeSchema().addArray("nested", MinorType.INT, 2).addList("list").addType(MinorType.VARCHAR).resumeSchema().buildSchema();
    schema.metadata("list").variantSchema().becomeSimple();
    var input = fixture.rowSetBuilder(schema).addRow(0, new int[] { 1, 2 }, new Object[] { new Object[] { 7, new String[] { "雪\u0000", "long-long-long-value" } } }, new Object[] { new int[] { 3, 4 }, new int[0] }, new String[] { "雪\u0000long-string-long-value", null }).addRow(1, new int[0], new Object[0], new Object[0], null).addRow(2, new int[] { 9 }, new Object[] { new Object[] { null, new String[0] } }, new Object[] { new int[0], new int[] { 5 } }, new String[0]).addRow(3, new int[] { 11 }, new Object[] { new Object[] { 11, new String[] { "tail" } } }, new Object[] { new int[] { 9 } }, new String[] { null, "tail" }).build();
    Map<Integer, List<JsonNode>> expected = new java.util.HashMap<>();
    List<org.apache.drill.exec.vector.ValueVector> vectors = new ArrayList<>();
    for (var wrapper : input.container()) {
      vectors.add(wrapper.getValueVector());
    }
    for (int row = 0; row < 4; ++row) {
      List<JsonNode> values = new ArrayList<>();
      for (var vector : vectors) {
        values.add(JSON.valueToTree(vector.getAccessor().getObject(row)));
      }
      Object[] leaves = { row == 0 ? Integer.valueOf(7) : row == 3 ? Integer.valueOf(11) : null, row == 0 ? "雪\u0000long-string-long-value" : null, row == 0 ? Integer.valueOf(3) : row == 3 ? Integer.valueOf(9) : null };
      for (var leaf : leaves) {
        values.add(JSON.valueToTree(leaf));
      }
      expected.put(row, values);
    }
    var endpoint = DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(64321).setDataPort(64322).build();
    var query = QueryId.newBuilder().setPart1(151).setPart2(153).build();
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
         {"ref":"`id`","expr":"`id`"},{"ref":"`n`","expr":"`n`"},
         {"ref":"`maps`","expr":"`maps`"},{"ref":"`nested`","expr":"`nested`"},
         {"ref":"`list`","expr":"`list`"},{"ref":"`leaf`","expr":"`maps`[0].`v`"},
         {"ref":"`first`","expr":"`list`[0]"},{"ref":"`inner`","expr":"`nested`[0][0]"}],
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
        throw new AssertionError("Native array execution failed", failure.get());
      }
      assertEquals(expected, actual);
    } finally {
      input.clear();
    }
  }
}
