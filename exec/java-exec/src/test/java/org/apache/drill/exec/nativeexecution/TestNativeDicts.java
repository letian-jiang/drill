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
 * Original DICT/REPEATED DICT vectors -> JNI ingress -> native -> original Java root.
 */
public class TestNativeDicts extends SubOperatorTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  @Test(timeout = 60000)
  public void dictsSurviveNativeLookupAndOriginalRoot() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue(library != null && Files.exists(Path.of(library)));
    var schema = new SchemaBuilder().add("id", MinorType.INT).addDict("d", MinorType.BIGINT).nullableValue(MinorType.VARCHAR).resumeSchema().addDictArray("r", MinorType.BIGINT).nullableValue(MinorType.VARCHAR).resumeSchema().addDict("m", MinorType.VARCHAR).mapValue().addNullable("n", MinorType.BIGINT).addArray("list", MinorType.BIGINT).resumeDict().resumeSchema().addDict("f", MinorType.FLOAT8).nullableValue(MinorType.VARCHAR).resumeSchema().addDict("g", MinorType.FLOAT4).nullableValue(MinorType.VARCHAR).resumeSchema().buildSchema();
    var duplicate = new java.util.LinkedHashMap<Long, String>();
    duplicate.put(7L, "first");
    duplicate.put(8L, "last");
    var nullable = new java.util.LinkedHashMap<Long, String>();
    nullable.put(9L, null);
    nullable.put(42L, "long-long-long-雪🚀\u0000");
    var floating = new java.util.LinkedHashMap<Double, String>();
    floating.put(-0.0, "minus");
    floating.put(0.0, "plus");
    floating.put(Double.NaN, "nan");
    var single = new java.util.LinkedHashMap<Float, String>();
    single.put(-0.0f, "minus4");
    single.put(0.0f, "plus4");
    single.put(Float.NaN, "nan4");
    var input = fixture.rowSetBuilder(schema).addRow(0, duplicate, new Object[] { Map.of(7L, "array") }, Map.of("k", new Object[] { 11L, new long[] { 1, 2 } }), floating, single).addRow(1, Map.of(), new Object[0], Map.of(), Map.of(), Map.of()).addRow(2, nullable, new Object[] { Map.of() }, Map.of("k", new Object[] { null, new long[0] }), floating, single).addRow(3, Map.of(1L, "a\u0000b"), new Object[] { Map.of(7L, "tail") }, Map.of("k", new Object[] { 33L, new long[] { 9 } }), floating, single).build();
    // Original DictVector explicitly permits duplicate keys and looks backwards.
    var dict = (org.apache.drill.exec.vector.complex.DictVector) input.container().getValueAccessorById(org.apache.drill.exec.vector.complex.DictVector.class, 1).getValueVector();
    ((org.apache.drill.exec.vector.BigIntVector) dict.getKeys()).getMutator().set(1, 7L);
    dict.getReader().setPosition(0);
    assertEquals(1, dict.getReader().find(7));
    var floatDict = (org.apache.drill.exec.vector.complex.DictVector) input.container().getValueAccessorById(org.apache.drill.exec.vector.complex.DictVector.class, 4).getValueVector();
    var singleDict = (org.apache.drill.exec.vector.complex.DictVector) input.container().getValueAccessorById(org.apache.drill.exec.vector.complex.DictVector.class, 5).getValueVector();
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
      Object[] leaves = { row == 0 ? "last" : null, row == 0 ? Long.valueOf(11) : row == 3 ? Long.valueOf(33) : null, row == 0 ? "array" : row == 3 ? "tail" : null };
      for (var leaf : leaves) {
        values.add(JSON.valueToTree(leaf));
      }
      for (var keyDict : new org.apache.drill.exec.vector.complex.DictVector[] { floatDict, singleDict }) {
        keyDict.getReader().setPosition(row);
        for (String key : new String[] { "-0.0", "0.0", "NaN" }) {
          int index = keyDict.getReader().find(key);
          values.add(JSON.valueToTree(index < 0 ? null : keyDict.getValues().getAccessor().getObject(index)));
        }
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
         {"ref":"`id`","expr":"`id`"},{"ref":"`d`","expr":"`d`"},
         {"ref":"`r`","expr":"`r`"},{"ref":"`m`","expr":"`m`"},
         {"ref":"`f`","expr":"`f`"},{"ref":"`g`","expr":"`g`"},
         {"ref":"`last`","expr":"`d`[7]"},{"ref":"`member`","expr":"`m`.`k`.`n`"},
         {"ref":"`array`","expr":"`r`[0][7]"},
         {"ref":"`minus`","expr":"`f`.`-0.0`"},{"ref":"`plus`","expr":"`f`.`0.0`"},
         {"ref":"`nan`","expr":"`f`.`NaN`"},
         {"ref":"`minus4`","expr":"`g`.`-0.0`"},{"ref":"`plus4`","expr":"`g`.`0.0`"},
         {"ref":"`nan4`","expr":"`g`.`NaN`"}],
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
        throw new AssertionError("Native DICT execution failed", failure.get());
      }
      assertEquals(expected, actual);
    } finally {
      input.clear();
    }
  }
}
