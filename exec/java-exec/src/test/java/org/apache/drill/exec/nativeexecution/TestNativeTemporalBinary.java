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

import io.netty.buffer.DrillBuf;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.expr.fn.impl.HashHelper;
import org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge;
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
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Original Java vectors -> native projection/hash -> original Java root decoder.
 */
public class TestNativeTemporalBinary extends SubOperatorTest {

  @Test(timeout = 60000)
  public void millisecondsAndOpaqueBytesSurviveNativeExecutionAndJavaRoot() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue(library != null && Files.exists(Path.of(library)));
    var schema = new SchemaBuilder().add("id", MinorType.INT).addNullable("ts", MinorType.TIMESTAMP).addNullable("binary", MinorType.VARBINARY).buildSchema();
    byte[] longBinary = new byte[9000];
    Arrays.fill(longBinary, (byte) 0xff);
    longBinary[27] = 0;
    Long[] millis = { -1L, 0L, 1L, null, -1001L, 1924992000123L, null, -86400001L, Long.MIN_VALUE, Long.MAX_VALUE };
    byte[][] binary = { new byte[] { 0, (byte) 0xff, (byte) 0x80, 0 }, new byte[0], longBinary, null, new byte[] { 'a', 0, 'b' }, "中文🚀".getBytes(java.nio.charset.StandardCharsets.UTF_8), new byte[] { (byte) 0x80 }, null, new byte[0], null };
    var builder = fixture.rowSetBuilder(schema);
    for (int i = 0; i < millis.length; i++) {
      builder.addRow(i, millis[i] == null ? null : Instant.ofEpochMilli(millis[i]), binary[i]);
    }
    var input = builder.build();
    var endpoint = DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(64311).setDataPort(64312).build();
    var query = QueryId.newBuilder().setPart1(121).setPart2(123).build();
    var failure = new AtomicReference<Throwable>();
    var engineRef = new AtomicReference<NativeEngine>();
    var terminal = new CountDownLatch(1);
    Map<Integer, List<Object>> actual = new ConcurrentHashMap<>();
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
            int payloadSize = payload.remaining();
            try (DrillBuf data = fixture.allocator().buffer(payloadSize)) {
              data.setBytes(0, payload);
              data.writerIndex(payloadSize);
              loader.load(batch.getDef(), data);
            }
            List<ValueVector> columns = new ArrayList<>();
            for (var wrapper : loader) {
              columns.add(wrapper.getValueVector());
            }
            assertEquals(MinorType.TIMESTAMP, columns.get(1).getField().getType().getMinorType());
            assertEquals(MinorType.VARBINARY, columns.get(2).getField().getType().getMinorType());
            for (int row = 0; row < loader.getRecordCount(); row++) {
              int id = (Integer) columns.get(0).getAccessor().getObject(row);
              List<Object> values = new ArrayList<>();
              for (int c = 1; c < columns.size(); c++) {
                values.add(columns.get(c).getAccessor().getObject(row));
              }
              assertNull("Duplicate native row", actual.putIfAbsent(id, values));
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
      NativeColumnarBridge.fields(input.container().getSchema());
      String node = Base64.getEncoder().encodeToString(endpoint.toByteArray());
      String plan = """
        {"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
         "destination":"%s","child":{"pop":"project","@id":1,"exprs":[
         {"ref":"`id`","expr":"`id`"},{"ref":"`ts`","expr":"`ts`"},
         {"ref":"`binary`","expr":"`binary`"},
         {"ref":"`timestamp_hash`","expr":"hash32asdouble(`ts`,1301011)"},
         {"ref":"`binary_hash`","expr":"hash32asdouble(`binary`,1301011)"}],
         "child":{"pop":"unordered-receiver","@id":2,"sender-major-fragment":2,
         "senders":[{"minorFragmentId":0,"endpoint":"%s"}]}}}
        """.formatted(node, node);
      var handle = FragmentHandle.newBuilder().setQueryId(query).setMajorFragmentId(1).setMinorFragmentId(0);
      var fragment = PlanFragment.newBuilder().setHandle(handle).setAssignment(endpoint).setForeman(endpoint).setFragmentJson(plan).build();
      engine.submitFragments(InitializeFragments.newBuilder().addFragment(fragment).build().toByteArray());
      var definition = RecordBatchDef.newBuilder().setRecordCount(millis.length);
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
        throw new AssertionError("Native temporal/binary execution failed", failure.get());
      }
      assertEquals(millis.length, actual.size());
      for (int i = 0; i < millis.length; i++) {
        var values = actual.get(i);
        var timestamp = (LocalDateTime) values.get(0);
        assertEquals(millis[i], timestamp == null ? null : Long.valueOf(timestamp.toInstant(ZoneOffset.UTC).toEpochMilli()));
        assertArrayEquals(binary[i], (byte[]) values.get(1));
        int timestampHash = millis[i] == null ? 1301011 : HashHelper.hash32(millis[i], 1301011);
        assertEquals(timestampHash, ((Number) values.get(2)).intValue());
        int binaryHash = 1301011;
        if (binary[i] != null) {
          try (DrillBuf buffer = fixture.allocator().buffer(binary[i].length)) {
            buffer.setBytes(0, binary[i]);
            binaryHash = HashHelper.hash32(0, binary[i].length, buffer, 1301011);
          }
        }
        assertEquals(binaryHash, ((Number) values.get(3)).intValue());
      }
    } finally {
      input.clear();
    }
  }
}
