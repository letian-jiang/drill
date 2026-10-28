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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.test.SubOperatorTest;
import org.junit.Test;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.when;

public class TestNativeColumnarBridge extends SubOperatorTest {

  @Test
  public void nonDecimalPrecisionAndScaleSurviveDescriptorWithoutRawType() throws Exception {
    var fields = NativeColumnarTestSupport.JSON.readTree("[" + "{\"name\":\"ts\",\"minor\":\"TIMESTAMP\",\"optional\":false,\"precision\":26,\"scale\":6}," + "{\"name\":\"id\",\"minor\":\"INT\",\"optional\":true,\"precision\":32,\"scale\":0}]");
    var result = NativeColumnarTestSupport.materialized(fields);
    assertEquals(26, result.get(0).getType().getPrecision());
    assertEquals(6, result.get(0).getType().getScale());
    assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.REQUIRED, result.get(0).getType().getMode());
    assertEquals(32, result.get(1).getType().getPrecision());
    assertEquals(org.apache.drill.common.types.TypeProtos.DataMode.OPTIONAL, result.get(1).getType().getMode());
  }

  @Test
  public void originalDictAndRepeatedDictMetadataRoundTrip() throws Exception {
    var schema = new SchemaBuilder().addDict("d", MinorType.BIGINT).nullableValue(MinorType.VARCHAR).resumeSchema().addDictArray("r", MinorType.BIGINT).nullableValue(MinorType.VARCHAR).resumeSchema().addDict("m", MinorType.VARCHAR).mapValue().addNullable("n", MinorType.BIGINT).addArray("tags", MinorType.VARCHAR).resumeDict().resumeSchema().buildSchema();
    var nullable = new java.util.LinkedHashMap<Long, String>();
    nullable.put(1L, null);
    nullable.put(2L, "雪🚀\u0000long-long-long");
    var input = fixture.rowSetBuilder(schema).addRow(nullable, new Object[] { java.util.Map.of(7L, "first"), java.util.Map.of(), java.util.Map.of(8L, "last") }, java.util.Map.of("k", new Object[] { 11L, new String[] { "a", "雪" } })).addRow(java.util.Map.of(), new Object[0], java.util.Map.of()).addRow(java.util.Map.of(9L, "tail"), new Object[] { java.util.Map.of() }, java.util.Map.of("k", new Object[] { null, new String[0] })).build();
    var loader = new RecordBatchLoader(fixture.allocator());
    try {
      var expected = new java.util.ArrayList<com.fasterxml.jackson.databind.JsonNode>();
      for (var wrapper : input.container()) {
        for (int row = 0; row < 3; ++row) {
          expected.add(NativeColumnarTestSupport.JSON.valueToTree(wrapper.getValueVector().getAccessor().getObject(row)));
        }
      }
      var batch = mock(RecordBatch.class);
      when(batch.getSchema()).thenReturn(input.container().getSchema());
      when(batch.getRecordCount()).thenReturn(3);
      when(batch.iterator()).thenAnswer(invocation -> input.container().iterator());
      var payload = ByteBuffer.allocateDirect(65536);
      var header = NativeColumnarTestSupport.JSON.readTree(NativeColumnarTestSupport.copyBatch(batch, payload));
      payload.flip();
      input.clear();
      NativeColumnarTestSupport.load(loader, fixture.allocator(), header, payload);
      var actual = new java.util.ArrayList<com.fasterxml.jackson.databind.JsonNode>();
      for (var wrapper : loader) {
        for (int row = 0; row < 3; ++row) {
          actual.add(NativeColumnarTestSupport.JSON.valueToTree(wrapper.getValueVector().getAccessor().getObject(row)));
        }
      }
      assertEquals(expected, actual);
      assertEquals(3, loader.getRecordCount());
    } finally {
      loader.clear();
      input.clear();
    }
  }

  @Test
  public void originalRepeatedAndNullableListBuffersRoundTrip() throws Exception {
    var schema = new SchemaBuilder().addArray("n", MinorType.INT).addArray("s", MinorType.VARCHAR).addMapArray("maps").addNullable("v", MinorType.INT).addArray("tags", MinorType.VARCHAR).resumeSchema().addArray("nested", MinorType.INT, 2).addList("list").addType(MinorType.VARCHAR).resumeSchema().buildSchema();
    schema.metadata("list").variantSchema().becomeSimple();
    var input = fixture.rowSetBuilder(schema).addRow(new int[] { 1, 2, 3 }, new String[] { "雪\u0000", "long-long-long-value" }, new Object[] { new Object[] { 7, new String[] { "a", "b" } }, new Object[] { null, new String[0] } }, new Object[] { new int[] { 3, 4 }, new int[0], new int[] { 5 } }, new String[] { "text", null }).addRow(new int[0], new String[0], new Object[0], new Object[0], null).addRow(new int[] { 9 }, new String[] { "tail" }, new Object[] { new Object[] { 9, new String[] { "end" } } }, new Object[] { new int[] { 9 } }, new String[0]).build();
    var loader = new RecordBatchLoader(fixture.allocator());
    try {
      var expected = new java.util.ArrayList<String>();
      for (var wrapper : input.container()) {
        for (int row = 0; row < 3; ++row) {
          expected.add(NativeColumnarTestSupport.JSON.writeValueAsString(wrapper.getValueVector().getAccessor().getObject(row)));
        }
      }
      var batch = mock(RecordBatch.class);
      when(batch.getSchema()).thenReturn(input.container().getSchema());
      when(batch.getRecordCount()).thenReturn(3);
      when(batch.iterator()).thenAnswer(invocation -> input.container().iterator());
      var payload = ByteBuffer.allocateDirect(65536);
      var header = NativeColumnarTestSupport.JSON.readTree(NativeColumnarTestSupport.copyBatch(batch, payload));
      payload.flip();
      input.clear();
      NativeColumnarTestSupport.load(loader, fixture.allocator(), header, payload);
      var actual = new java.util.ArrayList<String>();
      for (var wrapper : loader) {
        for (int row = 0; row < 3; ++row) {
          actual.add(NativeColumnarTestSupport.JSON.writeValueAsString(wrapper.getValueVector().getAccessor().getObject(row)));
        }
      }
      assertEquals(expected, actual);
      assertEquals(3, loader.getRecordCount());
    } finally {
      loader.clear();
      input.clear();
    }
  }

  @Test
  public void nestedMapBuffersLoadWithOriginalMapVectorMetadata() throws Exception {
    var schema = new SchemaBuilder().addMap("m").addNullable("n", MinorType.INT).addMap("inner").addNullable("s", MinorType.VARCHAR).resumeMap().resumeSchema().addMap("empty").resumeSchema().buildSchema();
    var input = fixture.rowSetBuilder(schema).addRow(new Object[] { 7, new Object[] { "雪\u0000long-string-long-string" } }, new Object[0]).addRow(new Object[] { null, new Object[] { null } }, new Object[0]).build();
    var loader = new RecordBatchLoader(fixture.allocator());
    try {
      var batch = mock(RecordBatch.class);
      when(batch.getSchema()).thenReturn(input.container().getSchema());
      when(batch.getRecordCount()).thenReturn(2);
      when(batch.iterator()).thenAnswer(invocation -> input.container().iterator());
      var payload = ByteBuffer.allocateDirect(65536);
      var header = NativeColumnarTestSupport.JSON.readTree(NativeColumnarTestSupport.copyBatch(batch, payload));
      payload.flip();
      input.clear();
      NativeColumnarTestSupport.load(loader, fixture.allocator(), header, payload);
      var map = (org.apache.drill.exec.vector.complex.MapVector) loader.getValueAccessorById(org.apache.drill.exec.vector.complex.MapVector.class, 0).getValueVector();
      assertEquals(7, map.getChild("n", org.apache.drill.exec.vector.NullableIntVector.class).getAccessor().get(0));
      var inner = map.getChild("inner", org.apache.drill.exec.vector.complex.MapVector.class);
      var strings = inner.getChild("s", org.apache.drill.exec.vector.NullableVarCharVector.class);
      assertEquals("雪\u0000long-string-long-string", strings.getAccessor().getObject(0).toString());
      assertNull(strings.getAccessor().getObject(1));
      assertEquals(2, loader.getRecordCount());
      assertEquals(2, loader.getValueAccessorById(org.apache.drill.exec.vector.complex.MapVector.class, 1).getValueVector().getAccessor().getValueCount());
    } finally {
      loader.clear();
      input.clear();
    }
  }

  @Test
  public void originalTimestampAndBinaryBuffersLoadWithGeneratedMetadata() throws Exception {
    var schema = new SchemaBuilder().addNullable("ts", MinorType.TIMESTAMP).addNullable("bytes", MinorType.VARBINARY).addNullable("time", MinorType.TIME).buildSchema();
    byte[] bytes = { 0, (byte) 0xff, (byte) 0x80 };
    var input = fixture.rowSetBuilder(schema).addRow(Instant.ofEpochMilli(-1001), bytes, LocalTime.ofNanoOfDay(86_399_999_000_000L)).addRow(null, null, null).addRow(Instant.EPOCH, new byte[0], LocalTime.MIDNIGHT).build();
    var loader = new RecordBatchLoader(fixture.allocator());
    try {
      var batch = mock(RecordBatch.class);
      when(batch.getSchema()).thenReturn(input.container().getSchema());
      when(batch.getRecordCount()).thenReturn(3);
      when(batch.iterator()).thenAnswer(invocation -> input.container().iterator());
      var payload = ByteBuffer.allocateDirect(65536);
      var header = NativeColumnarTestSupport.JSON.readTree(NativeColumnarTestSupport.copyBatch(batch, payload));
      payload.flip();
      input.clear();
      NativeColumnarTestSupport.load(loader, fixture.allocator(), header, payload);
      var timestamp = loader.getValueAccessorById(org.apache.drill.exec.vector.NullableTimeStampVector.class, 0).getValueVector();
      var binary = loader.getValueAccessorById(org.apache.drill.exec.vector.NullableVarBinaryVector.class, 1).getValueVector();
      var time = loader.getValueAccessorById(org.apache.drill.exec.vector.NullableTimeVector.class, 2).getValueVector();
      assertEquals(3, loader.getRecordCount());
      assertEquals(LocalDateTime.ofInstant(Instant.ofEpochMilli(-1001), ZoneOffset.UTC), timestamp.getAccessor().getObject(0));
      assertArrayEquals(bytes, (byte[]) binary.getAccessor().getObject(0));
      assertNull(timestamp.getAccessor().getObject(1));
      assertNull(binary.getAccessor().getObject(1));
      assertEquals(LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC), timestamp.getAccessor().getObject(2));
      assertArrayEquals(new byte[0], (byte[]) binary.getAccessor().getObject(2));
      assertEquals(LocalTime.ofNanoOfDay(86_399_999_000_000L), time.getAccessor().getObject(0));
      assertNull(time.getAccessor().getObject(1));
      assertEquals(LocalTime.MIDNIGHT, time.getAccessor().getObject(2));
    } finally {
      loader.clear();
      input.clear();
    }
  }
}
