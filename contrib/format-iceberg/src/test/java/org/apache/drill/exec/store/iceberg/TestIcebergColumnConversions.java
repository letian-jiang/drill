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
package org.apache.drill.exec.store.iceberg;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.OffsetDateTime;
import org.apache.drill.exec.store.iceberg.read.IcebergColumnConverterFactory;
import org.apache.drill.exec.vector.accessor.ValueWriter;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestIcebergColumnConversions {

  @Test
  public void timeLocalAndPhysicalMicrosecondValuesKeepTheirTimeOfDay() {
    for (Object value : new Object[] { java.time.LocalTime.ofNanoOfDay(1001001000), 1001001L }) {
      var writer = mock(ValueWriter.class);
      var metadata = IcebergColumnConverterFactory.getColumnMetadata(Types.NestedField.optional(1, "time", Types.TimeType.get()));
      new IcebergColumnConverterFactory(null).buildScalar(metadata, writer).convert(value);
      verify(writer).setTime(java.time.LocalTime.ofNanoOfDay(1001001000));
    }
  }

  @Test
  public void negativeMicrosecondsUseFloorMilliseconds() {
    var writer = mock(ValueWriter.class);
    var metadata = IcebergColumnConverterFactory.getColumnMetadata(Types.NestedField.optional(1, "ts", Types.TimestampType.withoutZone()));
    new IcebergColumnConverterFactory(null).buildScalar(metadata, writer).convert(-1L);
    verify(writer).setTimestamp(Instant.ofEpochMilli(-1));
  }

  @Test
  public void timestampWithOffsetKeepsItsInstant() {
    var writer = mock(ValueWriter.class);
    var metadata = IcebergColumnConverterFactory.getColumnMetadata(Types.NestedField.optional(1, "ts", Types.TimestampType.withZone()));
    new IcebergColumnConverterFactory(null).buildScalar(metadata, writer).convert(OffsetDateTime.parse("1970-01-01T07:59:59.999999+08:00"));
    verify(writer).setTimestamp(Instant.parse("1969-12-31T23:59:59.999999Z"));
  }

  @Test
  public void slicedAndDirectBinaryKeepOnlyTheirRemainingBytes() {
    for (boolean direct : new boolean[] { false, true }) {
      var writer = mock(ValueWriter.class);
      var metadata = IcebergColumnConverterFactory.getColumnMetadata(Types.NestedField.optional(2, "bytes", Types.BinaryType.get()));
      ByteBuffer value = direct ? ByteBuffer.allocateDirect(8) : ByteBuffer.allocate(8);
      value.put(new byte[] { 11, 22, 0, (byte) 0xff, (byte) 0x80, 66, 77, 88 });
      value.position(2).limit(5);
      new IcebergColumnConverterFactory(null).buildScalar(metadata, writer).convert(value);
      var bytes = ArgumentCaptor.forClass(byte[].class);
      verify(writer).setBytes(bytes.capture(), eq(3));
      assertArrayEquals(new byte[] { 0, (byte) 0xff, (byte) 0x80 }, bytes.getValue());
      assertEquals(2, value.position());
      assertEquals(5, value.limit());
    }
  }
}
