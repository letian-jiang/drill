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
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.exec.nativeexecution.NativeEngine;
import org.apache.drill.exec.nativeexecution.scan.ScanHost;

/**
 * Actual Java-host JNI1 -> Velox Task -> JNI2 -> Java reader -> root bridge.
 */
public final class NativeEngineJniTest {

  private static final class Proto {

    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    void varint(long value) {
      do {
        int next = (int) (value & 127);
        value >>>= 7;
        out.write(next | (value == 0 ? 0 : 128));
      } while (value != 0);
    }

    Proto integer(int field, long value) {
      varint(field << 3);
      varint(value);
      return this;
    }

    Proto bytes(int field, byte[] value) {
      varint((field << 3) | 2);
      varint(value.length);
      out.writeBytes(value);
      return this;
    }

    Proto text(int field, String value) {
      return bytes(field, value.getBytes(StandardCharsets.UTF_8));
    }

    Proto fixed(int field, long value) {
      varint((field << 3) | 1);
      out.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array());
      return this;
    }

    byte[] take() {
      return out.toByteArray();
    }
  }

  private static long varint(ByteBuffer input) {
    long value = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      int next = input.get() & 255;
      value |= (long) (next & 127) << shift;
      if (next < 128) {
        return value;
      }
    }
    throw new IllegalStateException("bad protobuf");
  }

  private static Object field(byte[] bytes, int expected) {
    ByteBuffer input = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    while (input.hasRemaining()) {
      int tag = (int) varint(input);
      Object value;
      switch(tag & 7) {
        case 0:
          value = varint(input);
          break;
        case 1:
          value = input.getLong();
          break;
        case 2:
          byte[] body = new byte[(int) varint(input)];
          input.get(body);
          value = body;
          break;
        default:
          throw new IllegalStateException("unexpected protobuf wire type");
      }
      if ((tag >>> 3) == expected) {
        return value;
      }
    }
    return null;
  }

  private static void require(boolean value, String message) {
    if (!value) {
      throw new IllegalStateException(message);
    }
  }

  public static void main(String[] args) throws Exception {
    System.setProperty("drill.native.engine.library", args[0]);
    byte[] endpoint = new Proto().text(1, "127.0.0.1").integer(3, 63011).integer(4, 63012).take();
    byte[] query = new Proto().fixed(1, 27).fixed(2, 29).take();
    byte[] handle = new Proto().bytes(1, query).integer(2, 1).integer(3, 0).take();
    var engineRef = new AtomicReference<NativeEngine>();
    var failure = new AtomicReference<Throwable>();
    CountDownLatch data = new CountDownLatch(1), terminal = new CountDownLatch(1);
    AtomicLong dataToken = new AtomicLong();
    AtomicInteger schemas = new AtomicInteger(), eos = new AtomicInteger(), state = new AtomicInteger();
    var callbacks = new NativeEngine.Callbacks() {

      @Override
      public void onStatus(byte[] bytes) {
        int value = ((Long) field((byte[]) field(bytes, 1), 1)).intValue();
        if (value >= 3) {
          state.set(value);
          terminal.countDown();
        }
      }

      @Override
      public boolean onRootBatch(long id, byte[] header, ByteBuffer payload) {
        try {
          require(((Long) field(header, 2)) == 0, "non-root bridge destination");
          Object last = field(header, 7);
          int rows = ((Long) field((byte[]) field(header, 6), 1)).intValue();
          if (last != null && ((Long) last) == 1) {
            require(!payload.hasRemaining(), "EOS payload");
            eos.incrementAndGet();
          } else if (rows == 0) {
            schemas.incrementAndGet();
          } else {
            require(rows == 1 && payload.remaining() == 9, "aggregate batch layout");
            require(payload.get(0) == 1, "NULL aggregate");
            require(payload.order(ByteOrder.LITTLE_ENDIAN).getLong(1) == 315, "reader work was lost or duplicated");
            dataToken.set(id);
            data.countDown();
            return true;
          }
          engineRef.get().completeRootBatch(id, 0);
          return true;
        } catch (Throwable error) {
          failure.set(error);
          data.countDown();
          engineRef.get().completeRootBatch(id, 2);
          return true;
        }
      }
    };
    try (var engine = new NativeEngine(endpoint, 4, callbacks)) {
      engineRef.set(engine);
      String plan = """
        {"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
         "destination":"%s","child":{"pop":"streaming-aggregate","@id":1,"keys":[],
         "exprs":[{"ref":"`s`","expr":"sum(`v`)"}],"child":{"pop":"test-scan","@id":2,
         "jniScan":{"provider":"test","mode":"barrier unicode-path","independentWorkList":"workList",
         "scan":{"workList":[0,1,2,3,4],"path":"/读取/🚀/part.parquet"}}}}}
        """.formatted(Base64.getEncoder().encodeToString(endpoint));
      byte[] fragment = new Proto().bytes(1, handle).text(8, plan).bytes(11, endpoint).take();
      engine.submitFragments(new Proto().bytes(1, fragment).take());
      require(data.await(10, TimeUnit.SECONDS), "no JNI root data");
      if (failure.get() != null) {
        throw new IllegalStateException("bridge failure", failure.get());
      }
      require(state.get() == 0 && eos.get() == 0, "terminal/EOS overtook data ACK");
      engine.completeRootBatch(dataToken.get(), 0);
      require(terminal.await(10, TimeUnit.SECONDS), "no native terminal status");
      require(state.get() == 3 && eos.get() == 1 && schemas.get() == 1, "unexpected native terminal result");
      require(ScanHost.activeScans() == 0 && ScanHost.activeReads() == 0, "reader leak");
      require(ScanHost.peakReads() >= 2, "JNI scan readers did not execute concurrently");
      boolean invalid = false;
      try {
        engine.completeRootBatch(dataToken.get(), 0);
      } catch (IllegalStateException expected) {
        invalid = true;
      }
      require(invalid, "duplicate ACK accepted");
      invalid = false;
      try {
        engine.acceptRecordBatch(new byte[0], ByteBuffer.allocate(8));
      } catch (IllegalArgumentException expected) {
        invalid = true;
      }
      require(invalid, "heap input accepted");
    }
    System.out.println("Java host JNI1 -> native Task/drivers -> borrowed JVM JNI2 scan -> owned root batch/ACK/EOS/status passed");
  }
}
