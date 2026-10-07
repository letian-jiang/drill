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

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.scan.ScanOperatorExec;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Production ScanHost, real resources/helpers and original reader after cancel.
 */
public class TestScanHostCancellation {

  static final Map<String, Fixture> FIXTURES = new ConcurrentHashMap<>();

  static final class Fixture {

    final String mode;

    final CountDownLatch entered = new CountDownLatch(1);

    final CountDownLatch helperEntered = new CountDownLatch(1);

    final CountDownLatch helperInterrupted = new CountDownLatch(1);

    final CountDownLatch helperFinish = new CountDownLatch(1);

    final AtomicInteger opened = new AtomicInteger(), closed = new AtomicInteger();

    volatile ScanServices services;

    volatile DrillBuf buffer;

    volatile Future<?> helper;

    Fixture(String mode) {
      this.mode = mode;
    }
  }

  public static final class BlockingFactory implements PluginScanFactory {

    public ScanOperatorExec open(JsonNode scan, ScanServices services) throws Exception {
      var fixture = FIXTURES.get(scan.get("testId").asText());
      fixture.opened.incrementAndGet();
      fixture.services = services;
      fixture.buffer = services.getManagedBuffer(32);
      fixture.buffer.setLong(0, 456789L);
      if (fixture.mode.equals("factory")) {
        fixture.helper = services.runCallableAs(UserGroupInformation.getCurrentUser(), () -> {
          fixture.helperEntered.countDown();
          boolean finish = false;
          while (!finish) {
            try {
              fixture.helperFinish.await();
              finish = true;
            } catch (InterruptedException expected) {
              fixture.helperInterrupted.countDown();
            }
          }
          assertEquals(456789L, fixture.buffer.getLong(0));
          return null;
        });
        if (!fixture.helperEntered.await(10, TimeUnit.SECONDS)) {
          throw new IllegalStateException("helper did not start");
        }
        fixture.entered.countDown();
        new CountDownLatch(1).await();
      }
      return new ScanOperatorExec(null, false) {

        @Override
        public void bind(OperatorContext context) {
        }

        @Override
        public boolean buildSchema() {
          fixture.entered.countDown();
          try {
            new CountDownLatch(1).await();
          } catch (InterruptedException expected) {
            throw new CancellationException("schema interrupted");
          }
          throw new AssertionError("Schema gate unexpectedly released");
        }

        @Override
        public void close() {
          fixture.closed.incrementAndGet();
        }
      };
    }
  }

  private record Outcome(Throwable error, boolean interrupted, ClassLoader loader) {
  }

  @Test(timeout = 90000)
  public void cancelledReservationNeverConstructsPlugin() throws Exception {
    String key = UUID.randomUUID().toString();
    var fixture = new Fixture("schema");
    FIXTURES.put(key, fixture);
    long handle = ScanHost.reserveScan();
    try {
      ScanHost.cancel(handle);
      try {
        ScanHost.initializeUtf8(handle, descriptor(key));
        fail("Initialized cancelled reader");
      } catch (CancellationException expected) {
      }
      assertEquals(0, fixture.opened.get());
      assertFalse(Thread.currentThread().isInterrupted());
    } finally {
      ScanHost.close(handle);
      FIXTURES.remove(key);
    }
    assertEquals(0, ScanHost.activeScans());
  }

  @Test(timeout = 90000)
  public void factoryCancellationWaitsForHelperBeforeReleasingBuffers() throws Exception {
    cancelledOpening("factory");
  }

  @Test(timeout = 90000)
  public void schemaCancellationClosesPartialReaderAndDoesNotPoisonThread() throws Exception {
    cancelledOpening("schema");
  }

  static byte[] descriptor(String key) throws Exception {
    var value = TestGenericPluginScan.descriptor(true, 0);
    value.put("provider", BlockingFactory.class.getName());
    value.with("scan").put("testId", key);
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  private void cancelledOpening(String mode) throws Exception {
    String key = UUID.randomUUID().toString();
    var fixture = new Fixture(mode);
    FIXTURES.put(key, fixture);
    var executor = Executors.newSingleThreadExecutor();
    long handle = ScanHost.reserveScan();
    try (var marker = new URLClassLoader(new URL[0], ScanHost.class.getClassLoader())) {
      var opening = executor.submit(() -> {
        Thread.currentThread().setContextClassLoader(marker);
        Throwable error = null;
        try {
          ScanHost.initializeUtf8(handle, descriptor(key));
        } catch (Throwable expected) {
          error = expected;
        }
        return new Outcome(error, Thread.currentThread().isInterrupted(), Thread.currentThread().getContextClassLoader());
      });
      assertTrue(fixture.entered.await(20, TimeUnit.SECONDS));
      assertEquals(1, ScanHost.activeScans());
      ScanHost.cancel(handle);
      if (mode.equals("factory")) {
        assertTrue(fixture.helperInterrupted.await(10, TimeUnit.SECONDS));
        assertTrue(fixture.helper.isCancelled());
        try {
          opening.get(50, TimeUnit.MILLISECONDS);
          fail("Freed resources before helper exit");
        } catch (TimeoutException expected) {
        }
        assertEquals(456789L, fixture.buffer.getLong(0));
        assertTrue(fixture.services.getAllocator().getAllocatedMemory() > 0);
        try {
          fixture.services.runCallableAs(UserGroupInformation.getCurrentUser(), () -> null);
          fail("Accepted helper after cancellation");
        } catch (IllegalStateException expected) {
        }
        fixture.helperFinish.countDown();
      }
      var result = opening.get(10, TimeUnit.SECONDS);
      assertNotNull(result.error());
      assertFalse("Open leaked a cancel interrupt", result.interrupted());
      assertSame(marker, result.loader());
      assertEquals(mode.equals("schema") ? 1 : 0, fixture.closed.get());
      assertEquals(0, fixture.services.getAllocator().getAllocatedMemory());
      ScanHost.close(handle);
      assertEquals(0, ScanHost.activeScans());
      // Reuse the exact opening thread for an original managed plugin reader.
      var next = executor.submit(() -> {
        long nextHandle = ScanHost.open(TestGenericPluginScan.descriptor(true, 0).toString());
        try {
          assertFalse(Thread.currentThread().isInterrupted());
          assertSame(marker, Thread.currentThread().getContextClassLoader());
          assertTrue(ScanHost.schema(nextHandle).contains("\"v\""));
        } finally {
          ScanHost.close(nextHandle);
        }
        return null;
      });
      next.get(20, TimeUnit.SECONDS);
      assertEquals(0, ScanHost.activeScans());
    } finally {
      fixture.helperFinish.countDown();
      ScanHost.cancel(handle);
      ScanHost.close(handle);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      FIXTURES.remove(key);
    }
  }
}
