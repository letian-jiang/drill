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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

public class TestScanServices {

  @Test(timeout = 60000)
  public void closingWaitsForCancelledHelperToExitBeforeFreeingBuffers() throws Exception {
    var services = new ScanServices("helper-drain-test");
    var closer = Executors.newSingleThreadExecutor();
    var started = new CountDownLatch(1);
    var cancelled = new CountDownLatch(1);
    var finish = new CountDownLatch(1);
    var helperFailure = new java.util.concurrent.atomic.AtomicReference<Throwable>();
    var buffer = services.getManagedBuffer(32);
    buffer.setLong(0, 456789L);
    try {
      var helper = services.runCallableAs(UserGroupInformation.getCurrentUser(), () -> {
        started.countDown();
        boolean released = false;
        while (!released) {
          try {
            finish.await();
            released = true;
          } catch (InterruptedException expected) {
            // Model a reader finishing I/O after its Future is cancelled.
            cancelled.countDown();
          }
        }
        try {
          assertEquals(456789L, buffer.getLong(0));
        } catch (Throwable error) {
          helperFailure.set(error);
        }
        return null;
      });
      assertTrue(started.await(10, TimeUnit.SECONDS));
      var closing = closer.submit(services::close);
      assertTrue(cancelled.await(10, TimeUnit.SECONDS));
      assertTrue(helper.isCancelled());
      try {
        closing.get(50, TimeUnit.MILLISECONDS);
        fail("Closed while helper still owns buffers");
      } catch (TimeoutException expected) {
      }
      assertTrue(services.getAllocator().getAllocatedMemory() > 0);
      try {
        services.runCallableAs(UserGroupInformation.getCurrentUser(), () -> null);
        fail("Accepted a helper after close began");
      } catch (IllegalStateException expected) {
      }
      finish.countDown();
      closing.get(10, TimeUnit.SECONDS);
      assertNull(helperFailure.get());
      assertEquals(0, services.getAllocator().getAllocatedMemory());
    } finally {
      finish.countDown();
      services.close();
      closer.shutdownNow();
      closer.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  @Test(timeout = 120000)
  public void concurrentReadersCanReopenAfterOthersClose() throws Exception {
    var executor = Executors.newFixedThreadPool(4);
    var barrier = new CyclicBarrier(4);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int worker = 0; worker < 4; worker++) {
        futures.add(executor.submit(() -> {
          for (int round = 0; round < 4; round++) {
            barrier.await(30, TimeUnit.SECONDS);
            try (var services = new ScanServices("reader-lifecycle-test")) {
              assertNotNull(services.options());
              var buffer = services.getManagedBuffer(32);
              buffer.setLong(0, 123456L);
              assertEquals(123456L, buffer.getLong(0));
              assertEquals(SchemaPath.getSimplePath("v"), services.mapper().readValue("\"`v`\"", SchemaPath.class));
              // Keep loading jar resources while other readers discover metadata
              // or close. A reader must never invalidate another reader's jars.
              var resources = ClassPathScanner.getConfigURLs("drill-module.conf");
              assertFalse(resources.isEmpty());
              for (URL resource : resources) {
                try (var input = resource.openStream()) {
                  input.readAllBytes();
                }
              }
            }
          }
          return null;
        }));
      }
      for (var future : futures) {
        future.get(90, TimeUnit.SECONDS);
      }
      try (var reopened = new ScanServices("after-all-readers-close")) {
        assertEquals(SchemaPath.getSimplePath("v"), reopened.mapper().readValue("\"`v`\"", SchemaPath.class));
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}
