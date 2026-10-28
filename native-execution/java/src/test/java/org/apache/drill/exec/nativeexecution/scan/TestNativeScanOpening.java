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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.exec.nativeexecution.NativeEngine;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Actual JNI1 control -> C++ MinorTaskRegistry -> JNI2 production ScanHost.
 */
public class TestNativeScanOpening extends NativeScanTestBase {

  @Test(timeout = 90000)
  public void cancelEarlyFinishAndCloseInterruptOpeningPluginThroughBothJniLayers() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue(library != null && Files.exists(Path.of(library)));
    for (int action = 0; action < 3; ++action) {
      String key = UUID.randomUUID().toString();
      var fixture = new TestScanHostCancellation.Fixture("schema");
      TestScanHostCancellation.FIXTURES.put(key, fixture);
      var descriptor = (ObjectNode) new ObjectMapper().readTree(TestScanHostCancellation.descriptor(key));
      var fragment = PlanFragment.parseFrom(Base64.getDecoder().decode(descriptor.path("fragmentContext").asText()));
      descriptor.remove("fragmentContext");
      var endpoint = fragment.getAssignment();
      var terminal = new CountDownLatch(1);
      var state = new AtomicInteger();
      var failure = new AtomicReference<Throwable>();
      var callbacks = new NativeEngine.Callbacks() {

        public void onStatus(byte[] bytes) {
          try {
            var profile = FragmentStatus.parseFrom(bytes).getProfile();
            if (profile.getState().getNumber() >= 3) {
              state.set(profile.getState().getNumber());
              terminal.countDown();
            }
          } catch (Throwable error) {
            failure.set(error);
            terminal.countDown();
          }
        }

        public boolean onRootBatch(long token, byte[] bytes, ByteBuffer payload) {
          failure.set(new AssertionError("Opening fragment emitted a batch"));
          return false;
        }
      };
      var closer = Executors.newSingleThreadExecutor();
      try (var engine = new NativeEngine(endpoint.toByteArray(), 4, callbacks)) {
        String plan = """
          {"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
           "destination":"%s","child":{"pop":"test-scan","@id":1,"jniScan":%s}}
          """.formatted(Base64.getEncoder().encodeToString(endpoint.toByteArray()), descriptor);
        var input = fragment.toBuilder().setForeman(endpoint).setFragmentJson(plan).build();
        engine.submitFragments(InitializeFragments.newBuilder().addFragment(input).build().toByteArray());
        assertTrue(fixture.entered.await(20, TimeUnit.SECONDS));
        assertEquals(1, ScanHost.activeScans());
        if (action == 0) {
          engine.cancel(input.getHandle().toByteArray());
        } else if (action == 1) {
          var receiver = input.getHandle().toBuilder().setMajorFragmentId(0).setMinorFragmentId(0);
          engine.receiverFinished(FinishedReceiver.newBuilder().setReceiver(receiver).setSender(input.getHandle()).build().toByteArray());
        } else {
          closer.submit(engine::close).get(10, TimeUnit.SECONDS);
        }
        if (action != 2) {
          assertTrue(terminal.await(10, TimeUnit.SECONDS));
          assertEquals(action == 1 ? 3 : 4, state.get());
        }
        engine.close();
        assertNull(failure.get());
        assertEquals(0, ScanHost.activeScans());
        assertEquals(1, fixture.closed.get());
        assertEquals(0, fixture.services.getAllocator().getAllocatedMemory());
      } finally {
        closer.shutdownNow();
        assertTrue(closer.awaitTermination(10, TimeUnit.SECONDS));
        TestScanHostCancellation.FIXTURES.remove(key);
      }
    }
  }
}
