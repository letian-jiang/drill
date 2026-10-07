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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.drill.exec.nativeexecution.NativeEngine;
import org.apache.drill.exec.proto.BitControl.FragmentStatus;
import org.apache.drill.exec.proto.BitControl.InitializeFragments;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.BitData.FragmentRecordBatch;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Real JNI1 -> Velox aggregate -> JNI2 original Java SubScan (no plugin adapter).
 */
public class TestGenericPluginNativeEngine {

  @Test(timeout = 60000)
  public void nativeAggregateReadsOriginalLegacyAndManagedScans() throws Exception {
    String library = System.getProperty("drill.native.engine.library");
    Assume.assumeTrue("Requires the optional native execution build", library != null && Files.exists(Path.of(library)));
    for (boolean extended : new boolean[] { false, true }) {
      var descriptor = TestGenericPluginScan.descriptor(extended, 130003);
      ((com.fasterxml.jackson.databind.node.ObjectNode) descriptor.path("scan").path("entries").get(0).path("types").get(0)).put("name", "列🚀");
      var context = PlanFragment.parseFrom(Base64.getDecoder().decode(descriptor.path("fragmentContext").asText()));
      var endpoint = context.getAssignment();
      var failure = new AtomicReference<Throwable>();
      var engineRef = new AtomicReference<NativeEngine>();
      var terminal = new CountDownLatch(1);
      var value = new java.util.concurrent.atomic.AtomicLong(-1);
      var state = new java.util.concurrent.atomic.AtomicInteger();
      var callbacks = new NativeEngine.Callbacks() {

        public void onStatus(byte[] bytes) {
          try {
            var status = FragmentStatus.parseFrom(bytes);
            if (status.getProfile().getState().getNumber() >= 3) {
              state.set(status.getProfile().getState().getNumber());
              if (state.get() != 3) {
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
          try {
            var batch = FragmentRecordBatch.parseFrom(bytes);
            assertEquals(0, batch.getReceivingMajorFragmentId());
            if (batch.getDef().getRecordCount() > 0) {
              assertEquals(1, batch.getDef().getRecordCount());
              assertTrue(payload.remaining() == 8 || payload.remaining() == 9);
              if (payload.remaining() == 9) {
                assertEquals(1, payload.get(payload.position()));
              }
              value.set(payload.order(ByteOrder.LITTLE_ENDIAN).getLong(payload.limit() - 8));
            }
          } catch (Throwable error) {
            failure.set(error);
            result = 2;
          }
          engineRef.get().completeRootBatch(token, result);
          return true;
        }
      };
      try (var engine = new NativeEngine(endpoint.toByteArray(), 4, callbacks)) {
        engineRef.set(engine);
        // Metadata is injected by NativeEngine from PlanFragment, not supplied by the plugin.
        descriptor.remove("fragmentContext");
        String plan = """
          {"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
           "destination":"%s","child":{"pop":"streaming-aggregate","@id":1,"keys":[],
           "exprs":[{"ref":"`n`","expr":"count(`列🚀`)"}],"child":%s}}
          """.formatted(Base64.getEncoder().encodeToString(endpoint.toByteArray()), ((com.fasterxml.jackson.databind.node.ObjectNode) descriptor.path("scan").deepCopy()).set("jniScan", descriptor));
        var fragment = context.toBuilder().setForeman(endpoint).setFragmentJson(plan).build();
        engine.submitFragments(InitializeFragments.newBuilder().addFragment(fragment).build().toByteArray());
        assertTrue("No terminal native status", terminal.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
          throw new AssertionError("Original plugin/native aggregate failed", failure.get());
        }
        assertEquals(3, state.get());
        assertEquals("Missing or duplicated original Java scan rows", 130003, value.get());
        assertEquals(0, ScanHost.activeScans());
      }
    }
  }
}
