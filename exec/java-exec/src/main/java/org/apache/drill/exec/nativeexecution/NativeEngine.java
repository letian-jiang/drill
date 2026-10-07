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

import java.nio.ByteBuffer;
import org.apache.drill.exec.nativeexecution.scan.ScanHost;

/**
 * Node-owned C++ engine. RPC is the Drillbit execution path; callbacks remain a test baseline.
 */
public final class NativeEngine implements AutoCloseable {

  public interface Lifecycle {

    void onFailure(byte[] reason);
  }

  public interface Callbacks {

    /**
     * Enqueue the original FragmentStatus; do not call QueryManager inline.
     */
    void onStatus(byte[] status);

    /**
     * Accept responsibility for a batch token and enqueue delivery to the Java
     * receiver. Complete the token exactly once after the direct view is no
     * longer used. False means a closed receiver, before publication.
     */
    boolean onRootBatch(long batchId, byte[] header, ByteBuffer payload);
  }

  private static boolean loaded;

  private volatile long engineId;

  private static synchronized void load() {
    if (!loaded) {
      String library = System.getProperty("drill.native.engine.library");
      if (library == null || library.isBlank()) {
        throw new IllegalStateException("Set drill.native.engine.library to the native engine library");
      }
      System.load(library);
      loaded = true;
    }
  }

  public NativeEngine(byte[] endpoint, int threads, Callbacks callbacks) {
    load();
    engineId = createNative(endpoint, callbacks, ScanHost.class, threads);
  }

  private NativeEngine(byte[] endpoint, int threads, String bindAddress, String advertisedAddress, Lifecycle lifecycle) {
    load();
    engineId = createRpcNative(endpoint, ScanHost.class, threads, bindAddress.getBytes(java.nio.charset.StandardCharsets.UTF_8), advertisedAddress.getBytes(java.nio.charset.StandardCharsets.UTF_8), lifecycle);
  }

  public static NativeEngine rpc(byte[] endpoint, int threads, String bindAddress, String advertisedAddress, Lifecycle lifecycle) {
    return new NativeEngine(endpoint, threads, bindAddress, advertisedAddress, lifecycle);
  }

  /**
   * Lifecycle only: called before the host publishes its single registration.
   */
  public byte[] endpoint() {
    return endpointNative(id());
  }

  private long id() {
    long id = engineId;
    if (id == 0) {
      throw new IllegalStateException("Native engine is closed");
    }
    return id;
  }

  public void submitFragments(byte[] initializeFragments) {
    submitNative(id(), initializeFragments);
  }

  public void cancel(byte[] handle) {
    cancelNative(id(), handle);
  }

  public void receiverFinished(byte[] finishedReceiver) {
    receiverFinishedNative(id(), finishedReceiver);
  }

  public void acceptRecordBatch(byte[] header, ByteBuffer payload) {
    if (payload != null && !payload.isDirect()) {
      throw new IllegalArgumentException("Native exchange requires a direct buffer");
    }
    acceptNative(id(), header, payload == null ? null : payload.slice());
  }

  /**
   * Buffers stay owned by the Java caller until this synchronous call returns.
   */
  public void acceptBatchBuffers(byte[] header, long[] addresses, long[] lengths) {
    acceptBuffersNative(id(), header, addresses, lengths);
  }

  /**
   * 0 accepted, 1 dropped, 2 failed, 3 cancelled.
   */
  public void completeRootBatch(long batchId, int result) {
    completeNative(id(), batchId, result);
  }

  public void fail(String reason) {
    failNative(id(), reason.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  public void quiesce() {
    quiesceNative(id());
  }

  public boolean awaitIdle(long timeoutMillis) {
    return awaitIdleNative(id(), timeoutMillis);
  }

  @Override
  public synchronized void close() {
    if (engineId == 0) {
      return;
    }
    // Keep the handle available to the bridge while close waits for its ACKs.
    closeNative(engineId);
    engineId = 0;
  }

  private static native long createRpcNative(byte[] endpoint, Class<?> scanHost, int threads, byte[] bindAddress, byte[] advertisedAddress, Lifecycle lifecycle);

  private static native byte[] endpointNative(long engineId);

  private static native long createNative(byte[] endpoint, Callbacks callbacks, Class<?> scanHost, int threads);

  private static native void submitNative(long engineId, byte[] fragments);

  private static native void cancelNative(long engineId, byte[] handle);

  private static native void receiverFinishedNative(long engineId, byte[] receiver);

  private static native void acceptNative(long engineId, byte[] header, ByteBuffer payload);

  private static native void acceptBuffersNative(long engineId, byte[] header, long[] addresses, long[] lengths);

  private static native void completeNative(long engineId, long batchId, int result);

  private static native void failNative(long engineId, byte[] reason);

  private static native void quiesceNative(long engineId);

  private static native boolean awaitIdleNative(long engineId, long timeoutMillis);

  private static native void closeNative(long engineId);
}
