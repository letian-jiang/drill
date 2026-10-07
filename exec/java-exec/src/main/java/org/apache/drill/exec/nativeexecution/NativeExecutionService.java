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

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.nativeexecution.scan.ScanHost;
import org.apache.drill.exec.nativeexecution.scan.ScanHostServices;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;

/**
 * Drillbit-owned listeners and engine. Query submission and output use RPC.
 */
public final class NativeExecutionService implements AutoCloseable {

  private final NativeEngine engine;

  private final AutoCloseable scanHost;

  private final DrillbitEndpoint endpoint;

  private boolean closed;

  private final java.util.concurrent.atomic.AtomicReference<String> failure = new java.util.concurrent.atomic.AtomicReference<>();

  private volatile java.util.function.Consumer<String> failureHandler;

  public void setFailureHandler(java.util.function.Consumer<String> handler) {
    failureHandler = handler;
    String reason = failure.get();
    if (reason != null) {
      dispatchFailure(reason);
    }
  }

  private void onFailure(byte[] bytes) {
    String reason = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    if (failure.compareAndSet(null, reason) && failureHandler != null) {
      dispatchFailure(reason);
    }
  }

  private void dispatchFailure(String reason) {
    Thread thread = new Thread(() -> failureHandler.accept(reason), "native-service-failure");
    thread.setDaemon(true);
    thread.start();
  }

  public NativeExecutionService(DrillbitContext context) throws Exception {
    scanHost = ScanHost.registerHost(ScanHostServices.borrow(context));
    NativeEngine created = null;
    try {
      created = NativeEngine.rpc(context.getEndpoint().toByteArray(), Integer.getInteger("drill.native.engine.threads", 0), context.getConfig().getString(ExecConstants.RPC_BIND_ADDR), context.getEndpoint().getAddress(), this::onFailure);
      if (failure.get() != null) {
        throw new IllegalStateException(failure.get());
      }
      endpoint = DrillbitEndpoint.parseFrom(created.endpoint());
      engine = created;
    } catch (Exception | Error error) {
      if (created != null) {
        try {
          created.close();
        } catch (Throwable cleanup) {
          error.addSuppressed(cleanup);
        }
      }
      try {
        scanHost.close();
      } catch (Throwable cleanup) {
        error.addSuppressed(cleanup);
      }
      throw error;
    }
  }

  public DrillbitEndpoint endpoint() {
    return endpoint;
  }

  public void fail(String reason) {
    engine.fail(reason);
  }

  public void quiesce() {
    engine.quiesce();
  }

  public boolean awaitIdle(long timeoutMillis) {
    return engine.awaitIdle(timeoutMillis);
  }

  @Override
  public synchronized void close() throws Exception {
    if (closed) {
      return;
    }
    // Reader close and all in-flight JNI calls finish before releasing host services.
    engine.close();
    scanHost.close();
    closed = true;
    System.out.println("Native RPC ScanHost released.");
  }
}
