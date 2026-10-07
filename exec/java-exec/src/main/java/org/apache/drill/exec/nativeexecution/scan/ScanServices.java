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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.Base64;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.server.options.OptionList;
import org.apache.drill.exec.server.options.FragmentOptionManager;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.BufferManagerImpl;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import io.netty.buffer.DrillBuf;

/**
 * Per-reader resources. Borrows node services when embedded; owns no Java fragment executor.
 */
public final class ScanServices implements OperatorContext, AutoCloseable {

  private final BufferAllocator allocator;

  private final BufferManagerImpl buffers;

  private final OperatorStats stats;

  private final ExecutorService executor;

  private final List<ListenableFuture<?>> submitted = new ArrayList<>();

  private final ThreadLocal<Boolean> runningHelper = new ThreadLocal<>();

  private int activeHelpers;

  private final List<DrillFileSystem> files = new ArrayList<>();

  private final OptionManager options;

  private final ScanHostServices host;

  private final PlanFragment fragment;

  private volatile ScanContext scanContext;

  private PhysicalOperator operator;

  private final ObjectMapper mapper;

  private volatile boolean closed;

  private volatile boolean cancelled;

  public ScanServices(String name, JsonNode descriptor) throws Exception {
    fragment = descriptor != null && descriptor.has("fragmentContext") ? PlanFragment.parseFrom(Base64.getDecoder().decode(descriptor.get("fragmentContext").asText())) : PlanFragment.getDefaultInstance();
    host = ScanHost.host(fragment.getAssignment());
    executor = host.executor();
    mapper = host.mapper();
    OptionList supplied = fragment.getOptionsJson().isEmpty() ? new OptionList() : mapper.readValue(fragment.getOptionsJson(), OptionList.class);
    options = new FragmentOptionManager(host.options(), supplied);
    allocator = host.allocator().newChildAllocator("jni-scan:" + name, 0, Long.MAX_VALUE);
    try {
      buffers = new BufferManagerImpl(allocator);
      stats = new OperatorStats(0, name, 1, allocator);
    } catch (Throwable error) {
      org.apache.drill.common.AutoCloseables.close(error, allocator);
      throw error;
    }
  }

  DrillConfig config() {
    return host.config();
  }

  OperatorCreatorRegistry creators() {
    return host.creators();
  }

  PhysicalPlanReader planReader() {
    return host.planReader();
  }

  ScanHostServices hostServices() {
    return host;
  }

  ScanContext getScanContext() {
    return scanContext;
  }

  PlanFragment fragment() {
    return fragment;
  }

  BufferManagerImpl bufferManager() {
    return buffers;
  }

  void attach(PhysicalOperator operator, ScanContext context) {
    this.operator = operator;
    scanContext = context;
    if (cancelled) {
      context.cancel();
    }
  }

  public void checkContinue() {
    if (cancelled) {
      throw new CancellationException("Plugin scan resources are cancelled");
    }
    if (closed) {
      throw new IllegalStateException("Plugin scan resources are closed");
    }
  }

  public void cancel() {
    List<ListenableFuture<?>> helpers;
    synchronized (submitted) {
      cancelled = true;
      if (scanContext != null) {
        scanContext.cancel();
      }
      helpers = new ArrayList<>(submitted);
    }
    for (var helper : helpers) {
      helper.cancel(true);
    }
  }

  public ObjectMapper mapper() {
    return mapper;
  }

  public OptionManager options() {
    return options;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public OperatorStats getStats() {
    return stats;
  }

  public DrillBuf replace(DrillBuf old, int size) {
    return buffers.replace(old, size);
  }

  public DrillBuf getManagedBuffer() {
    return buffers.getManagedBuffer();
  }

  public DrillBuf getManagedBuffer(int size) {
    return buffers.getManagedBuffer(size);
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public ExecutorService getScanExecutor() {
    return host.scanExecutor();
  }

  public ExecutorService getScanDecodeExecutor() {
    return host.decodeExecutor();
  }

  public <T extends PhysicalOperator> T getOperatorDefn() {
    if (operator == null) {
      throw new IllegalStateException("No SubScan bound to reader resources");
    }
    @SuppressWarnings("unchecked")
    T result = (T) operator;
    return result;
  }

  public FragmentContext getFragmentContext() {
    if (scanContext == null) {
      throw new IllegalStateException("No scan resource context bound");
    }
    return scanContext;
  }

  public ExecutionControls getExecutionControls() {
    throw new UnsupportedOperationException("Fault injection is not available in ScanServices");
  }

  public void setInjector(ControlsInjector injector) {
    throw new UnsupportedOperationException("Fault injection is not available in ScanServices");
  }

  public ControlsInjector getInjector() {
    throw new UnsupportedOperationException("Fault injection is not available in ScanServices");
  }

  public void injectUnchecked(String description) {
  }

  public <T extends Throwable> void injectChecked(String description, Class<T> type) throws T {
  }

  public DrillFileSystem newFileSystem(Configuration conf) throws IOException {
    DrillFileSystem file = new DrillFileSystem(conf, stats);
    files.add(file);
    return file;
  }

  public DrillFileSystem newNonTrackingFileSystem(Configuration conf) throws IOException {
    DrillFileSystem file = new DrillFileSystem(conf, null);
    files.add(file);
    return file;
  }

  public <T> ListenableFuture<T> runCallableAs(UserGroupInformation user, Callable<T> callable) {
    synchronized (submitted) {
      if (closed) {
        throw new IllegalStateException("Plugin scan resources are closed");
      }
      checkContinue();
      var future = MoreExecutors.listeningDecorator(executor).submit(() -> {
        synchronized (submitted) {
          if (closed || cancelled) {
            throw new CancellationException("Plugin scan resources are closing or cancelled");
          }
          ++activeHelpers;
        }
        runningHelper.set(true);
        try {
          return user.doAs((java.security.PrivilegedExceptionAction<T>) callable::call);
        } finally {
          runningHelper.remove();
          synchronized (submitted) {
            --activeHelpers;
            submitted.notifyAll();
          }
        }
      });
      submitted.add(future);
      return future;
    }
  }

  public void close() {
    if (Boolean.TRUE.equals(runningHelper.get())) {
      throw new IllegalStateException("A plugin helper cannot close its own scan resources");
    }
    closeResources();
  }

  private synchronized void closeResources() {
    if (closed) {
      return;
    }
    List<ListenableFuture<?>> cancel;
    synchronized (submitted) {
      closed = true;
      cancel = new ArrayList<>(submitted);
      submitted.clear();
    }
    for (var future : cancel) {
      future.cancel(true);
    }
    // Future.cancel completes the Future before an interrupted Callable has
    // necessarily exited. Its buffers must remain valid until actual exit.
    boolean interrupted = false;
    synchronized (submitted) {
      while (activeHelpers != 0) {
        try {
          submitted.wait();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    }
    try {
      List<AutoCloseable> resources = new ArrayList<>(files);
      if (scanContext != null) {
        resources.add(scanContext);
      }
      resources.add(buffers);
      resources.add(allocator);
      org.apache.drill.common.AutoCloseables.close(resources);
    } catch (Exception e) {
      throw new IllegalStateException("Closing plugin scan resources", e);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
