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
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CancellationException;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.physical.impl.velox.NativeColumnarBridge;

/**
 * JNI entrypoints only advance plugin readers; native owns all computation and exchange.
 */
public final class ScanHost {

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final AtomicLong IDS = new AtomicLong();

  private static final Map<Long, Session> READERS = new ConcurrentHashMap<>();

  private static final Map<String, ScanHostServices> HOSTS = new ConcurrentHashMap<>();

  private ScanHost() {
  }

  private static String hostKey(DrillbitEndpoint endpoint) {
    return endpoint.getAddress() + ":" + endpoint.getControlPort() + ":" + endpoint.getDataPort();
  }

  public static AutoCloseable registerHost(ScanHostServices services) {
    String key = hostKey(services.endpoint());
    if (HOSTS.putIfAbsent(key, services) != null) {
      throw new IllegalStateException("JNI scan services already registered for " + key);
    }
    return () -> HOSTS.remove(key, services);
  }

  static synchronized ScanHostServices host(DrillbitEndpoint endpoint) throws Exception {
    String key = hostKey(endpoint);
    var host = HOSTS.get(key);
    if (host == null) {
      host = new StandaloneScanHostServices(endpoint);
      HOSTS.put(key, host);
    }
    return host;
  }

  /**
   * Called only after native Tasks and readers have drained. Borrowed Java nodes are untouched.
   */
  public static synchronized void closeStandaloneHosts() throws Exception {
    if (!READERS.isEmpty()) {
      throw new IllegalStateException("JNI readers must drain before closing scan hosts");
    }
    var owned = new java.util.ArrayList<AutoCloseable>();
    for (var entry : HOSTS.entrySet()) {
      if (entry.getValue() instanceof StandaloneScanHostServices) {
        owned.add(() -> {
          ((StandaloneScanHostServices) entry.getValue()).close();
          HOSTS.remove(entry.getKey(), entry.getValue());
        });
      }
    }
    org.apache.drill.common.AutoCloseables.close(owned);
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        closeStandaloneHosts();
      } catch (Exception error) {
        error.printStackTrace();
      }
    }, "jni-plugin-host-close"));
  }

  private static final class Session {

    volatile ScanServices resources;

    private PluginScanReader scan;

    volatile boolean cancelled;

    private final Object ownerLock = new Object();

    private Thread owner;

    boolean first = true;

    private String schema;

    private ScanBatchLayout layout;

    private boolean closed;

    synchronized void initialize(String json) throws Exception {
      if (closed || scan != null) {
        throw new IllegalStateException("JNI scan already initialized or closed");
      }
      synchronized (ownerLock) {
        checkContinue();
        owner = Thread.currentThread();
      }
      ScanServices allocated = null;
      PluginScanReader opened = null;
      try {
        var descriptor = JSON.readTree(json);
        checkContinue();
        allocated = new ScanServices(descriptor.get("provider").asText(), descriptor);
        resources = allocated;
        // Cancellation may have happened before resources were published.
        if (cancelled) {
          allocated.cancel();
        }
        checkContinue();
        opened = PluginScanReader.open(descriptor, allocated);
        checkContinue();
        scan = opened;
        layout = new ScanBatchLayout(scan.schema());
        schema = JSON.writeValueAsString(NativeColumnarBridge.fields(scan.schema()));
        checkContinue();
      } catch (Throwable error) {
        org.apache.drill.common.AutoCloseables.close(error, opened == null ? null : (AutoCloseable) opened::close, allocated);
        scan = null;
        resources = null;
        closed = true;
        throw error;
      } finally {
        synchronized (ownerLock) {
          owner = null;
          Thread.interrupted();
        }
      }
    }

    private void checkContinue() {
      if (cancelled) {
        throw new CancellationException("JNI scan was cancelled during initialization");
      }
    }

    synchronized String schema() {
      if (scan == null || closed) {
        throw new IllegalStateException("JNI scan is not initialized");
      }
      return schema;
    }

    synchronized int read(long destination) throws Exception {
      synchronized (ownerLock) {
        if (cancelled) {
          return 0;
        }
        owner = Thread.currentThread();
      }
      try {
        if (scan == null || closed) {
          throw new IllegalStateException("JNI scan is not initialized");
        }
        boolean hasBatch;
        if (first) {
          first = false;
          hasBatch = true;
        } else {
          hasBatch = scan.next();
        }
        while (hasBatch && scan.rows() == 0 && !cancelled) {
          hasBatch = scan.next();
        }
        if (!hasBatch || cancelled) {
          return 0;
        }
        var batch = scan;
        layout.validate(batch.schema());
        int column = 0, index = 0;
        for (var wrapper : batch.vectors()) {
          DrillBuf[] buffers = wrapper.getValueVector().getBuffers(false);
          if (column >= layout.bufferCounts.length || buffers.length != layout.bufferCounts[column++]) {
            throw new IllegalStateException("JNI plugin buffer layout changed");
          }
          for (DrillBuf buffer : buffers) {
            layout.addresses[index] = buffer.memoryAddress() + buffer.readerIndex();
            layout.lengths[index++] = buffer.readableBytes();
          }
        }
        if (column != layout.bufferCounts.length || index != layout.addresses.length) {
          throw new IllegalStateException("JNI plugin buffer layout changed");
        }
        // Arrays and the fixed layout are reused. Import copies while the plugin
        // owns the batch, so the next read may safely overwrite its buffers.
        importBatch(destination, batch.rows(), layout.addresses, layout.lengths);
        return 1;
      } catch (Throwable error) {
        error.printStackTrace();
        throw error;
      } finally {
        synchronized (ownerLock) {
          owner = null;
          // Native pool threads retain their Java attachment across reads.
          Thread.interrupted();
        }
      }
    }

    void cancel() {
      ScanServices allocated;
      synchronized (ownerLock) {
        cancelled = true;
        allocated = resources;
        if (owner != null) {
          owner.interrupt();
        }
      }
      // Future cancellation can invoke listeners. Do not hold ownerLock while
      // notifying helper tasks, or block the reader's finally/interrupt reset.
      if (allocated != null) {
        allocated.cancel();
      }
    }

    synchronized void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        org.apache.drill.common.AutoCloseables.close(scan == null ? null : (AutoCloseable) scan::close, resources);
      } catch (Exception error) {
        throw new IllegalStateException("Closing JNI scan", error);
      } finally {
        scan = null;
        resources = null;
      }
    }
  }

  /**
   * Publish cancellation ownership before any plugin constructor or schema work.
   */
  public static long reserveScan() {
    long id = IDS.incrementAndGet();
    READERS.put(id, new Session());
    return id;
  }

  private static void initialize(long handle, String descriptor) throws Exception {
    Thread.interrupted();
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(ScanHost.class.getClassLoader());
    try {
      session(handle).initialize(descriptor);
    } finally {
      thread.setContextClassLoader(previous);
      Thread.interrupted();
    }
  }

  public static long open(String descriptor) throws Exception {
    long handle = reserveScan();
    try {
      initialize(handle, descriptor);
      return handle;
    } catch (Throwable error) {
      org.apache.drill.common.AutoCloseables.close(error, () -> close(handle));
      throw error;
    }
  }

  // Native plan JSON is standard UTF-8. JNI NewStringUTF uses modified UTF-8
  // and cannot carry its supplementary characters unchanged.
  public static long openUtf8(byte[] descriptor) throws Exception {
    return open(new String(descriptor, StandardCharsets.UTF_8));
  }

  public static void initializeUtf8(long handle, byte[] descriptor) throws Exception {
    initialize(handle, new String(descriptor, StandardCharsets.UTF_8));
  }

  public static int activeScans() {
    return READERS.size();
  }

  public static String schema(long handle) {
    return session(handle).schema();
  }

  public static byte[] schemaUtf8(long handle) {
    return schema(handle).getBytes(StandardCharsets.UTF_8);
  }

  public static int readBulk(long handle, long destination) throws Exception {
    Thread.interrupted();
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(ScanHost.class.getClassLoader());
    try {
      return session(handle).read(destination);
    } finally {
      thread.setContextClassLoader(previous);
      Thread.interrupted();
    }
  }

  public static void cancel(long handle) {
    Session s = READERS.get(handle);
    if (s != null) {
      s.cancel();
    }
  }

  public static void close(long handle) {
    Session s = READERS.remove(handle);
    if (s != null) {
      s.cancel();
      s.close();
    }
  }

  private static Session session(long handle) {
    Session session = READERS.get(handle);
    if (session == null) {
      throw new IllegalStateException("Unknown JNI scan handle " + handle);
    }
    return session;
  }

  private static native void importBatch(long destination, int rows, long[] addresses, long[] lengths);
}
