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

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * Isolated test host: production JNI code runs unchanged against this class.
 */
public final class ScanHost {

  private static final String SCHEMA = "[{\"name\":\"v\",\"minor\":\"BIGINT\",\"optional\":false}]";

  private static final Map<Long, Session> SESSIONS = new ConcurrentHashMap<>();

  private static final Map<String, CountDownLatch> BARRIERS = new ConcurrentHashMap<>();

  private static final AtomicLong IDS = new AtomicLong();

  private static final AtomicInteger ACTIVE_READS = new AtomicInteger();

  private static final AtomicInteger ACTIVE_OPENS = new AtomicInteger();

  private static final java.util.Set<Thread> READ_THREADS = ConcurrentHashMap.newKeySet();

  private static final AtomicInteger PREFETCH_DATA = new AtomicInteger();

  private static final CountDownLatch GATE = new CountDownLatch(1);

  private static final AtomicInteger PEAK = new AtomicInteger();

  private static final Field ADDRESS;

  static {
    try {
      ADDRESS = Buffer.class.getDeclaredField("address");
      ADDRESS.setAccessible(true);
    } catch (ReflectiveOperationException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private static final class Session {

    String descriptor;

    int[] works;

    final ByteBuffer buffer = ByteBuffer.allocateDirect(8).order(ByteOrder.LITTLE_ENDIAN);

    volatile boolean cancelled;

    final Object ownerLock = new Object();

    Thread owner;

    int position;

    void initialize(String text) throws Exception {
      synchronized (ownerLock) {
        if (cancelled) {
          throw new java.util.concurrent.CancellationException("cancelled before initialize");
        }
        owner = Thread.currentThread();
      }
      ACTIVE_OPENS.incrementAndGet();
      try {
        descriptor = text;
        if (text.contains("unicode-path") && !text.contains("/读取/🚀/part.parquet")) {
          throw new IllegalArgumentException("JNI descriptor changed the Unicode plugin path");
        }
        var matcher = Pattern.compile("\\\"workList\\\"\\s*:\\s*\\[([^]]*)]").matcher(text);
        if (!matcher.find()) {
          throw new IllegalArgumentException("missing workList");
        }
        works = java.util.Arrays.stream(matcher.group(1).split(",")).filter(s -> !s.isBlank()).mapToInt(s -> Integer.parseInt(s.trim())).toArray();
        if (text.contains("openBlocked") || (text.contains("laterOpenBlocked") && works[0] != 0)) {
          new CountDownLatch(1).await();
        }
        if (text.contains("openFailure")) {
          throw new IllegalStateException("injected plugin open failure");
        }
      } finally {
        ACTIVE_OPENS.decrementAndGet();
        synchronized (ownerLock) {
          owner = null;
          Thread.interrupted();
        }
      }
    }
  }

  public static long reserveScan() {
    long id = IDS.incrementAndGet();
    SESSIONS.put(id, new Session());
    return id;
  }

  public static void initializeUtf8(long handle, byte[] descriptor) throws Exception {
    SESSIONS.get(handle).initialize(new String(descriptor, StandardCharsets.UTF_8));
  }

  public static long open(String descriptor) throws Exception {
    long id = reserveScan();
    try {
      initializeUtf8(id, descriptor.getBytes(StandardCharsets.UTF_8));
      return id;
    } catch (Throwable error) {
      close(id);
      throw error;
    }
  }

  public static long openUtf8(byte[] descriptor) throws Exception {
    return open(new String(descriptor, StandardCharsets.UTF_8));
  }

  public static String schema(long handle) {
    Session session = SESSIONS.get(handle);
    if (session.descriptor.contains("unicode-schema")) {
      return SCHEMA.replace("\"v\"", "\"列🚀\"");
    }
    if (session.descriptor.contains("mismatch") && session.works[0] == 2) {
      return SCHEMA.replace("BIGINT", "INT");
    }
    return SCHEMA;
  }

  public static byte[] schemaUtf8(long handle) {
    return schema(handle).getBytes(StandardCharsets.UTF_8);
  }

  public static int readBulk(long handle, long target) throws Exception {
    Session session = SESSIONS.get(handle);
    synchronized (session.ownerLock) {
      session.owner = Thread.currentThread();
    }
    READ_THREADS.add(Thread.currentThread());
    int count = ACTIVE_READS.incrementAndGet();
    PEAK.accumulateAndGet(count, Math::max);
    try {
      if (session.cancelled || session.position == session.works.length * 3) {
        return 0;
      }
      if (session.descriptor.contains("barrier") && session.position == 0) {
        CountDownLatch barrier = BARRIERS.computeIfAbsent("parallel", k -> new CountDownLatch(2));
        barrier.countDown();
        if (!barrier.await(5, TimeUnit.SECONDS)) {
          throw new IllegalStateException("readers did not run concurrently");
        }
      }
      if (session.descriptor.contains("gated") && session.position == 0) {
        GATE.await();
      }
      if (session.descriptor.contains("blocked")) {
        try {
          new CountDownLatch(1).await();
        } catch (InterruptedException e) {
          if (!session.cancelled) {
            throw e;
          }
        }
        return 0;
      }
      if (session.descriptor.contains("checkInterrupt") && Thread.currentThread().isInterrupted()) {
        throw new IllegalStateException("cancel interrupt leaked to the next reader");
      }
      int work = session.works[session.position / 3];
      if (session.descriptor.contains("failure") && work == 2) {
        throw new IllegalStateException("injected work read failure");
      }
      session.buffer.putLong(0, work * 10L + session.position++ % 3);
      if (session.descriptor.contains("gated")) {
        PREFETCH_DATA.incrementAndGet();
      }
      if (session.descriptor.contains("badBuffers")) {
        importBatch(target, 1, new long[0], new long[0]);
        return 1;
      }
      importBatch(target, 1, new long[] { ADDRESS.getLong(session.buffer) }, new long[] { 8 });
      return 1;
    } finally {
      ACTIVE_READS.decrementAndGet();
      synchronized (session.ownerLock) {
        session.owner = null;
        Thread.interrupted();
      }
    }
  }

  public static void cancel(long handle) {
    Session session = SESSIONS.get(handle);
    if (session == null) {
      return;
    }
    synchronized (session.ownerLock) {
      session.cancelled = true;
      if (session.owner != null) {
        session.owner.interrupt();
      }
    }
  }

  public static void close(long handle) {
    cancel(handle);
    SESSIONS.remove(handle);
  }

  public static int activeScans() {
    return SESSIONS.size();
  }

  public static int activeReads() {
    return ACTIVE_READS.get();
  }

  public static int activeOpens() {
    return ACTIVE_OPENS.get();
  }

  public static int readThreadCount() {
    return READ_THREADS.size();
  }

  public static int prefetchData() {
    return PREFETCH_DATA.get();
  }

  public static int releaseGate() {
    GATE.countDown();
    return 1;
  }

  public static int peakReads() {
    return PEAK.get();
  }

  private static native void importBatch(long target, int rows, long[] addresses, long[] lengths);
}
