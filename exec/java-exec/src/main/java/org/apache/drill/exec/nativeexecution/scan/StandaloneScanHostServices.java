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

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.alias.AliasRegistryProvider;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.RootAllocatorFactory;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.StoragePluginRegistryImpl;
import org.apache.drill.exec.store.sys.store.provider.InMemoryStoreProvider;
import org.apache.drill.metastore.MetastoreRegistry;

/**
 * Shared scan services in the JVM of a C++ worker. No Java Drillbit, executor tree or network.
 */
public final class StandaloneScanHostServices implements ScanHostServices, AutoCloseable {

  private static final class Metadata {

    static final DrillConfig CONFIG = DrillConfig.create();

    static final ScanResult CLASSPATH = ClassPathScanner.fromPrescan(CONFIG);
  }

  private final DrillConfig config;

  private final ScanResult classpath;

  private final LogicalPlanPersistence persistence;

  private final InMemoryStoreProvider stores;

  private final SystemOptionManager options;

  private final BufferAllocator allocator;

  private final ExecutorService executor, scanExecutor, decodeExecutor;

  private final DrillbitEndpoint endpoint;

  private final ScanPluginContext context;

  private final StoragePluginRegistryImpl plugins;

  private final PhysicalPlanReader reader;

  private final OperatorCreatorRegistry creators;

  private final MetricRegistry metrics = new MetricRegistry();

  private FunctionImplementationRegistry functions;

  private CodeCompiler compiler;

  private MetastoreRegistry metastore;

  private AliasRegistryProvider aliases;

  private OAuthTokenProvider oauth;

  private volatile boolean closed;

  private int readers;

  public StandaloneScanHostServices(DrillbitEndpoint endpoint) throws Exception {
    this.endpoint = endpoint;
    config = Metadata.CONFIG;
    classpath = Metadata.CLASSPATH;
    persistence = new LogicalPlanPersistence(config, classpath);
    // Collect construction ownership locally so partial initialization cannot
    // leak a pool, store or allocator when a plugin class fails to initialize.
    List<AutoCloseable> construction = new ArrayList<>();
    try {
      stores = new InMemoryStoreProvider(100);
      construction.add(stores);
      stores.start();
      options = new SystemOptionManager(persistence, stores, config).init();
      construction.add(options);
      allocator = RootAllocatorFactory.newRoot(config);
      construction.add(allocator);
      executor = pool("jni-plugin-task");
      construction.add(() -> stop(executor));
      scanExecutor = pool("jni-plugin-io");
      construction.add(() -> stop(scanExecutor));
      decodeExecutor = pool("jni-plugin-decode");
      construction.add(() -> stop(decodeExecutor));
      context = new ScanPluginContext(this);
      plugins = new StoragePluginRegistryImpl(context, true);
      construction.add(plugins);
      reader = new PhysicalPlanReader(config, classpath, persistence, endpoint, plugins);
      creators = new OperatorCreatorRegistry(classpath);
      plugins.init();
    } catch (Throwable error) {
      java.util.Collections.reverse(construction);
      org.apache.drill.common.AutoCloseables.close(error, construction.toArray(new AutoCloseable[0]));
      throw error;
    }
  }

  private ExecutorService pool(String name) {
    return Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors()), runnable -> {
      Thread thread = new Thread(runnable, name);
      thread.setDaemon(true);
      return thread;
    });
  }

  private static void stop(ExecutorService executor) throws InterruptedException {
    executor.shutdownNow();
    while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
    }
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Standalone plugin scan host is closed");
    }
  }

  synchronized AutoCloseable readerLease() {
    ensureOpen();
    ++readers;
    var released = new java.util.concurrent.atomic.AtomicBoolean();
    return () -> {
      if (released.compareAndSet(false, true)) {
        synchronized (StandaloneScanHostServices.this) {
          --readers;
        }
      }
    };
  }

  public DrillConfig config() {
    return config;
  }

  public ScanResult classpathScan() {
    return classpath;
  }

  public ObjectMapper mapper() {
    return reader.copyMapper();
  }

  public PhysicalPlanReader planReader() {
    return reader;
  }

  public OperatorCreatorRegistry creators() {
    return creators;
  }

  public SystemOptionManager options() {
    return options;
  }

  public BufferAllocator allocator() {
    return allocator;
  }

  public ExecutorService executor() {
    return executor;
  }

  public ExecutorService scanExecutor() {
    return scanExecutor;
  }

  public ExecutorService decodeExecutor() {
    return decodeExecutor;
  }

  public DrillbitEndpoint endpoint() {
    return endpoint;
  }

  public DrillbitContext pluginContext() {
    return context;
  }

  StoragePluginRegistryImpl plugins() {
    return plugins;
  }

  InMemoryStoreProvider stores() {
    return stores;
  }

  LogicalPlanPersistence persistence() {
    return persistence;
  }

  MetricRegistry metrics() {
    return metrics;
  }

  synchronized FunctionImplementationRegistry functions() {
    ensureOpen();
    if (functions == null) {
      functions = new FunctionImplementationRegistry(config, classpath, options);
    }
    return functions;
  }

  synchronized CodeCompiler compiler() {
    ensureOpen();
    if (compiler == null) {
      compiler = new CodeCompiler(config, options);
    }
    return compiler;
  }

  synchronized MetastoreRegistry metastore() {
    ensureOpen();
    if (metastore == null) {
      metastore = new MetastoreRegistry(config);
    }
    return metastore;
  }

  synchronized AliasRegistryProvider aliases() {
    ensureOpen();
    if (aliases == null) {
      aliases = new AliasRegistryProvider(context);
    }
    return aliases;
  }

  synchronized OAuthTokenProvider oauth() {
    ensureOpen();
    if (oauth == null) {
      oauth = new OAuthTokenProvider(context);
    }
    return oauth;
  }

  @Override
  public void close() throws Exception {
    synchronized (this) {
      if (closed) {
        return;
      }
      if (readers != 0) {
        throw new IllegalStateException("Standalone scan resources still active: " + readers);
      }
      closed = true;
    }
    // Readers are drained by ScanHost before this method; their child tasks
    // cannot retain allocator buffers after the node services are released.
    List<AutoCloseable> resources = new ArrayList<>();
    resources.add(() -> stop(executor));
    resources.add(() -> stop(scanExecutor));
    resources.add(() -> stop(decodeExecutor));
    resources.add(plugins);
    if (functions != null) {
      resources.add(functions);
    }
    if (compiler != null) {
      resources.add(compiler::close);
    }
    if (metastore != null) {
      resources.add(metastore);
    }
    if (aliases != null) {
      resources.add(aliases);
    }
    if (oauth != null) {
      resources.add(oauth);
    }
    resources.add(options);
    resources.add(stores);
    resources.add(allocator);
    org.apache.drill.common.AutoCloseables.close(resources);
  }
}
