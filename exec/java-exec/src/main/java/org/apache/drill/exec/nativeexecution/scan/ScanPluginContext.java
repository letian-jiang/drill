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
import io.netty.channel.EventLoopGroup;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.alias.AliasRegistryProvider;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.fn.registry.RemoteFunctionRegistry;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.metrics.DrillCounters;
import org.apache.drill.exec.oauth.OAuthTokenProvider;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.sql.DrillOperatorTable;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.data.DataConnectionCreator;
import org.apache.drill.exec.rpc.security.AuthenticatorProvider;
import org.apache.drill.exec.rpc.user.UserServer.BitToUserConnection;
import org.apache.drill.exec.rpc.user.UserServer.BitToUserConnectionConfig;
import org.apache.drill.exec.schema.daffodil.DaffodilSchemaProvider;
import org.apache.drill.exec.schema.daffodil.RemoteDaffodilSchemaRegistry;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.SystemOptionManager;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.sys.PersistentStoreProvider;
import org.apache.drill.exec.work.foreman.rm.ResourceManager;
import org.apache.drill.metastore.MetastoreRegistry;

/**
 * Concrete-type compatibility for original plugin constructors. Owns no server services.
 */
final class ScanPluginContext extends DrillbitContext {

  private final StandaloneScanHostServices services;

  ScanPluginContext(StandaloneScanHostServices services) {
    super(true);
    this.services = services;
  }

  private UnsupportedOperationException unavailable(String name) {
    return new UnsupportedOperationException(name + " belongs to the C++ worker or Java Foreman, not the plugin scan host");
  }

  @Override
  public DrillConfig getConfig() {
    return services.config();
  }

  @Override
  public BufferAllocator getAllocator() {
    return services.allocator();
  }

  @Override
  public ScanResult getClasspathScan() {
    return services.classpathScan();
  }

  @Override
  public LogicalPlanPersistence getLpPersistence() {
    return services.persistence();
  }

  @Override
  public StoragePluginRegistry getStorage() {
    return services.plugins();
  }

  @Override
  public SchemaFactory getSchemaFactory() {
    return getStorage().getSchemaFactory();
  }

  @Override
  public SystemOptionManager getOptionManager() {
    return services.options();
  }

  @Override
  public PersistentStoreProvider getStoreProvider() {
    return services.stores();
  }

  @Override
  public PhysicalPlanReader getPlanReader() {
    return services.planReader();
  }

  @Override
  public OperatorCreatorRegistry getOperatorCreatorRegistry() {
    return services.creators();
  }

  @Override
  public DrillbitEndpoint getEndpoint() {
    return services.endpoint();
  }

  @Override
  public ExecutorService getExecutor() {
    return services.executor();
  }

  @Override
  public ExecutorService getScanExecutor() {
    return services.scanExecutor();
  }

  @Override
  public ExecutorService getScanDecodeExecutor() {
    return services.decodeExecutor();
  }

  @Override
  public CodeCompiler getCompiler() {
    return services.compiler();
  }

  @Override
  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return services.functions();
  }

  @Override
  public MetastoreRegistry getMetastoreRegistry() {
    return services.metastore();
  }

  @Override
  public AliasRegistryProvider getAliasRegistryProvider() {
    return services.aliases();
  }

  @Override
  public OAuthTokenProvider getOauthTokenProvider() {
    return services.oauth();
  }

  @Override
  public MetricRegistry getMetrics() {
    return services.metrics();
  }

  @Override
  public DrillCounters getCounters() {
    return DrillCounters.getInstance();
  }

  @Override
  public boolean isOnline(DrillbitEndpoint endpoint) {
    return endpoint.getState() == DrillbitEndpoint.State.ONLINE;
  }

  @Override
  public boolean isForeman(DrillbitEndpoint endpoint) {
    return false;
  }

  @Override
  public boolean isForemanOnline() {
    return false;
  }

  @Override
  public Collection<DrillbitEndpoint> getAvailableBits() {
    throw unavailable("Cluster discovery");
  }

  @Override
  public Collection<DrillbitEndpoint> getBits() {
    throw unavailable("Cluster discovery");
  }

  @Override
  public ClusterCoordinator getClusterCoordinator() {
    throw unavailable("Cluster discovery");
  }

  @Override
  public EventLoopGroup getBitLoopGroup() {
    throw unavailable("Java RPC event loops");
  }

  @Override
  public DataConnectionCreator getDataConnectionsPool() {
    throw unavailable("Java data connections");
  }

  @Override
  public Controller getController() {
    throw unavailable("Java controller");
  }

  @Override
  public WorkEventBus getWorkBus() {
    throw unavailable("Java work bus");
  }

  @Override
  public QueryProfileStoreContext getProfileStoreContext() {
    throw unavailable("Query profile store");
  }

  @Override
  public DrillOperatorTable getOperatorTable() {
    throw unavailable("SQL planning");
  }

  @Override
  public ResourceManager getResourceManager() {
    throw unavailable("Java resource manager");
  }

  @Override
  public void startRM() {
    throw unavailable("Java resource manager");
  }

  @Override
  public AuthenticatorProvider getAuthProvider() {
    throw unavailable("Java RPC authentication");
  }

  @Override
  public Set<Entry<BitToUserConnection, BitToUserConnectionConfig>> getUserConnections() {
    throw unavailable("Java user connections");
  }

  @Override
  public RemoteFunctionRegistry getRemoteFunctionRegistry() {
    throw unavailable("Remote UDF registration");
  }

  @Override
  public DaffodilSchemaProvider getDaffodilSchemaProvider() {
    throw unavailable("Named Daffodil schema registry");
  }

  @Override
  public RemoteDaffodilSchemaRegistry getRemoteDaffodilSchemaRegistry() {
    throw unavailable("Remote Daffodil registration");
  }

  @Override
  public void close() {
    throw unavailable("Node shutdown from a reader");
  }
}
