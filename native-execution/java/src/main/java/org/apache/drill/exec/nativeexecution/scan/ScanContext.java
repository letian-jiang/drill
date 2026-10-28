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

import com.google.common.base.Function;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.alias.AliasRegistryProvider;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetLoaderImpl;
import org.apache.drill.exec.physical.resultSet.impl.ResultSetOptionBuilder;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.UserBitShared.QueryId;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.rpc.control.Controller;
import org.apache.drill.exec.rpc.control.WorkEventBus;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.QueryProfileStoreContext;
import org.apache.drill.exec.server.options.OptionManager;
import org.apache.drill.exec.store.PartitionExplorer;
import org.apache.drill.exec.testing.ExecutionControls;
import org.apache.drill.exec.proto.helper.QueryIdHelper;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.filter.RuntimeFilterWritable;
import org.apache.drill.metastore.MetastoreRegistry;
import org.apache.hadoop.security.UserGroupInformation;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.ops.AccountingDataTunnel;
import org.apache.drill.exec.ops.AccountingUserConnection;
import org.apache.drill.exec.ops.BaseFragmentContext;
import org.apache.drill.exec.ops.BaseOperatorContext;
import org.apache.drill.exec.ops.BufferManager;
import org.apache.drill.exec.ops.ContextInformation;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.ops.FragmentStats;
import org.apache.drill.exec.ops.OpProfileDef;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.ops.ViewExpansionContext;

/**
 * Compatibility context for one original SubScan. Has no Java execution tree or RPC services.
 */
final class ScanContext extends BaseFragmentContext implements ExecutorFragmentContext {

  private final ScanServices services;

  private final List<AutoCloseable> owned = new ArrayList<>();

  private final Map<String, ValueHolder> constants = new HashMap<>();

  private final FragmentStats stats;

  private final ExecutionControls controls;

  private volatile boolean cancelled;

  private volatile Throwable failure;

  private org.apache.drill.exec.store.SchemaTreeProvider schemaTrees;

  private boolean closed;

  private final ExecutorState state = new ExecutorState() {

    public boolean shouldContinue() {
      return !cancelled && failure == null;
    }

    public void checkContinue() {
      if (cancelled) {
        throw new org.apache.drill.exec.ops.QueryCancelledException();
      }
      if (failure != null) {
        throw new IllegalStateException("Plugin scan failed", failure);
      }
    }

    public void fail(Throwable cause) {
      failure = cause;
    }

    public boolean isFailed() {
      return failure != null;
    }

    public Throwable getFailureCause() {
      return failure;
    }
  };

  ScanContext(ScanServices services) {
    super(null);
    this.services = services;
    stats = new FragmentStats(services.getAllocator(), getEndpoint());
    controls = new ExecutionControls(services.options());
  }

  void cancel() {
    cancelled = true;
  }

  private UnsupportedOperationException unavailable(String service) {
    return new UnsupportedOperationException(service + " is not a plugin scan service; computation and exchange execute in native");
  }

  public OptionManager getOptions() {
    return services.options();
  }

  public DrillConfig getConfig() {
    return services.config();
  }

  public boolean isImpersonationEnabled() {
    return getConfig().getBoolean(org.apache.drill.exec.ExecConstants.IMPERSONATION_ENABLED);
  }

  public ExecutionControls getExecutionControls() {
    return controls;
  }

  public ExecutorService getExecutor() {
    return services.getExecutor();
  }

  public ExecutorService getScanExecutor() {
    return services.getScanExecutor();
  }

  public ExecutorService getScanDecodeExecutor() {
    return services.getScanDecodeExecutor();
  }

  public ExecutorState getExecutorState() {
    return state;
  }

  public void setExecutorState(ExecutorState state) {
    throw unavailable("Java executor state replacement");
  }

  public BufferAllocator getAllocator() {
    return services.getAllocator();
  }

  public BufferAllocator getRootAllocator() {
    return services.hostServices().allocator();
  }

  public BufferAllocator getNewChildAllocator(String name, int id, long initial, long maximum) {
    return getAllocator().newChildAllocator("scan-op:" + id + ":" + name, initial, maximum);
  }

  protected BufferManager getBufferManager() {
    return services.bufferManager();
  }

  public FragmentHandle getHandle() {
    return services.fragment().getHandle();
  }

  public QueryId getQueryId() {
    return getHandle().getQueryId();
  }

  public String getQueryIdString() {
    return QueryIdHelper.getQueryId(getQueryId());
  }

  public String getFragIdString() {
    return QueryIdHelper.getFragmentId(getHandle());
  }

  public String getQueryUserName() {
    return services.fragment().getCredentials().getUserName();
  }

  public ContextInformation getContextInformation() {
    return new ContextInformation(services.fragment().getCredentials(), services.fragment().getContext());
  }

  public DrillbitEndpoint getEndpoint() {
    return services.fragment().getAssignment();
  }

  public DrillbitEndpoint getForemanEndpoint() {
    return services.fragment().getForeman();
  }

  public PhysicalPlanReader getPlanReader() {
    return services.planReader();
  }

  public OperatorCreatorRegistry getOperatorCreatorRegistry() {
    return services.creators();
  }

  public FragmentStats getStats() {
    return stats;
  }

  public OperatorContext newOperatorContext(PhysicalOperator pop) {
    return newOperatorContext(pop, null);
  }

  public OperatorContext newOperatorContext(PhysicalOperator pop, OperatorStats supplied) {
    if (!(pop instanceof org.apache.drill.exec.physical.base.SubScan)) {
      throw unavailable("Java compute operator " + pop.getClass().getName());
    }
    var child = getNewChildAllocator(pop.getClass().getSimpleName(), pop.getOperatorId(), pop.getInitialAllocation(), pop.getMaxAllocation());
    OperatorStats operatorStats = supplied == null ? stats.newOperatorStats(new OpProfileDef(pop.getOperatorId(), pop.getOperatorType(), 1), child) : supplied;
    var context = new BaseOperatorContext(this, child, pop) {

      private boolean operatorClosed;

      public OperatorStats getStats() {
        return operatorStats;
      }

      public <T> ListenableFuture<T> runCallableAs(UserGroupInformation user, Callable<T> callable) {
        return services.runCallableAs(user, callable);
      }

      public void close() {
        if (operatorClosed) {
          return;
        }
        operatorClosed = true;
        super.close();
      }
    };
    owned.add(context::close);
    return context;
  }

  public DrillbitContext getDrillbitContext() {
    return services.hostServices().pluginContext();
  }

  public FunctionImplementationRegistry getFunctionRegistry() {
    return getDrillbitContext().getFunctionImplementationRegistry();
  }

  public CodeCompiler getCompiler() {
    return getDrillbitContext().getCompiler();
  }

  public ResultSetLoader getResultSetLoader() {
    var loader = new ResultSetLoaderImpl(getAllocator(), new ResultSetOptionBuilder().build());
    owned.add(loader::close);
    return loader;
  }

  public ValueHolder getConstantValueHolder(String value, MinorType type, Function<DrillBuf, ValueHolder> initializer) {
    return constants.computeIfAbsent(type.name() + ":" + value, ignored -> initializer.apply(getManagedBuffer()));
  }

  public MetastoreRegistry getMetastoreRegistry() {
    return getDrillbitContext().getMetastoreRegistry();
  }

  public AliasRegistryProvider getAliasRegistryProvider() {
    return getDrillbitContext().getAliasRegistryProvider();
  }

  public Collection<DrillbitEndpoint> getBits() {
    return List.of(getEndpoint());
  }

  public boolean isUserAuthenticationEnabled() {
    return getConfig().getBoolean(org.apache.drill.exec.ExecConstants.USER_AUTHENTICATION_ENABLED);
  }

  public void requestMemory(RecordBatch requestor) {
  }

  public void addRuntimeFilter(RuntimeFilterWritable filter) {
    throw unavailable("Java runtime filter publication");
  }

  public RuntimeFilterWritable getRuntimeFilter(long id) {
    return null;
  }

  public RuntimeFilterWritable getRuntimeFilter(long id, long timeout, TimeUnit unit) {
    return null;
  }

  public synchronized SchemaPlus getFullRootSchema() {
    // Metadata readers such as information_schema need the original plugin
    // schema tree. This borrows node registries, not a Java query executor.
    if (schemaTrees == null) {
      schemaTrees = new org.apache.drill.exec.store.SchemaTreeProvider(getDrillbitContext());
      owned.add(schemaTrees);
    }
    var provider = new org.apache.drill.exec.store.SchemaConfig.SchemaConfigInfoProvider() {

      private final ViewExpansionContext views = new ViewExpansionContext(getConfig(), this);

      public ViewExpansionContext getViewExpansionContext() {
        return views;
      }

      public SchemaPlus getRootSchema(String user) {
        return schemaTrees.createRootSchema(user, this);
      }

      public String getQueryUserName() {
        return ScanContext.this.getQueryUserName();
      }

      public org.apache.drill.exec.proto.UserBitShared.UserCredentials getQueryUserCredentials() {
        return getContextInformation().getQueryUserCredentials();
      }

      public org.apache.drill.exec.server.options.OptionValue getOption(String name) {
        return getOptions().getOption(name);
      }

      public String getTemporaryTableName(String table) {
        throw unavailable("Temporary table resolution");
      }

      public String getTemporaryWorkspace() {
        throw unavailable("Temporary workspace resolution");
      }
    };
    boolean impersonation = isImpersonationEnabled();
    var config = org.apache.drill.exec.store.SchemaConfig.newBuilder(impersonation ? getQueryUserName() : org.apache.drill.exec.util.ImpersonationUtil.getProcessUserName(), provider).setIgnoreAuthErrors(impersonation).build();
    return schemaTrees.createRootSchema(config);
  }

  public PartitionExplorer getPartitionExplorer() {
    throw unavailable("Schema partition exploration");
  }

  public ClusterCoordinator getClusterCoordinator() {
    throw unavailable("Java cluster coordinator");
  }

  public QueryProfileStoreContext getProfileStoreContext() {
    throw unavailable("Java profile store");
  }

  public WorkEventBus getWorkEventBus() {
    throw unavailable("Java work bus");
  }

  public Set<Map.Entry<UserServer.BitToUserConnection, UserServer.BitToUserConnectionConfig>> getUserConnections() {
    throw unavailable("Java user connections");
  }

  public void waitForSendComplete() {
    throw unavailable("Java sender");
  }

  public AccountingDataTunnel getDataTunnel(DrillbitEndpoint endpoint) {
    throw unavailable("Java data tunnel");
  }

  public AccountingUserConnection getUserDataTunnel() {
    throw unavailable("Java user tunnel");
  }

  public Controller getController() {
    throw unavailable("Java controller");
  }

  public IncomingBuffers getBuffers() {
    throw unavailable("Java receiver");
  }

  public void setBuffers(IncomingBuffers buffers) {
    throw unavailable("Java receiver");
  }

  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    cancelled = true;
    try {
      org.apache.drill.common.AutoCloseables.close(owned);
    } catch (Exception error) {
      throw new IllegalStateException("Closing original plugin scan context", error);
    }
  }
}
