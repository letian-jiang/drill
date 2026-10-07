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

import java.util.Collection;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.velox.NativeExecutionNode;
import org.apache.drill.exec.proto.BitControl.FragmentExecutionRoute;
import org.apache.drill.exec.proto.BitControl.NativeExecutionContext;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.server.options.OptionManager;

/**
 * Resolves transport once without modifying original minor plans or assignments.
 */
public final class NativeExecutionRoutes {

  private NativeExecutionRoutes() {
  }

  public static boolean enabled(OptionManager options) {
    return options.getBoolean(ExecConstants.NATIVE_FRAGMENT_ENABLED.getOptionName()) || options.getBoolean(ExecConstants.NATIVE_FRAGMENT_STRICT.getOptionName());
  }

  public static DrillbitEndpoint control(DrillbitEndpoint assignment) {
    if (!NativeExecutionNode.isRpcEngine(assignment)) {
      throw new IllegalStateException("Assigned Drillbit has no native RPC capability");
    }
    var nativeEndpoint = assignment.getNativeExecution();
    return assignment.toBuilder().setAddress(nativeEndpoint.getAddress()).setControlPort(nativeEndpoint.getControlPort()).setDataPort(nativeEndpoint.getDataPort()).build();
  }

  public static NativeExecutionContext freeze(PlanFragment root, Collection<PlanFragment> nonRoot) {
    var context = NativeExecutionContext.newBuilder().setRoot(root.getHandle());
    context.addRoute(route(root, false));
    var seen = new java.util.HashSet<FragmentHandle>();
    seen.add(root.getHandle());
    for (var plan : nonRoot) {
      if (!plan.getHandle().getQueryId().equals(root.getHandle().getQueryId()) || !seen.add(plan.getHandle())) {
        throw new IllegalArgumentException("Duplicate or foreign fragment route");
      }
      context.addRoute(route(plan, true));
    }
    return context.build();
  }

  private static FragmentExecutionRoute route(PlanFragment plan, boolean nativeBackend) {
    var assignment = plan.getAssignment();
    var transport = nativeBackend ? control(assignment) : assignment;
    return FragmentExecutionRoute.newBuilder().setHandle(plan.getHandle()).setAssignment(assignment).setBackend(nativeBackend ? FragmentExecutionRoute.Backend.NATIVE : FragmentExecutionRoute.Backend.JAVA).setAddress(transport.getAddress()).setControlPort(transport.getControlPort()).setDataPort(transport.getDataPort()).build();
  }
}
