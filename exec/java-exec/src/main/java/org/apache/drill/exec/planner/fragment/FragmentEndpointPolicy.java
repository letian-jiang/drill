/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.planner.fragment;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

/**
 * Root placement is independent of the non-root execution candidate set.
 */
public final class FragmentEndpointPolicy {

  private FragmentEndpointPolicy() {
  }

  public static boolean sameNode(DrillbitEndpoint a, DrillbitEndpoint b) {
    return a.getAddress().equals(b.getAddress()) && a.getControlPort() == b.getControlPort();
  }

  public static List<DrillbitEndpoint> candidates(Collection<DrillbitEndpoint> online, DrillbitEndpoint foreman, boolean root) throws PhysicalOperatorSetupException {
    if (root) {
      return List.of(foreman);
    }
    List<DrillbitEndpoint> workers = online.stream().filter(ep -> !sameNode(ep, foreman)).toList();
    if (workers.isEmpty()) {
      throw new PhysicalOperatorSetupException("Foreman root-only policy requires another online execution node");
    }
    return workers;
  }

  public static void validateAffinities(Collection<DrillbitEndpoint> candidates, Map<DrillbitEndpoint, EndpointAffinity> affinities) throws PhysicalOperatorSetupException {
    for (var entry : affinities.entrySet()) {
      if (entry.getValue().isAssignmentRequired() && candidates.stream().noneMatch(ep -> sameNode(ep, entry.getKey()))) {
        throw new PhysicalOperatorSetupException("Required fragment affinity conflicts with Foreman root-only policy: " + entry.getKey());
      }
    }
  }

  public static List<DrillbitEndpoint> nativeCandidates(Collection<DrillbitEndpoint> online, DrillbitEndpoint foreman) throws PhysicalOperatorSetupException {
    List<DrillbitEndpoint> candidates = online.stream().filter(org.apache.drill.exec.physical.impl.velox.NativeExecutionNode::isRpcEngine).toList();
    if (candidates.isEmpty()) {
      throw new PhysicalOperatorSetupException("Native scheduling requires a Drillbit with ready native control/data services");
    }
    return candidates;
  }
}
