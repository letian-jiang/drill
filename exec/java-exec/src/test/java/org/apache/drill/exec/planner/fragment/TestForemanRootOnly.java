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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestForemanRootOnly extends ClusterTest {

  private static DrillbitEndpoint endpoint(int port) {
    return DrillbitEndpoint.newBuilder().setAddress("127.0.0.1").setControlPort(port).build();
  }

  @BeforeClass
  public static void setup() throws Exception {
    java.nio.file.Path directory = dirTestWatcher.getRootDir().toPath().resolve("parallel");
    java.nio.file.Files.createDirectories(directory);
    for (int i = 0; i < 3; i++) {
      try (var source = TestForemanRootOnly.class.getResourceAsStream("/tpch/lineitem.parquet")) {
        java.nio.file.Files.copy(source, directory.resolve("part-" + i + ".parquet"));
      }
    }
    startCluster(ClusterFixture.builder(dirTestWatcher).clusterSize(4).saveProfiles().maxParallelization(3).sessionOption("planner.slice_target", 1L).sessionOption("exec.foreman.root_only", true));
  }

  @Test
  public void rootStaysLocalAndScanAndComputeLeaveForeman() throws Exception {
    var result = client.queryBuilder().sql("select sum(l_extendedprice) from dfs.root.`parallel`").run();
    var profile = cluster.drillbit().getContext().getProfileStoreContext().getCompletedProfileStore().get(result.queryIdString());
    for (int i = 0; profile == null && i < 200; i++) {
      Thread.sleep(10);
      profile = cluster.drillbit().getContext().getProfileStoreContext().getCompletedProfileStore().get(result.queryIdString());
    }
    assertTrue(profile != null);
    var foreman = cluster.drillbit().getContext().getEndpoint();
    int nonRoot = 0;
    for (var major : profile.getFragmentProfileList()) {
      for (var minor : major.getMinorFragmentProfileList()) {
        if (major.getMajorFragmentId() == 0) {
          assertEquals(foreman, minor.getEndpoint());
        } else {
          assertFalse(FragmentEndpointPolicy.sameNode(foreman, minor.getEndpoint()));
          nonRoot++;
        }
      }
    }
    assertTrue(nonRoot > 0);
  }

  @Test
  public void rejectsMandatoryForemanAffinity() {
    var foreman = endpoint(1);
    assertThrows(PhysicalOperatorSetupException.class, () -> FragmentEndpointPolicy.validateAffinities(List.of(endpoint(2)), Map.of(foreman, new EndpointAffinity(foreman, 1, true, 1))));
  }

  @Test
  public void preservesMandatoryWorkerAffinity() throws Exception {
    var worker = endpoint(2);
    FragmentEndpointPolicy.validateAffinities(List.of(worker), Map.of(worker, new EndpointAffinity(worker, 1, true, 1)));
  }

  @Test
  public void rejectsNonRootWithoutWorker() {
    var foreman = endpoint(1);
    assertThrows(PhysicalOperatorSetupException.class, () -> FragmentEndpointPolicy.candidates(List.of(foreman), foreman, false));
  }

  @Test
  public void nativeRootOnlyRequiresWorkersWithoutExplicitStrictFlag() throws Exception {
    client.alterSession("exec.native_fragment.enabled", true);
    client.alterSession("exec.native_fragment.strict", false);
    try {
      if (org.apache.drill.exec.physical.impl.velox.NativeExecutionNode.isRpcEngine(cluster.drillbit().getContext().getEndpoint())) {
        rootStaysLocalAndScanAndComputeLeaveForeman();
      } else {
        Exception failure = assertThrows(Exception.class, () -> client.queryBuilder().sql("select sum(l_extendedprice) from dfs.root.`parallel`").run());
        assertTrue(failure.toString().contains("Native scheduling requires"));
      }
    } finally {
      client.alterSession("exec.native_fragment.enabled", false);
    }
  }

  @Test
  public void rootNeedsNoRemoteWorker() throws Exception {
    var foreman = endpoint(1);
    assertEquals(List.of(foreman), FragmentEndpointPolicy.candidates(List.of(foreman), foreman, true));
  }
}
