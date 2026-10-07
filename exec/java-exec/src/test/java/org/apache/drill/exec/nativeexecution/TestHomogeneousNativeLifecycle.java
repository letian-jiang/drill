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

import java.nio.file.Files;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.physical.impl.velox.NativeExecutionNode;
import org.apache.drill.exec.planner.fragment.FragmentEndpointPolicy;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.junit.Assume;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Actual Java host, borrowed JSON plugin, native listeners and one ZK registration.
 */
public class TestHomogeneousNativeLifecycle extends ClusterTest {

  @Test
  public void capabilityWithdrawalKeepsRegistrationAndJavaQueries() throws Exception {
    Assume.assumeNotNull(System.getProperty("drill.native.engine.library"));
    var input = dirTestWatcher.getRootDir().toPath().resolve("homogeneous");
    Files.createDirectories(input);
    Files.writeString(input.resolve("a.json"), "{\"v\":1}\n{\"v\":2}\n{\"v\":3}\n");
    Files.writeString(input.resolve("b.json"), "{\"v\":4}\n{\"v\":5}\n{\"v\":6}\n");
    startCluster(ClusterFixture.builder(dirTestWatcher).withLocalZk().saveProfiles().maxParallelization(1).sessionOption("planner.slice_target", 1L));
    var bit = cluster.drillbit();
    var context = bit.getContext();
    var canonical = context.getEndpoint();
    assertTrue(NativeExecutionNode.isRpcEngine(canonical));
    assertTrue(canonical.getRoles().getJavaExecutor());
    assertTrue(canonical.getRoles().getSqlQuery());
    assertNotEquals(canonical.getControlPort(), canonical.getNativeExecution().getControlPort());
    assertNotEquals(canonical.getDataPort(), canonical.getNativeExecution().getDataPort());
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (context.getBits().size() != 1 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(1, context.getBits().size());
    var coordinator = (org.apache.drill.exec.coord.zk.ZKClusterCoordinator) context.getClusterCoordinator();
    String servicePath = "/" + context.getConfig().getString(ExecConstants.SERVICE_NAME);
    var registrationIds = coordinator.getCurator().getChildren().forPath(servicePath);
    assertEquals(1, registrationIds.size());
    String sql = "select sum(v) from dfs.root.`homogeneous`";
    client.alterSession("exec.native_fragment.enabled", true);
    client.alterSession("exec.native_fragment.strict", true);
    client.alterSession("exec.native_scan.enabled", false);
    assertEquals(21, client.queryBuilder().sql(sql).singletonLong());
    bit.disableNativeExecution("controlled lifecycle test");
    deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
    while (context.getBits().stream().anyMatch(NativeExecutionNode::isRpcEngine) && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(1, context.getBits().size());
    var published = context.getBits().iterator().next();
    assertTrue(FragmentEndpointPolicy.sameNode(canonical, published));
    assertEquals(canonical.getUserPort(), published.getUserPort());
    assertFalse(published.hasNativeExecution());
    assertFalse(context.getEndpoint().hasNativeExecution());
    assertEquals(registrationIds, coordinator.getCurator().getChildren().forPath(servicePath));
    assertThrows(Exception.class, () -> client.queryBuilder().sql(sql).run());
    client.alterSession("exec.native_fragment.enabled", false);
    client.alterSession("exec.native_fragment.strict", false);
    assertEquals(21, client.queryBuilder().sql(sql).singletonLong());
  }
}
