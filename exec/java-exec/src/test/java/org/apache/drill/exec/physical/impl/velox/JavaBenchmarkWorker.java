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
package org.apache.drill.exec.physical.impl.velox;

import com.typesafe.config.ConfigFactory;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.server.Drillbit;

/**
 * Separate stock Java worker process for the same Foreman-only-root benchmark.
 */
public final class JavaBenchmarkWorker {

  public static void main(String[] args) throws Exception {
    var values = new HashMap<String, Object>();
    values.put(ExecConstants.ZK_CONNECTION, args[0]);
    values.put(ExecConstants.ZK_ROOT, args[1]);
    values.put(ExecConstants.SERVICE_NAME, args[2]);
    values.put(ExecConstants.INITIAL_USER_PORT, Integer.parseInt(args[3]));
    values.put(ExecConstants.INITIAL_BIT_PORT, Integer.parseInt(args[4]));
    values.put(ExecConstants.INITIAL_DATA_PORT, Integer.parseInt(args[5]));
    values.put(ExecConstants.DRILL_PORT_HUNT, false);
    values.put(ExecConstants.HTTP_ENABLE, false);
    values.put("drill.exec.grace_period_ms", 0);
    values.put("drill.exec.sys.store.provider.local.path", Files.createTempDirectory("drill-java-benchmark-").toString());
    DrillConfig config = DrillConfig.create(ConfigFactory.parseMap(values).withFallback(DrillConfig.create().root().toConfig()));
    Drillbit bit = new Drillbit(config, null);
    bit.run();
    CountDownLatch done = new CountDownLatch(1);
    // Drillbit owns its shutdown hook. Keep ZooKeeper alive until it has exited.
    System.out.println("Java worker ready: " + bit.getContext().getEndpoint());
    done.await();
  }
}
