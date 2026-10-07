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
package org.apache.drill.exec.store.paimon;

import java.nio.file.Path;
import java.util.Map;
import org.apache.drill.exec.physical.impl.velox.PureNativeBenchmark;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPluginConfig;

public class PureNativePaimonBenchmark extends PureNativeBenchmark {

  @Override
  protected void defineScan(String dataset) throws Exception {
    cluster.defineFormat("dfs", "paimon", PaimonFormatPluginConfig.builder().properties(Map.of("scan.split.target-size", "16777216")).build());
    cluster.defineWorkspace("dfs", "tpch", Path.of(dataset).resolve("warehouse/default.db").toString(), "paimon");
  }
}
