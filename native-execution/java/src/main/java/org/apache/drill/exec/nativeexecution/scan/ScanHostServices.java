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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.ExecutorService;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.server.options.OptionManager;

/**
 * Node resources borrowed by readers. Closing a reader must not close these services.
 */
public interface ScanHostServices {

  DrillConfig config();

  ScanResult classpathScan();

  ObjectMapper mapper();

  PhysicalPlanReader planReader();

  OperatorCreatorRegistry creators();

  OptionManager options();

  BufferAllocator allocator();

  ExecutorService executor();

  ExecutorService scanExecutor();

  ExecutorService decodeExecutor();

  DrillbitEndpoint endpoint();

  /**
   * Compatibility resource view for existing plugin code; not a Java fragment executor.
   */
  DrillbitContext pluginContext();

  static ScanHostServices borrow(DrillbitContext node) {
    return new ScanHostServices() {

      public DrillConfig config() {
        return node.getConfig();
      }

      public ScanResult classpathScan() {
        return node.getClasspathScan();
      }

      public ObjectMapper mapper() {
        return node.getPlanReader().copyMapper();
      }

      public PhysicalPlanReader planReader() {
        return node.getPlanReader();
      }

      public OperatorCreatorRegistry creators() {
        return node.getOperatorCreatorRegistry();
      }

      public OptionManager options() {
        return node.getOptionManager();
      }

      public BufferAllocator allocator() {
        return node.getAllocator();
      }

      public ExecutorService executor() {
        return node.getExecutor();
      }

      public ExecutorService scanExecutor() {
        return node.getScanExecutor();
      }

      public ExecutorService decodeExecutor() {
        return node.getScanDecodeExecutor();
      }

      public DrillbitEndpoint endpoint() {
        return node.getEndpoint();
      }

      public DrillbitContext pluginContext() {
        return node;
      }
    };
  }
}
