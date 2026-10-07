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

import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

/**
 * A C++ execution service speaks ordinary BitControl and BitData directly.
 */
public final class NativeExecutionNode {

  private NativeExecutionNode() {
  }

  /**
   * Native ability is independent of node roles; both listeners must be published.
   */
  public static boolean isRpcEngine(DrillbitEndpoint endpoint) {
    if (!endpoint.hasNativeExecution()) {
      return false;
    }
    var service = endpoint.getNativeExecution();
    return !endpoint.getAddress().isBlank() && endpoint.getControlPort() > 0 && endpoint.getDataPort() > 0 && !service.getAddress().isBlank() && service.getProtocolVersion() == 2 && service.getControlPort() > 0 && service.getControlPort() <= 65535 && service.getDataPort() > 0 && service.getDataPort() <= 65535 && service.getControlPort() != service.getDataPort();
  }
}
