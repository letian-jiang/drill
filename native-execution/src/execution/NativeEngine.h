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
#pragma once
#include "worker/MinorTaskRegistry.h"
namespace drill::nativeexec {
// Each homogeneous Java Drillbit owns one engine; isolated tests use the same core.
// Network listeners and Java bindings belong to the host, not to the engine.
class NativeEngine {
public:
  explicit NativeEngine(std::string endpoint,
                        unsigned threads = availableCpuCount(),
                        EngineCallbacks callbacks = {});
  ~NativeEngine();
  protocol::RpcMessage control(const protocol::RpcMessage &request);
  protocol::RpcMessage data(const protocol::RpcMessage &request);
  void submitFragments(std::string initializeFragments);
  void cancel(std::string handle);
  void receiverFinished(std::string finishedReceiver);
  void acceptRecordBatch(std::string header, std::string payload);
  void close();
  void fail(std::string error) { tasks_.fail(std::move(error)); }
  void quiesce() { tasks_.quiesce(); }
  bool awaitIdle(uint64_t timeoutMillis) {
    return tasks_.awaitIdle(timeoutMillis);
  }
  void setEndpoint(std::string endpoint) {
    tasks_.setEndpoint(std::move(endpoint));
  }
  unsigned threads() const { return runtime_.threads(); }

private:
  VeloxRuntime runtime_;
  MinorTaskRegistry tasks_;
};
} // namespace drill::nativeexec
