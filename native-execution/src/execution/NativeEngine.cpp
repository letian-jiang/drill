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
#include "execution/NativeEngine.h"
namespace drill::nativeexec {
NativeEngine::NativeEngine(std::string endpoint, unsigned threads,
                           EngineCallbacks callbacks)
    : runtime_(threads), tasks_(runtime_, std::move(endpoint),
                               std::move(callbacks)) {}
NativeEngine::~NativeEngine() { close(); }
protocol::RpcMessage NativeEngine::control(const protocol::RpcMessage &request) {
  return tasks_.control(request);
}
protocol::RpcMessage NativeEngine::data(const protocol::RpcMessage &request) {
  return tasks_.data(request);
}
void NativeEngine::submitFragments(std::string body) {
  control({protocol::RpcMode::Request, 0, 3, std::move(body), {}});
}
void NativeEngine::cancel(std::string body) {
  control({protocol::RpcMode::Request, 0, 6, std::move(body), {}});
}
void NativeEngine::receiverFinished(std::string body) {
  control({protocol::RpcMode::Request, 0, 7, std::move(body), {}});
}
void NativeEngine::acceptRecordBatch(std::string header, std::string payload) {
  data({protocol::RpcMode::Request, 0, 3, std::move(header), std::move(payload)});
}
void NativeEngine::close() { tasks_.stop(); }
} // namespace drill::nativeexec
