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
#include "protocol/RpcMessage.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
namespace drill::nativeexec::protocol {
struct RequestTiming {
  // Pending-map enqueue mutex; never includes ACK wait.
  uint64_t lockWaitNanos = 0;
  // Queued frame waiting for the dedicated writer thread.
  uint64_t queueWaitNanos = 0;
  uint64_t sendNanos = 0;
  uint64_t responseWaitNanos = 0;
};
class RpcClient {
public:
  using Handler = std::function<RpcMessage(const RpcMessage &)>;
  using Completion =
      std::function<void(std::exception_ptr, RpcMessage, RequestTiming)>;
  RpcClient(const std::string &address, uint16_t port, RpcMessage handshake,
            Handler handler = {});
  ~RpcClient();
  void shutdown();
  RpcClient(const RpcClient &) = delete;
  RpcMessage request(int32_t type, std::string body, std::string raw = {},
                     RequestTiming *timing = nullptr);

  // Enqueues an owned frame. Completion runs outside transport locks.
  void requestAsync(int32_t type, std::string body, std::string raw,
                    Completion completion);

private:
  struct PendingRequest;
  void readLoop();
  void writeLoop();
  void failPending(std::exception_ptr error);
  void complete(std::shared_ptr<PendingRequest> pending, RpcMessage response);
  void send(const RpcMessage &message);
  int socket_ = -1;
  int32_t nextId_ = 1;
  std::mutex sendMutex_, pendingMutex_;
  std::condition_variable condition_;
  FrameDecoder decoder_;
  Handler handler_;
  std::thread reader_, writer_;
  std::unordered_map<int32_t, std::shared_ptr<PendingRequest>> pending_;
  std::deque<RpcMessage> outgoing_;
  std::exception_ptr failure_;
  std::atomic<bool> stopped_{false};
};
// Protocol callbacks run on a dedicated event thread. They enqueue batches or
// tasks and must not wait for Task completion or perform Scan I/O.
class RpcServer {
public:
  using Handler = std::function<RpcMessage(const RpcMessage &)>;
  RpcServer(const std::string &address, uint16_t port, Handler handler,
            std::function<void(std::string)> onFailure = {});
  ~RpcServer();
  uint16_t port() const { return port_; }
  void stop();

private:
  void serve();
  std::function<void(std::string)> onFailure_;
  int listener_ = -1;
  uint16_t port_;
  Handler handler_;
  std::atomic<bool> stopped_{false};
  std::thread thread_;
};
} // namespace drill::nativeexec::protocol
