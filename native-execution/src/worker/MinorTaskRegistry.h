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
#include "exchange/ReceiverInbox.h"
#include "execution/VeloxRuntime.h"
#include "protocol/FragmentRoutes.h"
#include "protocol/RpcTransport.h"
#include <unordered_map>
#include <unordered_set>
namespace drill::nativeexec {
// Java hosts enqueue these notifications onto their bridge lane. The root
// future completes when the Java receiver has accepted or dropped the batch.
// An empty future denotes synchronous delivery. All arguments are owned.
struct EngineCallbacks {
  std::function<folly::dynamic()> diagnostics;
  std::function<void(std::string)> status;
  std::function<ContinueFuture(std::string, std::string)> rootBatch;
};
class MinorTaskRegistry {
public:
  MinorTaskRegistry(VeloxRuntime &runtime, std::string endpoint,
                    EngineCallbacks callbacks = {});
  ~MinorTaskRegistry();
  protocol::RpcMessage control(const protocol::RpcMessage &request);
  protocol::RpcMessage data(const protocol::RpcMessage &request);
  void stop();
  void setEndpoint(std::string endpoint);
  void quiesce();
  void fail(std::string error);
  bool awaitIdle(uint64_t timeoutMillis);

private:
  struct Minor;
  struct Destination;
  std::shared_ptr<ReceiverInbox> inbox(const std::string &task, uint32_t major);
  void initialize(std::string_view fragment,
                  std::shared_ptr<const protocol::FragmentRoutes> routes);
  void receiverFinished(folly::dynamic targets, std::string query,
                        std::string receiver, uint32_t major,
                        std::shared_ptr<const protocol::FragmentRoutes> routes);
  void prepare(const std::shared_ptr<Minor> &minor);
  void run(const std::shared_ptr<Minor> &minor);
  void status(const std::shared_ptr<Minor> &minor, uint32_t state,
              std::string error = {});
  std::shared_ptr<protocol::RpcClient> client(std::string address,
                                              uint16_t port, bool control);
  bool localData(const Destination &) const;
  protocol::RpcMessage sendControl(std::string_view endpoint, uint32_t type,
                                   std::string body);
  ContinueFuture sendData(const Destination &, std::string body,
                          std::string raw = {});
  void accept(std::string_view body, WireBatch batch);
  void acceptLocal(const Destination &, const std::shared_ptr<Minor> &,
                   RowVectorPtr);
  VeloxRuntime &runtime_;
  std::string endpoint_;
  EngineCallbacks callbacks_;
  std::mutex mutex_;
  bool stopped_ = false, admitting_ = true;
  std::string failure_;
  size_t pendingNotifications_ = 0;
  std::condition_variable notificationsFinished_;
  std::unordered_map<std::string, std::shared_ptr<Minor>> tasks_;
  std::unordered_set<std::string> cancelledBeforeSubmit_;
  std::unordered_map<std::string, std::shared_ptr<ReceiverInbox>> inboxes_;
  std::unordered_map<std::string, std::shared_ptr<protocol::RpcClient>>
      clients_;
};
} // namespace drill::nativeexec
