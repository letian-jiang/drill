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
#include "protocol/Protobuf.h"
#include <optional>
namespace drill::nativeexec::protocol {
enum class RpcMode : uint32_t {
  Request = 0,
  Response = 1,
  Failure = 2,
  Ping = 3,
  Pong = 4
};
struct RpcMessage {
  RpcMode mode{RpcMode::Request};
  int32_t coordination{};
  int32_t type{};
  std::string body, raw;
};
std::string encodeFrame(const RpcMessage &message);
RpcMessage decodePayload(std::string_view payload);
class FrameDecoder {
public:
  void append(std::string_view input) { buffer_.append(input); }
  std::optional<RpcMessage> next();

private:
  std::string buffer_;
};
} // namespace drill::nativeexec::protocol
