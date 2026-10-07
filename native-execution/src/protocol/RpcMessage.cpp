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
#include "protocol/RpcMessage.h"
#include <stdexcept>
namespace drill::nativeexec::protocol {
std::string encodeFrame(const RpcMessage &m) {
  auto header = Writer()
                    .integer(1, uint32_t(m.mode))
                    .integer(2, uint64_t(int64_t(m.coordination)))
                    .integer(3, uint64_t(int64_t(m.type)))
                    .take();
  Writer payload;
  payload.bytes(1, header).bytes(2, m.body);
  if (!m.raw.empty())
    payload.bytes(3, m.raw);
  std::string frame;
  appendVarint(frame, payload.data().size());
  frame += payload.data();
  return frame;
}
RpcMessage decodePayload(std::string_view input) {
  Message payload(input);
  auto h = payload.bytes(1);
  if (h.empty())
    throw std::runtime_error("Missing Drill RPC header");
  Message header(h);
  auto mode = header.integer(1);
  if (mode > 4)
    throw std::runtime_error("Unknown Drill RPC mode");
  return {RpcMode(mode), int32_t(header.integer(2)), int32_t(header.integer(3)),
          std::string(payload.bytes(2)), std::string(payload.bytes(3))};
}
std::optional<RpcMessage> FrameDecoder::next() {
  uint64_t size = 0;
  unsigned prefix = 0;
  for (; prefix < buffer_.size(); ++prefix) {
    uint8_t b = buffer_[prefix];
    if (prefix >= 5 || (prefix == 4 && b > 15))
      throw std::runtime_error("Invalid Drill RPC frame length");
    size |= uint64_t(b & 127) << (7 * prefix);
    if (!(b & 128)) {
      ++prefix;
      if (size > buffer_.size() - prefix)
        return std::nullopt;
      auto result =
          decodePayload(std::string_view(buffer_).substr(prefix, size));
      buffer_.erase(0, prefix + size);
      return result;
    }
  }
  return std::nullopt;
}
} // namespace drill::nativeexec::protocol
