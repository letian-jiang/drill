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
#include "protocol/Protobuf.h"
#include <stdexcept>
namespace drill::nativeexec::protocol {
namespace {
std::string_view take(std::string_view &input, uint64_t size) {
  if (size > input.size())
    throw std::runtime_error("Truncated Drill protobuf field");
  auto result = input.substr(0, size);
  input.remove_prefix(size);
  return result;
}
} // namespace
uint64_t readVarint(std::string_view &input) {
  uint64_t value = 0;
  for (unsigned n = 0; n < 10; ++n) {
    if (input.empty())
      throw std::runtime_error("Truncated Drill protobuf varint");
    auto byte = static_cast<uint8_t>(input.front());
    input.remove_prefix(1);
    if (n == 9 && byte > 1)
      throw std::runtime_error("Overflow in Drill protobuf varint");
    value |= uint64_t(byte & 127) << (7 * n);
    if (!(byte & 128))
      return value;
  }
  throw std::runtime_error("Overflow in Drill protobuf varint");
}
void appendVarint(std::string &output, uint64_t value) {
  while (value >= 128) {
    output += char((value & 127) | 128);
    value >>= 7;
  }
  output += char(value);
}
Message::Message(std::string_view input) {
  while (!input.empty()) {
    auto tag = readVarint(input);
    auto number = uint32_t(tag >> 3);
    auto wire = uint8_t(tag & 7);
    if (number == 0 || tag >> 3 > 536870911)
      throw std::runtime_error("Invalid Drill protobuf tag");
    Field f{number, wire};
    if (wire == 0)
      f.value = readVarint(input);
    else if (wire == 2)
      f.bytes = take(input, readVarint(input));
    else if (wire == 1 || wire == 5) {
      auto bytes = take(input, wire == 1 ? 8 : 4);
      for (size_t n = 0; n < bytes.size(); ++n)
        f.value |= uint64_t(static_cast<uint8_t>(bytes[n])) << (8 * n);
    } else
      throw std::runtime_error(
          "Unsupported protobuf wire type in Drill message");
    fields_.push_back(f);
  }
}
uint64_t Message::integer(uint32_t n, uint64_t fallback) const {
  for (auto it = fields_.rbegin(); it != fields_.rend(); ++it)
    if (it->number == n && it->wire != 2)
      return it->value;
  return fallback;
}
std::string_view Message::bytes(uint32_t n) const {
  for (auto it = fields_.rbegin(); it != fields_.rend(); ++it)
    if (it->number == n && it->wire == 2)
      return it->bytes;
  return {};
}
std::vector<std::string_view> Message::messages(uint32_t n) const {
  std::vector<std::string_view> out;
  for (auto &f : fields_)
    if (f.number == n && f.wire == 2)
      out.push_back(f.bytes);
  return out;
}
std::vector<uint32_t> Message::repeatedIntegers(uint32_t n) const {
  std::vector<uint32_t> out;
  for (auto &f : fields_)
    if (f.number == n) {
      if (f.wire == 0)
        out.push_back(uint32_t(f.value));
      else if (f.wire == 2) {
        auto bytes = f.bytes;
        while (!bytes.empty())
          out.push_back(uint32_t(readVarint(bytes)));
      }
    }
  return out;
}
Writer &Writer::integer(uint32_t n, uint64_t v) {
  appendVarint(data_, uint64_t(n) << 3);
  appendVarint(data_, v);
  return *this;
}
Writer &Writer::fixed64(uint32_t n, uint64_t v) {
  appendVarint(data_, uint64_t(n) << 3 | 1);
  for (int i = 0; i < 8; ++i)
    data_ += char(v >> (8 * i));
  return *this;
}
Writer &Writer::bytes(uint32_t n, std::string_view v) {
  appendVarint(data_, uint64_t(n) << 3 | 2);
  appendVarint(data_, v.size());
  data_.append(v);
  return *this;
}
} // namespace drill::nativeexec::protocol
