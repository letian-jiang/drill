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
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
namespace drill::nativeexec::protocol {
// A wire reader for Drill's existing protobuf messages. Views borrow the input
// buffer; callers retain that buffer for the lifetime of the message.
struct Field {
  uint32_t number;
  uint8_t wire;
  uint64_t value{};
  std::string_view bytes{};
};
uint64_t readVarint(std::string_view &input);
void appendVarint(std::string &output, uint64_t value);
class Message {
public:
  explicit Message(std::string_view input);
  uint64_t integer(uint32_t number, uint64_t fallback = 0) const;
  std::string_view bytes(uint32_t number) const;
  std::vector<std::string_view> messages(uint32_t number) const;
  std::vector<uint32_t> repeatedIntegers(uint32_t number) const;
  const std::vector<Field> &fields() const { return fields_; }

private:
  std::vector<Field> fields_;
};
class Writer {
public:
  Writer &integer(uint32_t number, uint64_t value);
  Writer &fixed64(uint32_t number, uint64_t value);
  Writer &bytes(uint32_t number, std::string_view value);
  const std::string &data() const { return data_; }
  std::string take() { return std::move(data_); }

private:
  std::string data_;
};
} // namespace drill::nativeexec::protocol
