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
#include <map>
#include <stdexcept>
#include <string>
namespace drill::nativeexec::protocol {
// Query-owned immutable routes. The plan and status retain canonical
// assignment.
class FragmentRoutes {
public:
  struct Route {
    std::string assignment, address;
    uint16_t control, data;
    bool native;
  };
  explicit FragmentRoutes(std::string_view context) {
    Message parsed(context);
    root_ = parsed.bytes(1);
    if (root_.empty())
      throw std::runtime_error("Missing query root route");
    query_ = Message(root_).bytes(1);
    for (auto bytes : parsed.messages(2)) {
      Message route(bytes);
      std::string handle(route.bytes(1));
      if (Message(handle).bytes(1) != query_)
        throw std::runtime_error("Fragment route belongs to another query");
      auto control = route.integer(5), data = route.integer(6);
      if (route.bytes(4).empty() || !control || control > 65535 || !data ||
          data > 65535 || route.integer(3) > 1)
        throw std::runtime_error("Invalid fragment transport route");
      Route value{std::string(route.bytes(2)), std::string(route.bytes(4)),
                  uint16_t(control), uint16_t(data), route.integer(3) == 1};
      Message assignment(value.assignment);
      auto native = Message(assignment.bytes(9));
      auto address = value.native ? native.bytes(1) : assignment.bytes(1);
      auto expectedControl =
          value.native ? native.integer(5) : assignment.integer(3);
      auto expectedData =
          value.native ? native.integer(6) : assignment.integer(4);
      if (value.address != address || control != expectedControl ||
          data != expectedData || (value.native && native.integer(3) != 2))
        throw std::runtime_error(
            "Route does not match assigned Drillbit capability");
      if ((handle == root_) == value.native)
        throw std::runtime_error(
            "Root must use Java; all non-root routes must use native");
      auto key = id(handle);
      if (!routes_.emplace(key, std::move(value)).second)
        throw std::runtime_error("Duplicate fragment route");
    }
    get(root_);
  }
  const Route &get(std::string_view handle) const {
    if (Message(handle).bytes(1) != query_)
      throw std::runtime_error("Route lookup belongs to another query");
    auto it = routes_.find(id(handle));
    if (it == routes_.end())
      throw std::runtime_error("Missing fragment route");
    return it->second;
  }
  const Route &get(uint32_t major, uint32_t minor) const {
    return get(
        Writer().bytes(1, query_).integer(2, major).integer(3, minor).take());
  }
  bool root(std::string_view handle) const { return id(handle) == id(root_); }

private:
  static std::pair<uint32_t, uint32_t> id(std::string_view bytes) {
    Message h(bytes);
    return {h.integer(2), h.integer(3)};
  }
  std::string root_, query_;
  std::map<std::pair<uint32_t, uint32_t>, Route> routes_;
};
} // namespace drill::nativeexec::protocol
