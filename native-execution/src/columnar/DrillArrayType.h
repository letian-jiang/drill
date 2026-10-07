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
#include <folly/json.h>
#include <velox/type/Type.h>
namespace drill::nativeexec {
// Keep the original Drill LIST/REPEATED representation on pass-through arrays.
// Velox's ordinary ARRAY comparison/parameters still govern computation.
class DrillArrayType final : public facebook::velox::ArrayType {
 public:
  DrillArrayType(facebook::velox::TypePtr element, folly::dynamic field)
      : ArrayType(std::move(element)), field_(std::move(field)) {}
  const folly::dynamic &field() const { return field_; }
 private:
  const folly::dynamic field_;
};
inline bool arrayField(const folly::dynamic &field) {
  return field.getDefault("repeated", false).asBool() || field["minor"] == "LIST";
}
inline size_t arrayPrefix(const folly::dynamic &field) {
  return field.getDefault("repeated", false).asBool() ? 1 : 2;
}
} // namespace drill::nativeexec
