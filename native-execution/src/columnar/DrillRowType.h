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
#include <folly/dynamic.h>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <velox/type/Type.h>
namespace drill::nativeexec {
// Drill MAP is a fixed struct, with no parent validity even in OPTIONAL mode.
// RowType uses strong C++ type identity, unlike ArrayType/MapType. Keep an
// ordinary RowType and annotate that particular instance without changing
// Velox's equality/hash contract. Weak ownership never extends a Task's pools.
struct DrillRowFields {
  using TypePtr = facebook::velox::TypePtr;
  struct Entry {
    std::weak_ptr<const facebook::velox::Type> type;
    folly::dynamic field;
  };
  std::mutex mutex;
  std::unordered_map<const facebook::velox::Type *, Entry> fields;
};
inline DrillRowFields &drillRowFields() {
  static DrillRowFields fields;
  return fields;
}
inline facebook::velox::RowTypePtr
drillRowType(const facebook::velox::RowTypePtr &row, folly::dynamic field) {
  // Even an empty struct needs its own instance: the ordinary empty ROW may
  // be shared by required and optional fields with different annotations.
  auto type = std::make_shared<facebook::velox::RowType>(
      std::vector<std::string>(row->names()),
      std::vector<facebook::velox::TypePtr>(row->children()));
  auto &registry = drillRowFields();
  std::lock_guard lock(registry.mutex);
  std::erase_if(registry.fields,
                [](const auto &item) { return item.second.type.expired(); });
  registry.fields.emplace(type.get(),
                          DrillRowFields::Entry{type, std::move(field)});
  return type;
}
// Top-level batch fields are an array; a nested MAP annotation is an object.
// Metadata belongs to a RowType instance, never to shared primitive types.
inline void annotateDrillSchema(const facebook::velox::RowTypePtr &type,
                                folly::dynamic fields) {
  auto &registry = drillRowFields();
  std::lock_guard lock(registry.mutex);
  std::erase_if(registry.fields,
                [](const auto &item) { return item.second.type.expired(); });
  registry.fields.insert_or_assign(type.get(),
      DrillRowFields::Entry{type, std::move(fields)});
}
inline std::optional<folly::dynamic>
drillRowField(const facebook::velox::TypePtr &type) {
  auto &registry = drillRowFields();
  std::lock_guard lock(registry.mutex);
  auto entry = registry.fields.find(type.get());
  if (entry == registry.fields.end())
    return std::nullopt;
  if (entry->second.type.expired()) {
    registry.fields.erase(entry);
    return std::nullopt;
  }
  return entry->second.field;
}
} // namespace drill::nativeexec
