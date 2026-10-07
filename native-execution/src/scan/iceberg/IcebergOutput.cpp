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
#include "scan/iceberg/IcebergOutput.h"
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillArrayType.h"
#include <velox/vector/FlatVector.h>
namespace drill::nativeexec {
namespace {
using Expr = core::TypedExprPtr;
template <TypeKind kind>
VectorPtr zero(const TypePtr &type, memory::MemoryPool *pool) {
  auto value = BaseVector::create(type, 1, pool);
  using T = typename TypeTraits<kind>::NativeType;
  value->as<FlatVector<T>>()->set(0, T{});
  return value;
}
Expr normalize(Expr input, const folly::dynamic &field,
               memory::MemoryPool *pool, size_t &next) {
  const auto type = columnarType(folly::dynamic::array(field))->childAt(0);
  if (arrayField(field)) {
    VELOX_USER_CHECK(input->type()->isArray());
    auto name = "__drill_scan_element_" + std::to_string(next++);
    auto elementType = input->type()->childAt(0);
    auto element =
        std::make_shared<core::FieldAccessTypedExpr>(elementType, name);
    if (elementType->isPrimitiveType() &&
        !field["element"]["optional"].asBool()) {
      // ScalarArrayWriter.save() is a no-op: the original converter's NULL
      // scalar element performs no write and therefore adds no array entry.
      auto missing = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), std::vector<Expr>{element}, "isnull");
      auto present = std::make_shared<core::CallTypedExpr>(
          BOOLEAN(), std::vector<Expr>{missing}, "not");
      auto predicate = std::make_shared<core::LambdaTypedExpr>(
          ROW({name}, {elementType}), present);
      input = std::make_shared<core::CallTypedExpr>(
          input->type(), std::vector<Expr>{input, predicate}, "filter");
    }
    auto body = normalize(element, field["element"], pool, next);
    auto lambda = std::make_shared<core::LambdaTypedExpr>(
        ROW({name}, {elementType}), body);
    input = std::make_shared<core::CallTypedExpr>(
        type, std::vector<Expr>{input, lambda}, "transform");
    if (!field["optional"].asBool()) {
      auto empty = BaseVector::create<ArrayVector>(type, 1, pool);
      empty->setOffsetAndSize(0, 0, 0);
      input = std::make_shared<core::CallTypedExpr>(
          type,
          std::vector<Expr>{input,
                            std::make_shared<core::ConstantTypedExpr>(empty)},
          "coalesce");
    }
    return input;
  }
  if (field["minor"] == "DICT") {
    VELOX_USER_CHECK(input->type()->isMap());
    auto keyName = "__drill_scan_key_" + std::to_string(next++);
    auto valueName = "__drill_scan_value_" + std::to_string(next++);
    auto keyType = input->type()->childAt(0),
         valueType = input->type()->childAt(1);
    auto key = std::make_shared<core::FieldAccessTypedExpr>(keyType, keyName);
    auto value =
        std::make_shared<core::FieldAccessTypedExpr>(valueType, valueName);
    auto keys = std::make_shared<core::CallTypedExpr>(
        ARRAY(keyType), std::vector<Expr>{input}, "map_keys");
    auto normalizeKey = std::make_shared<core::LambdaTypedExpr>(
        ROW({keyName}, {keyType}),
        normalize(key, field["children"][0], pool, next));
    keys = std::make_shared<core::CallTypedExpr>(
        ARRAY(type->childAt(0)), std::vector<Expr>{keys, normalizeKey},
        "transform");
    auto values = std::make_shared<core::CallTypedExpr>(
        ARRAY(valueType), std::vector<Expr>{input}, "map_values");
    auto normalizeValue = std::make_shared<core::LambdaTypedExpr>(
        ROW({valueName}, {valueType}),
        normalize(value, field["children"][1], pool, next));
    values = std::make_shared<core::CallTypedExpr>(
        ARRAY(type->childAt(1)), std::vector<Expr>{values, normalizeValue},
        "transform");
    input = std::make_shared<core::CallTypedExpr>(
        type, std::vector<Expr>{keys, values}, "drill_dict");
    // Null maps leave DictVector offsets unchanged, hence become empty dicts.
    auto empty = BaseVector::create<MapVector>(type, 1, pool);
    empty->setOffsetAndSize(0, 0, 0);
    return std::make_shared<core::CallTypedExpr>(
        type,
        std::vector<Expr>{input,
                          std::make_shared<core::ConstantTypedExpr>(empty)},
        "coalesce");
  }
  if (field["minor"] == "MAP") {
    VELOX_USER_CHECK(input->type()->isRow());
    // Original MapVector has no parent NULL bit in either mode. Dereference
    // before constructing a present row, preserving each child's NULL/default.
    std::vector<Expr> children;
    for (const auto &child : field["children"]) {
      const auto index =
          input->type()->asRow().getChildIdx(child["name"].asString());
      auto value = std::make_shared<core::DereferenceTypedExpr>(
          input->type()->childAt(index), input, index);
      children.push_back(normalize(value, child, pool, next));
    }
    return std::make_shared<core::CallTypedExpr>(type, std::move(children),
                                                 "row_constructor");
  }
  if (type->isTimestamp())
    input = std::make_shared<core::CallTypedExpr>(
        TIMESTAMP(), std::vector<Expr>{input}, "drill_timestamp_millis");
  if (type->equivalent(*TIME()))
    input = std::make_shared<core::CallTypedExpr>(
        TIME(), std::vector<Expr>{input}, "drill_time_millis");
  if (!field["optional"].asBool()) {
    auto value =
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(zero, type->kind(), type, pool);
    input = std::make_shared<core::CallTypedExpr>(
        type,
        std::vector<Expr>{input,
                          std::make_shared<core::ConstantTypedExpr>(value)},
        "coalesce");
  }
  return input;
}
} // namespace
core::TypedExprPtr icebergOutput(core::TypedExprPtr input,
                                 const folly::dynamic &field,
                                 memory::MemoryPool *pool) {
  size_t next = 0;
  return normalize(std::move(input), field, pool, next);
}
} // namespace drill::nativeexec
