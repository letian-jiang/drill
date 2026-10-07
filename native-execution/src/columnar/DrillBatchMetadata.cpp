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
#include "columnar/DrillBatchMetadata.h"
#include "columnar/DrillArrayType.h"
#include "columnar/DrillMapType.h"
#include "columnar/DrillRowType.h"
#include "protocol/Protobuf.h"
#include <folly/base64.h>
#include <unordered_map>
namespace drill::nativeexec {
using protocol::Message;
using protocol::Writer;
namespace {
const std::unordered_map<std::string, int> minorCodes = {
    {"INT", 5},        {"BIGINT", 6}, {"DATE", 12},      {"FLOAT4", 18},
    {"FLOAT8", 19},    {"BIT", 20},   {"VARCHAR", 24},   {"VARDECIMAL", 43},
    {"TIMESTAMP", 16}, {"TIME", 13},  {"VARBINARY", 26}, {"MAP", 1},
    {"LIST", 40},      {"DICT", 44}};
std::string major(const folly::dynamic &field, bool optional) {
  Writer out;
  out.integer(1, minorCodes.at(field["minor"].asString()))
      .integer(2, field.getDefault("repeated", false).asBool() ? 2
                  : optional                                   ? 0
                                                               : 1);
  bool precisionPresent = false, scalePresent = false;
  if (field.count("type")) {
    auto original = folly::base64Decode(field["type"].asString());
    // Preserve storage-plugin MajorType attributes, overriding only the mode
    // and the explicitly represented precision/scale below.
    Message originalType(original);
    for (const auto &f : originalType.fields()) {
      precisionPresent |= f.number == 4;
      scalePresent |= f.number == 5;
      if (f.number <= 2 || f.number == 4 || f.number == 5)
        continue;
      if (f.wire == 0)
        out.integer(f.number, f.value);
      else if (f.wire == 1)
        out.fixed64(f.number, f.value);
      else if (f.wire == 2)
        out.bytes(f.number, f.bytes);
      else
        VELOX_USER_FAIL("Unsupported MajorType wire kind {}", f.wire);
    }
  }
  // Proto2 presence is part of MajorType equality. A zero-valued display
  // attribute absent in the original type must not become explicitly present.
  if (precisionPresent || field.getDefault("precision", 0).asInt() != 0 ||
      field["minor"] == "VARDECIMAL")
    out.integer(4, field.getDefault("precision", 0).asInt());
  if (scalePresent || field.getDefault("scale", 0).asInt() != 0 ||
      field["minor"] == "VARDECIMAL")
    out.integer(5, field.getDefault("scale", 0).asInt());
  return out.take();
}
std::string serialized(std::string_view name, std::string_view type, int rows,
                       int bytes,
                       const std::vector<std::string> &children = {}) {
  Writer out;
  out.bytes(1, type)
      .bytes(2, Writer().integer(1, 0).bytes(2, name).data())
      .integer(4, rows)
      .integer(7, bytes);
  for (auto &child : children)
    out.bytes(3, child);
  return out.take();
}
std::string metadata(const folly::dynamic &field, int rows, bool optional,
                     size_t start) {
  auto &lengths = field["lengths"];
  int total = 0;
  for (size_t i = start; i < lengths.size(); ++i)
    total += lengths[i].asInt();
  std::vector<std::string> children;
  if (arrayField(field)) {
    const auto prefix = arrayPrefix(field);
    VELOX_USER_CHECK_GE(lengths.size(), prefix);
    auto offsetType = Writer().integer(1, 31).integer(2, 1).take();
    children.push_back(
        serialized("$offsets$", offsetType, rows + 1, lengths[0].asInt()));
    if (prefix == 2) {
      auto bitsType = Writer().integer(1, 29).integer(2, 1).take();
      children.push_back(
          serialized("$bits$", bitsType, rows, lengths[1].asInt()));
    }
    const auto &element = field["element"];
    auto count = element.getDefault("count", 0).asInt();
    folly::dynamic flattened = folly::dynamic::array;
    for (size_t i = 0; i < prefix; ++i)
      flattened.push_back(lengths[i]);
    for (const auto &length : element["lengths"])
      flattened.push_back(length);
    VELOX_USER_CHECK(flattened == lengths,
                     "Drill array child buffers disagree");
    if (prefix == 1 && field["minor"] == "MAP") {
      for (const auto &child : element["children"])
        children.push_back(
            metadata(child, count, child["optional"].asBool(), 0));
    } else
      children.push_back(
          metadata(element, count, element["optional"].asBool(), 0));
  } else if (field["minor"] == "DICT") {
    VELOX_USER_CHECK_EQ(field["children"].size(), 2);
    auto offsetType = Writer().integer(1, 31).integer(2, 1).take();
    children.push_back(
        serialized("$offsets$", offsetType, rows + 1, lengths[0].asInt()));
    const auto count = field.getDefault("entryCount", 0).asInt();
    folly::dynamic flattened = folly::dynamic::array(lengths[0]);
    for (const auto &child : field["children"]) {
      children.push_back(metadata(child, count, child["optional"].asBool(), 0));
      for (const auto &length : child["lengths"])
        flattened.push_back(length);
    }
    VELOX_USER_CHECK(flattened == lengths, "DICT child buffers disagree");
  } else if (field["minor"] == "MAP") {
    folly::dynamic flattened = folly::dynamic::array;
    for (const auto &child : field["children"]) {
      children.push_back(metadata(child, rows, child["optional"].asBool(), 0));
      for (const auto &length : child["lengths"])
        flattened.push_back(length);
    }
    VELOX_USER_CHECK(flattened == lengths,
                     "Drill MAP child buffer lengths disagree");
  } else if (optional) {
    auto bitsType = Writer().integer(1, 29).integer(2, 1).take();
    children.push_back(
        serialized("$bits$", bitsType, rows, lengths[start].asInt()));
    children.push_back(metadata(field, rows, false, start + 1));
  } else if (field["minor"] == "VARCHAR" || field["minor"] == "VARBINARY" ||
             field["minor"] == "VARDECIMAL") {
    auto offsetType = Writer().integer(1, 31).integer(2, 1).take();
    children.push_back(
        serialized("$offsets$", offsetType, rows + 1, lengths[start].asInt()));
  }
  return serialized(field["name"].asString(), major(field, optional), rows,
                    total, children);
}
} // namespace
namespace {
void clearRuntimeShape(folly::dynamic &field) {
  field.erase("lengths");
  field.erase("count");
  field.erase("entryCount");
  if (field.count("element"))
    clearRuntimeShape(field["element"]);
  if (field.count("children"))
    for (auto &child : field["children"])
      clearRuntimeShape(child);
}
} // namespace
folly::dynamic fieldsFromType(const RowTypePtr &type) {
  if (auto original = drillRowField(type); original && original->isArray()) {
    auto fields = std::move(*original);
    for (auto &field : fields)
      clearRuntimeShape(field);
    return fields;
  }
  folly::dynamic fields = folly::dynamic::array;
  for (size_t i = 0; i < type->size(); ++i) {
    folly::dynamic field =
        folly::dynamic::object("name", type->nameOf(i))("optional", true);
    auto child = type->childAt(i);
    std::string minor;
    if (child->isArray()) {
      if (auto *drill = dynamic_cast<const DrillArrayType *>(child.get())) {
        field = drill->field();
        clearRuntimeShape(field);
        field["name"] = type->nameOf(i);
      } else {
        field["minor"] = "LIST";
        field["element"] =
            fieldsFromType(ROW({"$data$"}, {child->childAt(0)}))[0];
      }
      fields.push_back(std::move(field));
      continue;
    } else if (child->isMap()) {
      if (auto *drill = dynamic_cast<const DrillMapType *>(child.get())) {
        field = drill->field();
        clearRuntimeShape(field);
        field["name"] = type->nameOf(i);
      } else {
        field["minor"] = "DICT";
        field["children"] = fieldsFromType(
            ROW({"key", "value"}, {child->childAt(0), child->childAt(1)}));
        field["children"][0]["optional"] = false;
      }
      fields.push_back(std::move(field));
      continue;
    } else if (child->isDecimal()) {
      minor = "VARDECIMAL";
      auto [p, s] = getDecimalPrecisionScale(*child);
      field["precision"] = p;
      field["scale"] = s;
    } else if (child->isRow()) {
      if (auto original = drillRowField(child)) {
        field = std::move(*original);
        clearRuntimeShape(field);
        field["name"] = type->nameOf(i);
        fields.push_back(std::move(field));
        continue;
      }
      minor = "MAP";
      field["optional"] = false;
      field["children"] =
          fieldsFromType(std::static_pointer_cast<const RowType>(child));
    } else if (child->isDate())
      minor = "DATE";
    else if (child->equivalent(*TIME()))
      minor = "TIME";
    else if (child->equivalent(*TIME_MICRO_UTC()))
      VELOX_USER_FAIL("Iceberg TIME must be normalized before Drill exchange");
    else
      switch (child->kind()) {
      case TypeKind::INTEGER:
        minor = "INT";
        break;
      case TypeKind::BIGINT:
        minor = "BIGINT";
        break;
      case TypeKind::REAL:
        minor = "FLOAT4";
        break;
      case TypeKind::DOUBLE:
        minor = "FLOAT8";
        break;
      case TypeKind::BOOLEAN:
        minor = "BIT";
        break;
      case TypeKind::VARCHAR:
        minor = "VARCHAR";
        break;
      case TypeKind::VARBINARY:
        minor = "VARBINARY";
        break;
      case TypeKind::TIMESTAMP:
        minor = "TIMESTAMP";
        break;
      default:
        VELOX_USER_FAIL("Unsupported Drill exchange type {}",
                        child->toString());
      }
    field["minor"] = minor;
    fields.push_back(field);
  }
  return fields;
}
namespace {
folly::dynamic fieldHeader(std::string_view encoded, uint64_t rows) {
  Message field(encoded);
  Message type(field.bytes(1)), name(field.bytes(2));
  std::string minor;
  for (const auto &[key, value] : minorCodes)
    if (uint64_t(value) == type.integer(1)) {
      minor = key;
      break;
    }
  VELOX_USER_CHECK(!minor.empty(), "Unsupported Drill exchange minor type {}",
                   type.integer(1));
  const auto optional = type.integer(2) == 0;
  const auto repeated = type.integer(2) == 2;
  VELOX_USER_CHECK(type.integer(2) <= 2, "Invalid Drill field mode");
  folly::dynamic out = folly::dynamic::object("name",
                                              std::string(name.bytes(2)))(
      "minor", minor)("optional", optional)("lengths", folly::dynamic::array);
  out["type"] = folly::base64Encode(field.bytes(1));
  out["precision"] = type.integer(4);
  out["scale"] = type.integer(5);
  auto children = field.messages(3);
  uint64_t valuesLength = field.integer(7);
  if (repeated || minor == "LIST") {
    const size_t prefix = repeated ? 1 : 2;
    VELOX_USER_CHECK_GE(children.size(), prefix);
    VELOX_USER_CHECK_EQ(field.integer(4), rows, "Array row count mismatch");
    out["repeated"] = repeated;
    uint64_t total = 0;
    for (size_t i = 0; i < prefix; ++i) {
      auto length = Message(children[i]).integer(7);
      out["lengths"].push_back(length);
      total += length;
    }
    folly::dynamic element;
    if (repeated && minor == "MAP") {
      const uint64_t count =
          children.size() > 1 ? Message(children[1]).integer(4) : 0;
      element = folly::dynamic::object("name", "$data$")("minor", "MAP")(
          "optional", false)("children", folly::dynamic::array)(
          "lengths", folly::dynamic::array);
      for (size_t i = 1; i < children.size(); ++i) {
        auto nested = fieldHeader(children[i], count);
        for (const auto &length : nested["lengths"])
          element["lengths"].push_back(length);
        element["children"].push_back(std::move(nested));
      }
    } else {
      VELOX_USER_CHECK_EQ(children.size(), prefix + 1);
      auto count = Message(children[prefix]).integer(4);
      element = fieldHeader(children[prefix], count);
      element["count"] = count;
    }
    for (const auto &length : element["lengths"]) {
      out["lengths"].push_back(length);
      total += length.asInt();
    }
    VELOX_USER_CHECK_EQ(total, valuesLength, "Array child payload mismatch");
    out["element"] = std::move(element);
    return out;
  }
  if (minor == "DICT") {
    VELOX_USER_CHECK_EQ(field.integer(4), rows, "DICT row count mismatch");
    VELOX_USER_CHECK_EQ(children.size(), 3, "DICT requires offsets/key/value");
    Message offsets(children[0]);
    const auto count = Message(children[1]).integer(4);
    out["entryCount"] = count;
    out["lengths"].push_back(offsets.integer(7));
    out["children"] = folly::dynamic::array;
    uint64_t total = offsets.integer(7);
    for (size_t i = 1; i < children.size(); ++i) {
      VELOX_USER_CHECK_EQ(Message(children[i]).integer(4), count,
                          "DICT child count mismatch");
      auto nested = fieldHeader(children[i], count);
      for (const auto &length : nested["lengths"]) {
        out["lengths"].push_back(length);
        total += length.asInt();
      }
      out["children"].push_back(std::move(nested));
    }
    VELOX_USER_CHECK_EQ(total, valuesLength, "DICT child payload mismatch");
    return out;
  }
  if (minor == "MAP") {
    VELOX_USER_CHECK_EQ(field.integer(4), rows, "MAP row count mismatch");
    out["children"] = folly::dynamic::array;
    uint64_t total = 0;
    for (auto child : children) {
      auto nested = fieldHeader(child, rows);
      for (const auto &length : nested["lengths"]) {
        out["lengths"].push_back(length);
        total += length.asInt();
      }
      out["children"].push_back(std::move(nested));
    }
    VELOX_USER_CHECK_EQ(total, valuesLength, "MAP child payload mismatch");
    return out;
  }
  if (optional) {
    VELOX_USER_CHECK_EQ(children.size(), 2);
    Message bits(children[0]);
    out["lengths"].push_back(bits.integer(7));
    field = Message(children[1]);
    valuesLength = field.integer(7);
    children = field.messages(3);
  }
  if (minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL") {
    VELOX_USER_CHECK_EQ(children.size(), 1);
    Message offsets(children[0]);
    auto length = offsets.integer(7);
    VELOX_USER_CHECK_LE(length, valuesLength);
    out["lengths"].push_back(length);
    out["lengths"].push_back(valuesLength - length);
  } else
    out["lengths"].push_back(valuesLength);
  return out;
}
void emptyLengths(folly::dynamic &fields) {
  for (auto &field : fields) {
    field["lengths"] = folly::dynamic::array;
    if (arrayField(field)) {
      folly::dynamic elementFields = folly::dynamic::array(field["element"]);
      emptyLengths(elementFields);
      field["element"] = std::move(elementFields[0]);
      field["element"]["count"] = 0;
      for (size_t i = 0; i < arrayPrefix(field); ++i)
        field["lengths"].push_back(0);
      for (const auto &length : field["element"]["lengths"])
        field["lengths"].push_back(length);
    } else if (field["minor"] == "DICT") {
      field["lengths"].push_back(0);
      field["entryCount"] = 0;
      emptyLengths(field["children"]);
      for (const auto &child : field["children"])
        for (const auto &length : child["lengths"])
          field["lengths"].push_back(length);
    } else if (field["minor"] == "MAP") {
      emptyLengths(field["children"]);
      for (const auto &child : field["children"])
        for (const auto &length : child["lengths"])
          field["lengths"].push_back(length);
    } else {
      if (field["optional"].asBool())
        field["lengths"].push_back(0);
      if (field["minor"] == "VARCHAR" || field["minor"] == "VARBINARY" ||
          field["minor"] == "VARDECIMAL")
        field["lengths"].push_back(0);
      field["lengths"].push_back(0);
    }
  }
}
} // namespace
folly::dynamic batchHeaderFromDefinition(std::string_view bytes) {
  Message def(bytes);
  VELOX_USER_CHECK(!def.integer(3), "Exchange must have no selection vector");
  folly::dynamic header = folly::dynamic::object("rows", def.integer(1))(
      "fields", folly::dynamic::array);
  for (auto field : def.messages(2))
    header["fields"].push_back(fieldHeader(field, def.integer(1)));
  return header;
}
std::string batchDefinition(const WireBatch &batch) {
  Writer out;
  auto rows = batch.header["rows"].asInt();
  out.integer(1, rows);
  for (auto &field : batch.header["fields"])
    out.bytes(2, metadata(field, rows, field["optional"].asBool(), 0));
  return out.take();
}
std::string schemaDefinition(const folly::dynamic &fields) {
  auto copy = fields;
  emptyLengths(copy);
  return batchDefinition(
      {folly::dynamic::object("rows", 0)("fields", copy), {}, {}});
}
} // namespace drill::nativeexec
