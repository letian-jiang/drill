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
#include "scan/iceberg/IcebergArrowBatch.h"
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillArrayType.h"
#include "columnar/DrillMapType.h"
#include "columnar/DrillRowType.h"
#include <charconv>
#include <cstring>
#include <limits>
#include <string_view>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
#include <velox/vector/arrow/Bridge.h>
namespace drill::nativeexec {
using namespace facebook::velox;
namespace {
struct BatchOwner {
  ArrowSchema schema;
  ArrowArray array;
  BatchOwner(ArrowSchema sourceSchema, ArrowArray sourceArray)
      : schema(sourceSchema), array(sourceArray) {}
  BatchOwner(const BatchOwner &) = delete;
  ~BatchOwner() {
    if (schema.release)
      schema.release(&schema);
    if (array.release)
      array.release(&array);
  }
};
using Lease = std::shared_ptr<BatchOwner>;
void releaseSchema(ArrowSchema *view) {
  delete static_cast<Lease *>(view->private_data);
  view->release = nullptr;
}
void releaseArray(ArrowArray *view) {
  delete static_cast<Lease *>(view->private_data);
  view->release = nullptr;
}
int64_t timestampScale(std::string_view format) {
  if (format.starts_with("tss:"))
    return 1;
  if (format.starts_with("tsm:"))
    return 1000;
  if (format.starts_with("tsu:"))
    return 1000000;
  if (format.starts_with("tsn:"))
    return 1000000000;
  return 0;
}
VectorPtr fixedBinary(const ArrowSchema &schema, const ArrowArray &array,
                      memory::MemoryPool *pool) {
  std::string_view format(schema.format);
  int32_t width = 0;
  auto parsed =
      std::from_chars(format.data() + 2, format.data() + format.size(), width);
  VELOX_USER_CHECK(parsed.ec == std::errc() &&
                   parsed.ptr == format.data() + format.size() && width > 0);
  VELOX_USER_CHECK_EQ(array.offset, 0);
  VELOX_USER_CHECK_EQ(array.n_buffers, 2);
  VELOX_USER_CHECK(array.length >= 0 &&
                   array.length <= std::numeric_limits<vector_size_t>::max());
  VELOX_USER_CHECK(array.buffers[1] || array.length == 0);
  auto result = BaseVector::create(VARBINARY(), array.length, pool);
  auto *flat = result->as<FlatVector<StringView>>();
  auto bytes = size_t(array.length) * width;
  auto *data = bytes ? flat->getRawStringBufferWithSpace(bytes, true) : nullptr;
  if (bytes)
    std::memcpy(data, array.buffers[1], bytes);
  const auto *validity = static_cast<const uint8_t *>(array.buffers[0]);
  VELOX_USER_CHECK(validity || array.null_count == 0);
  for (vector_size_t row = 0; row < array.length; ++row) {
    if (array.null_count != 0 && !(validity[row / 8] & (1 << (row % 8))))
      flat->setNull(row, true);
    else
      flat->setNoCopy(row, StringView(data + size_t(row) * width, width));
  }
  return result;
}
struct BufferLease {
  Lease owner;
  void addRef() const {}
  void release() const {}
};
BufferPtr nulls(const ArrowArray &array, const Lease &owner) {
  if (array.null_count == 0 || array.length == 0)
    return nullptr;
  VELOX_USER_CHECK(array.n_buffers > 0 && array.buffers && array.buffers[0],
                   "Missing Arrow validity buffer");
  return facebook::velox::BufferView<BufferLease>::create(
      static_cast<const uint8_t *>(array.buffers[0]), (array.length + 7) / 8,
      BufferLease{owner});
}
VectorPtr importValue(const ArrowSchema &schema, const ArrowArray &array,
                      const Lease &owner, memory::MemoryPool *pool) {
  VELOX_USER_CHECK(schema.format && schema.release && array.release);
  // A shallow primitive view must not let Velox consume original dictionary
  // callbacks. SDK plain arrays are supported; dictionary leases need their
  // own recursive importer before this encoding can be accepted.
  VELOX_USER_CHECK(!schema.dictionary && !array.dictionary,
                   "Iceberg Arrow dictionary encoding is not supported");
  VELOX_USER_CHECK_EQ(array.offset, 0,
                      "SDK Arrow arrays must have zero offset");
  VELOX_USER_CHECK(array.length >= 0 &&
                   array.length <= std::numeric_limits<vector_size_t>::max());
  VELOX_USER_CHECK_EQ(schema.n_children, array.n_children);
  const std::string_view format(schema.format);
  if (format == "+s") {
    VELOX_USER_CHECK_EQ(array.n_buffers, 1);
    std::vector<VectorPtr> children;
    std::vector<TypePtr> types;
    std::vector<std::string> names;
    for (int64_t i = 0; i < schema.n_children; ++i) {
      VELOX_USER_CHECK_EQ(array.children[i]->length, array.length);
      auto child =
          importValue(*schema.children[i], *array.children[i], owner, pool);
      names.emplace_back(schema.children[i]->name ? schema.children[i]->name
                                                  : "");
      types.push_back(child->type());
      children.push_back(std::move(child));
    }
    return std::make_shared<RowVector>(
        pool, ROW(std::move(names), std::move(types)), nulls(array, owner),
        array.length, std::move(children));
  }
  if (format == "+m") {
    VELOX_USER_CHECK_EQ(array.n_buffers, 2);
    VELOX_USER_CHECK_EQ(array.n_children, 1);
    VELOX_USER_CHECK(array.buffers && array.buffers[1]);
    auto entries =
        importValue(*schema.children[0], *array.children[0], owner, pool);
    VELOX_USER_CHECK(entries->type()->isRow() && entries->type()->size() == 2);
    auto *members = entries->as<RowVector>();
    auto offsets = AlignedBuffer::allocate<vector_size_t>(array.length, pool);
    auto sizes = AlignedBuffer::allocate<vector_size_t>(array.length, pool);
    auto raw = static_cast<const int32_t *>(array.buffers[1]);
    for (int64_t row = 0; row < array.length; ++row) {
      const int64_t start = raw[row], end = raw[row + 1];
      VELOX_USER_CHECK(start >= 0 && start <= end && end <= entries->size(),
                       "Invalid Iceberg map offsets");
      offsets->asMutable<vector_size_t>()[row] = start;
      sizes->asMutable<vector_size_t>()[row] = end - start;
    }
    return std::make_shared<MapVector>(
        pool, MAP(members->childAt(0)->type(), members->childAt(1)->type()),
        nulls(array, owner), array.length, std::move(offsets), std::move(sizes),
        members->childAt(0), members->childAt(1));
  }
  if (format == "+l" || format == "+L") {
    VELOX_USER_CHECK_EQ(array.n_buffers, 2);
    VELOX_USER_CHECK_EQ(array.n_children, 1);
    VELOX_USER_CHECK(array.buffers && array.buffers[1]);
    auto elements =
        importValue(*schema.children[0], *array.children[0], owner, pool);
    auto offsets = AlignedBuffer::allocate<vector_size_t>(array.length, pool);
    auto sizes = AlignedBuffer::allocate<vector_size_t>(array.length, pool);
    // SDK readers export large_list. Velox uses signed 32-bit offsets; narrow
    // only after validating every range, including hidden NULL-list payloads.
    auto offset = [&](int64_t row) -> int64_t {
      return format == "+L"
                 ? static_cast<const int64_t *>(array.buffers[1])[row]
                 : static_cast<const int32_t *>(array.buffers[1])[row];
    };
    for (int64_t row = 0; row < array.length; ++row) {
      const auto begin = offset(row), end = offset(row + 1);
      VELOX_USER_CHECK(begin >= 0 && begin <= end && end <= elements->size(),
                       "Invalid Iceberg list offsets");
      offsets->asMutable<vector_size_t>()[row] = begin;
      sizes->asMutable<vector_size_t>()[row] = end - begin;
    }
    return std::make_shared<ArrayVector>(
        pool, ARRAY(elements->type()), nulls(array, owner), array.length,
        std::move(offsets), std::move(sizes), std::move(elements));
  }
  if (format.starts_with("w:"))
    return fixedBinary(schema, array, pool);
  VELOX_USER_CHECK_EQ(schema.n_children, 0,
                      "Unsupported Iceberg Arrow nested type");
  auto view = schema;
  auto data = array;
  auto scale = timestampScale(format);
  const bool time32 = format == "tts" || format == "ttm";
  const bool time64 = format == "ttu" || format == "ttn";
  // Leaf views retain the complete original SDK C Data tree. Its nested
  // release callbacks and schema formats are never mutated by the importer.
  if (scale || time64)
    view.format = "l";
  if (time32)
    view.format = "i";
  auto schemaLease = std::make_unique<Lease>(owner);
  auto arrayLease = std::make_unique<Lease>(owner);
  view.private_data = schemaLease.release();
  view.release = releaseSchema;
  data.private_data = arrayLease.release();
  data.release = releaseArray;
  VectorPtr input = importFromArrowAsOwner(view, data, pool);
  if (time32 || time64) {
    DecodedVector raw(*input);
    auto output = BaseVector::create(TIME_MICRO_UTC(), input->size(), pool);
    auto *flat = output->as<FlatVector<int64_t>>();
    const int64_t unitsPerDay = format == "tts"   ? 86400
                                : format == "ttm" ? 86400000
                                : format == "ttu" ? 86400000000LL
                                                  : 86400000000000LL;
    for (vector_size_t row = 0; row < input->size(); ++row) {
      if (raw.isNullAt(row)) {
        flat->setNull(row, true);
        continue;
      }
      auto value = time32 ? int64_t(raw.valueAt<int32_t>(row))
                          : raw.valueAt<int64_t>(row);
      VELOX_USER_CHECK(value >= 0 && value < unitsPerDay,
                       "Iceberg TIME is outside a day");
      if (format == "tts")
        value *= 1000000;
      else if (format == "ttm")
        value *= 1000;
      else if (format == "ttn")
        value /= 1000;
      flat->set(row, value);
    }
    input = std::move(output);
  }
  if (scale) {
    DecodedVector raw(*input);
    auto output = BaseVector::create(TIMESTAMP(), input->size(), pool);
    auto *flat = output->as<FlatVector<Timestamp>>();
    for (vector_size_t row = 0; row < input->size(); ++row) {
      if (raw.isNullAt(row)) {
        flat->setNull(row, true);
        continue;
      }
      auto value = raw.valueAt<int64_t>(row);
      auto seconds = value / scale;
      auto remainder = value % scale;
      if (remainder < 0) {
        --seconds;
        remainder += scale;
      }
      flat->set(row, Timestamp(seconds, remainder * (1000000000 / scale)));
    }
    input = std::move(output);
  }
  return input;
}
TypePtr readFieldType(const folly::dynamic &field) {
  if (arrayField(field))
    return std::make_shared<DrillArrayType>(readFieldType(field["element"]),
                                            field);
  if (field["minor"] == "MAP")
    return drillRowType(icebergReadType(field["children"]), field);
  if (field["minor"] == "DICT")
    return std::make_shared<DrillMapType>(readFieldType(field["children"][0]),
                                          readFieldType(field["children"][1]),
                                          field);
  auto type = columnarType(folly::dynamic::array(field))->childAt(0);
  return type->equivalent(*TIME()) ? TIME_MICRO_UTC() : type;
}
} // namespace
RowTypePtr icebergReadType(const folly::dynamic &fields) {
  std::vector<TypePtr> types;
  std::vector<std::string> names;
  for (const auto &field : fields) {
    names.push_back(field["name"].asString());
    types.push_back(readFieldType(field));
  }
  return ROW(std::move(names), std::move(types));
}
RowVectorPtr importIcebergBatch(ArrowSchema &schema, ArrowArray &array,
                                memory::MemoryPool *pool) {
  VELOX_USER_CHECK_EQ(std::string_view(schema.format), "+s");
  VELOX_USER_CHECK_EQ(array.null_count, 0,
                      "SDK record batches cannot have NULL parent rows");
  auto owner = std::make_shared<BatchOwner>(schema, array);
  schema.release = nullptr;
  array.release = nullptr;
  return std::dynamic_pointer_cast<RowVector>(
      importValue(owner->schema, owner->array, owner, pool));
}
} // namespace drill::nativeexec
