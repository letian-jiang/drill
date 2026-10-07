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
#include "execution/VeloxRuntime.h"
#include "scan/iceberg/IcebergArrowBatch.h"
#include <array>
#include <cstring>
#include <iostream>
#include <limits>
#include <velox/vector/DecodedVector.h>
using namespace drill::nativeexec;
using namespace facebook::velox;
namespace {
struct SchemaData {
  std::string format;
  std::string binaryFormat;
  int &released;
  ArrowSchema timestamp{}, binary{};
  std::array<ArrowSchema *, 2> children;
  SchemaData(std::string unit, int &count, int fixedWidth)
      : format(std::move(unit)), binaryFormat(fixedWidth ? "w:" + std::to_string(fixedWidth) : "z"), released(count) {
    timestamp.format = format.c_str(); timestamp.name = "ts";
    binary.format = binaryFormat.c_str(); binary.name = "bytes";
    timestamp.release = binary.release = [](ArrowSchema *value) { value->release = nullptr; };
    children = {&timestamp, &binary};
  }
  static void release(ArrowSchema *value) {
    auto *owner = static_cast<SchemaData *>(value->private_data);
    if (std::strcmp(value->children[0]->format, owner->format.c_str()) != 0) std::terminate();
    ++owner->released;
    delete owner;
    value->release = nullptr;
  }
};
struct ArrayData {
  int &released;
  std::array<int64_t, 6> values = {INT64_MIN, -1001, -1, 0, 1001, INT64_MAX};
  uint8_t valid = 0x37; // Row 3 is NULL; empty binary at row 1 is valid.
  std::array<int32_t, 7> offsets = {0, 3, 3, 1030, 1030, 1032, 1033};
  std::string bytes;
  std::array<const void *, 1> rootBuffers = {nullptr};
  std::array<const void *, 2> timestampBuffers;
  std::array<const void *, 3> binaryBuffers;
  ArrowArray timestamp{}, binary{};
  std::array<ArrowArray *, 2> children;
  explicit ArrayData(int &count) : released(count), bytes(1033, 0) {
    for (size_t i = 0; i < bytes.size(); ++i) bytes[i] = static_cast<char>(i % 256);
    timestampBuffers = {&valid, values.data()};
    binaryBuffers = {&valid, offsets.data(), bytes.data()};
    timestamp.length = binary.length = 6;
    timestamp.null_count = binary.null_count = 1;
    timestamp.n_buffers = 2; binary.n_buffers = 3;
    timestamp.release = binary.release = [](ArrowArray *value) { value->release = nullptr; };
    timestamp.buffers = timestampBuffers.data(); binary.buffers = binaryBuffers.data();
    children = {&timestamp, &binary};
  }
  static void release(ArrowArray *value) {
    auto *owner = static_cast<ArrayData *>(value->private_data);
    ++owner->released; delete owner; value->release = nullptr;
  }
};
void check(std::string unit, std::array<Timestamp, 6> expected, memory::MemoryPool *pool,
           bool malformed = false, int fixedWidth = 0) {
  int schemasReleased = 0, arraysReleased = 0;
  auto *schemaData = new SchemaData(unit, schemasReleased, fixedWidth);
  auto *arrayData = new ArrayData(arraysReleased);
  auto bytes = arrayData->bytes;
  auto offsets = arrayData->offsets;
  if (fixedWidth) {
    arrayData->binary.n_buffers = 2;
    arrayData->binaryBuffers[1] = arrayData->bytes.data();
    for (int i = 0; i <= 6; ++i) offsets[i] = i * fixedWidth;
  }
  ArrowSchema schema{};
  schema.format = "+s"; schema.n_children = 2; schema.children = schemaData->children.data();
  schema.release = SchemaData::release; schema.private_data = schemaData;
  ArrowArray array{};
  array.length = 6; array.n_children = 2; array.children = arrayData->children.data();
  array.n_buffers = 1; array.buffers = arrayData->rootBuffers.data();
  array.release = ArrayData::release; array.private_data = arrayData;
  if (malformed) arrayData->timestamp.n_buffers = 1;
  RowVectorPtr rows;
  try { rows = importIcebergBatch(schema, array, pool); }
  catch (const std::exception &) { if (!malformed) throw; }
  VELOX_CHECK(!schema.release && !array.release);
  if (malformed) {
    VELOX_CHECK_EQ(schemasReleased, 1); VELOX_CHECK_EQ(arraysReleased, 1);
    return;
  }
  VELOX_CHECK(rows && rows->childAt(0)->type()->kind() == TypeKind::TIMESTAMP);
  {
    DecodedVector timestamps(*rows->childAt(0));
    for (int row = 0; row < 6; ++row) {
      if (row == 3) VELOX_CHECK(timestamps.isNullAt(row));
      else {
        auto value = timestamps.valueAt<Timestamp>(row);
        VELOX_CHECK(value == expected[row], "{} row {}: ({},{}) vs ({},{})", unit, row,
            value.getSeconds(), value.getNanos(), expected[row].getSeconds(), expected[row].getNanos());
      }
    }
  }
  auto binary = rows->childAt(1);
  rows.reset(); // The retained binary column must own the SDK schema/array.
  VELOX_CHECK_EQ(schemasReleased, fixedWidth ? 1 : 0);
  VELOX_CHECK_EQ(arraysReleased, fixedWidth ? 1 : 0);
  {
    DecodedVector decoded(*binary);
    for (int row = 0; row < 6; ++row) {
      if (row == 3) VELOX_CHECK(decoded.isNullAt(row));
      else VELOX_CHECK(decoded.valueAt<StringView>(row).str() ==
                      bytes.substr(offsets[row], offsets[row + 1] - offsets[row]));
    }
  }
  binary.reset();
  VELOX_CHECK_EQ(schemasReleased, 1); VELOX_CHECK_EQ(arraysReleased, 1);
}
void checkTime(std::string unit, memory::MemoryPool *pool, bool invalid = false) {
  int schemasReleased = 0, arraysReleased = 0;
  auto *schemaData = new SchemaData(unit, schemasReleased, 0);
  auto *arrayData = new ArrayData(arraysReleased);
  const int64_t day = unit == "tts" ? 86400 : unit == "ttm" ? 86400000
      : unit == "ttu" ? 86400000000LL : 86400000000000LL;
  arrayData->values = {0, 1, 1001, 0, day - 1001, day - 1};
  if (invalid) arrayData->values[1] = day;
  std::array<int32_t, 6> narrow{};
  if (unit == "tts" || unit == "ttm") {
    for (size_t i = 0; i < narrow.size(); ++i) narrow[i] = arrayData->values[i];
    arrayData->timestampBuffers[1] = narrow.data();
  }
  const auto physical = arrayData->values;
  ArrowSchema schema{};
  schema.format = "+s"; schema.n_children = 2; schema.children = schemaData->children.data();
  schema.release = SchemaData::release; schema.private_data = schemaData;
  ArrowArray array{};
  array.length = 6; array.n_children = 2; array.children = arrayData->children.data();
  array.n_buffers = 1; array.buffers = arrayData->rootBuffers.data();
  array.release = ArrayData::release; array.private_data = arrayData;
  RowVectorPtr rows;
  try { rows = importIcebergBatch(schema, array, pool); }
  catch (const VeloxException &) { if (!invalid) throw; }
  VELOX_CHECK(!schema.release && !array.release);
  if (invalid) {
    VELOX_CHECK(!rows && schemasReleased == 1 && arraysReleased == 1);
    return;
  }
  VELOX_CHECK(rows->childAt(0)->type()->equivalent(*TIME_MICRO_UTC()));
  {
    DecodedVector values(*rows->childAt(0));
    for (vector_size_t row = 0; row < 6; ++row) {
      if (row == 3) { VELOX_CHECK(values.isNullAt(row)); continue; }
      const auto expected = unit == "tts" ? physical[row] * 1000000
          : unit == "ttm" ? physical[row] * 1000
          : unit == "ttn" ? physical[row] / 1000 : physical[row];
      VELOX_CHECK_EQ(values.valueAt<int64_t>(row), expected);
    }
  }
  auto retained = rows->childAt(1); rows.reset();
  VELOX_CHECK_EQ(arraysReleased, 0); VELOX_CHECK_EQ(schemasReleased, 0);
  retained.reset();
  VELOX_CHECK_EQ(arraysReleased, 1); VELOX_CHECK_EQ(schemasReleased, 1);
}
}
int main() {
  try {
    VeloxRuntime runtime(2);
    auto root = memory::memoryManager()->addRootPool("iceberg-arrow-test");
    auto pool = root->addLeafChild("import");
    check("tsm:", {Timestamp(-9223372036854776LL,192000000), Timestamp(-2,999000000),
        Timestamp(-1,999000000), {}, Timestamp(1,1000000), Timestamp(9223372036854775LL,807000000)}, pool.get());
    check("tsu:", {Timestamp(-9223372036855LL,224192000), Timestamp(-1,998999000),
        Timestamp(-1,999999000), {}, Timestamp(0,1001000), Timestamp(9223372036854LL,775807000)}, pool.get());
    check("tsn:UTC", {Timestamp(-9223372037LL,145224192), Timestamp(-1,999998999),
        Timestamp(-1,999999999), {}, Timestamp(0,1001), Timestamp(9223372036LL,854775807)}, pool.get());
    check("tsu:UTC", {Timestamp(-9223372036855LL,224192000), Timestamp(-1,998999000),
        Timestamp(-1,999999000), {}, Timestamp(0,1001000), Timestamp(9223372036854LL,775807000)}, pool.get(), false, 64);
    check("tsu:", {}, pool.get(), true);
    check("l", {}, pool.get(), true);
    for (const auto *unit : {"tts", "ttm", "ttu", "ttn"}) {
      checkTime(unit, pool.get());
      checkTime(unit, pool.get(), true);
    }
    VELOX_CHECK_EQ(pool->usedBytes(), 0);
    std::cout << "Iceberg Arrow import: signed unit boundaries, NULL/binary bytes, "
                 "original schema and retained-column ownership, error release passed.\n";
    return 0;
  } catch (const std::exception &error) { std::cerr << error.what() << '\n'; return 1; }
}
