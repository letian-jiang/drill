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
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillBatchMetadata.h"
#include "exchange/ReceiverInbox.h"
#include "exchange/SenderBuffer.h"
#include "execution/VeloxRuntime.h"
#include "plan/ExpressionBinder.h"
#include <cstring>
#include <iostream>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
// Encoding diagnostics in DictionaryVector::toString are not part of values.
std::string valueString(const BaseVector *vector, vector_size_t row) {
  DecodedVector decoded(*vector);
  if (decoded.isNullAt(row))
    return "null";
  if (!vector->type()->isRow())
    return decoded.base()->toString(decoded.index(row));
  auto *map = decoded.base()->as<RowVector>();
  std::string result = "{";
  for (const auto &child : map->children()) {
    auto value = valueString(child.get(), decoded.index(row));
    result += std::to_string(value.size()) + ":" + value;
  }
  return result + "}";
}
VectorPtr wrapped(VectorPtr value, int encoding, memory::MemoryPool *pool) {
  if (encoding == 2)
    return BaseVector::wrapInConstant(5, 3, value);
  if (encoding != 1)
    return value;
  const std::vector<vector_size_t> mapping{3, 0, 2, 1, 3};
  auto indices = AlignedBuffer::allocate<vector_size_t>(mapping.size(), pool);
  std::memcpy(indices->asMutable<vector_size_t>(), mapping.data(),
              mapping.size() * sizeof(vector_size_t));
  return BaseVector::wrapInDictionary(nullptr, indices, mapping.size(), value);
}
void retag(const VectorPtr &value, const TypePtr &type) {
  value->setType(type);
  if (!type->isRow())
    return;
  DecodedVector decoded(*value);
  auto *row = const_cast<RowVector *>(decoded.base()->as<RowVector>());
  row->setType(type);
  for (size_t i = 0; i < row->childrenSize(); ++i)
    retag(row->childAt(i), type->childAt(i));
}
RowVectorPtr input(memory::MemoryPool *pool, int encoding,
                   bool optional = false) {
  auto nestedType =
      ROW({"s", "binary", "ts", "t", "d"},
          {VARCHAR(), VARBINARY(), TIMESTAMP(), TIME(), DECIMAL(30, 2)});
  auto mapType = ROW({"n", "inner"}, {BIGINT(), nestedType});
  auto map = BaseVector::create<RowVector>(mapType, 4, pool);
  auto *inner = map->childAt(1)->as<RowVector>();
  for (vector_size_t row = 0; row < 4; ++row) {
    map->childAt(0)->as<FlatVector<int64_t>>()->set(row, 10 + row);
    std::string text =
        std::string(row == 1 ? 0 : 8192 + row, 'a' + row) + "雪🚀";
    text.push_back('\0');
    inner->childAt(0)->as<FlatVector<StringView>>()->set(row, StringView(text));
    const std::string bytes =
        std::string(1000 + row, '\xff') + std::string("\0\x80", 2);
    inner->childAt(1)->as<FlatVector<StringView>>()->set(row,
                                                         StringView(bytes));
    inner->childAt(2)->as<FlatVector<Timestamp>>()->set(
        row, Timestamp(-1, 999000000));
    inner->childAt(3)->as<FlatVector<int64_t>>()->set(row, 86399999 - row);
    inner->childAt(4)->as<FlatVector<int128_t>>()->set(
        row, (int128_t(1) << 85) + row);
  }
  map->childAt(0)->setNull(2, true);
  inner->childAt(0)->setNull(2, true);
  inner->childAt(1)->setNull(1, true);
  if (encoding >= 3) {
    // Equal-length nested dictionary/constant, independent of the parent map.
    auto nested = wrapped(map->childAt(1), encoding == 3 ? 1 : 2, pool);
    nested->resize(4);
    map->childAt(1) = std::move(nested);
  }
  VectorPtr column = wrapped(map, encoding < 3 ? encoding : 0, pool);
  auto type =
      ROW({"m", "literal.dot", "empty"}, {mapType, VARCHAR(), ROW({}, {})});
  auto row = BaseVector::create<RowVector>(type, column->size(), pool);
  row->childAt(0) = std::move(column);
  for (vector_size_t i = 0; i < row->size(); ++i)
    row->childAt(1)->as<FlatVector<StringView>>()->set(
        i, StringView("quoted-root-name"));
  if (optional) {
    auto fields = fieldsFromType(type);
    fields[0]["optional"] = true;
    fields[0]["children"][1]["optional"] = true;
    fields[2]["optional"] = true;
    retag(row, columnarType(fields));
  }
  return row;
}
void checkTree(const BaseVector *vector, memory::MemoryPool *pool,
               vector_size_t rows, std::vector<const void *> &addresses) {
  VELOX_CHECK(vector->pool() == pool);
  VELOX_CHECK_EQ(vector->size(), rows);
  if (vector->type()->isRow()) {
    for (const auto &child : vector->as<RowVector>()->children())
      checkTree(child.get(), pool, rows, addresses);
  } else
    addresses.push_back(vector->values() ? vector->values()->as<char>()
                                         : nullptr);
}
void caseTest(int encoding, bool selected, int route, bool optional) {
  auto receiving = memory::memoryManager()->addRootPool();
  auto pool = receiving->addLeafChild("receiving");
  auto inbox = std::make_shared<ReceiverInbox>();
  inbox->senders({7});
  std::vector<std::string> expected;
  std::vector<const void *> originalValues;
  RowVectorPtr result;
  {
    auto sending = memory::memoryManager()->addRootPool();
    auto sourcePool = sending->addLeafChild("source");
    auto source = input(sourcePool.get(), encoding, optional);
    auto fields = fieldsFromType(source->rowType());
    std::vector<vector_size_t> rows{3, 0, 3, 2, 1};
    const vector_size_t count = selected ? rows.size() : source->size();
    for (vector_size_t i = 0; i < count; ++i)
      expected.push_back(valueString(source.get(), selected ? rows[i] : i));
    if (route == 0) {
      auto wire = encodeBatch(source, fields, selected ? &rows : nullptr);
      auto header = batchHeaderFromDefinition(batchDefinition(wire));
      VELOX_CHECK(header["fields"][0]["minor"] == "MAP");
      VELOX_CHECK_EQ(header["fields"][0]["optional"].asBool(), optional);
      result = decodeBatch({header, wire.data, {}}, pool.get());
      std::vector<drill::nativeexec::BufferView> views;
      size_t offset = 0;
      for (const auto &f : header["fields"])
        for (const auto &n : f["lengths"]) {
          views.push_back({wire.data.data() + offset, size_t(n.asInt())});
          offset += n.asInt();
        }
      auto cached = decodeBatchBuffers(columnarLayout(fields), result->size(),
                                       views, pool.get());
      for (vector_size_t i = 0; i < result->size(); ++i)
        VELOX_CHECK(result->equalValueAt(cached.get(), i, i));
      std::fill(wire.data.begin(), wire.data.end(), 0);
    } else if (route == 1) {
      result = copyLocalBatch(source, source->rowType(), fields, pool.get(),
                              selected ? &rows : nullptr);
    } else if (route == 2) {
      auto snapshot = selected ? copyLocalBatch(source, source->rowType(),
                                                fields, sourcePool.get(), &rows)
                               : source;
      inbox->pushLocal(snapshot, fields, 7);
    } else {
      auto senderPool = sending->addLeafChild("sender");
      auto buffer =
          route == 4 ? inbox->senderBuffer(source->rowType(), 65535)
                     : std::make_shared<SenderBuffer>(source->rowType(),
                                                      senderPool.get(), 65535);
      VELOX_CHECK(buffer->append(source, selected ? &rows : nullptr).empty());
      auto snapshot = buffer->flush();
      // Snapshot children still have capacity; capture leaf addresses only.
      auto capture = [&](auto &&self, const BaseVector *v) -> void {
        if (v->type()->isRow())
          for (const auto &c : v->as<RowVector>()->children())
            self(self, c.get());
        else
          originalValues.push_back(v->values() ? v->values()->as<char>()
                                               : nullptr);
      };
      capture(capture, snapshot.get());
      VELOX_CHECK(inbox->pushOwnedLocal(std::move(snapshot), fields, 7));
    }
  }
  if (route >= 2) {
    inbox->end(7);
    auto reader = inbox->factory()(pool.get());
    ContinueFuture future;
    auto next = reader->next(future);
    VELOX_CHECK(next && *next);
    result = std::move(*next);
    VELOX_CHECK(!reader->next(future));
    reader.reset();
    inbox.reset();
  }
  std::vector<const void *> values;
  auto outputFields = fieldsFromType(result->rowType());
  VELOX_CHECK_EQ(outputFields[0]["optional"].asBool(), optional);
  VELOX_CHECK_EQ(outputFields[0]["children"][1]["optional"].asBool(), optional);
  VELOX_CHECK_EQ(outputFields[2]["optional"].asBool(), optional);
  checkTree(result.get(), pool.get(), expected.size(), values);
  if (route >= 3)
    VELOX_CHECK(values == originalValues,
                "Nested owned handoff copied leaf values");
  for (vector_size_t row = 0; row < result->size(); ++row)
    VELOX_CHECK(valueString(result.get(), row) == expected[row],
                "MAP value mismatch: encoding={} selected={} route={} row={}",
                encoding, selected, route, row);
  auto retained =
      result->childAt(0)->as<RowVector>()->childAt(1)->as<RowVector>()->childAt(
          0);
  result.reset();
  VELOX_CHECK(retained->pool() == pool.get());
  retained.reset();
  VELOX_CHECK_EQ(pool->usedBytes(), 0);
}
void rejectedAndShared(memory::MemoryPool *pool) {
  auto row = input(pool, 0);
  auto fields = fieldsFromType(row->rowType());
  row->childAt(0)->setNull(1, true);
  bool rejected = false;
  try {
    encodeBatch(row, fields);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected);
  row->childAt(0)->setNull(1, false);
  auto malformed = encodeBatch(row, fields);
  malformed.header["fields"][0]["children"][1]["children"][0]["lengths"][0] =
      123;
  rejected = false;
  try {
    decodeBatch(malformed, pool);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected,
              "Nested descriptor disagrees with flattened parent lengths");
  auto inbox = std::make_shared<ReceiverInbox>();
  inbox->senders({7});
  auto buffer = inbox->senderBuffer(row->rowType(), 32);
  VELOX_CHECK(buffer->append(row).empty());
  auto snapshot = buffer->flush();
  auto held = snapshot->childAt(0)
                  ->as<RowVector>()
                  ->childAt(1)
                  ->as<RowVector>()
                  ->childAt(0);
  const auto originalPool = held->pool();
  VELOX_CHECK(!inbox->pushOwnedLocal(std::move(snapshot), fields, 7),
              "Shared nested child must copy");
  inbox->end(7);
  auto reader = inbox->factory()(pool);
  ContinueFuture wait;
  auto next = reader->next(wait);
  VELOX_CHECK(next && *next);
  VELOX_CHECK(held->pool() == originalPool,
              "Nested dequeue changed a retained source pool");
  auto strings = (*next)
                     ->childAt(0)
                     ->as<RowVector>()
                     ->childAt(1)
                     ->as<RowVector>()
                     ->childAt(0)
                     ->as<FlatVector<StringView>>();
  strings->set(0, StringView("changed"));
  VELOX_CHECK(held->as<FlatVector<StringView>>()->valueAt(0) !=
              StringView("changed"));
  auto schema = batchHeaderFromDefinition(schemaDefinition(fields));
  VELOX_CHECK_EQ(schema["rows"].asInt(), 0);
  VELOX_CHECK(columnarType(schema["fields"])->equivalent(*row->type()));
  VELOX_CHECK(
      bindExpression("`m`.`inner`.`s`", row->rowType())->type()->isVarchar());
  VELOX_CHECK(
      bindExpression("`literal.dot`", row->rowType())->type()->isVarchar());
  auto optional = input(pool, 0, true);
  auto optionalFields = fieldsFromType(optional->rowType());
  optional->childAt(0)->setNull(1, true);
  for (bool local : {false, true}) {
    rejected = false;
    try {
      if (local)
        copyLocalBatch(optional, optional->rowType(), optionalFields, pool);
      else
        encodeBatch(optional, optionalFields);
    } catch (const VeloxException &) {
      rejected = true;
    }
    VELOX_CHECK(rejected, "OPTIONAL MAP must not invent a parent validity bit");
  }
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(4);
    for (bool optional : {false, true})
      for (int encoding = 0; encoding < 5; ++encoding)
        for (bool selected : {false, true})
          for (int route = 0; route < 5; ++route)
            caseTest(encoding, selected, route, optional);
    auto root = memory::memoryManager()->addRootPool();
    auto pool = root->addLeafChild("checks");
    rejectedAndShared(pool.get());
    VELOX_CHECK_EQ(pool->usedBytes(), 0);
    std::cout << "MAP recursive wire/cache, empty struct, nested "
                 "encodings/selection, owned copy/handoff, retained/shared "
                 "descendants and field paths passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
