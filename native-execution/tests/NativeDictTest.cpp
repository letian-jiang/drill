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
#include "plan/FragmentPlanConverter.h"
#include <atomic>
#include <cstring>
#include <iostream>
#include <velox/expression/Expr.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class Input final : public BatchSource {
public:
  Input(RowVectorPtr data, std::shared_ptr<std::atomic<bool>> used)
      : data_(data), used_(used) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    if (used_->exchange(true))
      return std::nullopt;
    return data_;
  }
  void cancel() override {}

private:
  RowVectorPtr data_;
  std::shared_ptr<std::atomic<bool>> used_;
};
folly::dynamic fields() {
  return folly::parseJson(R"([
    {"name":"d","minor":"DICT","optional":false,"children":[
      {"name":"key","minor":"BIGINT","optional":false},
      {"name":"value","minor":"VARCHAR","optional":true}]},
    {"name":"r","minor":"DICT","optional":false,"repeated":true,
     "element":{"name":"$data$","minor":"DICT","optional":false,"children":[
       {"name":"key","minor":"BIGINT","optional":false},
       {"name":"value","minor":"VARCHAR","optional":true}]}},
    {"name":"m","minor":"DICT","optional":true,"children":[
      {"name":"key","minor":"VARCHAR","optional":false},
      {"name":"value","minor":"MAP","optional":false,"children":[
        {"name":"n","minor":"BIGINT","optional":true},
        {"name":"list","minor":"BIGINT","optional":false,"repeated":true,
         "element":{"name":"$data$","minor":"BIGINT","optional":false}}]}]}
  ])");
}
VectorPtr encoded(VectorPtr value, int encoding, memory::MemoryPool *pool) {
  if (!encoding)
    return value;
  if (encoding == 2)
    return BaseVector::wrapInConstant(5, 0, value);
  const vector_size_t mapping[]{3, 2, 0, 1, 2};
  auto indices = AlignedBuffer::allocate<vector_size_t>(5, pool);
  std::memcpy(indices->asMutable<vector_size_t>(), mapping, sizeof(mapping));
  return BaseVector::wrapInDictionary(nullptr, indices, 5, value);
}
MapVectorPtr map(TypePtr type, VectorPtr keys, VectorPtr values,
                 memory::MemoryPool *pool, std::vector<vector_size_t> offsets,
                 std::vector<vector_size_t> sizes) {
  auto o = AlignedBuffer::allocate<vector_size_t>(offsets.size(), pool);
  auto s = AlignedBuffer::allocate<vector_size_t>(sizes.size(), pool);
  std::memcpy(o->asMutable<vector_size_t>(), offsets.data(),
              offsets.size() * 4);
  std::memcpy(s->asMutable<vector_size_t>(), sizes.data(), sizes.size() * 4);
  return std::make_shared<MapVector>(pool, type, nullptr, offsets.size(), o, s,
                                     keys, values);
}
RowVectorPtr input(memory::MemoryPool *pool, int encoding) {
  auto type = columnarType(fields());
  auto keys = BaseVector::create(BIGINT(), 5, pool);
  const int64_t numbers[]{7, 7, 9, 42, 1};
  for (int i = 0; i < 5; ++i)
    keys->as<FlatVector<int64_t>>()->set(i, numbers[i]);
  auto values = BaseVector::create(VARCHAR(), 5, pool);
  const std::string strings[]{"first", "last", "",
                              std::string(4099, 'x') + "雪🚀",
                              std::string("a\0b", 3)};
  for (int i = 0; i < 5; ++i)
    values->as<FlatVector<StringView>>()->set(i, StringView(strings[i]));
  values->setNull(2, true);
  auto d =
      map(type->childAt(0), keys, values, pool, {0, 2, 2, 4}, {2, 0, 2, 1});
  // Repeated DICT contains independent inner dictionaries and two offsets
  // levels.
  auto inner = map(type->childAt(1)->childAt(0), keys, values, pool,
                   {0, 2, 2, 4}, {2, 0, 2, 1});
  auto ao = AlignedBuffer::allocate<vector_size_t>(4, pool),
       as = AlignedBuffer::allocate<vector_size_t>(4, pool);
  const vector_size_t offsets[]{0, 2, 2, 3}, sizes[]{2, 0, 1, 1};
  std::memcpy(ao->asMutable<vector_size_t>(), offsets, sizeof(offsets));
  std::memcpy(as->asMutable<vector_size_t>(), sizes, sizeof(sizes));
  auto repeated = std::make_shared<ArrayVector>(pool, type->childAt(1), nullptr,
                                                4, ao, as, inner);
  auto skeys = BaseVector::create(VARCHAR(), 4, pool);
  for (int i = 0; i < 4; ++i)
    skeys->as<FlatVector<StringView>>()->set(i, StringView("k"));
  auto members =
      BaseVector::create<RowVector>(type->childAt(2)->childAt(1), 4, pool);
  for (int i = 0; i < 4; ++i)
    members->childAt(0)->as<FlatVector<int64_t>>()->set(i, i * 11);
  auto list = members->childAt(1)->as<ArrayVector>();
  for (int i = 0; i < 4; ++i)
    list->setOffsetAndSize(i, 0, 0);
  auto complex =
      map(type->childAt(2), skeys, members, pool, {0, 1, 2, 3}, {1, 1, 1, 1});
  std::vector<VectorPtr> columns{encoded(d, encoding, pool),
                                 encoded(repeated, encoding, pool),
                                 encoded(complex, encoding, pool)};
  return std::make_shared<RowVector>(pool, type, nullptr, encoding ? 5 : 4,
                                     columns);
}
void check(const RowVectorPtr &rows, int encoding,
           const std::vector<vector_size_t> *selected) {
  DecodedVector parent(*rows->childAt(0));
  auto *d = parent.base()->as<MapVector>();
  DecodedVector keys(*d->mapKeys()), values(*d->mapValues());
  DecodedVector repeated(*rows->childAt(1));
  auto *arrays = repeated.base()->as<ArrayVector>();
  DecodedVector inner(*arrays->elements());
  auto *dicts = inner.base()->as<MapVector>();
  DecodedVector innerKeys(*dicts->mapKeys()), innerValues(*dicts->mapValues());
  const vector_size_t mapping[]{3, 2, 0, 1, 2};
  for (int i = 0; i < rows->size(); ++i) {
    int original = selected ? (*selected)[i] : i;
    original = encoding == 2 ? 0 : encoding == 1 ? mapping[original] : original;
    const int sizes[]{2, 0, 2, 1};
    VELOX_CHECK_EQ(d->sizeAt(parent.index(i)), sizes[original]);
    auto first = d->offsetAt(parent.index(i));
    if (original == 0) {
      VELOX_CHECK_EQ(keys.valueAt<int64_t>(first), 7);
      VELOX_CHECK_EQ(keys.valueAt<int64_t>(first + 1), 7);
      VELOX_CHECK_EQ(values.valueAt<StringView>(first).str(), "first");
      VELOX_CHECK_EQ(values.valueAt<StringView>(first + 1).str(), "last");
    }
    if (original == 2) {
      VELOX_CHECK_EQ(keys.valueAt<int64_t>(first), 9);
      VELOX_CHECK(values.isNullAt(first));
      VELOX_CHECK_EQ(values.valueAt<StringView>(first + 1).str(),
                     std::string(4099, 'x') + "雪🚀");
    }
    if (original == 3)
      VELOX_CHECK_EQ(values.valueAt<StringView>(first).str(),
                     std::string("a\0b", 3));
    const int arraySizes[]{2, 0, 1, 1};
    const int arrayStarts[]{0, 2, 2, 3};
    auto arrayIndex = repeated.index(i);
    VELOX_CHECK_EQ(arrays->sizeAt(arrayIndex), arraySizes[original]);
    for (int item = 0; item < arraySizes[original]; ++item) {
      auto dictIndex = inner.index(arrays->offsetAt(arrayIndex) + item);
      int expectedDict = arrayStarts[original] + item;
      VELOX_CHECK_EQ(dicts->sizeAt(dictIndex), sizes[expectedDict]);
      auto entry = dicts->offsetAt(dictIndex);
      if (expectedDict == 0) {
        VELOX_CHECK_EQ(innerKeys.valueAt<int64_t>(entry), 7);
        VELOX_CHECK_EQ(innerKeys.valueAt<int64_t>(entry + 1), 7);
        VELOX_CHECK_EQ(innerValues.valueAt<StringView>(entry).str(), "first");
        VELOX_CHECK_EQ(innerValues.valueAt<StringView>(entry + 1).str(),
                       "last");
      } else if (expectedDict == 2) {
        VELOX_CHECK(innerValues.isNullAt(entry));
        VELOX_CHECK_EQ(innerKeys.valueAt<int64_t>(entry + 1), 42);
      } else if (expectedDict == 3) {
        VELOX_CHECK_EQ(innerValues.valueAt<StringView>(entry).str(),
                       std::string("a\0b", 3));
      }
    }
    DecodedVector complex(*rows->childAt(2));
    auto *map = complex.base()->as<MapVector>();
    auto *members = map->mapValues()->as<RowVector>();
    DecodedVector n(*members->childAt(0));
    VELOX_CHECK_EQ(n.valueAt<int64_t>(map->offsetAt(complex.index(i))),
                   original * 11);
  }
}
void codec(int encoding, bool selection, int route) {
  auto receiving = memory::memoryManager()->addRootPool();
  auto pool = receiving->addLeafChild("receive");
  auto inbox = std::make_shared<ReceiverInbox>();
  inbox->senders({7});
  RowVectorPtr result;
  const std::vector<vector_size_t> selected{3, 0, 3, 2, 1};
  {
    auto root = memory::memoryManager()->addRootPool();
    auto sourcePool = root->addLeafChild("source");
    auto source = input(sourcePool.get(), encoding);
    auto schema = fieldsFromType(source->rowType());
    if (route == 0) {
      auto wire = encodeBatch(source, schema, selection ? &selected : nullptr);
      auto header = batchHeaderFromDefinition(batchDefinition(wire));
      result = decodeBatch({header, wire.data, {}}, pool.get());
      std::vector<drill::nativeexec::BufferView> views;
      size_t offset = 0;
      for (const auto &f : header["fields"])
        for (const auto &n : f["lengths"]) {
          views.push_back({wire.data.data() + offset, size_t(n.asInt())});
          offset += n.asInt();
        }
      auto cached = decodeBatchBuffers(columnarLayout(schema), result->size(),
                                       views, pool.get());
      check(cached, encoding, selection ? &selected : nullptr);
      std::fill(wire.data.begin(), wire.data.end(), 0);
    } else if (route == 1)
      result = copyLocalBatch(source, source->rowType(), schema, pool.get(),
                              selection ? &selected : nullptr);
    else if (route == 2)
      inbox->pushLocal(selection
                           ? copyLocalBatch(source, source->rowType(), schema,
                                            sourcePool.get(), &selected)
                           : source,
                       schema, 7);
    else {
      auto buffer = inbox->senderBuffer(source->rowType(), 32);
      VELOX_CHECK(
          buffer->append(source, selection ? &selected : nullptr).empty());
      auto snapshot = buffer->flush();
      VELOX_CHECK(inbox->pushOwnedLocal(std::move(snapshot), schema, 7));
    }
  }
  if (route >= 2) {
    inbox->end(7);
    auto reader = inbox->factory()(pool.get());
    ContinueFuture wait;
    auto next = reader->next(wait);
    VELOX_CHECK(next && *next);
    result = std::move(*next);
    VELOX_CHECK(!reader->next(wait));
    reader.reset();
    inbox.reset();
  }
  check(result, encoding, selection ? &selected : nullptr);
  auto retained = result->childAt(0)->as<MapVector>()->mapValues();
  result.reset();
  {
    DecodedVector values(*retained);
    VELOX_CHECK(retained->pool() == pool.get());
    for (int row = 0; row < retained->size(); ++row)
      if (!values.isNullAt(row))
        VELOX_CHECK(values.valueAt<StringView>(row).size() > 0);
  }
  retained.reset();
  inbox.reset();
  VELOX_CHECK_EQ(pool->usedBytes(), 0);
}
void wideDict(memory::MemoryPool *pool) {
  constexpr vector_size_t count = 70003;
  auto type = columnarType(folly::dynamic::array(fields()[0]));
  auto keys = BaseVector::create(BIGINT(), count, pool);
  auto values = BaseVector::create(VARCHAR(), count, pool);
  for (int row = 0; row < count; ++row) {
    keys->as<FlatVector<int64_t>>()->set(row, row * 3);
    auto text = std::to_string(row) + ":雪🚀";
    values->as<FlatVector<StringView>>()->set(row, StringView(text));
    values->setNull(row, row % 17 == 0);
  }
  auto dict = map(type->childAt(0), keys, values, pool, {0, count}, {count, 0});
  auto source = std::make_shared<RowVector>(pool, type, nullptr, 2,
                                            std::vector<VectorPtr>{dict});
  auto wire = encodeBatch(source, fieldsFromType(type));
  auto header = batchHeaderFromDefinition(batchDefinition(wire));
  VELOX_CHECK_EQ(header["fields"][0]["entryCount"].asInt(), count);
  auto rows = decodeBatch({header, wire.data, {}}, pool);
  auto *result = rows->childAt(0)->as<MapVector>();
  VELOX_CHECK_EQ(result->sizeAt(0), count);
  VELOX_CHECK_EQ(result->sizeAt(1), 0);
  DecodedVector actualKeys(*result->mapKeys()),
      actualValues(*result->mapValues());
  for (int row = 0; row < count; ++row) {
    VELOX_CHECK_EQ(actualKeys.valueAt<int64_t>(row), row * 3);
    VELOX_CHECK_EQ(actualValues.isNullAt(row), row % 17 == 0);
    if (row % 17)
      VELOX_CHECK_EQ(actualValues.valueAt<StringView>(row).str(),
                     std::to_string(row) + ":雪🚀");
  }
}
void rejection(memory::MemoryPool *pool) {
  auto data = input(pool, 0);
  auto wire = encodeBatch(data, fields());
  wire.header["fields"][0]["children"][0]["lengths"][0] = 1;
  bool rejected = false;
  try {
    decodeBatch(wire, pool);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected);
  auto schema = batchHeaderFromDefinition(schemaDefinition(fields()));
  VELOX_CHECK(columnarType(schema["fields"])->equivalent(*data->type()));
}
void lookup(VeloxRuntime &runtime) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("lookup");
  auto data = input(pool.get(), 0);
  auto used = std::make_shared<std::atomic<bool>>(false);
  int rows = 0;
  FragmentPlanConverter converter(
      pool.get(),
      [&](const folly::dynamic &) {
        return SourceBinding{data->rowType(), SourceKind::Receiver,
                             [data, used](memory::MemoryPool *) {
                               return std::make_shared<Input>(data, used);
                             }};
      },
      [&](const folly::dynamic &, const RowTypePtr &) {
        return SinkFactory([&](memory::MemoryPool *) {
          return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
            if (!batch)
              return {};
            DecodedVector seven(*batch->childAt(0)),
                missing(*batch->childAt(1)), member(*batch->childAt(2));
            for (int i = 0; i < batch->size(); ++i, ++rows) {
              VELOX_CHECK(rows < 4 && missing.isNullAt(i));
              VELOX_CHECK(rows == 0
                              ? seven.valueAt<StringView>(i).str() == "last"
                              : seven.isNullAt(i));
              VELOX_CHECK_EQ(member.valueAt<int64_t>(i), rows * 11);
            }
            return {};
          });
        });
      });
  auto plan = converter.convert(
      folly::parseJson(R"({"pop":"single-sender","@id":0,"child":{
    "pop":"project","@id":1,"exprs":[{"ref":"`seven`","expr":"`d`[7]"},
      {"ref":"`missing`","expr":"`d`[999]"},{"ref":"`member`","expr":"`m`.`k`.`n`"}],
    "child":{"pop":"unordered-receiver","@id":2}}})"));
  auto task = runtime.task("dict-lookup", plan, root);
  auto done = task->taskCompletionFuture();
  task->start(4);
  std::move(done).get();
  auto error = task->error();
  auto deletion = task->taskDeletionFuture();
  task.reset();
  std::move(deletion).get();
  if (error)
    std::rethrow_exception(error);
  VELOX_CHECK_EQ(rows, 4);
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(4);
    for (int encoding = 0; encoding < 3; ++encoding)
      for (bool selected : {false, true})
        for (int route = 0; route < 4; ++route)
          codec(encoding, selected, route);
    lookup(runtime);
    auto root = memory::memoryManager()->addRootPool();
    auto pool = root->addLeafChild("checks");
    rejection(pool.get());
    wideDict(pool.get());
    VELOX_CHECK_EQ(pool->usedBytes(), 0);
    std::cout
        << "DICT wire/cache/local/owned handoff, duplicate keys, NULL values, "
           "repeated DICT, nested ROW/array, selection and metadata passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
