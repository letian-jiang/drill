// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file for copyright ownership.
// Licensed under the Apache License, Version 2.0. You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0 .
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillBatchMetadata.h"
#include "exchange/ReceiverInbox.h"
#include "exchange/SenderBuffer.h"
#include <bit>
#include <cstring>
#include <iostream>
#include <limits>
#include <type_traits>
#include <velox/common/memory/Memory.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

using namespace drill::nativeexec;

namespace {
RowVectorPtr decodeAndCheckCachedLayout(const WireBatch &wire, memory::MemoryPool *pool) {
  auto decoded = decodeBatch(wire, pool);
  auto layout = columnarLayout(wire.header["fields"]);
  std::vector<drill::nativeexec::BufferView> views;
  size_t pos = 0;
  for (const auto &field : wire.header["fields"])
    for (const auto &length : field["lengths"]) {
      views.push_back({wire.data.data() + pos, static_cast<size_t>(length.asInt())});
      pos += length.asInt();
    }
  auto cached = decodeBatchBuffers(layout, wire.header["rows"].asInt(), views, pool);
  for (vector_size_t row = 0; row < decoded->size(); ++row)
    VELOX_CHECK(decoded->equalValueAt(cached.get(), row, row));
  return cached;
}
template <typename T>
void roundTrip(const VectorPtr &input, const char *minor, bool optional,
               memory::MemoryPool *pool,
               const std::vector<vector_size_t> *selected = nullptr) {
  auto row =
      std::make_shared<RowVector>(pool, ROW({"v"}, {input->type()}), nullptr,
                                  input->size(), std::vector<VectorPtr>{input});
  auto schema = folly::dynamic::array(folly::dynamic::object("name", "v")(
      "minor", minor)("optional", optional));
  auto wire = encodeBatch(row, schema, selected);
  auto result = decodeAndCheckCachedLayout(wire, pool)->childAt(0);
  DecodedVector expected(*input), actual(*result);
  VELOX_CHECK_EQ(result->size(),
                 selected ? vector_size_t(selected->size()) : input->size());
  for (vector_size_t r = 0; r < result->size(); ++r) {
    auto source = selected ? (*selected)[r] : r;
    VELOX_CHECK_EQ(actual.isNullAt(r), expected.isNullAt(source));
    if (actual.isNullAt(r)) {
      const auto wireWidth = input->type()->equivalent(*TIME()) ? 4 : sizeof(T);
      for (size_t b = 0; b < wireWidth; ++b)
        VELOX_CHECK_EQ(wire.data[result->size() + r * wireWidth + b], 0);
    } else {
      T a = actual.valueAt<T>(r), e = expected.valueAt<T>(source);
      // Compare bits, including signed zero and noncanonical NaN payloads.
      VELOX_CHECK_EQ(std::memcmp(&a, &e, sizeof(T)), 0);
    }
  }
}

template <typename T>
void checkType(TypePtr type, const char *minor, memory::MemoryPool *pool) {
  auto base = BaseVector::create(type, 5, pool);
  auto flat = base->as<FlatVector<T>>();
  for (vector_size_t r = 0; r < 5; ++r)
    flat->set(r, T(r * 31 - 47));
  if constexpr (std::is_floating_point_v<T>) {
    flat->set(0, T(-0.0));
    flat->set(1, T(0.0));
    if constexpr (sizeof(T) == 8)
      flat->set(4, std::bit_cast<double>(uint64_t(0x7ff8000000001234)));
    else
      flat->set(4, std::bit_cast<float>(uint32_t(0x7fc01234)));
  } else {
    flat->set(0, type->equivalent(*TIME()) ? INT32_MIN : std::numeric_limits<T>::min());
    flat->set(4, type->equivalent(*TIME()) ? INT32_MAX : std::numeric_limits<T>::max());
  }
  // Flat/no-null bulk copy, selected gather, and constant mappings.
  roundTrip<T>(base, minor, false, pool);
  std::vector<vector_size_t> selected{4, 0, 4, 1};
  roundTrip<T>(base, minor, true, pool, &selected);
  auto constant = BaseVector::wrapInConstant(7, 4, base);
  roundTrip<T>(constant, minor, false, pool);
  flat->setNull(2, true);
  roundTrip<T>(base, minor, true, pool);
  auto indices = AlignedBuffer::allocate<vector_size_t>(7, pool);
  std::vector<vector_size_t> mapping{4, 1, 0, 3, 4, 2, 1};
  std::memcpy(indices->asMutable<vector_size_t>(), mapping.data(),
              mapping.size() * sizeof(vector_size_t));
  auto nulls = AlignedBuffer::allocate<uint64_t>(1, pool, bits::kNotNull64);
  bits::setNull(nulls->asMutable<uint64_t>(), 3, true);
  auto dictionary = BaseVector::wrapInDictionary(nulls, indices, 7, base);
  roundTrip<T>(dictionary, minor, true, pool);
  std::vector<vector_size_t> subset{6, 4, 3, 1, 0, 5};
  roundTrip<T>(dictionary, minor, true, pool, &subset);
  roundTrip<T>(BaseVector::wrapInConstant(7, 2, base), minor, true, pool);
  bool rejected = false;
  try {
    roundTrip<T>(dictionary, minor, false, pool);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected, "Required columns must reject NULL values");
}

void localCopy(int encoding, bool selected, bool exchange = false,
               int owned = 0) {
  auto receiver =
      memory::memoryManager()->addRootPool("receiver-copy", 64 << 20);
  auto destination = receiver->addLeafChild("vectors");
  RowVectorPtr copy;
  auto inbox = exchange ? std::make_shared<ReceiverInbox>() : nullptr;
  std::vector<std::vector<std::optional<std::string>>> expected;
  std::weak_ptr<memory::MemoryPool> producer;
  std::vector<const void *> ownedValues;
  {
    auto root = memory::memoryManager()->addRootPool("producer-copy", 64 << 20);
    auto pool = root->addLeafChild("vectors");
    producer = pool;
    folly::dynamic fields = folly::dynamic::array;
    std::vector<std::string> names;
    std::vector<TypePtr> types{INTEGER(),      BIGINT(),       REAL(),
                               DOUBLE(),       BOOLEAN(),      DATE(),
                               DECIMAL(12, 2), DECIMAL(30, 8), VARCHAR(),
                               TIMESTAMP(), VARBINARY(), TIME()};
    const std::vector<std::string> minors{
        "INT",  "BIGINT",     "FLOAT4",     "FLOAT8", "BIT",
        "DATE", "VARDECIMAL", "VARDECIMAL", "VARCHAR", "TIMESTAMP", "VARBINARY", "TIME"};
    std::vector<VectorPtr> children;
    for (size_t c = 0; c < types.size(); ++c) {
      names.push_back("v" + std::to_string(c));
      fields.push_back(folly::dynamic::object("name", names.back())(
          "minor", minors[c])("optional", true)("precision", c == 6 ? 12 : 30)(
          "scale", c == 6 ? 2 : 8));
      auto v = BaseVector::create(types[c], 4, pool.get());
      for (vector_size_t r = 0; r < 4; ++r) {
        if (c == 0 || c == 5)
          v->as<FlatVector<int32_t>>()->set(r, r * 101 - 9);
        else if (c == 1 || c == 6)
          v->as<FlatVector<int64_t>>()->set(r, (int64_t(1) << 35) + r);
        else if (c == 2)
          v->as<FlatVector<float>>()->set(r, float(r) / 3);
        else if (c == 3)
          v->as<FlatVector<double>>()->set(r, double(r) / 3);
        else if (c == 4)
          v->as<FlatVector<bool>>()->set(r, r % 2);
        else if (c == 7)
          v->as<FlatVector<int128_t>>()->set(r, (int128_t(1) << 85) + r);
        else if (c == 9)
          v->as<FlatVector<Timestamp>>()->set(r, Timestamp(-1, 987000000 + r * 1000000));
        else if (c == 11)
          v->as<FlatVector<int64_t>>()->set(r, 86399999 - r);
        else {
          // Longer than StringView's inline storage, including UTF-8 and NUL.
          std::string text = std::string(800 + r, 'a' + r) + "中文";
          text.push_back('\0');
          if (c == 10) text.append("\xff\x80", 2);
          v->as<FlatVector<StringView>>()->set(r, StringView(text));
        }
      }
      v->setNull(2, true);
      if (encoding == 1) {
        auto indices = AlignedBuffer::allocate<vector_size_t>(5, pool.get());
        std::vector<vector_size_t> mapping{3, 0, 2, 1, 3};
        std::memcpy(indices->asMutable<vector_size_t>(), mapping.data(),
                    mapping.size() * sizeof(vector_size_t));
        auto nulls =
            AlignedBuffer::allocate<uint64_t>(1, pool.get(), bits::kNotNull64);
        bits::setNull(nulls->asMutable<uint64_t>(), 1, true);
        v = BaseVector::wrapInDictionary(nulls, indices, 5, v);
      } else if (encoding == 2) {
        v = BaseVector::wrapInConstant(5, c % 2 == 0 ? 2 : 3, v);
      }
      children.push_back(v);
    }
    auto source =
        std::make_shared<RowVector>(pool.get(), ROW(names, types), nullptr,
                                    encoding == 0 ? 4 : 5, children);
    std::vector<vector_size_t> rows{3, 0, 3, 2, 1};
    const auto count = selected ? rows.size() : source->size();
    for (size_t r = 0; r < count; ++r) {
      std::vector<std::optional<std::string>> values;
      for (const auto &v : children) {
        DecodedVector decoded(*v);
        auto inputRow = selected ? rows[r] : vector_size_t(r);
        if (decoded.isNullAt(inputRow))
          values.push_back(std::nullopt);
        else
          values.push_back(decoded.base()->toString(decoded.index(inputRow)));
      }
      expected.push_back(std::move(values));
    }
    if (exchange) {
      fields[0]["name"] = "receiver_0";
      inbox->senders({0});
      if (owned) {
        // Actual sender coalescing flattens every input encoding and owns its
        // strings. Snapshot -> inbox -> receiver must preserve value buffers.
        auto senderPool = root->addLeafChild("sender-buffer");
        auto buffer = owned == 2 ? inbox->senderBuffer(source->rowType(), 65535)
            : std::make_shared<SenderBuffer>(source->rowType(), senderPool.get(), 65535);
        VELOX_CHECK(buffer && buffer->append(source, selected ? &rows : nullptr).empty());
        auto batch = buffer->flush();
        for (const auto &child : batch->children())
          ownedValues.push_back(child->values() ? child->values()->as<uint8_t>() : nullptr);
        VELOX_CHECK(inbox->pushOwnedLocal(std::move(batch), fields, 0));
        VELOX_CHECK(!batch);
      } else {
        // Borrowed input must still copy dictionary/constant children.
        auto batch = selected ? copyLocalBatch(source, columnarType(fields), fields,
                                               pool.get(), &rows) : source;
        inbox->pushLocal(batch, fields, 0);
      }
      inbox->end(0);
    } else {
      copy = copyLocalBatch(source, columnarType(fields), fields,
                            destination.get(), selected ? &rows : nullptr);
    }
  }
  VELOX_CHECK(producer.expired(), "Copy retained the sending pool");
  if (exchange) {
    VELOX_CHECK_EQ(destination->usedBytes(), 0);
    auto reader = inbox->factory()(destination.get());
    ContinueFuture wait;
    auto batch = reader->next(wait);
    VELOX_CHECK(batch && *batch);
    copy = std::move(*batch);
    VELOX_CHECK_EQ(copy->rowType()->nameOf(0), "receiver_0");
    VELOX_CHECK(!reader->next(wait));
    reader->cancel();
    reader.reset();
    inbox.reset();
    // The receiving vector and any derived child must survive destruction
    // of both the sender's pool and the inbox's private pool.
  }
  VELOX_CHECK_GT(destination->usedBytes(), 0);
  for (vector_size_t r = 0; r < copy->size(); ++r)
    for (size_t c = 0; c < copy->childrenSize(); ++c) {
      VELOX_CHECK(copy->childAt(c)->pool() == destination.get());
      VELOX_CHECK_EQ(copy->childAt(c)->size(), copy->size());
      if (owned)
        VELOX_CHECK((copy->childAt(c)->values() ? copy->childAt(c)->values()->as<uint8_t>() : nullptr) == ownedValues[c],
                    "Owned enqueue/dequeue copied a values buffer");
      VELOX_CHECK_EQ(copy->childAt(c)->isNullAt(r),
                     !expected[r][c].has_value());
      if (expected[r][c])
        VELOX_CHECK_EQ(copy->childAt(c)->toString(r), *expected[r][c]);
    }
  auto retainedStrings = copy->childAt(8);
  copy.reset();
  for (vector_size_t r = 0; r < retainedStrings->size(); ++r) {
    VELOX_CHECK_EQ(retainedStrings->isNullAt(r), !expected[r][8].has_value());
    if (expected[r][8])
      VELOX_CHECK_EQ(retainedStrings->toString(r), *expected[r][8]);
  }
  retainedStrings.reset();
  VELOX_CHECK_EQ(destination->usedBytes(), 0);
}

void wireStringOwnership(memory::MemoryPool *pool) {
  const std::vector<std::string> values{
      "", "short", std::string(8192, 'x') + "中文" + std::string(1, '\0'),
      "last"};
  RowVectorPtr result;
  {
    auto input = BaseVector::create(VARCHAR(), values.size(), pool);
    for (size_t row = 0; row < values.size(); ++row)
      input->as<FlatVector<StringView>>()->set(row, StringView(values[row]));
    input->setNull(1, true);
    auto source = std::make_shared<RowVector>(pool, ROW({"v"}, {VARCHAR()}),
                                              nullptr, values.size(),
                                              std::vector<VectorPtr>{input});
    auto fields = folly::dynamic::array(folly::dynamic::object("name", "v")(
        "minor", "VARCHAR")("optional", true));
    auto wire = encodeBatch(source, fields);
    result = decodeAndCheckCachedLayout(wire, pool);
    // A Java scan and a network receiver can overwrite/free their input next.
    // Long StringViews must remain valid after the source payload is destroyed.
    std::fill(wire.data.begin(), wire.data.end(), '\0');
  }
  auto strings = result->childAt(0)->as<FlatVector<StringView>>();
  VELOX_CHECK(strings->isNullAt(1));
  for (size_t row : {size_t(0), size_t(2), size_t(3)})
    VELOX_CHECK_EQ(strings->valueAt(row).str(), values[row]);
}

void temporalBinaryWire(memory::MemoryPool *pool) {
  // Independent Drill payload, including both signed millisecond boundaries.
  std::vector<int64_t> millis{INT64_MIN, -1001, -1, 0, 1, 1001, INT64_MAX};
  std::string timestamps(reinterpret_cast<const char *>(millis.data()), millis.size() * 8);
  std::string valid(millis.size(), 1);
  valid[3] = 0;
  std::vector<std::string> blobs{"", std::string("\0\xff\x80", 3),
      std::string(8192, '\xff'), "", "ascii", std::string("a\0b", 3), "中文🚀"};
  std::vector<uint32_t> offsets{0};
  std::string data;
  for (const auto &blob : blobs) { data += blob; offsets.push_back(data.size()); }
  std::string index(reinterpret_cast<const char *>(offsets.data()), offsets.size() * 4);
  auto fields = folly::dynamic::array(
      folly::dynamic::object("name", "ts")("minor", "TIMESTAMP")("optional", true)(
          "lengths", folly::dynamic::array(valid.size(), timestamps.size())),
      folly::dynamic::object("name", "binary")("minor", "VARBINARY")("optional", true)(
          "lengths", folly::dynamic::array(valid.size(), index.size(), data.size())));
  WireBatch wire{folly::dynamic::object("rows", millis.size())("fields", fields),
                 valid + timestamps + valid + index + data, {}};
  auto output = decodeAndCheckCachedLayout(wire, pool);
  auto ts = output->childAt(0)->as<FlatVector<Timestamp>>();
  auto binary = output->childAt(1)->as<FlatVector<StringView>>();
  for (vector_size_t row = 0; row < output->size(); ++row) {
    VELOX_CHECK_EQ(ts->isNullAt(row), row == 3);
    VELOX_CHECK_EQ(binary->isNullAt(row), row == 3);
    if (row == 3) continue;
    VELOX_CHECK_EQ(ts->valueAt(row).toMillis(), millis[row]);
    VELOX_CHECK_EQ(binary->valueAt(row).str(), blobs[row]);
  }
  VELOX_CHECK_EQ(ts->valueAt(2).getSeconds(), -1);
  VELOX_CHECK_EQ(ts->valueAt(2).getNanos(), 999000000);
  // Original wire metadata must expose Drill's TIMESTAMP/VARBINARY codes.
  auto encoded = encodeBatch(output, fieldsFromType(output->rowType()));
  VELOX_CHECK_EQ(encoded.data, wire.data);
  auto header = batchHeaderFromDefinition(batchDefinition(encoded));
  VELOX_CHECK_EQ(header["fields"][0]["minor"].asString(), "TIMESTAMP");
  VELOX_CHECK_EQ(header["fields"][1]["minor"].asString(), "VARBINARY");
  std::vector<vector_size_t> selected{6, 1, 3, 0, 2, 6};
  auto gathered = encodeBatch(output, fieldsFromType(output->rowType()), &selected);
  auto decoded = decodeAndCheckCachedLayout(gathered, pool);
  for (vector_size_t row = 0; row < decoded->size(); ++row)
    VELOX_CHECK(decoded->equalValueAt(output.get(), row, selected[row]));
}

void timeWire(memory::MemoryPool *pool) {
  std::vector<int32_t> millis{INT32_MIN, -1, 0, 1, 1001, 86399999, INT32_MAX};
  std::string validity(millis.size(), 1); validity[2] = 0;
  std::string payload(reinterpret_cast<const char *>(millis.data()), millis.size() * 4);
  auto fields = folly::dynamic::array(folly::dynamic::object("name", "t")(
      "minor", "TIME")("optional", true)("lengths", folly::dynamic::array(millis.size(), payload.size())));
  WireBatch wire{folly::dynamic::object("rows", millis.size())("fields", fields), validity + payload, {}};
  auto rows = decodeAndCheckCachedLayout(wire, pool);
  VELOX_CHECK(rows->childAt(0)->type()->equivalent(*TIME()));
  auto *values = rows->childAt(0)->as<FlatVector<int64_t>>();
  for (vector_size_t row = 0; row < rows->size(); ++row) {
    VELOX_CHECK_EQ(values->isNullAt(row), row == 2);
    if (row != 2) VELOX_CHECK_EQ(values->valueAt(row), millis[row]);
  }
  auto encoded = encodeBatch(rows, fieldsFromType(rows->rowType()));
  VELOX_CHECK_EQ(encoded.data, wire.data);
  auto metadata = batchHeaderFromDefinition(batchDefinition(encoded));
  VELOX_CHECK_EQ(metadata["fields"][0]["minor"].asString(), "TIME");
  VELOX_CHECK_EQ(metadata["fields"][0]["lengths"][1].asInt(), millis.size() * 4);
  values->set(0, int64_t(INT32_MAX) + 1);
  bool rejected = false;
  try { encodeBatch(rows, fieldsFromType(rows->rowType())); }
  catch (const VeloxException &) { rejected = true; }
  VELOX_CHECK(rejected, "TIME narrowing must not silently wrap");
  rejected = false;
  try { fieldsFromType(ROW({"raw"}, {TIME_MICRO_UTC()})); }
  catch (const VeloxException &) { rejected = true; }
  VELOX_CHECK(rejected, "SDK microseconds must never leak into Drill exchange");
}

void decimalWireBoundaries(bool shortDecimal, memory::MemoryPool *pool) {
  const auto type = DECIMAL(shortDecimal ? 18 : 38, 0);
  std::vector<int128_t> values{0,
                               127,
                               128,
                               -1,
                               -128,
                               -129,
                               32767,
                               32768,
                               -32768,
                               -32769,
                               999999999999999999LL,
                               -999999999999999999LL};
  std::vector<unsigned> widths{1, 1, 2, 1, 1, 2, 2, 3, 2, 3, 8, 8};
  if (!shortDecimal) {
    int128_t max = 1;
    for (int i = 0; i < 38; ++i)
      max *= 10;
    --max;
    values.insert(values.end(),
                  {int128_t(1) << 85, -(int128_t(1) << 85), max, -max});
    widths.insert(widths.end(), {11, 11, 16, 16});
  }
  auto vector = BaseVector::create(type, values.size(), pool);
  for (size_t i = 0; i < values.size(); ++i) {
    if (shortDecimal)
      vector->as<FlatVector<int64_t>>()->set(i, values[i]);
    else
      vector->as<FlatVector<int128_t>>()->set(i, values[i]);
  }
  auto row = std::make_shared<RowVector>(pool, ROW({"v"}, {type}), nullptr,
                                         values.size(),
                                         std::vector<VectorPtr>{vector});
  auto schema = folly::dynamic::array(
      folly::dynamic::object("name", "v")("minor", "VARDECIMAL")(
          "optional", true)("precision", shortDecimal ? 18 : 38)("scale", 0));
  for (bool nullable : {false, true}) {
    if (nullable)
      vector->setNull(2, true);
    auto wire = encodeBatch(row, schema);
    const char *offsets = wire.data.data() + values.size();
    auto decoded = decodeAndCheckCachedLayout(wire, pool)->childAt(0);
    DecodedVector actual(*decoded);
    for (size_t i = 0; i < values.size(); ++i) {
      uint32_t begin, end;
      std::memcpy(&begin, offsets + i * 4, 4);
      std::memcpy(&end, offsets + (i + 1) * 4, 4);
      const bool null = nullable && i == 2;
      VELOX_CHECK_EQ(end - begin, null ? 0 : widths[i]);
      VELOX_CHECK_EQ(actual.isNullAt(i), null);
      if (!null) {
        int128_t value = shortDecimal ? int128_t(actual.valueAt<int64_t>(i))
                                      : actual.valueAt<int128_t>(i);
        VELOX_CHECK_EQ(value, values[i]);
      }
    }
  }
}

void rejectLocalNull(memory::MemoryPool *pool) {
  auto input = BaseVector::create(INTEGER(), 2, pool);
  input->setNull(0, true);
  input->as<FlatVector<int32_t>>()->set(1, 7);
  auto type = ROW({"v"}, {INTEGER()});
  auto row = std::make_shared<RowVector>(pool, type, nullptr, 2,
                                         std::vector<VectorPtr>{input});
  auto fields = folly::dynamic::array(
      folly::dynamic::object("name", "v")("minor", "INT")("optional", false));
  bool rejected = false;
  try {
    copyLocalBatch(row, type, fields, pool);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected, "Required local columns must reject NULL values");
  std::vector<vector_size_t> selection{1, 1};
  auto valid = copyLocalBatch(row, type, fields, pool, &selection);
  VELOX_CHECK_EQ(valid->size(), 2);
  VELOX_CHECK_EQ(valid->childAt(0)->as<FlatVector<int32_t>>()->valueAt(1), 7);
}

void sharedAndCancelledOwnedExchange(memory::MemoryPool *pool) {
  auto type = ROW({"v", "s"}, {BIGINT(), VARCHAR()});
  auto fields = fieldsFromType(type);
  fields[0]["name"] = "receiver_value";
  const std::string text(8192, 'x');
  auto snapshot = [&]() {
    auto input = BaseVector::create<RowVector>(type, 1, pool);
    input->childAt(0)->as<FlatVector<int64_t>>()->set(0, 42);
    input->childAt(1)->as<FlatVector<StringView>>()->set(0, StringView(text));
    SenderBuffer buffer(type, pool, 4);
    VELOX_CHECK(buffer.append(input).empty());
    return buffer.flush();
  };
  auto destinationRoot = memory::memoryManager()->addRootPool("owned-alias-test");
  auto destination = destinationRoot->addLeafChild("reader");
  // A retained row, child, values or string buffer prevents ownership change.
  for (int alias = 0; alias < 8; ++alias) {
    auto inbox = std::make_shared<ReceiverInbox>();
    auto batch = snapshot();
    if (alias >= 4) {
      auto buffer = inbox->senderBuffer(type, 4);
      VELOX_CHECK(buffer && buffer->append(batch).empty());
      batch = buffer->flush();
    }
    auto *snapshotPool = batch->pool();
    RowVectorPtr row;
    VectorPtr child;
    BufferPtr buffer;
    if (alias % 4 == 0) row = batch;
    if (alias % 4 == 1) child = batch->childAt(0);
    if (alias % 4 == 2) buffer = batch->childAt(0)->values();
    if (alias % 4 == 3) buffer = batch->childAt(1)->as<FlatVector<StringView>>()->stringBuffers().at(0);
    VELOX_CHECK(!inbox->pushOwnedLocal(std::move(batch), fields, 0));
    if (row) VELOX_CHECK(row->pool() == snapshotPool && row->rowType()->nameOf(0) == "v");
    if (child) VELOX_CHECK(child->pool() == snapshotPool);
    if (buffer) VELOX_CHECK(buffer->pool() == snapshotPool);
    auto reader = inbox->factory()(destination.get());
    ContinueFuture wait;
    auto value = reader->next(wait);
    VELOX_CHECK(value && *value);
    if (row)
      for (const auto &buf : row->childAt(1)->as<FlatVector<StringView>>()->stringBuffers())
        VELOX_CHECK(buf->pool() == snapshotPool, "Dequeue migrated another recipient's payload");
    if (buffer) VELOX_CHECK(buffer->pool() == snapshotPool);
    VELOX_CHECK_EQ((*value)->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), 42);
    VELOX_CHECK_EQ((*value)->childAt(1)->as<FlatVector<StringView>>()->valueAt(0).str(), text);
    value.reset(); reader->cancel();
  }
  // Broadcast's first delivery copies. Its last exclusive delivery can move;
  // changing receiver one afterwards must leave receiver two untouched.
  auto first = std::make_shared<ReceiverInbox>();
  auto second = std::make_shared<ReceiverInbox>();
  auto batch = snapshot();
  VELOX_CHECK(!first->pushOwnedLocal(batch, fields, 0));
  VELOX_CHECK(second->pushOwnedLocal(std::move(batch), fields, 0));
  auto a = first->factory()(destination.get());
  auto b = second->factory()(destination.get());
  ContinueFuture wait;
  auto av = a->next(wait), bv = b->next(wait);
  VELOX_CHECK(av && *av && bv && *bv);
  (*av)->childAt(0)->as<FlatVector<int64_t>>()->set(0, -1);
  (*av)->childAt(1)->as<FlatVector<StringView>>()->set(0, StringView("changed"));
  VELOX_CHECK_EQ((*bv)->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), 42);
  VELOX_CHECK_EQ((*bv)->childAt(1)->as<FlatVector<StringView>>()->valueAt(0).str(), text);
  av.reset(); bv.reset(); a->cancel(); b->cancel();
  auto cancelled = std::make_shared<ReceiverInbox>();
  auto bytes = pool->usedBytes();
  cancelled->cancel();
  VELOX_CHECK(!cancelled->pushOwnedLocal(snapshot(), fields, 0));
  VELOX_CHECK_EQ(pool->usedBytes(), bytes);
  fields[0]["optional"] = false;
  bool rejected = false;
  auto required = std::make_shared<ReceiverInbox>();
  auto invalid = snapshot(); invalid->childAt(0)->setNull(0, true);
  try { required->pushOwnedLocal(std::move(invalid), fields, 0); }
  catch (const VeloxException &) { rejected = true; }
  VELOX_CHECK(rejected);
  VELOX_CHECK_EQ(destination->usedBytes(), 0);
}

void inboxWakeupProtocol(memory::MemoryPool *pool) {
  auto inbox = std::make_shared<ReceiverInbox>();
  inbox->senders({7, 8});
  std::vector<std::shared_ptr<BatchSource>> readers;
  std::vector<ContinueFuture> waits(4);
  for (int i = 0; i < 4; ++i) {
    readers.push_back(inbox->factory()(pool));
    auto pending = readers.back()->next(waits[i]);
    VELOX_CHECK(pending && !*pending && !waits[i].isReady());
  }
  auto type = ROW({"v"}, {BIGINT()});
  auto fields = fieldsFromType(type);
  auto row = BaseVector::create<RowVector>(type, 1, pool);
  for (int i = 0; i < 2; ++i) {
    row->childAt(0)->as<FlatVector<int64_t>>()->set(0, 42 + i);
    inbox->push(encodeBatch(row, fields), 7, false);
    int ready = 0;
    for (const auto &wait : waits) ready += wait.isReady();
    VELOX_CHECK_EQ(ready, 1, "A single DATA batch woke idle drivers without data");
    VELOX_CHECK(waits[i].isReady(), "Waiting drivers must get FIFO progress");
    auto data = readers[i]->next(waits[i]);
    VELOX_CHECK(data && *data);
    VELOX_CHECK_EQ((*data)->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), 42 + i);
    auto pending = readers[i]->next(waits[i]);
    VELOX_CHECK(pending && !*pending && !waits[i].isReady());
  }
  inbox->end(7);
  for (int i = 0; i < 4; ++i) {
    VELOX_CHECK(waits[i].isReady());
    auto pending = readers[i]->next(waits[i]);
    VELOX_CHECK(pending && !*pending && !waits[i].isReady());
  }
  inbox->end(8);
  for (int i = 0; i < 4; ++i) {
    VELOX_CHECK(waits[i].isReady());
    VELOX_CHECK(!readers[i]->next(waits[i]));
  }
  auto metrics = inbox->statistics();
  VELOX_CHECK_EQ(metrics["data_batches"].asInt(), 2);
  VELOX_CHECK_EQ(metrics["data_rows"].asInt(), 2);
  VELOX_CHECK_EQ(metrics["data_wakes"].asInt(), 2);
  VELOX_CHECK_EQ(metrics["terminal_wakes"].asInt(), 8);
  auto cancelled = std::make_shared<ReceiverInbox>();
  readers.clear();
  for (int i = 0; i < 4; ++i) {
    readers.push_back(cancelled->factory()(pool));
    auto pending = readers.back()->next(waits[i]);
    VELOX_CHECK(pending && !*pending && !waits[i].isReady());
  }
  cancelled->cancel();
  for (int i = 0; i < 4; ++i) {
    VELOX_CHECK(waits[i].isReady());
    VELOX_CHECK(!readers[i]->next(waits[i]));
  }
}

} // namespace

int main() {
  try {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    auto root = memory::memoryManager()->addRootPool("codec-test", 64 << 20);
    auto pool = root->addLeafChild("vectors");
    checkType<int32_t>(INTEGER(), "INT", pool.get());
    checkType<int64_t>(BIGINT(), "BIGINT", pool.get());
    checkType<float>(REAL(), "FLOAT4", pool.get());
    checkType<double>(DOUBLE(), "FLOAT8", pool.get());
    checkType<int64_t>(TIME(), "TIME", pool.get());
    rejectLocalNull(pool.get());
    sharedAndCancelledOwnedExchange(pool.get());
    inboxWakeupProtocol(pool.get());
    wireStringOwnership(pool.get());
    temporalBinaryWire(pool.get());
    timeWire(pool.get());
    decimalWireBoundaries(true, pool.get());
    decimalWireBoundaries(false, pool.get());
    for (int encoding = 0; encoding < 3; ++encoding)
      for (bool selected : {false, true}) {
        localCopy(encoding, selected);
        localCopy(encoding, selected, true);
        localCopy(encoding, selected, true, true);
        localCopy(encoding, selected, true, 2);
      }
    VELOX_CHECK_EQ(pool->usedBytes(), 0);
    std::cout << "Native numeric codec: 35 wire round trips, 6 ownership "
                 "copies, 6 copied and 12 owned inbox transfers (12 types), shared/broadcast/cancelled owned deliveries, timestamp/time/binary wire boundaries, string payload ownership, decimal signed "
                 "boundaries, 5 NULL rejections "
                 "passed\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
