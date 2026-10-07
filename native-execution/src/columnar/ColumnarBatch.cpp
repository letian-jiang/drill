// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file for copyright ownership.
// Licensed under the Apache License, Version 2.0. You may obtain a copy at
// http://www.apache.org/licenses/LICENSE-2.0 .
#include "columnar/ColumnarBatch.h"
#include "columnar/DrillArrayType.h"
#include "columnar/DrillMapType.h"
#include "columnar/DrillRowType.h"
#include <bit>
#include <cstring>
#include <limits>
#include <numeric>
#include <span>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>

namespace drill::nativeexec {
namespace {
TypePtr fieldType(const folly::dynamic &f) {
  if (arrayField(f))
    return std::make_shared<DrillArrayType>(fieldType(f["element"]), f);
  auto minor = f["minor"].asString();
  if (minor == "INT")
    return INTEGER();
  if (minor == "BIGINT")
    return BIGINT();
  if (minor == "FLOAT4")
    return REAL();
  if (minor == "FLOAT8")
    return DOUBLE();
  if (minor == "BIT")
    return BOOLEAN();
  if (minor == "VARCHAR")
    return VARCHAR();
  if (minor == "VARBINARY")
    return VARBINARY();
  if (minor == "TIMESTAMP")
    return TIMESTAMP();
  if (minor == "TIME")
    return TIME();
  if (minor == "DATE")
    return DATE();
  if (minor == "VARDECIMAL")
    return DECIMAL(f["precision"].asInt(), f["scale"].asInt());
  if (minor == "MAP") {
    return drillRowType(columnarType(f["children"]), f);
  }
  if (minor == "DICT") {
    VELOX_USER_CHECK_EQ(f["children"].size(), 2);
    const auto &key = f["children"][0], &value = f["children"][1];
    VELOX_USER_CHECK(key["name"] == "key" && value["name"] == "value" &&
                         !key["optional"].asBool(),
                     "Invalid Drill DICT children");
    auto keyType = fieldType(key);
    VELOX_USER_CHECK(keyType->isPrimitiveType(),
                     "Drill DICT key must be scalar");
    return std::make_shared<DrillMapType>(keyType, fieldType(value), f);
  }
  VELOX_UNSUPPORTED("Unsupported native wire type: {}", minor);
}
int width(const std::string &minor) {
  if (minor == "INT" || minor == "FLOAT4" || minor == "TIME")
    return 4;
  if (minor == "BIGINT" || minor == "FLOAT8" || minor == "DATE" ||
      minor == "TIMESTAMP")
    return 8;
  return 0;
}
template <typename T> T load(const char *p) {
  T v;
  std::memcpy(&v, p, sizeof(T));
  return v;
}
template <typename T> void append(std::string &s, T v) {
  s.append(reinterpret_cast<const char *>(&v), sizeof(T));
}
// Drill VARDECIMAL uses signed big-endian variable-width values. Include a
// sign bit and omit redundant 0x00/0xff prefixes, preserving all 128-bit
// values.
unsigned decimalWireBytes(int128_t value) {
  auto magnitude = value < 0 ? ~static_cast<__uint128_t>(value)
                             : static_cast<__uint128_t>(value);
  auto high = static_cast<uint64_t>(magnitude >> 64);
  unsigned significant = high
                             ? 64 + std::bit_width(high)
                             : std::bit_width(static_cast<uint64_t>(magnitude));
  return (significant + 1 + 7) / 8;
}
template <typename T>
std::string fixedValues(const DecodedVector &decoded, vector_size_t count,
                        const std::vector<vector_size_t> *selected,
                        bool optional) {
  std::string values(count * sizeof(T), 0);
  if (!selected && decoded.isIdentityMapping() && !decoded.mayHaveNulls()) {
    if (!values.empty())
      std::memcpy(values.data(), decoded.data<T>(), values.size());
    return values;
  }
  // Specialize by column type once. Dictionary/constant vectors and hash
  // selections gather into a pre-sized buffer, preserving NULL payload zeros.
  for (vector_size_t row = 0; row < count; ++row) {
    auto input = selected ? (*selected)[row] : row;
    bool null = decoded.isNullAt(input);
    VELOX_CHECK(!null || optional, "NULL in required output");
    T value = null ? T(0) : decoded.valueAt<T>(input);
    std::memcpy(values.data() + row * sizeof(T), &value, sizeof(T));
  }
  return values;
}
void ownLocalField(VectorPtr &child, const folly::dynamic &field,
                   memory::MemoryPool *pool) {
  const auto &type = child->type();
  const auto count = child->size();
  if (type->isRow()) {
    auto *row = child->as<RowVector>();
    for (size_t i = 0; i < row->childrenSize(); ++i)
      ownLocalField(row->childAt(i), field["children"][i], pool);
  } else if (type->isMap()) {
    auto *map = child->as<MapVector>();
    ownLocalField(map->mapKeys(), field["children"][0], pool);
    ownLocalField(map->mapValues(), field["children"][1], pool);
    // Original DictVector has no parent validity, even for OPTIONAL mode.
    for (vector_size_t row = 0; row < count; ++row)
      if (map->isNullAt(row)) {
        map->setOffsetAndSize(row, 0, 0);
        map->setNull(row, false);
      }
  } else if (type->isArray())
    ownLocalField(child->as<ArrayVector>()->elements(), field["element"], pool);
  if (type->isVarchar() || type->isVarbinary()) {
    auto *strings = child->as<FlatVector<StringView>>();
    // Velox copy shares string storage when the source uses the same pool.
    // A local queue must remain exclusive even for an inbox-owned snapshot
    // retained by another recipient; otherwise dequeue would change that
    // recipient's buffer ownership/accounting too.
    bool shared = false;
    for (const auto &buffer : strings->stringBuffers())
      shared |= !buffer->isMutable() || buffer->pool() != pool;
    if (shared) {
      auto owned = BaseVector::create(type, count, pool);
      auto *output = owned->as<FlatVector<StringView>>();
      for (vector_size_t row = 0; row < count; ++row) {
        if (strings->isNullAt(row))
          output->setNull(row, true);
        else
          output->set(row, strings->valueAt(row));
      }
      child = std::move(owned);
    }
  }
}
void validateLocalField(const VectorPtr &value, const folly::dynamic &field,
                        const std::vector<vector_size_t> &rows) {
  const bool complex = value->type()->isRow() || value->type()->isArray() ||
                       value->type()->isMap();
  if (!complex && (field["optional"].asBool() || !value->mayHaveNulls()))
    return;
  DecodedVector decoded(*value);
  std::vector<vector_size_t> indices;
  if (complex)
    indices.reserve(rows.size());
  const ArrayVector *array =
      value->type()->isArray() ? decoded.base()->as<ArrayVector>() : nullptr;
  const MapVector *dict =
      value->type()->isMap() ? decoded.base()->as<MapVector>() : nullptr;
  // Scalar elements without a required-NULL check need no selection list.
  // Still check each visible array's range; NULL parents hide backing elements.
  const bool inspectElements =
      array && (array->elements()->type()->isRow() ||
                array->elements()->type()->isArray() ||
                (!field["element"]["optional"].asBool() &&
                 array->elements()->mayHaveNulls()));
  for (auto row : rows) {
    bool null = decoded.isNullAt(row);
    // OPTIONAL MapVector has no parent validity, just like REQUIRED MAP.
    VELOX_CHECK(!null ||
                    (!value->type()->isRow() && field["optional"].asBool()),
                "NULL is not representable in this Drill output field");
    if (null)
      continue;
    auto index = decoded.index(row);
    if (array) {
      const int64_t start = array->offsetAt(index), size = array->sizeAt(index);
      VELOX_CHECK(start >= 0 && size >= 0 &&
                      start + size <= array->elements()->size(),
                  "Invalid local array range");
      if (inspectElements)
        for (int64_t element = start; element < start + size; ++element)
          indices.push_back(element);
    } else if (dict) {
      const int64_t start = dict->offsetAt(index), size = dict->sizeAt(index);
      VELOX_CHECK(start >= 0 && size >= 0 &&
                      start + size <= dict->mapKeys()->size() &&
                      start + size <= dict->mapValues()->size(),
                  "Invalid local DICT range");
      for (int64_t element = start; element < start + size; ++element)
        indices.push_back(element);
    } else if (complex)
      indices.push_back(index);
  }
  if (inspectElements)
    validateLocalField(array->elements(), field["element"], indices);
  else if (dict) {
    validateLocalField(dict->mapKeys(), field["children"][0], indices);
    validateLocalField(dict->mapValues(), field["children"][1], indices);
  } else if (value->type()->isRow()) {
    auto *map = decoded.base()->as<RowVector>();
    for (size_t i = 0; i < map->childrenSize(); ++i)
      validateLocalField(map->childAt(i), field["children"][i], indices);
  }
}
void validateBufferLengths(const folly::dynamic &fields,
                           const ColumnarBatchLayout &layout,
                           std::span<const BufferView> views) {
  VELOX_CHECK_EQ(views.size(), layout.bufferCount,
                 "Invalid column buffer count");
  size_t start = 0;
  for (size_t i = 0; i < layout.fields.size(); ++i) {
    const auto &field = layout.fields[i];
    const auto &lengths = fields[i]["lengths"];
    VELOX_CHECK_EQ(lengths.size(), field.bufferCount,
                   "Invalid column buffer shape");
    auto buffers = views.subspan(start, field.bufferCount);
    for (size_t j = 0; j < buffers.size(); ++j)
      VELOX_CHECK(lengths[j].asInt() >= 0 &&
                      buffers[j].size == size_t(lengths[j].asInt()),
                  "Invalid column buffer size");
    if (field.arrayPrefix)
      validateBufferLengths(
          field.type->isMap() ? fields[i]["children"]
                              : folly::dynamic::array(fields[i]["element"]),
          *field.children, buffers.subspan(field.arrayPrefix));
    else if (field.children)
      validateBufferLengths(fields[i]["children"], *field.children, buffers);
    start += field.bufferCount;
  }
}
} // namespace
RowVectorPtr decodeRows(const ColumnarBatchLayout &, int64_t,
                        const std::vector<BufferView> &, memory::MemoryPool *);
RowTypePtr columnarType(const folly::dynamic &fields) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto &field : fields) {
    names.push_back(field["name"].asString());
    types.push_back(fieldType(field));
  }
  auto type = std::make_shared<RowType>(std::move(names), std::move(types));
  annotateDrillSchema(type, fields);
  return type;
}
void validateLocalBatch(const RowVectorPtr &value,
                        const folly::dynamic &fields) {
  bool needed = false;
  for (size_t i = 0; i < fields.size(); ++i) {
    const auto &child = value->childAt(i);
    needed |= child->type()->isRow() || child->type()->isArray() ||
              child->type()->isMap() ||
              (!fields[i]["optional"].asBool() && child->mayHaveNulls());
  }
  if (!needed)
    return;
  std::vector<vector_size_t> rows(value->size());
  std::iota(rows.begin(), rows.end(), 0);
  for (size_t i = 0; i < fields.size(); ++i)
    validateLocalField(value->childAt(i), fields[i], rows);
}
RowVectorPtr copyLocalBatch(const RowVectorPtr &source, const RowTypePtr &type,
                            const folly::dynamic &fields,
                            memory::MemoryPool *pool,
                            const std::vector<vector_size_t> *selected) {
  VELOX_CHECK(source->type()->equivalent(*type),
              "Local exchange type mismatch");
  const auto count = selected ? selected->size() : source->size();
  VELOX_CHECK_LE(count, 65535);
  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(type, count, pool));
  result->copy(source.get(), SelectivityVector(count),
               selected ? selected->data() : nullptr);
  for (size_t i = 0; i < fields.size(); ++i) {
    ownLocalField(result->childAt(i), fields[i], pool);
  }
  validateLocalBatch(result, fields);
  return result;
}
ColumnarBatchLayout columnarLayout(const folly::dynamic &fields) {
  ColumnarBatchLayout layout;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (const auto &field : fields) {
    auto minor = field["minor"].asString();
    bool optional = field["optional"].asBool();
    size_t count =
        (optional ? 1 : 0) +
        ((minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL")
             ? 2
             : 1);
    auto type = fieldType(field);
    std::shared_ptr<const ColumnarBatchLayout> children;
    size_t prefix = 0;
    if (arrayField(field)) {
      prefix = arrayPrefix(field);
      children = std::make_shared<ColumnarBatchLayout>(
          columnarLayout(folly::dynamic::array(field["element"])));
      count = prefix + children->bufferCount;
    } else if (minor == "DICT") {
      prefix = 1;
      children = std::make_shared<ColumnarBatchLayout>(
          columnarLayout(field["children"]));
      count = prefix + children->bufferCount;
    } else if (minor == "MAP") {
      children = std::make_shared<ColumnarBatchLayout>(
          columnarLayout(field["children"]));
      count = children->bufferCount;
    }
    names.push_back(field["name"].asString());
    types.push_back(type);
    layout.fields.push_back({std::move(type), std::move(minor), optional, count,
                             std::move(children), prefix});
    layout.bufferCount += count;
  }
  layout.type = std::make_shared<RowType>(std::move(names), std::move(types));
  annotateDrillSchema(layout.type, fields);
  return layout;
}
RowVectorPtr decodeBatchBuffers(const folly::dynamic &header,
                                const std::vector<BufferView> &views,
                                memory::MemoryPool *pool) {
  auto layout = columnarLayout(header["fields"]);
  validateBufferLengths(header["fields"], layout, views);
  return decodeBatchBuffers(layout, header["rows"].asInt(), views, pool);
}
RowVectorPtr decodeBatchBuffers(const ColumnarBatchLayout &layout,
                                int64_t count,
                                const std::vector<BufferView> &views,
                                memory::MemoryPool *pool) {
  VELOX_CHECK(count >= 0 && count <= 65535, "Invalid batch row count");
  return decodeRows(layout, count, views, pool);
}
RowVectorPtr decodeRows(const ColumnarBatchLayout &layout, int64_t count,
                        const std::vector<BufferView> &views,
                        memory::MemoryPool *pool) {
  VELOX_CHECK(count >= 0 && count <= INT32_MAX, "Invalid nested vector count");
  VELOX_CHECK_EQ(views.size(), layout.bufferCount,
                 "Invalid column buffer count");
  std::vector<VectorPtr> children;
  children.reserve(layout.fields.size());
  size_t pos = 0;
  for (const auto &field : layout.fields) {
    const auto &type = field.type;
    const auto &minor = field.minor;
    auto buffers =
        std::span<const BufferView>(views).subspan(pos, field.bufferCount);
    pos += field.bufferCount;
    if (field.arrayPrefix) {
      VELOX_CHECK_EQ(buffers[0].size, (count + 1) * 4,
                     "Invalid array offsets size");
      VELOX_CHECK_EQ(load<uint32_t>(buffers[0].data), 0,
                     "Array offsets must start at zero");
      const uint32_t elements = load<uint32_t>(buffers[0].data + count * 4);
      VELOX_CHECK_LE(elements, INT32_MAX, "Array element count overflow");
      if (field.arrayPrefix == 2)
        VELOX_CHECK_EQ(buffers[1].size, count, "Invalid LIST validity");
      std::vector<BufferView> childViews(buffers.begin() + field.arrayPrefix,
                                         buffers.end());
      auto child = decodeRows(*field.children, elements, childViews, pool);
      auto offsets = AlignedBuffer::allocate<vector_size_t>(count, pool);
      auto sizes = AlignedBuffer::allocate<vector_size_t>(count, pool);
      VectorPtr output;
      if (type->isMap()) {
        output = std::make_shared<MapVector>(pool, type, nullptr, count,
                                             offsets, sizes, child->childAt(0),
                                             child->childAt(1));
      } else
        output = std::make_shared<ArrayVector>(
            pool, type, nullptr, count, offsets, sizes, child->childAt(0));
      for (int64_t row = 0; row < count; ++row) {
        auto start = load<uint32_t>(buffers[0].data + row * 4);
        auto end = load<uint32_t>(buffers[0].data + (row + 1) * 4);
        VELOX_CHECK(start <= end && end <= elements, "Invalid array offsets");
        offsets->asMutable<vector_size_t>()[row] = start;
        sizes->asMutable<vector_size_t>()[row] = end - start;
        if (field.arrayPrefix == 2 && !buffers[1].data[row])
          output->setNull(row, true);
      }
      children.push_back(std::move(output));
      continue;
    }
    if (minor == "MAP") {
      std::vector<BufferView> childViews(buffers.begin(), buffers.end());
      children.push_back(decodeRows(*field.children, count, childViews, pool));
      continue;
    }
    auto vector = BaseVector::create(type, count, pool);
    size_t index = 0;
    const char *validity = nullptr;
    if (field.optional) {
      VELOX_CHECK(!buffers.empty() &&
                      buffers[0].size == static_cast<size_t>(count),
                  "Invalid validity buffer");
      validity = buffers[index++].data;
    }
    const auto expected =
        index +
        ((minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL")
             ? 2
             : 1);
    VELOX_CHECK_EQ(buffers.size(), expected);
    const auto [values, length] = buffers[index];
    const char *data = nullptr;
    size_t dataLength = 0;
    if (minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL") {
      VELOX_CHECK_EQ(length, (count + 1) * 4);
      data = buffers[index + 1].data;
      dataLength = buffers[index + 1].size;
      VELOX_CHECK_EQ(load<uint32_t>(values), 0);
    } else {
      VELOX_CHECK_EQ(length,
                     minor == "BIT" ? (count + 7) / 8 : count * width(minor));
    }
    // Numeric payloads are copied in bulk. Only format conversions and validity
    // need a column pass; there are no Java objects or per-value JNI calls.
    if (minor == "INT" || minor == "BIGINT" || minor == "FLOAT4" ||
        minor == "FLOAT8") {
      if (length)
        std::memcpy(vector->values()->asMutable<char>(), values, length);
      if (validity)
        for (int row = 0; row < count; ++row)
          if (validity[row] == 0)
            vector->setNull(row, true);
      children.push_back(std::move(vector));
      continue;
    }
    // One owned copy of the variable payload per column. StringViews retain
    // this vector's buffer, never a Java/plugin buffer or a transport frame.
    auto *strings = minor == "VARCHAR" || minor == "VARBINARY"
                        ? vector->as<FlatVector<StringView>>()
                        : nullptr;
    char *stringData = nullptr;
    if (strings && dataLength) {
      stringData = strings->getRawStringBufferWithSpace(dataLength, true);
      std::memcpy(stringData, data, dataLength);
    }
    // Newly allocated flat values are owned by this vector. Resolve typed
    // storage once per column; per-row dynamic_cast/set would repeat type and
    // writability checks millions of times for plugin DATE/DECIMAL columns.
    auto *dateValues = count && minor == "DATE"
                           ? vector->values()->asMutable<int32_t>()
                           : nullptr;
    auto *timestampValues = count && minor == "TIMESTAMP"
                                ? vector->values()->asMutable<Timestamp>()
                                : nullptr;
    auto *timeValues = count && minor == "TIME"
                           ? vector->values()->asMutable<int64_t>()
                           : nullptr;
    const bool shortDecimal = minor == "VARDECIMAL" && type->isShortDecimal();
    auto *decimal64Values = count && shortDecimal
                                ? vector->values()->asMutable<int64_t>()
                                : nullptr;
    auto *decimal128Values = count && minor == "VARDECIMAL" && !shortDecimal
                                 ? vector->values()->asMutable<int128_t>()
                                 : nullptr;
    auto *bits = minor == "BIT" ? vector->as<FlatVector<bool>>() : nullptr;
    for (int row = 0; row < count; ++row) {
      bool null = validity && validity[row] == 0;
      if (null)
        vector->setNull(row, true);
      if (minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL") {
        auto start = load<uint32_t>(values + row * 4);
        auto end = load<uint32_t>(values + (row + 1) * 4);
        VELOX_CHECK(start <= end && end <= dataLength,
                    "Invalid variable offsets");
        if (null)
          continue;
        if (strings) {
          strings->setNoCopy(
              row,
              StringView(stringData ? stringData + start : "", end - start));
        } else {
          VELOX_CHECK(end > start && end - start <= 16,
                      "Invalid decimal width");
          __uint128_t bits = static_cast<unsigned char>(data[start]) & 128
                                 ? ~__uint128_t(0)
                                 : 0;
          for (auto i = start; i < end; ++i)
            bits = (bits << 8) | static_cast<unsigned char>(data[i]);
          auto value = static_cast<int128_t>(bits);
          if (shortDecimal) {
            VELOX_CHECK(value >= std::numeric_limits<int64_t>::min() &&
                        value <= std::numeric_limits<int64_t>::max());
            decimal64Values[row] = static_cast<int64_t>(value);
          } else
            decimal128Values[row] = value;
        }
      } else if (!null && minor == "BIT") {
        bits->set(row,
                  (static_cast<unsigned char>(values[row / 8]) >> (row % 8)) &
                      1);
      } else if (!null && minor == "DATE") {
        auto millis = load<int64_t>(values + row * 8);
        VELOX_CHECK_EQ(millis % 86400000, 0, "DATE is not midnight UTC");
        auto days = millis / 86400000;
        VELOX_CHECK(days >= INT32_MIN && days <= INT32_MAX);
        dateValues[row] = static_cast<int32_t>(days);
      } else if (!null && minor == "TIME") {
        // Drill TimeVector stores milliseconds as int32; Velox TIME is int64.
        timeValues[row] = load<int32_t>(values + row * 4);
      } else if (!null && minor == "TIMESTAMP") {
        const auto millis = load<int64_t>(values + row * 8);
        auto seconds = millis / 1000;
        auto remainder = millis % 1000;
        if (remainder < 0) {
          --seconds;
          remainder += 1000;
        }
        // Normalize negative epochs without multiplying the quotient back:
        // that intermediate can overflow at INT64_MIN milliseconds.
        timestampValues[row] = Timestamp(seconds, remainder * 1000000);
      }
    }
    children.push_back(std::move(vector));
  }
  VELOX_CHECK_EQ(pos, views.size(), "Trailing column buffers");
  return std::make_shared<RowVector>(pool, layout.type, nullptr, count,
                                     std::move(children));
}

RowVectorPtr decodeBatch(const WireBatch &batch, memory::MemoryPool *pool) {
  std::vector<BufferView> views;
  size_t pos = 0;
  for (const auto &field : batch.header["fields"]) {
    for (const auto &n : field["lengths"]) {
      int64_t length = n.asInt();
      VELOX_CHECK(length >= 0 &&
                      static_cast<size_t>(length) <= batch.data.size() - pos,
                  "Truncated columnar batch");
      views.push_back({batch.data.data() + pos, static_cast<size_t>(length)});
      pos += length;
    }
  }
  VELOX_CHECK_EQ(pos, batch.data.size(), "Trailing batch data");
  return decodeBatchBuffers(batch.header, views, pool);
}

WireBatch encodeBatch(const RowVectorPtr &vector, const folly::dynamic &schema,
                      const std::vector<vector_size_t> *selected) {
  WireBatch batch{folly::dynamic::object("rows", selected ? selected->size()
                                                          : vector->size())(
                      "fields", folly::dynamic::array),
                  {},
                  {}};
  VELOX_CHECK_EQ(schema.size(), vector->childrenSize());
  auto count = batch.header["rows"].asInt();
  for (size_t col = 0; col < schema.size(); ++col) {
    auto field = schema[col];
    auto minor = field["minor"].asString();
    VELOX_CHECK(*fieldType(field) == *vector->childAt(col)->type(),
                "Output type mismatch");
    DecodedVector decoded(*vector->childAt(col));
    bool optional = field["optional"].asBool();
    if (arrayField(field)) {
      auto *array = decoded.base()->as<ArrayVector>();
      std::vector<vector_size_t> elements;
      std::string offsets, validity;
      append<uint32_t>(offsets, 0);
      const auto prefix = arrayPrefix(field);
      for (vector_size_t row = 0; row < count; ++row) {
        auto input = selected ? (*selected)[row] : row;
        bool null = decoded.isNullAt(input);
        VELOX_USER_CHECK(!null || optional, "NULL in required Drill array");
        if (prefix == 2)
          validity.push_back(!null);
        if (!null) {
          auto index = decoded.index(input);
          const int64_t start = array->offsetAt(index),
                        size = array->sizeAt(index);
          VELOX_CHECK(start >= 0 && size >= 0 &&
                          start + size <= array->elements()->size(),
                      "Invalid array range");
          VELOX_CHECK_LE(elements.size() + size, INT32_MAX,
                         "Array element count overflow");
          for (int64_t i = start; i < start + size; ++i)
            elements.push_back(i);
        }
        append<uint32_t>(offsets, elements.size());
      }
      auto childRow = std::make_shared<RowVector>(
          array->pool(), ROW({"$data$"}, {array->elements()->type()}), nullptr,
          array->elements()->size(), std::vector<VectorPtr>{array->elements()});
      auto child = encodeBatch(
          childRow, folly::dynamic::array(field["element"]), &elements);
      field["element"] = child.header["fields"][0];
      field["element"]["count"] = elements.size();
      field["lengths"] = folly::dynamic::array(offsets.size());
      batch.data += offsets;
      if (prefix == 2) {
        field["lengths"].push_back(validity.size());
        batch.data += validity;
      }
      for (const auto &length : field["element"]["lengths"])
        field["lengths"].push_back(length);
      batch.data += child.data;
      batch.header["fields"].push_back(std::move(field));
      continue;
    }
    if (minor == "DICT") {
      auto *dict = decoded.base()->as<MapVector>();
      std::vector<vector_size_t> entries;
      std::string offsets;
      append<uint32_t>(offsets, 0);
      for (vector_size_t row = 0; row < count; ++row) {
        auto input = selected ? (*selected)[row] : row;
        if (!decoded.isNullAt(input)) {
          auto index = decoded.index(input);
          const int64_t start = dict->offsetAt(index),
                        size = dict->sizeAt(index);
          VELOX_CHECK(start >= 0 && size >= 0 &&
                          start + size <= dict->mapKeys()->size() &&
                          start + size <= dict->mapValues()->size(),
                      "Invalid DICT range");
          VELOX_CHECK_LE(entries.size() + size, INT32_MAX,
                         "DICT entry count overflow");
          for (int64_t i = start; i < start + size; ++i)
            entries.push_back(i);
        }
        append<uint32_t>(offsets, entries.size());
      }
      auto row = std::make_shared<RowVector>(
          dict->pool(),
          ROW({"key", "value"},
              {dict->mapKeys()->type(), dict->mapValues()->type()}),
          nullptr, dict->mapKeys()->size(),
          std::vector<VectorPtr>{dict->mapKeys(), dict->mapValues()});
      auto child = encodeBatch(row, field["children"], &entries);
      field["children"] = std::move(child.header["fields"]);
      field["entryCount"] = entries.size();
      field["lengths"] = folly::dynamic::array(offsets.size());
      batch.data += offsets;
      for (const auto &nested : field["children"])
        for (const auto &length : nested["lengths"])
          field["lengths"].push_back(length);
      batch.data += child.data;
      batch.header["fields"].push_back(std::move(field));
      continue;
    }
    if (minor == "MAP") {
      // A parent dictionary/constant maps every child to the same source row.
      // Required Drill MAP has no parent validity buffer; reject NULL parents.
      std::vector<vector_size_t> indices(count);
      for (vector_size_t row = 0; row < count; ++row) {
        auto input = selected ? (*selected)[row] : row;
        VELOX_USER_CHECK(!decoded.isNullAt(input),
                         "NULL parent cannot be encoded as required Drill MAP");
        indices[row] = decoded.index(input);
      }
      // BaseVector has no shared_from_this. The parent encoding owns this
      // decoded base; alias that owner for the read-only recursive encoder.
      RowVectorPtr parent(
          vector->childAt(col),
          const_cast<RowVector *>(decoded.base()->as<RowVector>()));
      auto child = encodeBatch(parent, field["children"], &indices);
      field["children"] = std::move(child.header["fields"]);
      field["lengths"] = folly::dynamic::array;
      for (const auto &nested : field["children"])
        for (const auto &length : nested["lengths"])
          field["lengths"].push_back(length);
      batch.data += child.data;
      batch.header["fields"].push_back(std::move(field));
      continue;
    }
    std::vector<std::string> buffers;
    if (optional) {
      std::string bits(count, 0);
      for (int row = 0; row < count; ++row)
        bits[row] = !decoded.isNullAt(selected ? (*selected)[row] : row);
      buffers.push_back(std::move(bits));
    }
    std::string values, data;
    if (minor == "BIT")
      values.resize((count + 7) / 8, 0);
    bool variable =
        minor == "VARCHAR" || minor == "VARBINARY" || minor == "VARDECIMAL";
    if (variable) {
      values.reserve((count + 1) * sizeof(uint32_t));
      append<uint32_t>(values, 0);
      if (minor == "VARDECIMAL")
        data.reserve(count *
                     (vector->childAt(col)->type()->isShortDecimal() ? 8 : 16));
    }
    if (minor == "INT")
      values = fixedValues<int32_t>(decoded, count, selected, optional);
    else if (minor == "BIGINT")
      values = fixedValues<int64_t>(decoded, count, selected, optional);
    else if (minor == "FLOAT4")
      values = fixedValues<float>(decoded, count, selected, optional);
    else if (minor == "FLOAT8")
      values = fixedValues<double>(decoded, count, selected, optional);
    else
      for (int row = 0; row < count; ++row) {
        auto input = selected ? (*selected)[row] : row;
        bool null = decoded.isNullAt(input);
        VELOX_CHECK(!null || optional, "NULL in required output");
        if (minor == "DATE")
          append<int64_t>(
              values,
              null ? 0 : int64_t(decoded.valueAt<int32_t>(input)) * 86400000);
        else if (minor == "TIMESTAMP")
          append<int64_t>(
              values, null ? 0 : decoded.valueAt<Timestamp>(input).toMillis());
        else if (minor == "TIME") {
          auto millis = null ? 0 : decoded.valueAt<int64_t>(input);
          VELOX_USER_CHECK(
              millis >= INT32_MIN && millis <= INT32_MAX,
              "TIME does not fit Drill's 32-bit millisecond value");
          append<int32_t>(values, static_cast<int32_t>(millis));
        } else if (minor == "BIT") {
          if (!null && decoded.valueAt<bool>(input))
            values[row / 8] |= 1 << (row % 8);
        } else if (minor == "VARCHAR" || minor == "VARBINARY") {
          if (!null) {
            auto v = decoded.valueAt<StringView>(input);
            data.append(v.data(), v.size());
          }
        } else if (minor == "VARDECIMAL") {
          if (!null) {
            int128_t v = vector->childAt(col)->type()->isShortDecimal()
                             ? int128_t(decoded.valueAt<int64_t>(input))
                             : decoded.valueAt<int128_t>(input);
            for (int byte = decimalWireBytes(v) - 1; byte >= 0; --byte)
              data.push_back(static_cast<__uint128_t>(v) >> (byte * 8));
          }
        }
        if (variable)
          append<uint32_t>(values, data.size());
      }
    buffers.push_back(std::move(values));
    if (variable)
      buffers.push_back(std::move(data));
    field["lengths"] = folly::dynamic::array;
    for (auto &buffer : buffers) {
      field["lengths"].push_back(buffer.size());
      batch.data += buffer;
    }
    batch.header["fields"].push_back(std::move(field));
  }
  return batch;
}
} // namespace drill::nativeexec
