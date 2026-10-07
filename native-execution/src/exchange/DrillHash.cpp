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
#include "exchange/DrillHash.h"
#include <bit>
#include <charconv>
#include <cmath>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/VectorFunction.h>
#include <velox/type/DecimalUtil.h>
#include <velox/vector/FlatVector.h>
namespace drill::nativeexec {
uint32_t murmur(const char *bytes, size_t size, uint32_t seed) {
  uint32_t h = seed;
  size_t i = 0;
  auto mix = [](uint32_t k) {
    k *= 0xcc9e2d51;
    k = std::rotl(k, 15);
    return k * 0x1b873593;
  };
  for (; i + 4 <= size; i += 4) {
    uint32_t k;
    memcpy(&k, bytes + i, 4);
    h ^= mix(k);
    h = std::rotl(h, 13) * 5 + 0xe6546b64;
  }
  uint32_t tail = 0;
  for (size_t j = i; j < size; ++j)
    tail |= uint32_t(static_cast<unsigned char>(bytes[j])) << (8 * (j - i));
  if (i < size)
    h ^= mix(tail);
  h ^= size;
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}
std::vector<HashKey> decodeHashKeys(const RowVectorPtr &v,
                                    const folly::dynamic &keys) {
  std::vector<HashKey> decoded;
  decoded.reserve(keys.size());
  for (const auto &key : keys) {
    const auto &column = v->childAt(v->rowType()->getChildIdx(key.asString()));
    decoded.push_back(
        {column->type(), std::make_unique<DecodedVector>(*column)});
  }
  return decoded;
}
uint32_t hashRow(const std::vector<HashKey> &keys, int row, bool prehashed,
                 uint32_t seed) {
  uint32_t hash = seed;
  for (const auto &key : keys) {
    const auto &d = *key.decoded;
    if (d.isNullAt(row))
      continue;
    if (prehashed)
      return d.valueAt<int32_t>(row);
    const auto &type = key.type;
    if (type->kind() == TypeKind::VARCHAR || type->kind() == TypeKind::VARBINARY) {
      auto s = d.valueAt<StringView>(row);
      hash = murmur(s.data(), s.size(), hash);
      continue;
    }
    double value;
    if (type->isDecimal()) {
      // Match BigDecimal.doubleValue with one correctly rounded conversion;
      // converting the unscaled integer and then dividing can round twice.
      auto decimal = DecimalUtil::toString(
          type->isShortDecimal() ? int128_t(d.valueAt<int64_t>(row))
                                 : d.valueAt<int128_t>(row),
          type);
      auto parsed = std::from_chars(decimal.data(),
                                    decimal.data() + decimal.size(), value);
      VELOX_CHECK(parsed.ec == std::errc() &&
                  parsed.ptr == decimal.data() + decimal.size());
    } else if (type->kind() == TypeKind::INTEGER)
      value = type->isDate() ? int64_t(d.valueAt<int32_t>(row)) * 86400000
                             : d.valueAt<int32_t>(row);
    else if (type->kind() == TypeKind::BIGINT)
      value = d.valueAt<int64_t>(row);
    else if (type->kind() == TypeKind::TIMESTAMP)
      value = d.valueAt<Timestamp>(row).toMillis();
    else if (type->kind() == TypeKind::REAL)
      value = d.valueAt<float>(row);
    else if (type->kind() == TypeKind::DOUBLE)
      value = d.valueAt<double>(row);
    else if (type->kind() == TypeKind::BOOLEAN)
      value = d.valueAt<bool>(row) ? 1.0 : 0.0;
    else
      VELOX_UNSUPPORTED("Unsupported exchange hash type {}", type->toString());
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    if (std::isnan(value))
      bits =
          0x7ff8000000000000ULL; // Java Double.doubleToLongBits canonical NaN.
    hash = murmur(reinterpret_cast<const char *>(&bits), sizeof(bits), hash);
  }
  return hash;
}

namespace {
class HashFunction final : public exec::VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx,
             VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    auto seed = std::make_unique<DecodedVector>(*args[1], rows);
    std::vector<HashKey> keys;
    keys.push_back(
        {args[0]->type(), std::make_unique<DecodedVector>(*args[0], rows)});
    rows.applyToSelected([&](vector_size_t row) {
      VELOX_USER_CHECK(!seed->isNullAt(row), "Drill hash seed cannot be NULL");
      auto value =
          hashRow(keys, row, false, uint32_t(seed->valueAt<int64_t>(row)));
      result->as<FlatVector<int32_t>>()->set(row, int32_t(value));
    });
  }
};
} // namespace
void registerHashFunctions() {
  auto signature = exec::FunctionSignatureBuilder()
                       .typeVariable("T")
                       .returnType("integer")
                       .argumentType("T")
                       .argumentType("bigint")
                       .build();
  exec::VectorFunctionMetadata metadata;
  metadata.defaultNullBehavior = false;
  exec::registerVectorFunction("drill_hash32_double", {signature},
                               std::make_unique<HashFunction>(), metadata);
}
} // namespace drill::nativeexec
