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
#include "velox/functions/DrillDict.h"
#include <bit>
#include <cmath>
#include <velox/expression/VectorFunction.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
namespace drill::nativeexec {
using namespace facebook::velox;
namespace {
class DictGet final : public exec::VectorFunction {
public:
  explicit DictGet(bool bits = false) : bits_(bits) {}
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx,
             VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    DecodedVector maps(*args[0], rows), requested(*args[1], rows);
    auto *map = maps.base()->as<MapVector>();
    DecodedVector keys(*map->mapKeys());
    std::vector<vector_size_t> indices(rows.end());
    SelectivityVector matched(rows.end(), false);
    rows.applyToSelected([&](vector_size_t row) {
      result->setNull(row, true);
      if (maps.isNullAt(row) || requested.isNullAt(row))
        return;
      auto parent = maps.index(row);
      const int64_t start = map->offsetAt(parent), size = map->sizeAt(parent);
      VELOX_USER_CHECK(start >= 0 && size >= 0 &&
                           start + size <= map->mapKeys()->size() &&
                           start + size <= map->mapValues()->size(),
                       "Invalid DICT lookup range");
      // SingleDictReaderImpl.find searches backwards: the last duplicate wins.
      for (int64_t index = start + size; index-- > start;) {
        VELOX_USER_CHECK(!keys.isNullAt(index), "NULL DICT key");
        bool equal;
        if (bits_) {
          auto bits = requested.valueAt<int64_t>(row);
          if (map->mapKeys()->type()->isReal()) {
            auto left = keys.valueAt<float>(index);
            auto right = std::bit_cast<float>(uint32_t(bits));
            equal = (std::isnan(left) && std::isnan(right)) ||
                    (left == right &&
                     (left != 0 || std::signbit(left) == std::signbit(right)));
          } else {
            VELOX_USER_CHECK(map->mapKeys()->type()->isDouble());
            auto left = keys.valueAt<double>(index);
            auto right = std::bit_cast<double>(bits);
            equal = (std::isnan(left) && std::isnan(right)) ||
                    (left == right &&
                     (left != 0 || std::signbit(left) == std::signbit(right)));
          }
        } else
          equal = map->mapKeys()->equalValueAt(args[1].get(), index, row);
        // Java Float/Double.equals distinguishes signed zero; Velox == does
        // not.
        if (equal && args[1]->type()->isReal()) {
          auto left = keys.valueAt<float>(index),
               right = requested.valueAt<float>(row);
          if (left == 0 && right == 0)
            equal = std::signbit(left) == std::signbit(right);
        } else if (equal && args[1]->type()->isDouble()) {
          auto left = keys.valueAt<double>(index),
               right = requested.valueAt<double>(row);
          if (left == 0 && right == 0)
            equal = std::signbit(left) == std::signbit(right);
        }
        if (equal) {
          matched.setValid(row, true);
          indices[row] = index;
          break;
        }
      }
    });
    matched.updateBounds();
    if (matched.hasSelections())
      result->copy(map->mapValues().get(), matched, indices.data());
  }

private:
  const bool bits_;
};
} // namespace
void registerDictFunctions() {
  auto signature = exec::FunctionSignatureBuilder()
                       .typeVariable("K")
                       .typeVariable("V")
                       .returnType("V")
                       .argumentType("map(K,V)")
                       .argumentType("K")
                       .build();
  exec::registerVectorFunction("drill_dict_get", {signature},
                               std::make_unique<DictGet>());
  std::vector<std::shared_ptr<exec::FunctionSignature>> bits;
  for (const auto &key : {"real", "double"})
    bits.push_back(exec::FunctionSignatureBuilder()
                       .typeVariable("V")
                       .returnType("V")
                       .argumentType(std::string("map(") + key + ",V)")
                       .argumentType("bigint")
                       .build());
  exec::registerVectorFunction("drill_dict_get_bits", bits,
                               std::make_unique<DictGet>(true));
}
} // namespace drill::nativeexec
