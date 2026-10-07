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
#include "velox/functions/DrillNumeric.h"
#include <velox/expression/FunctionSignature.h>
#include <velox/expression/VectorFunction.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
namespace drill::nativeexec {
using namespace facebook::velox;
namespace {
class Divide final : public exec::VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx,
             VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    DecodedVector left(*args[0], rows), right(*args[1], rows);
    rows.applyToSelected([&](vector_size_t row) {
      // Java FLOAT8 division preserves IEEE infinity, NaN and signed zero.
      result->as<FlatVector<double>>()->set(
          row, left.valueAt<double>(row) / right.valueAt<double>(row));
    });
  }
};
class TimestampMillis final : public exec::VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx, VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    DecodedVector input(*args[0], rows);
    auto *output = result->as<FlatVector<Timestamp>>();
    rows.applyToSelected([&](vector_size_t row) {
      auto value = input.valueAt<Timestamp>(row);
      output->set(row, Timestamp(value.getSeconds(),
          value.getNanos() / 1000000 * 1000000));
    });
  }
};
class TimestampFromMillis final : public exec::VectorFunction {
public:
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx, VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    DecodedVector input(*args[0], rows);
    auto *output = result->as<FlatVector<Timestamp>>();
    rows.applyToSelected([&](vector_size_t row) {
      auto value = input.valueAt<int64_t>(row);
      auto seconds = value / 1000, remainder = value % 1000;
      if (remainder < 0) { --seconds; remainder += 1000; }
      output->set(row, Timestamp(seconds, remainder * 1000000));
    });
  }
};
enum class TimeConversion { FromMillis, FromString, ToMillis, ToMicros, Normalize };
class TimeConvert final : public exec::VectorFunction {
public:
  explicit TimeConvert(TimeConversion conversion) : conversion_(conversion) {}
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx, VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    DecodedVector input(*args[0], rows);
    auto *output = result->as<FlatVector<int64_t>>();
    rows.applyToSelected([&](vector_size_t row) {
      int64_t value = conversion_ == TimeConversion::FromString
          ? TIME()->valueToTime(input.valueAt<StringView>(row))
          : input.valueAt<int64_t>(row);
      if (conversion_ == TimeConversion::FromMillis)
        VELOX_USER_CHECK(value >= INT32_MIN && value <= INT32_MAX,
                         "TIME does not fit Drill's 32-bit millisecond value");
      if (conversion_ == TimeConversion::ToMicros) {
        VELOX_USER_CHECK(value >= INT32_MIN && value <= INT32_MAX,
                         "TIME does not fit Drill's 32-bit millisecond value");
        value *= 1000;
      }
      if (conversion_ == TimeConversion::Normalize) {
        VELOX_USER_CHECK(value >= 0 && value < 86400000000LL,
                         "Iceberg TIME is outside a day");
        // DateUtilities.toDrillTime rounds LocalTime to milliseconds. Keep
        // 86400000 at the last half-millisecond, exactly like the Java reader.
        value = (value + 500) / 1000;
      }
      output->set(row, value);
    });
  }
private:
  TimeConversion conversion_;
};
} // namespace
void registerNumericFunctions() {
  const auto time = [&](const char *name, const char *input, const char *output,
                        TimeConversion conversion) {
    exec::registerVectorFunction(name,
        {exec::FunctionSignatureBuilder().returnType(output).argumentType(input).build()},
        std::make_unique<TimeConvert>(conversion));
  };
  time("drill_time_from_millis", "bigint", "time", TimeConversion::FromMillis);
  time("drill_time_from_string", "varchar", "time", TimeConversion::FromString);
  time("drill_time_to_millis", "time", "bigint", TimeConversion::ToMillis);
  time("drill_time_to_micros", "time", "time micro utc", TimeConversion::ToMicros);
  time("drill_time_micro_value", "time micro utc", "bigint", TimeConversion::ToMillis);
  time("drill_time_millis", "time micro utc", "time", TimeConversion::Normalize);
  exec::registerVectorFunction("drill_timestamp_from_millis",
      {exec::FunctionSignatureBuilder().returnType("timestamp")
           .argumentType("bigint").build()}, std::make_unique<TimestampFromMillis>());
  exec::registerVectorFunction("drill_timestamp_millis",
      {exec::FunctionSignatureBuilder().returnType("timestamp")
           .argumentType("timestamp").build()}, std::make_unique<TimestampMillis>());
  exec::registerVectorFunction("drill_ieee_divide",
                               {exec::FunctionSignatureBuilder()
                                    .returnType("double")
                                    .argumentType("double")
                                    .argumentType("double")
                                    .build()},
                               std::make_unique<Divide>());
}
} // namespace drill::nativeexec
