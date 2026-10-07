/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/DrillDecimal.h"
#include <array>
#include <boost/multiprecision/cpp_int.hpp>
#include <velox/expression/DecodedArgs.h>
#include <velox/expression/VectorFunction.h>
#include <velox/vector/FlatVector.h>

namespace drill::nativeexec {
using namespace facebook::velox;
using boost::multiprecision::cpp_int;
namespace {
const cpp_int &power(int n) {
  // Decimal operands have <=38 digits; intermediate division scale can
  // exceed 38.
  static const auto powers = [] {
    std::array<cpp_int, 160> values;
    values[0] = 1;
    for (size_t i = 1; i < values.size(); ++i)
      values[i] = values[i - 1] * 10;
    return values;
  }();
  VELOX_CHECK_GE(n, 0);
  VELOX_CHECK_LT(n, powers.size());
  return powers[n];
}
cpp_int magnitude(const cpp_int &v) { return v < 0 ? -v : v; }
int digits(const cpp_int &v) {
  auto a = magnitude(v);
  int low = 1, high = 159;
  while (low < high) {
    int mid = (low + high) / 2;
    if (a < power(mid))
      high = mid;
    else
      low = mid + 1;
  }
  return low;
}
cpp_int roundedDivide(cpp_int n, cpp_int d) {
  VELOX_USER_CHECK(d != 0, "Division by zero");
  bool negative = (n < 0) != (d < 0);
  n = magnitude(n);
  d = magnitude(d);
  cpp_int q = n / d;
  cpp_int rem = n % d;
  if (rem * 2 >= d)
    ++q;
  return negative ? -q : q;
}
cpp_int rescale(cpp_int value, int from, int to) {
  if (to >= from)
    return value * power(to - from);
  return roundedDivide(std::move(value), power(from - to));
}

class DrillDecimal : public exec::VectorFunction {
public:
  DrillDecimal(const std::string &name,
               const std::vector<exec::VectorFunctionArg> &args,
               const core::QueryConfig &)
      : operation_(name.substr(std::string("drill_decimal_").size())) {
    auto a = getDecimalPrecisionScale(*args[0].type);
    auto b = getDecimalPrecisionScale(*args[1].type);
    auto out = getDecimalPrecisionScale(*args[2].type);
    aScale_ = a.second;
    bScale_ = b.second;
    precision_ = out.first;
    scale_ = out.second;
  }
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &resultType, exec::EvalCtx &ctx,
             VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, resultType, ctx.pool(), result);
    exec::DecodedArgs decoded(rows, args, ctx);
    auto valueAt = [&](int arg, vector_size_t row) -> cpp_int {
      auto *v = decoded.at(arg);
      if (args[arg]->type()->isShortDecimal())
        return cpp_int(v->valueAt<int64_t>(row));
      return cpp_int(v->valueAt<int128_t>(row));
    };
    ctx.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
      cpp_int a = valueAt(0, row), b = valueAt(1, row), value;
      int intermediateScale;
      if (operation_ == "divide") {
        VELOX_USER_CHECK(b != 0, "Division by zero");
        if (a == 0) {
          value = 0;
          intermediateScale = scale_;
        } else {
          auto n = magnitude(a), d = magnitude(b);
          int exponent = digits(n) - digits(d);
          bool below = exponent >= 0 ? n < d * power(exponent)
                                     : n * power(-exponent) < d;
          if (below)
            --exponent;
          // MathContext(precision, HALF_UP), before setScale(scale, HALF_UP).
          intermediateScale = precision_ - 1 - (exponent - aScale_ + bScale_);
          int shift = intermediateScale + bScale_ - aScale_;
          if (shift >= 0)
            a *= power(shift);
          else
            b *= power(-shift);
          value = roundedDivide(std::move(a), std::move(b));
        }
      } else {
        if (operation_ == "multiply") {
          value = a * b;
          intermediateScale = aScale_ + bScale_;
        } else {
          intermediateScale = std::max(aScale_, bScale_);
          a *= power(intermediateScale - aScale_);
          b *= power(intermediateScale - bScale_);
          if (operation_ == "add")
            value = a + b;
          else
            value = a - b;
        }
        int drop = digits(value) - precision_;
        if (drop > 0) {
          value = roundedDivide(std::move(value), power(drop));
          intermediateScale -= drop;
        }
      }
      value = rescale(std::move(value), intermediateScale, scale_);
      VELOX_USER_CHECK(magnitude(value) < power(precision_),
                       "Decimal overflow");
      if (resultType->isShortDecimal())
        result->asUnchecked<FlatVector<int64_t>>()->set(
            row, value.convert_to<int64_t>());
      else
        result->asUnchecked<FlatVector<int128_t>>()->set(
            row, value.convert_to<int128_t>());
    });
  }

private:
  const std::string operation_;
  int aScale_, bScale_, precision_, scale_;
};
class DrillDecimalComparison final : public exec::VectorFunction {
public:
  DrillDecimalComparison(const std::string &name,
                         const std::vector<exec::VectorFunctionArg> &args,
                         const core::QueryConfig &)
      : operation_(name.substr(std::string("drill_decimal_").size())) {
    int a = getDecimalPrecisionScale(*args[0].type).second;
    int b = getDecimalPrecisionScale(*args[1].type).second;
    aFactor_ = power(std::max(a, b) - a);
    bFactor_ = power(std::max(a, b) - b);
  }
  void apply(const SelectivityVector &rows, std::vector<VectorPtr> &args,
             const TypePtr &type, exec::EvalCtx &ctx,
             VectorPtr &result) const override {
    BaseVector::ensureWritable(rows, type, ctx.pool(), result);
    exec::DecodedArgs decoded(rows, args, ctx);
    rows.applyToSelected([&](vector_size_t row) {
      cpp_int a = args[0]->type()->isShortDecimal()
                      ? cpp_int(decoded.at(0)->valueAt<int64_t>(row))
                      : cpp_int(decoded.at(0)->valueAt<int128_t>(row));
      cpp_int b = args[1]->type()->isShortDecimal()
                      ? cpp_int(decoded.at(1)->valueAt<int64_t>(row))
                      : cpp_int(decoded.at(1)->valueAt<int128_t>(row));
      a *= aFactor_;
      b *= bFactor_;
      bool value = operation_ == "equalto"           ? a == b
                   : operation_ == "lessthan"        ? a < b
                   : operation_ == "greaterthan"     ? a > b
                   : operation_ == "lessthanorequal" ? a <= b
                                                     : a >= b;
      result->as<FlatVector<bool>>()->set(row, value);
    });
  }

private:
  std::string operation_;
  cpp_int aFactor_, bFactor_;
};
} // namespace
void registerDecimalFunctions() {
  // The third argument is a typed zero carrying Drill's inferred result
  // precision/scale.
  auto signature = exec::FunctionSignatureBuilder()
                       .integerVariable("ap")
                       .integerVariable("as")
                       .integerVariable("bp")
                       .integerVariable("bs")
                       .integerVariable("rp")
                       .integerVariable("rs")
                       .returnType("decimal(rp, rs)")
                       .argumentType("decimal(ap, as)")
                       .argumentType("decimal(bp, bs)")
                       .argumentType("decimal(rp, rs)")
                       .build();
  for (const auto &op : {"add", "subtract", "multiply", "divide"}) {
    exec::registerStatefulVectorFunction(
        std::string("drill_decimal_") + op, {signature},
        exec::makeVectorFunctionFactory<DrillDecimal>());
  }
  auto comparison = exec::FunctionSignatureBuilder()
                        .integerVariable("ap")
                        .integerVariable("as")
                        .integerVariable("bp")
                        .integerVariable("bs")
                        .returnType("boolean")
                        .argumentType("decimal(ap,as)")
                        .argumentType("decimal(bp,bs)")
                        .build();
  for (auto op : {"equalto", "lessthan", "greaterthan", "lessthanorequal",
                  "greaterthanorequal"})
    exec::registerStatefulVectorFunction(
        std::string("drill_decimal_") + op, {comparison},
        exec::makeVectorFunctionFactory<DrillDecimalComparison>());
}
} // namespace drill::nativeexec
