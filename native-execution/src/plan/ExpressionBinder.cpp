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
#include "plan/ExpressionBinder.h"
#include <bit>
#include <cctype>
#include <limits>
#include <unordered_map>
#include <velox/exec/Aggregate.h>
#include <velox/exec/WindowFunction.h>
#include <velox/expression/FunctionCallToSpecialForm.h>
#include <velox/expression/SignatureBinder.h>
#include <velox/functions/FunctionRegistry.h>
namespace drill::nativeexec {
using namespace facebook::velox;
using Expr = core::TypedExprPtr;
namespace {
std::string lower(std::string s) {
  for (auto &c : s)
    c = std::tolower(static_cast<unsigned char>(c));
  return s;
}
Expr castTo(TypePtr type, Expr arg) {
  if (type->equivalent(*arg->type()))
    return arg;
  if (type->equivalent(*TIME())) {
    if (arg->type()->isInteger() || arg->type()->equivalent(*BIGINT()))
      return std::make_shared<core::CallTypedExpr>(type,
          std::vector<Expr>{castTo(BIGINT(), arg)}, "drill_time_from_millis");
    if (arg->type()->isVarchar())
      return std::make_shared<core::CallTypedExpr>(type,
          std::vector<Expr>{arg}, "drill_time_from_string");
  }
  if (arg->type()->equivalent(*TIME()) &&
      (type->isInteger() || type->equivalent(*BIGINT()))) {
    auto millis = std::make_shared<core::CallTypedExpr>(BIGINT(),
        std::vector<Expr>{arg}, "drill_time_to_millis");
    return type->isInteger() ? castTo(type, millis) : millis;
  }
  if (type->equivalent(*BIGINT()) && arg->type()->equivalent(*TIME_MICRO_UTC()))
    return std::make_shared<core::CallTypedExpr>(BIGINT(),
        std::vector<Expr>{arg}, "drill_time_micro_value");
  if (type->equivalent(*TIME_MICRO_UTC()) && arg->type()->equivalent(*TIME()))
    return std::make_shared<core::CallTypedExpr>(type,
        std::vector<Expr>{arg}, "drill_time_to_micros");
  if (type->kind() == TypeKind::TIMESTAMP &&
      (arg->type()->isInteger() || arg->type()->equivalent(*BIGINT()))) {
    return std::make_shared<core::CallTypedExpr>(type,
        std::vector<Expr>{castTo(BIGINT(), arg)}, "drill_timestamp_from_millis");
  }
  return std::make_shared<core::CastTypedExpr>(type, arg, false);
}
Expr call(std::string name, std::vector<Expr> args, bool windowCall = false) {
  static const std::unordered_map<std::string, std::string> aliases = {
      {"hash32asdouble", "drill_hash32_double"},
      {"equal", "equalto"},
      {"not_equal", "notequal"},
      {"greater_than", "greaterthan"},
      {"less_than", "lessthan"},
      {"greater_than_or_equal_to", "greaterthanorequal"},
      {"less_than_or_equal_to", "lessthanorequal"},
      {"booleanand", "and"},
      {"booleanor", "or"},
      {"extractyear", "year"},
      {"extractmonth", "month"},
      {"extractday", "dayofmonth"},
      {"substr", "substring"},
      {"repeated_count", "size"},
      {"$sum0", "sum"},
      {"single_value", "min"}};
  name = lower(std::move(name));
  if (windowCall && (name == "row_number" || name == "rank" ||
      name == "dense_rank" || name == "percent_rank" || name == "cume_dist")) {
    // WindowPrel.toDrill adds a constant 1 argument to every zero-arg call.
    if (!args.empty()) {
      auto dummy = args.size() == 1
          ? std::dynamic_pointer_cast<const core::ConstantTypedExpr>(args[0]) : nullptr;
      VELOX_USER_CHECK(dummy && !dummy->value().isNull() &&
          ((dummy->type()->isInteger() && dummy->value().value<TypeKind::INTEGER>() == 1) ||
           (dummy->type()->isBigint() && dummy->value().value<TypeKind::BIGINT>() == 1)),
          "Invalid original Drill ranking placeholder");
      args.clear();
    }
  }
  bool drillDateExtract =
      name == "extractyear" || name == "extractmonth" || name == "extractday";
  if (name == "casthigh") {
    VELOX_USER_CHECK_EQ(args.size(), 1);
    if (args[0]->type()->isDecimal())
      return args[0];
    return castTo(DOUBLE(), args[0]);
  }
  if (name.starts_with("converttonullable")) {
    VELOX_USER_CHECK_EQ(args.size(), 1);
    return args[0];
  }
  if (auto it = aliases.find(name); it != aliases.end())
    name = it->second;
  if (name == "size" && args.size() == 1 && args[0]->type()->isArray())
    args.push_back(std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), Variant(false)));
  if (name == "sum" && args.size() == 1 && args[0]->type()->isVarchar()) {
    // Drill ResolverTypePrecedence chooses VARCHAR -> BIGINT for SUM
    // (cost 100), ahead of INT (101) and FLOAT8 (102).
    args[0] = castTo(BIGINT(), args[0]);
  }
  if (name == "drill_hash32_double") {
    VELOX_USER_CHECK_EQ(args.size(), 2);
    args[1] = castTo(BIGINT(), args[1]);
    return std::make_shared<core::CallTypedExpr>(INTEGER(), std::move(args),
                                                 name);
  }
  if (name == "notequal")
    return call("not", {call("equalto", std::move(args))});
  if (args.size() == 2 &&
      (name == "equalto" || name == "greaterthan" || name == "lessthan" ||
       name == "greaterthanorequal" || name == "lessthanorequal") &&
      args[0]->type()->isTime() && args[1]->type()->isTime()) {
    // Pushed Iceberg TIME filters compare microseconds before the output
    // projection normalizes to Drill's milliseconds.
    if (!args[0]->type()->equivalent(*args[1]->type())) {
      args[0] = castTo(TIME_MICRO_UTC(), args[0]);
      args[1] = castTo(TIME_MICRO_UTC(), args[1]);
    }
    // Spark's scalar comparison registry doesn't register the TIME logical
    // types. Compare physical values only here; columns keep their TIME type.
    args[0] = castTo(BIGINT(), args[0]);
    args[1] = castTo(BIGINT(), args[1]);
  }
  if (args.size() == 2 &&
      args[0]->type()->isDecimal() != args[1]->type()->isDecimal()) {
    auto index = args[0]->type()->isDecimal() ? 1 : 0;
    auto type = args[index]->type();
    if (type->isInteger())
      args[index] = castTo(DECIMAL(10, 0), args[index]);
    else if (type->isBigint())
      args[index] = castTo(DECIMAL(19, 0), args[index]);
  }
  if (args.size() == 2 && args[0]->type()->isDecimal() &&
      args[1]->type()->isDecimal()) {
    auto [ap, as] = getDecimalPrecisionScale(*args[0]->type());
    auto [bp, bs] = getDecimalPrecisionScale(*args[1]->type());
    if (name == "add" || name == "subtract" || name == "multiply" ||
        name == "divide") {
      int scale, precision;
      if (name == "multiply") {
        scale = as + bs;
        precision = ap + bp;
      } else if (name == "divide") {
        int integral = std::min(ap - as + bs, 38);
        scale = std::min(std::max(6, as + bp + 1), 38 - integral);
        precision = integral + scale;
      } else {
        scale = std::max(as, bs);
        precision = std::max(ap - as, bp - bs) + scale + 1;
      }
      if (precision > 38) {
        scale = std::max(0, scale - (precision - 38));
        precision = 38;
      }
      auto type = DECIMAL(precision, scale);
      auto zero =
          type->isShortDecimal() ? Variant(int64_t(0)) : Variant(int128_t(0));
      args.push_back(std::make_shared<core::ConstantTypedExpr>(type, zero));
      return std::make_shared<core::CallTypedExpr>(type, std::move(args),
                                                   "drill_decimal_" + name);
    }
    if (name == "equalto" || name == "greaterthan" || name == "lessthan" ||
        name == "greaterthanorequal" || name == "lessthanorequal") {
      return std::make_shared<core::CallTypedExpr>(BOOLEAN(), std::move(args),
                                                   "drill_decimal_" + name);
    }
  }
  if (name == "divide" && args.size() == 2 &&
      (args[0]->type()->isDouble() || args[1]->type()->isDouble())) {
    return std::make_shared<core::CallTypedExpr>(
        DOUBLE(),
        std::vector<Expr>{castTo(DOUBLE(), args[0]), castTo(DOUBLE(), args[1])},
        "drill_ieee_divide");
  }
  if (name == "substring") {
    VELOX_USER_CHECK(args.size() == 2 || args.size() == 3);
    auto inputs = args;
    auto nullInput = call("isnull", {args[0]});
    Expr empty =
        std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), Variant(false));
    auto zero = std::make_shared<core::ConstantTypedExpr>(INTEGER(),
                                                          Variant(int32_t(0)));
    for (size_t i = 1; i < args.size(); ++i) {
      inputs[i] = castTo(INTEGER(), inputs[i]);
      nullInput = call("or", {nullInput, call("isnull", {inputs[i]})});
      empty = call("or", {empty, call("lessthanorequal", {inputs[i], zero})});
    }
    auto value =
        std::make_shared<core::CallTypedExpr>(VARCHAR(), inputs, "substring");
    return call("if", {nullInput, core::ConstantTypedExpr::makeNull(VARCHAR()),
                       call("if", {empty,
                                   std::make_shared<core::ConstantTypedExpr>(
                                       VARCHAR(), Variant(std::string())),
                                   value})});
  }
  std::vector<TypePtr> types;
  for (auto &arg : args)
    types.push_back(arg->type());
  std::vector<TypePtr> coercions;
  TypePtr type;
  if (exec::isFunctionCallToSpecialFormRegistered(name))
    type = exec::resolveTypeForSpecialFormWithCoercions(
        name, types, coercions, TypeCoercer::defaults());
  else
    type = resolveFunctionWithCoercions(name, types, coercions,
                                        TypeCoercer::defaults());
  if (!type && windowCall && exec::getWindowFunctionSignatures(name))
    type = exec::resolveWindowResultTypeWithCoercions(
        name, types, coercions, TypeCoercer::defaults());
  if (!type) {
    if (auto signatures = exec::getAggregateFunctionSignatures(name)) {
      for (const auto &signature : *signatures) {
        exec::SignatureBinder binder(*signature, types,
                                     TypeCoercer::defaults());
        if (binder.tryBind()) {
          type = binder.tryResolveReturnType();
          break;
        }
      }
    }
  }
  if (type && name == "sum" && args.size() == 1 &&
      args[0]->type()->isDecimal()) {
    // Drill SUM exposes DECIMAL(38,s) between phases, not Spark's p+10.
    type = DECIMAL(38, getDecimalPrecisionScale(*args[0]->type()).second);
  }
  VELOX_USER_CHECK(type, "Unsupported Drill function or argument types: {}",
                   name);
  for (size_t i = 0; i < coercions.size(); ++i)
    if (coercions[i])
      args[i] = castTo(coercions[i], args[i]);
  auto result =
      std::make_shared<core::CallTypedExpr>(type, std::move(args), name);
  return drillDateExtract ? castTo(BIGINT(), result) : result;
}
class Parser {
public:
  Parser(const std::string &text, const RowTypePtr &schema, bool windowCall = false)
      : text_(text), schema_(schema), windowCall_(windowCall) {
    next();
  }
  Expr parse() {
    auto result = expr(0);
    expect("");
    return result;
  }

private:
  enum Kind { End, Word, Number, String, Field, Symbol } kind_ = End;
  const std::string &text_;
  RowTypePtr schema_;
  const bool windowCall_;
  size_t offset_ = 0;
  std::string token_;
  void next() {
    while (offset_ < text_.size() &&
           std::isspace(static_cast<unsigned char>(text_[offset_])))
      ++offset_;
    token_.clear();
    if (offset_ == text_.size()) {
      kind_ = End;
      return;
    }
    char c = text_[offset_++];
    if (c == '\'' || c == '`') {
      kind_ = c == '\'' ? String : Field;
      while (offset_ < text_.size()) {
        char d = text_[offset_++];
        if (d == '\\') {
          VELOX_USER_CHECK_LT(offset_, text_.size(),
                              "Incomplete expression escape");
          token_ += text_[offset_++];
        } else if (d == c) {
          if (offset_ < text_.size() && text_[offset_] == c) {
            token_ += c;
            ++offset_;
          } else
            return;
        } else
          token_ += d;
      }
      VELOX_USER_FAIL("Unterminated quoted Drill expression");
    }
    token_ += c;
    if (std::isdigit(static_cast<unsigned char>(c)) ||
        (c == '.' && offset_ < text_.size() &&
         std::isdigit(static_cast<unsigned char>(text_[offset_])))) {
      kind_ = Number;
      while (offset_ < text_.size()) {
        char d = text_[offset_];
        if (std::isdigit(static_cast<unsigned char>(d)) || d == '.' ||
            d == 'e' || d == 'E') {
          token_ += d;
          ++offset_;
        } else if ((d == '+' || d == '-') &&
                   (token_.back() == 'e' || token_.back() == 'E')) {
          token_ += d;
          ++offset_;
        } else
          break;
      }
    } else if (std::isalpha(static_cast<unsigned char>(c)) || c == '_' ||
               c == '$') {
      kind_ = Word;
      while (offset_ < text_.size() &&
             (std::isalnum(static_cast<unsigned char>(text_[offset_])) ||
              text_[offset_] == '_' || text_[offset_] == '$'))
        token_ += text_[offset_++];
    } else {
      kind_ = Symbol;
      if (offset_ < text_.size() &&
          ((c == '<' && (text_[offset_] == '=' || text_[offset_] == '>')) ||
           ((c == '>' || c == '!' || c == '=') && text_[offset_] == '=')))
        token_ += text_[offset_++];
    }
  }
  void expect(std::string_view s) {
    VELOX_USER_CHECK_EQ(lower(token_), s,
                        "Expected '{}' in Drill expression at offset {}", s,
                        offset_);
    next();
  }
  int precedence() const {
    auto s = lower(token_);
    if (kind_ == End || kind_ == String || kind_ == Field)
      return -1;
    if (s == "or")
      return 1;
    if (s == "and")
      return 2;
    if (s == "=" || s == "==" || s == "!=" || s == "<>" || s == "<" ||
        s == ">" || s == "<=" || s == ">=" || s == "like")
      return 3;
    if (s == "+" || s == "-")
      return 4;
    if (s == "*" || s == "/")
      return 5;
    return -1;
  }
  Expr expr(int min) {
    auto lhs = primary();
    static const std::unordered_map<std::string, std::string> ops = {
        {"+", "add"},
        {"-", "subtract"},
        {"*", "multiply"},
        {"/", "divide"},
        {"=", "equal"},
        {"==", "equal"},
        {"!=", "not_equal"},
        {"<>", "not_equal"},
        {"<", "less_than"},
        {">", "greater_than"},
        {"<=", "less_than_or_equal_to"},
        {">=", "greater_than_or_equal_to"},
        {"and", "and"},
        {"or", "or"},
        {"like", "like"}};
    while (precedence() >= min) {
      int p = precedence();
      auto op = ops.at(lower(token_));
      next();
      auto rhs = expr(p + 1);
      lhs = call(op, {lhs, rhs});
    }
    return lhs;
  }
  TypePtr type() {
    auto name = lower(token_);
    next();
    if (name == "vardecimal" || name == "decimal") {
      expect("(");
      int p = std::stoi(token_);
      next();
      expect(",");
      int s = std::stoi(token_);
      next();
      expect(")");
      return DECIMAL(p, s);
    }
    if (name == "varchar" || name == "varbinary") {
      if (token_ == "(") {
        next();
        VELOX_USER_CHECK(kind_ == Number);
        next();
        expect(")");
      }
      if (name == "varchar")
        return VARCHAR();
      return VARBINARY();
    }
    static const std::unordered_map<std::string, TypePtr> types = {
        {"bit", BOOLEAN()},     {"boolean", BOOLEAN()},
        {"tinyint", TINYINT()}, {"smallint", SMALLINT()},
        {"int", INTEGER()},     {"integer", INTEGER()},
        {"bigint", BIGINT()},   {"float4", REAL()},
        {"float8", DOUBLE()},   {"double", DOUBLE()},
        {"date", DATE()},       {"time", TIME()}, {"timestamp", TIMESTAMP()}};
    auto it = types.find(name);
    VELOX_USER_CHECK(it != types.end(), "Unsupported Drill cast type {}", name);
    return it->second;
  }
  Expr primary() {
    if (token_ == "(") {
      next();
      auto value = expr(0);
      expect(")");
      return value;
    }
    if (token_ == "-" || token_ == "+") {
      auto sign = token_;
      next();
      if (kind_ == Number) {
        auto s = sign + token_;
        next();
        return number(s);
      }
      auto value = primary();
      return sign == "+" ? value : call("negate", {value});
    }
    if (kind_ == Number) {
      auto s = token_;
      next();
      return number(s);
    }
    if (kind_ == String) {
      auto s = token_;
      next();
      return std::make_shared<core::ConstantTypedExpr>(VARCHAR(), Variant(s));
    }
    auto name = token_;
    auto k = kind_;
    next();
    auto n = lower(name);
    if (k == Word && n == "cast") {
      expect("(");
      auto value = expr(0);
      expect("as");
      auto target = type();
      expect(")");
      // Drill DATE constants serialize as UTC milliseconds, while Velox stores
      // days.
      if (target->isDate() &&
          (value->type()->isBigint() || value->type()->isInteger())) {
        auto constant =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(value);
        VELOX_USER_CHECK(constant,
                         "Drill numeric DATE casts require a constant");
        int64_t millis = value->type()->isInteger()
                             ? constant->value().value<TypeKind::INTEGER>()
                             : constant->value().value<TypeKind::BIGINT>();
        VELOX_USER_CHECK_EQ(millis % 86400000, 0,
                            "DATE constant must represent midnight UTC");
        auto days = millis / 86400000;
        VELOX_USER_CHECK(days >= INT32_MIN && days <= INT32_MAX);
        return std::make_shared<core::ConstantTypedExpr>(
            DATE(), Variant(static_cast<int32_t>(days)));
      }
      return castTo(target, value);
    }
    if (k == Word && (n == "true" || n == "false"))
      return std::make_shared<core::ConstantTypedExpr>(BOOLEAN(),
                                                       Variant(n == "true"));
    if (k == Word && n == "null")
      return core::ConstantTypedExpr::makeNull(UNKNOWN());
    if (k == Word && token_ == "(") {
      next();
      std::vector<Expr> args;
      if (token_ != ")") {
        while (true) {
          args.push_back(expr(0));
          if (token_ != ",")
            break;
          next();
        }
      }
      expect(")");
      if (n == "if" && lower(token_) == "then") {
        VELOX_USER_CHECK_EQ(args.size(), 1);
        next();
        args.push_back(primary());
        expect("else");
        args.push_back(primary());
        expect("end");
      }
      return call(name, std::move(args), windowCall_);
    }
    VELOX_USER_CHECK(k == Field || k == Word, "Expected expression, got {}",
                     name);
    VELOX_USER_CHECK(schema_->containsChild(name),
                     "Unknown Drill field {} in {}", name, schema_->toString());
    Expr value = std::make_shared<core::FieldAccessTypedExpr>(schema_->findChild(name), name);
    while (token_ == "." || token_ == "[") {
      if (token_ == "[") {
        VELOX_USER_CHECK(value->type()->isArray() || value->type()->isMap(), "Index access requires Drill array/DICT, got {}", value->type()->toString());
        next();
        auto index = expr(0);
        expect("]");
        // Spark get has Drill's zero-based, NULL-on-out-of-range semantics.
        value = value->type()->isMap()
            ? call("drill_dict_get", {value, castTo(value->type()->childAt(0), index)})
            : call("get", {value, castTo(INTEGER(), index)});
        continue;
      }
      next();
      VELOX_USER_CHECK(kind_ == Field || kind_ == Word, "Expected nested Drill field");
      auto child = token_;
      next();
      if (value->type()->isMap()) {
        auto keyType = value->type()->childAt(0);
        if (keyType->isReal() || keyType->isDouble()) {
          // Velox constant CSE treats -0 and +0 as equal. Keep the original
          // named key as integer IEEE bits so DICT's Java equality survives.
          size_t consumed;
          int64_t bits = keyType->isReal()
              ? int64_t(std::bit_cast<uint32_t>(std::stof(child, &consumed)))
              : std::bit_cast<int64_t>(std::stod(child, &consumed));
          VELOX_USER_CHECK_EQ(consumed, child.size(), "Invalid floating DICT key");
          auto key = std::make_shared<core::ConstantTypedExpr>(BIGINT(), Variant(bits));
          value = call("drill_dict_get_bits", {value, key});
          continue;
        }
        auto key = std::make_shared<core::ConstantTypedExpr>(VARCHAR(), Variant(child));
        value = call("drill_dict_get", {value, castTo(value->type()->childAt(0), key)});
        continue;
      }
      VELOX_USER_CHECK(value->type()->isRow(), "Nested access requires Drill MAP, got {}", value->type()->toString());
      const auto &row = value->type()->asRow();
      VELOX_USER_CHECK(row.containsChild(child), "Unknown nested Drill field {} in {}", child, row.toString());
      value = std::make_shared<core::DereferenceTypedExpr>(row.findChild(child), value, row.getChildIdx(child));
    }
    return value;
  }
  Expr number(const std::string &s) {
    if (s.find_first_of(".eE") != std::string::npos)
      return std::make_shared<core::ConstantTypedExpr>(DOUBLE(),
                                                       Variant(std::stod(s)));
    auto value = std::stoll(s);
    if (value >= INT32_MIN && value <= INT32_MAX)
      return std::make_shared<core::ConstantTypedExpr>(INTEGER(),
                                                       Variant(int32_t(value)));
    return std::make_shared<core::ConstantTypedExpr>(BIGINT(),
                                                     Variant(int64_t(value)));
  }
};
} // namespace
core::TypedExprPtr bindExpression(const std::string &text,
                                  const RowTypePtr &schema) {
  return Parser(text, schema).parse();
}
core::TypedExprPtr bindWindowExpression(const std::string &text,
                                       const RowTypePtr &schema) {
  return Parser(text, schema, true).parse();
}
} // namespace drill::nativeexec
