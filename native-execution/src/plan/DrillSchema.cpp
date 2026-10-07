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
#include "plan/DrillSchema.h"
#include "columnar/DrillBatchMetadata.h"
#include "columnar/DrillRowType.h"
#include "protocol/Protobuf.h"
#include <algorithm>
#include <folly/base64.h>
namespace drill::nativeexec {
namespace {
using Expr = core::TypedExprPtr;
folly::dynamic defaultField(const TypePtr &type) {
  // UNKNOWN is the typed NULL/error intermediate; FUNCTION is a lambda
  // argument. Neither is a wire column, but both participate in expressions.
  if (type->kind() == TypeKind::UNKNOWN || type->kind() == TypeKind::FUNCTION)
    return folly::dynamic::object("name", "value")("minor", "LATE")("optional",
                                                                    true);
  return fieldsFromType(ROW({"value"}, {type}))[0];
}
void nullable(folly::dynamic &field, bool optional) {
  if (field["optional"].asBool() != optional && field.count("type")) {
    auto original = folly::base64Decode(field["type"].asString());
    protocol::Message type(original);
    protocol::Writer out;
    for (const auto &f : type.fields()) {
      if (f.number == 2)
        continue;
      if (f.wire == 0)
        out.integer(f.number, f.value);
      else if (f.wire == 1)
        out.fixed64(f.number, f.value);
      else if (f.wire == 2)
        out.bytes(f.number, f.bytes);
      else
        VELOX_USER_FAIL("Unsupported MajorType wire kind {}", f.wire);
    }
    out.integer(2, field.getDefault("repeated", false).asBool() ? 2
                   : optional                                   ? 0
                                                                : 1);
    field["type"] = folly::base64Encode(out.data());
  }
  field["optional"] = optional;
}
folly::dynamic expressionField(const Expr &expr, const RowTypePtr &input) {
  if (auto field =
          dynamic_cast<const core::FieldAccessTypedExpr *>(expr.get())) {
    if (field->inputs().empty() || field->isInputColumn())
      return fieldsFromType(input)[input->getChildIdx(field->name())];
  }
  if (auto field =
          dynamic_cast<const core::DereferenceTypedExpr *>(expr.get())) {
    auto parent = expressionField(expr->inputs()[0], input);
    if (parent.count("children")) {
      auto child = parent["children"][field->index()];
      if (parent["optional"].asBool())
        nullable(child, true);
      return child;
    }
  }
  if (dynamic_cast<const core::LambdaTypedExpr *>(expr.get()))
    return defaultField(expr->type());
  auto result = defaultField(expr->type());
  std::vector<folly::dynamic> args;
  for (auto &arg : expr->inputs())
    args.push_back(expressionField(arg, input));
  bool optional = true;
  if (auto constant = dynamic_cast<const core::ConstantTypedExpr *>(expr.get()))
    optional = constant->isNull();
  else if (auto cast = dynamic_cast<const core::CastTypedExpr *>(expr.get()))
    optional = cast->isTryCast() || args[0]["optional"].asBool();
  else if (auto call = dynamic_cast<const core::CallTypedExpr *>(expr.get())) {
    auto name = call->name();
    // A missing index/key returns NULL even when the container and its
    // elements are REQUIRED. Keep element attributes without that mode.
    if (name == "get" && args[0].count("element"))
      result = args[0]["element"];
    else if ((name == "drill_dict_get" || name == "drill_dict_get_bits") &&
             args[0].count("children"))
      result = args[0]["children"][1];
    auto anyNull = [&] {
      return std::any_of(args.begin(), args.end(), [](const auto &arg) {
        return arg["optional"].asBool();
      });
    };
    if (name == "if")
      optional = args[1]["optional"].asBool() || args[2]["optional"].asBool();
    else if (name == "coalesce")
      optional = std::all_of(args.begin(), args.end(), [](const auto &arg) {
        return arg["optional"].asBool();
      });
    else if (name == "isnull" || name == "isnotnull" || name == "count" ||
             name == "row_number" || name == "rank" || name == "dense_rank" ||
             name == "drill_dict" || name == "row_constructor")
      optional = false;
    else if (name == "sum" || name == "min" || name == "max" || name == "avg" ||
             name == "first" || name == "last" || name == "single_value" ||
             name == "raise_error" || name == "try" || name == "element_at" ||
             name == "subscript" || name == "get" ||
             name == "drill_dict_get" || name == "drill_dict_get_bits" ||
             name == "divide" ||
             name.starts_with("drill_decimal_divide"))
      optional = true;
    else
      optional = anyNull();
  }
  nullable(result, optional);
  return result;
}
folly::dynamic passField(const std::string &name,
                         const std::vector<core::PlanNodePtr> &sources,
                         const folly::dynamic &fallback) {
  for (auto &source : sources) {
    auto fields = fieldsFromType(source->outputType());
    for (auto &field : fields)
      if (field["name"] == name)
        return field;
  }
  return fallback;
}
} // namespace
void preserveDrillSchema(const core::PlanNodePtr &plan) {
  if (auto known = drillRowField(plan->outputType()); known && known->isArray())
    return;
  for (auto &source : plan->sources())
    preserveDrillSchema(source);
  auto fields = fieldsFromType(plan->outputType());
  if (auto project = dynamic_cast<const core::ProjectNode *>(plan.get())) {
    for (size_t i = 0; i < fields.size(); ++i)
      fields[i] = expressionField(project->projections()[i],
                                  plan->sources()[0]->outputType());
  } else if (auto aggregate =
                 dynamic_cast<const core::AggregationNode *>(plan.get())) {
    auto input = plan->sources()[0]->outputType();
    auto groups = aggregate->groupingKeys().size();
    for (size_t i = 0; i < groups; ++i)
      fields[i] = expressionField(aggregate->groupingKeys()[i], input);
    for (size_t i = groups; i < fields.size(); ++i)
      fields[i] =
          expressionField(aggregate->aggregates()[i - groups].call, input);
  } else if (auto window = dynamic_cast<const core::WindowNode *>(plan.get())) {
    auto input = plan->sources()[0]->outputType();
    for (size_t i = 0; i < input->size(); ++i)
      fields[i] = fieldsFromType(input)[i];
    for (size_t i = input->size(); i < fields.size(); ++i)
      fields[i] = expressionField(
          window->windowFunctions()[i - input->size()].functionCall, input);
  } else {
    for (size_t i = 0; i < fields.size(); ++i)
      fields[i] =
          passField(plan->outputType()->nameOf(i), plan->sources(), fields[i]);
    if (plan->sources().size() > 1 &&
        dynamic_cast<const core::LocalPartitionNode *>(plan.get())) {
      for (auto &source : plan->sources()) {
        auto input = fieldsFromType(source->outputType());
        for (size_t i = 0; i < fields.size(); ++i)
          if (input[i]["optional"].asBool())
            nullable(fields[i], true);
      }
    }
    auto widenJoin = [&](core::JoinType type) {
      for (size_t i = 0; i < fields.size(); ++i) {
        auto name = plan->outputType()->nameOf(i);
        auto leftFields = fieldsFromType(plan->sources()[0]->outputType());
        bool left =
            std::any_of(leftFields.begin(), leftFields.end(),
                        [&](const auto &f) { return f["name"] == name; });
        if (type == core::JoinType::kFull ||
            (left && type == core::JoinType::kRight) ||
            (!left && type == core::JoinType::kLeft))
          nullable(fields[i], true);
      }
    };
    if (auto join = dynamic_cast<const core::HashJoinNode *>(plan.get()))
      widenJoin(join->joinType());
    if (auto join = dynamic_cast<const core::NestedLoopJoinNode *>(plan.get()))
      widenJoin(join->joinType());
    if (auto unnest = dynamic_cast<const core::UnnestNode *>(plan.get())) {
      auto input = fieldsFromType(plan->sources()[0]->outputType());
      for (size_t i = 0; i < unnest->unnestVariables().size(); ++i) {
        auto index = plan->sources()[0]->outputType()->getChildIdx(
            unnest->unnestVariables()[i]->name());
        auto output = plan->outputType()->getChildIdx(unnest->unnestNames()[i]);
        fields[output] = input[index]["element"];
      }
    }
  }
  for (size_t i = 0; i < fields.size(); ++i)
    fields[i]["name"] = plan->outputType()->nameOf(i);
  annotateDrillSchema(plan->outputType(), std::move(fields));
}
} // namespace drill::nativeexec
