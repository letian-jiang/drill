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
#include "plan/FragmentPlanConverter.h"
#include "plan/ExpressionBinder.h"
#include "plan/DrillSchema.h"
#include "columnar/DrillRowType.h"
#include <unordered_set>
#include <velox/exec/HashPartitionFunction.h>
#include <velox/exec/PartitionFunction.h>
namespace drill::nativeexec {
namespace {
std::string fieldName(std::string name) {
  if (name.size() > 1 && name.front() == '`' && name.back() == '`')
    name = name.substr(1, name.size() - 2);
  return name;
}
core::FieldAccessTypedExprPtr field(core::TypedExprPtr expression) {
  auto result =
      std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(expression);
  VELOX_USER_CHECK(result, "Drill key must be a materialized field: {}",
                   expression->toString());
  return result;
}
core::PlanNodePtr gather(const std::string &id, core::PlanNodePtr input) {
  return std::make_shared<core::LocalPartitionNode>(
      id, core::LocalPartitionNode::Type::kGather, false,
      std::make_shared<core::GatherPartitionFunctionSpec>(),
      std::vector<core::PlanNodePtr>{input});
}
core::JoinType joinType(const folly::dynamic &node) {
  auto name = node["joinType"].asString();
  if (node.getDefault("semiJoin", false).asBool() || name == "SEMI")
    return core::JoinType::kLeftSemiFilter;
  if (name == "INNER")
    return core::JoinType::kInner;
  if (name == "LEFT")
    return core::JoinType::kLeft;
  if (name == "RIGHT")
    return core::JoinType::kRight;
  if (name == "FULL")
    return core::JoinType::kFull;
  if (name == "ANTI")
    return core::JoinType::kAnti;
  VELOX_USER_FAIL("Unsupported Drill join type {}", name);
}
RowTypePtr combined(RowTypePtr first, RowTypePtr second) {
  auto names = first->names();
  auto types = first->children();
  std::unordered_set<std::string> seen(names.begin(), names.end());
  for (size_t i = 0; i < second->size(); ++i) {
    auto name = second->nameOf(i);
    VELOX_USER_CHECK(seen.insert(name).second, "Ambiguous Drill join field {}",
                     name);
    names.push_back(name);
    types.push_back(second->childAt(i));
  }
  return ROW(std::move(names), std::move(types));
}
} // namespace
core::TypedExprPtr FragmentPlanConverter::expression(std::string text,
                                                     const RowTypePtr &schema) {
  return bindExpression(text, schema);
}
core::PlanNodePtr FragmentPlanConverter::convert(const folly::dynamic &node) {
  auto result = convertImpl(node);
  preserveDrillSchema(result);
  return result;
}
core::PlanNodePtr FragmentPlanConverter::convertImpl(const folly::dynamic &node) {
  VELOX_USER_CHECK(node.isObject(),
                   "Expected original minor fragment operator object");
  auto pop = node["pop"].asString();
  auto id = "drill_" + std::to_string(node["@id"].asInt());
  if (pop == "unordered-receiver" || pop == "merging-receiver" ||
      pop.ends_with("-scan") || node.count("nativeScan") ||
      node.count("jniScan")) {
    auto binding = source_(node);
    core::PlanNodePtr source = std::make_shared<DrillSourceNode>(
        id, binding.schema, binding.kind, std::move(binding.factory));
    if (node.count("nativeScan")) {
      const auto &scan = node["nativeScan"];
      if (scan.count("filter") && !scan["filter"].isNull())
        source = std::make_shared<core::FilterNode>(
            id + "_filter",
            expression(scan["filter"].asString(), source->outputType()),
            source);
      std::vector<std::string> names;
      std::vector<core::TypedExprPtr> values;
      for (const auto &column : scan["outputFields"]) {
        auto name = column["name"].asString();
        names.push_back(name);
        core::TypedExprPtr value = std::make_shared<core::FieldAccessTypedExpr>(
            source->outputType()->findChild(name), name);
        if (binding.normalizeOutput)
          value = binding.normalizeOutput(std::move(value), column, pool_);
        values.push_back(std::move(value));
      }
      source = std::make_shared<core::ProjectNode>(id + "_projection", names,
                                                   values, source);
      annotateDrillSchema(source->outputType(), scan["outputFields"]);
    }
    if (pop == "merging-receiver") {
      std::vector<core::FieldAccessTypedExprPtr> keys;
      std::vector<core::SortOrder> orders;
      for (auto &order : node["orderings"]) {
        keys.push_back(
            field(expression(order["expr"].asString(), source->outputType())));
        orders.emplace_back(
            order["order"].asString() == "ASC",
            order.getDefault("nullDirection", "LAST").asString() == "FIRST");
      }
      // Preserve receiver ordering across concurrent input drivers.
      source = std::make_shared<core::OrderByNode>(
          id + "_merge", keys, orders, false, gather(id + "_gather", source));
    }
    return source;
  }
  if (pop == "hash-join" || pop == "merge-join" || pop == "nested-loop-join") {
    auto left = convert(node["left"]), right = convert(node["right"]);
    auto type = joinType(node);
    if (pop == "nested-loop-join") {
      auto output = combined(left->outputType(), right->outputType());
      auto filter = expression(node["condition"].asString(), output);
      return std::make_shared<core::NestedLoopJoinNode>(id, type, filter, left,
                                                        right, output);
    }
    std::vector<core::FieldAccessTypedExprPtr> leftKeys, rightKeys;
    for (auto &condition : node["conditions"]) {
      auto relationship = condition["relationship"].asString();
      VELOX_USER_CHECK(relationship == "==" || relationship == "equal" ||
                           relationship == "EQUALS",
                       "Unsupported join relationship {}", relationship);
      leftKeys.push_back(
          field(expression(condition["left"].asString(), left->outputType())));
      rightKeys.push_back(field(
          expression(condition["right"].asString(), right->outputType())));
      VELOX_USER_CHECK(
          leftKeys.back()->type()->equivalent(*rightKeys.back()->type()),
          "Join key types must agree");
    }
    // Drill hash/merge joins expose build columns before probe columns.
    auto output =
        type == core::JoinType::kLeftSemiFilter || type == core::JoinType::kAnti
            ? left->outputType()
            : combined(right->outputType(), left->outputType());
    return std::make_shared<core::HashJoinNode>(
        id, type, false, leftKeys, rightKeys, nullptr, left, right, output);
  }
  if (pop == "union-all") {
    std::vector<core::PlanNodePtr> children;
    for (auto &child : node["children"])
      children.push_back(convert(child));
    VELOX_USER_CHECK(!children.empty());
    auto names = children[0]->outputType()->names();
    // Give the union its own output RowType. Widening its field metadata must
    // not mutate the first input's source schema or its REQUIRED wire layout.
    for (size_t n = 0; n < children.size(); ++n) {
      auto schema = children[n]->outputType();
      VELOX_USER_CHECK_EQ(schema->size(), names.size());
      std::vector<core::TypedExprPtr> expressions;
      for (size_t i = 0; i < names.size(); ++i) {
        auto target = children[0]->outputType()->childAt(i);
        auto value = std::make_shared<core::FieldAccessTypedExpr>(
            schema->childAt(i), schema->nameOf(i));
        if (target->equivalent(*value->type()))
          expressions.push_back(value);
        else
          expressions.push_back(
              std::make_shared<core::CastTypedExpr>(target, value, false));
      }
      children[n] = std::make_shared<core::ProjectNode>(
          id + "_input_" + std::to_string(n), names, expressions, children[n]);
    }
    return std::make_shared<core::LocalPartitionNode>(
        id, core::LocalPartitionNode::Type::kGather, false,
        std::make_shared<core::GatherPartitionFunctionSpec>(),
        std::move(children));
  }
  VELOX_USER_CHECK(node.count("child"),
                   "Unsupported original Drill operator {}", pop);
  auto child = convert(node["child"]);
  if (pop == "single-sender" || pop == "hash-partition-sender" ||
      pop == "broadcast-sender" || pop == "ordered-partition-sender") {
    auto sink = sender_(node, child->outputType());
    if (pop == "hash-partition-sender") {
      auto names = child->outputType()->names();
      std::vector<core::TypedExprPtr> values;
      for (size_t i = 0; i < names.size(); ++i)
        values.push_back(std::make_shared<core::FieldAccessTypedExpr>(
            child->outputType()->childAt(i), names[i]));
      auto name = id + "_partition_hash";
      while (std::find(names.begin(), names.end(), name) != names.end())
        name += "_";
      names.push_back(name);
      values.push_back(
          expression(node["expr"].asString(), child->outputType()));
      VELOX_USER_CHECK(values.back()->type()->isInteger(),
                       "Drill partition expression must return INT");
      child = std::make_shared<core::ProjectNode>(id + "_hash", names, values,
                                                  child);
    }
    return std::make_shared<DrillSenderNode>(id, child, std::move(sink));
  }
  if (pop == "selection-vector-remover")
    return child;
  if (pop == "window") {
    using Window = core::WindowNode;
    const bool rows = node["frameUnitsRows"].asBool();
    auto bound = [](const folly::dynamic &value, bool start) {
      if (value["unbounded"].asBool())
        return start ? Window::BoundType::kUnboundedPreceding
                     : Window::BoundType::kUnboundedFollowing;
      // Original WindowPOP currently stores Long.MIN_VALUE for unsupported
      // finite offsets. Do not reinterpret that sentinel as a real bound.
      VELOX_USER_CHECK_EQ(value["offset"].asInt(), 0,
                          "Original Drill Window does not encode finite bounds");
      return Window::BoundType::kCurrentRow;
    };
    Window::Frame frame{rows ? Window::WindowType::kRows : Window::WindowType::kRange,
                        bound(node["start"], true), nullptr,
                        bound(node["end"], false), nullptr};
    const auto originalNames = child->outputType()->names();
    auto projectionNames = originalNames;
    std::vector<core::TypedExprPtr> projections;
    std::unordered_set<std::string> reserved(originalNames.begin(), originalNames.end());
    for (const auto &name : originalNames)
      projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          child->outputType()->findChild(name), name));
    for (const auto &entry : node["aggregations"])
      VELOX_USER_CHECK(reserved.insert(fieldName(entry["ref"].asString())).second,
                       "Duplicate Drill Window output field");
    auto materialize = [&](core::TypedExprPtr value) -> core::TypedExprPtr {
      if (std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(value) ||
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(value)) return value;
      auto name = id + "_window_arg_" + std::to_string(projectionNames.size());
      while (!reserved.insert(name).second) name += "_";
      projectionNames.push_back(name); projections.push_back(value);
      return std::make_shared<core::FieldAccessTypedExpr>(value->type(), name);
    };
    std::vector<core::FieldAccessTypedExprPtr> partitions, keys;
    std::vector<core::SortOrder> orders;
    // WindowPOP.getWithins() serializes the partition keys as "withins".
    for (const auto &key : node["withins"])
      partitions.push_back(field(materialize(expression(key["expr"].asString(), child->outputType()))));
    for (const auto &order : node["orderings"]) {
      keys.push_back(field(materialize(expression(order["expr"].asString(), child->outputType()))));
      orders.emplace_back(order["order"].asString() == "ASC",
          order.getDefault("nullDirection", "LAST").asString() == "FIRST");
    }
    std::vector<std::string> names;
    std::vector<Window::Function> functions;
    for (const auto &entry : node["aggregations"]) {
      auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
          bindWindowExpression(entry["expr"].asString(), child->outputType()));
      VELOX_USER_CHECK(call, "Expected original Drill Window function call");
      auto arguments = call->inputs();
      for (auto &argument : arguments) argument = materialize(argument);
      call = std::make_shared<core::CallTypedExpr>(call->type(), std::move(arguments), call->name());
      names.push_back(fieldName(entry["ref"].asString()));
      functions.push_back({call, frame, false});
    }
    if (projectionNames.size() != originalNames.size())
      child = std::make_shared<core::ProjectNode>(id + "_arguments", projectionNames, projections, child);
    if (partitions.empty()) child = gather(id + "_gather", child);
    else {
      std::vector<column_index_t> channels;
      for (const auto &key : partitions) channels.push_back(child->outputType()->getChildIdx(key->name()));
      child = std::make_shared<core::LocalPartitionNode>(id + "_partitions",
          core::LocalPartitionNode::Type::kRepartition, false,
          std::make_shared<exec::HashPartitionFunctionSpec>(child->outputType(), channels),
          std::vector<core::PlanNodePtr>{child});
    }
    core::PlanNodePtr result = std::make_shared<Window>(id, partitions, keys, orders,
        names, functions, false, child);
    // Window passes through its helper channels as well; expose only the
    // original Drill input columns followed by the declared Window outputs.
    if (projectionNames.size() != originalNames.size()) {
      auto outputNames = originalNames;
      outputNames.insert(outputNames.end(), names.begin(), names.end());
      std::vector<core::TypedExprPtr> outputs;
      for (const auto &name : outputNames)
        outputs.push_back(std::make_shared<core::FieldAccessTypedExpr>(result->outputType()->findChild(name), name));
      result = std::make_shared<core::ProjectNode>(id + "_outputs", outputNames, outputs, result);
    }
    return result;
  }
  if (pop == "filter")
    return std::make_shared<core::FilterNode>(
        id, expression(node["expr"].asString(), child->outputType()), child);
  if (pop == "flatten") {
    auto target = field(expression(node["column"].asString(), child->outputType()));
    VELOX_USER_CHECK(target->type()->isArray(), "Drill Flatten requires an array");
    std::vector<core::FieldAccessTypedExprPtr> replicate;
    for (size_t i = 0; i < child->outputType()->size(); ++i)
      if (child->outputType()->nameOf(i) != target->name())
        replicate.push_back(std::make_shared<core::FieldAccessTypedExpr>(
            child->outputType()->childAt(i), child->outputType()->nameOf(i)));
    // One array per original Flatten POP. Nested POPs retain Drill's sequential
    // expansion; no multi-array zip and no outer row for empty/NULL arrays.
    auto unnest = std::make_shared<core::UnnestNode>(id + "_unnest", replicate,
        std::vector<core::FieldAccessTypedExprPtr>{target}, std::vector<std::string>{target->name()},
        std::nullopt, std::nullopt, true, child);
    std::vector<core::TypedExprPtr> values;
    for (const auto &name : child->outputType()->names())
      values.push_back(std::make_shared<core::FieldAccessTypedExpr>(unnest->outputType()->findChild(name), name));
    return std::make_shared<core::ProjectNode>(id, child->outputType()->names(), values, unnest);
  }
  if (pop == "project") {
    std::vector<std::string> names;
    std::vector<core::TypedExprPtr> expressions;
    for (auto &entry : node["exprs"]) {
      auto outputName = fieldName(entry["ref"].asString());
      auto inputName = fieldName(entry["expr"].asString());
      if (inputName.ends_with("**")) {
        VELOX_USER_CHECK(outputName.ends_with("**"),
                         "Wildcard projection requires a wildcard reference");
        auto inputPrefix = inputName.substr(0, inputName.size() - 2);
        auto outputPrefix = outputName.substr(0, outputName.size() - 2);
        for (size_t i = 0; i < child->outputType()->size(); ++i) {
          auto name = child->outputType()->nameOf(i);
          if (!name.starts_with(inputPrefix))
            continue;
          auto target = outputPrefix + name.substr(inputPrefix.size());
          if (std::find(names.begin(), names.end(), target) != names.end())
            continue;
          names.push_back(target);
          expressions.push_back(std::make_shared<core::FieldAccessTypedExpr>(
              child->outputType()->childAt(i), name));
        }
      } else {
        auto value = expression(entry["expr"].asString(), child->outputType());
        auto it = std::find(names.begin(), names.end(), outputName);
        if (it == names.end()) {
          names.push_back(outputName);
          expressions.push_back(value);
        } else
          expressions[it - names.begin()] = value;
      }
    }
    return std::make_shared<core::ProjectNode>(id, names, expressions, child);
  }
  if (pop == "hash-aggregate" || pop == "streaming-aggregate") {
    std::vector<core::FieldAccessTypedExprPtr> groups;
    std::vector<core::TypedExprPtr> projections;
    auto projectionNames = child->outputType()->names();
    for (size_t i = 0; i < child->outputType()->size(); ++i)
      projections.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          child->outputType()->childAt(i), child->outputType()->nameOf(i)));
    const auto &keyEntries =
        node.count("keys") ? node["keys"] : node["groupByExprs"];
    const auto &aggregateEntries =
        node.count("exprs") ? node["exprs"] : node["aggrExprs"];
    for (auto &key : keyEntries) {
      auto name = fieldName(key["ref"].asString());
      auto value = expression(key["expr"].asString(), child->outputType());
      auto it = std::find(projectionNames.begin(), projectionNames.end(), name);
      if (it == projectionNames.end()) {
        projectionNames.push_back(name);
        projections.push_back(value);
      } else
        projections[it - projectionNames.begin()] = value;
      groups.push_back(
          std::make_shared<core::FieldAccessTypedExpr>(value->type(), name));
    }
    if (!groups.empty())
      child = std::make_shared<core::ProjectNode>(id + "_keys", projectionNames,
                                                  projections, child);
    std::vector<std::string> names;
    std::vector<core::AggregationNode::Aggregate> aggregates;
    std::unordered_map<std::string, bool> sumZeros;
    std::unordered_map<std::string, std::string> singleCounts;
    std::unordered_set<std::string> reservedNames;
    auto argumentNames = child->outputType()->names();
    std::vector<core::TypedExprPtr> argumentProjections;
    for (size_t i = 0; i < child->outputType()->size(); ++i) {
      reservedNames.insert(child->outputType()->nameOf(i));
      argumentProjections.push_back(std::make_shared<core::FieldAccessTypedExpr>(
          child->outputType()->childAt(i), child->outputType()->nameOf(i)));
    }
    for (auto &group : groups)
      reservedNames.insert(group->name());
    for (auto &entry : aggregateEntries)
      reservedNames.insert(fieldName(entry["ref"].asString()));
    for (auto &entry : aggregateEntries) {
      auto original = entry["expr"].asString();
      auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(
          expression(entry["expr"].asString(), child->outputType()));
      VELOX_USER_CHECK(expr, "Expected Drill aggregate call");
      auto inputs = expr->inputs();
      bool changed = false;
      for (auto &arg : inputs) {
        if (std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(arg) ||
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(arg) ||
            std::dynamic_pointer_cast<const core::LambdaTypedExpr>(arg))
          continue;
        // Velox aggregates consume channels/constants/lambdas, whereas Drill
        // aggregate arguments may contain arbitrary bound scalar expressions.
        auto name = id + "_aggregate_arg_" + std::to_string(argumentNames.size());
        while (!reservedNames.insert(name).second)
          name += "_";
        argumentNames.push_back(name);
        argumentProjections.push_back(arg);
        arg = std::make_shared<core::FieldAccessTypedExpr>(arg->type(), name);
        changed = true;
      }
      if (changed)
        expr = std::make_shared<core::CallTypedExpr>(expr->type(), std::move(inputs),
                                                   expr->name());
      names.push_back(fieldName(entry["ref"].asString()));
      std::vector<TypePtr> rawTypes;
      for (auto &arg : expr->inputs())
        rawTypes.push_back(arg->type());
      aggregates.push_back({expr, rawTypes});
      auto name = names.back();
      if (original.starts_with("$sum0("))
        sumZeros[name] = true;
      if (original.starts_with("single_value(")) {
        auto countName =
            id + "_single_count_" + std::to_string(singleCounts.size());
        while (!reservedNames.insert(countName).second)
          countName += "_";
        singleCounts[name] = countName;
        names.push_back(countName);
        auto count = std::make_shared<core::CallTypedExpr>(
            BIGINT(), expr->inputs(), "count");
        aggregates.push_back({count, rawTypes});
      }
    }
    if (argumentNames.size() > child->outputType()->size())
      child = std::make_shared<core::ProjectNode>(id + "_arguments", argumentNames,
                                                argumentProjections, child);
    core::PlanNodePtr input;
    if (groups.empty())
      input = gather(id + "_gather", child);
    else {
      std::vector<column_index_t> channels;
      for (auto &key : groups)
        channels.push_back(child->outputType()->getChildIdx(key->name()));
      input = std::make_shared<core::LocalPartitionNode>(
          id + "_partition", core::LocalPartitionNode::Type::kRepartition,
          false,
          std::make_shared<exec::HashPartitionFunctionSpec>(child->outputType(),
                                                            channels),
          std::vector<core::PlanNodePtr>{child});
    }
    // Drill phase-two calls consume scalar outputs, not Velox aggregate state.
    core::PlanNodePtr result = std::make_shared<core::AggregationNode>(
        id, core::AggregationNode::Step::kSingle, groups,
        std::vector<core::FieldAccessTypedExprPtr>{}, names, aggregates, false,
        false, input);
    if (!sumZeros.empty() || !singleCounts.empty()) {
      std::vector<std::string> outputNames;
      std::vector<core::TypedExprPtr> expressions;
      for (auto &group : groups) {
        outputNames.push_back(group->name());
        expressions.push_back(group);
      }
      for (auto &entry : aggregateEntries) {
        auto name = fieldName(entry["ref"].asString());
        auto type = result->outputType()->findChild(name);
        core::TypedExprPtr value =
            std::make_shared<core::FieldAccessTypedExpr>(type, name);
        if (singleCounts.count(name)) {
          auto count = std::make_shared<core::FieldAccessTypedExpr>(
              BIGINT(), singleCounts.at(name));
          auto condition = std::make_shared<core::CallTypedExpr>(
              BOOLEAN(),
              std::vector<core::TypedExprPtr>{
                  count, std::make_shared<core::ConstantTypedExpr>(
                             BIGINT(), Variant(int64_t(1)))},
              "greaterthan");
          auto error = std::make_shared<core::CallTypedExpr>(
              UNKNOWN(),
              std::vector<core::TypedExprPtr>{
                  std::make_shared<core::ConstantTypedExpr>(
                      VARCHAR(),
                      Variant(std::string("Input for single_value function has "
                                          "more than one row")))},
              "raise_error");
          value = std::make_shared<core::CallTypedExpr>(
              type,
              std::vector<core::TypedExprPtr>{
                  condition,
                  std::make_shared<core::CastTypedExpr>(type, error, false),
                  value},
              "if");
        }
        if (sumZeros.count(name)) {
          Variant zero;
          if (type->isLongDecimal())
            zero = Variant(int128_t(0));
          else if (type->isBigint() || type->isShortDecimal())
            zero = Variant(int64_t(0));
          else if (type->isInteger())
            zero = Variant(int32_t(0));
          else if (type->isDouble())
            zero = Variant(0.0);
          else if (type->isReal())
            zero = Variant(0.0f);
          else
            VELOX_USER_FAIL("Unsupported $sum0 result {}", type->toString());
          value = std::make_shared<core::CallTypedExpr>(
              type,
              std::vector<core::TypedExprPtr>{
                  value, std::make_shared<core::ConstantTypedExpr>(type, zero)},
              "coalesce");
        }
        outputNames.push_back(name);
        expressions.push_back(value);
      }
      result = std::make_shared<core::ProjectNode>(
          id + "_finalize", outputNames, expressions, result);
    }
    return result;
  }
  if (pop == "sort" || pop == "external-sort" || pop == "top-n") {
    VELOX_USER_CHECK(!node.getDefault("reverse", false).asBool(),
                     "Reverse Drill sort is unsupported");
    std::vector<core::FieldAccessTypedExprPtr> keys;
    std::vector<core::SortOrder> orders;
    std::unordered_set<std::string> seenKeys;
    for (auto &order : node["orderings"]) {
      auto key = field(expression(order["expr"].asString(), child->outputType()));
      // Once a key compares equal, later occurrences cannot break the tie.
      // Velox TopN requires unique keys, including for opposing directions.
      if (!seenKeys.insert(key->name()).second)
        continue;
      keys.push_back(std::move(key));
      orders.emplace_back(
          order["order"].asString() == "ASC",
          order.getDefault("nullDirection", "LAST").asString() == "FIRST");
    }
    if (pop == "top-n") {
      auto count = node["limit"].asInt();
      VELOX_USER_CHECK_GE(count, 0, "Drill TopN limit must be nonnegative");
      VELOX_USER_CHECK_LE(count, std::numeric_limits<int32_t>::max(),
                          "Drill TopN limit must fit its original INT field");
      if (!count) {
        auto empty = std::dynamic_pointer_cast<RowVector>(
            BaseVector::create(child->outputType(), 0, pool_));
        return std::make_shared<core::ValuesNode>(
            id, std::vector<RowVectorPtr>{empty});
      }
      // Each input driver retains only its best N rows. Gather at most
      // drivers * N candidates, then choose the minor's globally ordered N.
      auto partial = std::make_shared<core::TopNNode>(
          id + "_partial", keys, orders, static_cast<int32_t>(count), true,
          child);
      return std::make_shared<core::TopNNode>(
          id, keys, orders, static_cast<int32_t>(count), false,
          gather(id + "_gather", partial));
    }
    return std::make_shared<core::OrderByNode>(
        id, keys, orders, false, gather(id + "_gather", child));
  }
  if (pop == "limit") {
    auto first = !node.count("first") || node["first"].isNull()
                     ? 0
                     : node["first"].asInt();
    auto last = !node.count("last") || node["last"].isNull()
                    ? std::numeric_limits<int64_t>::max()
                    : node["last"].asInt();
    VELOX_USER_CHECK_GE(first, 0);
    if (last <= first) {
      auto empty = std::dynamic_pointer_cast<RowVector>(
          BaseVector::create(child->outputType(), 0, pool_));
      return std::make_shared<core::ValuesNode>(
          id, std::vector<RowVectorPtr>{empty});
    }
    return std::make_shared<core::LimitNode>(id, first, last - first, false,
                                             gather(id + "_gather", child));
  }
  VELOX_USER_FAIL("Unsupported original Drill compute operator {}", pop);
}
} // namespace drill::nativeexec
