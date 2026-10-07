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
#pragma once
#include "velox/plan/DrillPlanNodes.h"
#include "scan/NativeScanPlugin.h"
#include <folly/dynamic.h>
namespace drill::nativeexec {
struct SourceBinding {
  RowTypePtr schema;
  SourceKind kind;
  SourceFactory factory;
  NormalizeScanOutput normalizeOutput{};
};
using BindSource = std::function<SourceBinding(const folly::dynamic &)>;
using BindSender =
    std::function<SinkFactory(const folly::dynamic &, const RowTypePtr &)>;
class FragmentPlanConverter {
public:
  FragmentPlanConverter(memory::MemoryPool *pool, BindSource source,
                        BindSender sender)
      : pool_(pool), source_(std::move(source)), sender_(std::move(sender)) {}
  core::PlanNodePtr convert(const folly::dynamic &node);
  core::TypedExprPtr expression(std::string text, const RowTypePtr &schema);

private:
  core::PlanNodePtr convertImpl(const folly::dynamic &node);
  memory::MemoryPool *pool_;
  BindSource source_;
  BindSender sender_;
};
} // namespace drill::nativeexec
