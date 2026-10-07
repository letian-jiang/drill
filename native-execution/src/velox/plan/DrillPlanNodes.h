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
#include "scan/ScanReader.h"
#include <velox/core/PlanNode.h>
namespace drill::nativeexec {
enum class SourceKind { Receiver, JniScan, NativeScan };
class DrillSourceNode : public core::PlanNode {
public:
  DrillSourceNode(std::string id, RowTypePtr type, SourceKind kind,
                  SourceFactory factory)
      : PlanNode(std::move(id)), type_(std::move(type)), kind_(kind),
        factory_(std::move(factory)) {}
  const RowTypePtr &outputType() const override { return type_; }
  const std::vector<core::PlanNodePtr> &sources() const override {
    return sources_;
  }
  std::string_view name() const override {
    switch (kind_) {
    case SourceKind::Receiver:
      return "DrillReceiver";
    case SourceKind::JniScan:
      return "JniPluginScan";
    case SourceKind::NativeScan:
      return "NativeScan";
    }
    return "InvalidDrillSource";
  }
  const SourceFactory &factory() const { return factory_; }

private:
  void addDetails(std::stringstream &s) const override {
    s << type_->toString();
  }
  RowTypePtr type_;
  SourceKind kind_;
  SourceFactory factory_;
  std::vector<core::PlanNodePtr> sources_;
};
class DrillSenderNode final : public core::PlanNode {
public:
  DrillSenderNode(std::string id, core::PlanNodePtr child, SinkFactory sink)
      : PlanNode(std::move(id)), sources_{std::move(child)},
        sink_(std::move(sink)), type_(ROW({})) {}
  const RowTypePtr &outputType() const override { return type_; }
  const std::vector<core::PlanNodePtr> &sources() const override {
    return sources_;
  }
  std::string_view name() const override { return "DrillSender"; }
  const SinkFactory &sink() const { return sink_; }

private:
  void addDetails(std::stringstream &) const override {}
  std::vector<core::PlanNodePtr> sources_;
  SinkFactory sink_;
  RowTypePtr type_;
};
void registerDrillOperators();
} // namespace drill::nativeexec
