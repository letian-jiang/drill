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
#include "velox/plan/DrillPlanNodes.h"
#include <mutex>
#include <velox/exec/Operator.h>
namespace drill::nativeexec {
namespace {
class DrillSourceOperator final : public exec::SourceOperator {
public:
  DrillSourceOperator(exec::DriverCtx *ctx, int32_t id,
                      std::shared_ptr<const DrillSourceNode> node)
      : SourceOperator(ctx, node->outputType(), id, node->id(), node->name()),
        node_(std::move(node)) {}
  RowVectorPtr getOutput() override {
    if (finished_)
      return nullptr;
    if (!source_)
      source_ = node_->factory()(pool());
    VELOX_CHECK_NOT_NULL(source_);
    auto result = source_->next(future_);
    if (!result) {
      finished_ = true;
      return nullptr;
    }
    return *result;
  }
  exec::BlockingReason isBlocked(ContinueFuture *future) override {
    if (!future_.valid())
      return exec::BlockingReason::kNotBlocked;
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForProducer;
  }
  bool isFinished() override { return finished_; }
  void close() override {
    if (source_)
      source_->cancel();
    exec::SourceOperator::close();
  }

private:
  std::shared_ptr<const DrillSourceNode> node_;
  std::shared_ptr<BatchSource> source_;
  ContinueFuture future_;
  bool finished_{false};
};
class DrillSender final : public exec::Operator {
public:
  DrillSender(exec::DriverCtx *ctx, int32_t id,
              std::shared_ptr<const DrillSenderNode> node)
      : Operator(ctx, node->outputType(), id, node->id(), "DrillSender"),
        node_(std::move(node)) {}
  bool needsInput() const override { return !noMoreInput_ && !future_.valid(); }
  void addInput(RowVectorPtr vector) override {
    if (!sink_)
      sink_ = node_->sink()(pool());
    future_ = sink_(std::move(vector));
  }
  void noMoreInput() override {
    exec::Operator::noMoreInput();
    if (sink_)
      future_ = sink_(nullptr);
  }
  RowVectorPtr getOutput() override { return nullptr; }
  bool isFinished() override { return noMoreInput_ && !future_.valid(); }
  exec::BlockingReason isBlocked(ContinueFuture *future) override {
    if (!future_.valid())
      return exec::BlockingReason::kNotBlocked;
    if (future_.isReady()) {
      // Observe ready failures here; pending failures are handled by Driver.
      std::move(future_).get();
      return exec::BlockingReason::kNotBlocked;
    }
    *future = std::move(future_);
    return exec::BlockingReason::kWaitForConsumer;
  }
  void close() override {
    sink_ = {};
    exec::Operator::close();
  }

private:
  std::shared_ptr<const DrillSenderNode> node_;
  BatchSink sink_;
  ContinueFuture future_;
};
class Translator final : public exec::Operator::PlanNodeTranslator {
public:
  std::unique_ptr<exec::Operator>
  toOperator(exec::DriverCtx *ctx, int32_t id,
             const core::PlanNodePtr &node) override {
    if (auto source = std::dynamic_pointer_cast<const DrillSourceNode>(node))
      return std::make_unique<DrillSourceOperator>(ctx, id, std::move(source));
    if (auto sender = std::dynamic_pointer_cast<const DrillSenderNode>(node))
      return std::make_unique<DrillSender>(ctx, id, std::move(sender));
    return nullptr;
  }
};
} // namespace
void registerDrillOperators() {
  static std::once_flag once;
  std::call_once(once, [] {
    exec::Operator::registerOperator(std::make_unique<Translator>());
  });
}
} // namespace drill::nativeexec
