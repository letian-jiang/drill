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
#include "execution/VeloxRuntime.h"
#include "velox/plan/DrillPlanNodes.h"
#include <atomic>
#include <iostream>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
using namespace drill::nativeexec;
int main() {
  try {
    VeloxRuntime runtime(4);
    for (int mode = 0; mode < 3; ++mode) {
      std::atomic<int> received{0};
      std::weak_ptr<memory::MemoryPool> weakRoot, weakPlan;
      auto task = [&] {
        auto root = memory::memoryManager()->addRootPool("plan-lifetime");
        auto leaf = root->addLeafChild("constants");
        weakRoot = root;
        weakPlan = leaf;
        auto arrayType = ARRAY(BIGINT());
        auto values = BaseVector::create<RowVector>(ROW({"a"}, {arrayType}), 1,
                                                    leaf.get());
        values->childAt(0)->setNull(0, true);
        auto empty = BaseVector::create<ArrayVector>(arrayType, 1, leaf.get());
        empty->setOffsetAndSize(0, 0, 0);
        auto expression = std::make_shared<core::CallTypedExpr>(
            arrayType,
            std::vector<core::TypedExprPtr>{
                std::make_shared<core::FieldAccessTypedExpr>(arrayType, "a"),
                std::make_shared<core::ConstantTypedExpr>(empty)}, "coalesce");
        auto source = std::make_shared<core::ValuesNode>(
            "values", std::vector<RowVectorPtr>{values});
        auto project = std::make_shared<core::ProjectNode>(
            "normalize", std::vector<std::string>{"a"},
            std::vector<core::TypedExprPtr>{expression}, source);
        auto sender = std::make_shared<DrillSenderNode>(
            "sender", project, [&](memory::MemoryPool *) {
              return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
                if (!batch) return {};
                VELOX_CHECK_EQ(batch->size(), 1);
                DecodedVector decoded(*batch->childAt(0));
                auto array = decoded.base()->as<ArrayVector>();
                VELOX_CHECK(!decoded.isNullAt(0));
                VELOX_CHECK_EQ(array->sizeAt(decoded.index(0)), 0);
                ++received;
                if (mode == 2) VELOX_FAIL("injected sender failure");
                return {};
              });
            });
        return runtime.task("plan-lifetime", sender, root, leaf);
      }();
      // Every caller-owned plan, vector and pool has now left scope.
      VELOX_CHECK(!weakPlan.expired());
      auto complete = task->taskCompletionFuture();
      if (mode == 1) task->requestCancel();
      task->start(4);
      std::move(complete).get();
      VELOX_CHECK(!weakPlan.expired(), "Completion must retain plan constants");
      VELOX_CHECK_EQ(task->state(), mode == 0 ? exec::TaskState::kFinished
          : mode == 1 ? exec::TaskState::kCanceled : exec::TaskState::kFailed);
      VELOX_CHECK_EQ(received.load(), mode == 1 ? 0 : 1);
      auto deleted = task->taskDeletionFuture();
      task.reset();
      std::move(deleted).get();
      VELOX_CHECK(weakPlan.expired());
      VELOX_CHECK(weakRoot.expired());
    }
    std::cout << "Plan pools survive caller scope and Task completion; released "
                 "after success, cancellation and failure deletion.\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
