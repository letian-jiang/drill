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
#include "plan/FragmentPlanConverter.h"
#include <atomic>
#include <array>
#include <folly/json.h>
#include <iostream>
#include <mutex>
#include <map>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
class Rows final : public BatchSource {
public:
  Rows(memory::MemoryPool *pool, int batches)
      : pool_(pool), batches_(batches) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    auto n = next_.fetch_add(1);
    if (n >= batches_)
      return std::nullopt;
    auto column = BaseVector::create(BIGINT(), 10, pool_);
    for (int r = 0; r < 10; ++r)
      column->as<FlatVector<int64_t>>()->set(r, n * 10 + r);
    return std::make_shared<RowVector>(pool_, ROW({"v"}, {BIGINT()}), nullptr,
                                       10, std::vector<VectorPtr>{column});
  }
  void cancel() override { next_.store(batches_); }

private:
  memory::MemoryPool *pool_;
  int batches_;
  std::atomic<int> next_{0};
};
int main() {
  try {
    VeloxRuntime runtime(4);
    for (bool grouped : {false, true}) {
      auto pool = memory::memoryManager()->addRootPool("aggregate-arguments");
      auto leaf = pool->addLeafChild("source");
      auto rows = std::make_shared<Rows>(leaf.get(), 100);
      std::mutex mutex;
      std::map<int, std::array<int64_t, 3>> actual;
      FragmentPlanConverter converter(leaf.get(), [&](const folly::dynamic &) {
        return SourceBinding{ROW({"v"}, {BIGINT()}), SourceKind::NativeScan,
                             [rows](memory::MemoryPool *) { return rows; }};
      }, [&](const folly::dynamic &, const RowTypePtr &) {
        return SinkFactory([&](memory::MemoryPool *) {
          return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
            if (!batch) return {};
            for (int r = 0; r < batch->size(); ++r) {
              int key = 0;
              if (grouped) {
                DecodedVector group(*batch->childAt(0));
                key = group.valueAt<int32_t>(r);
              }
              std::array<int64_t, 3> result;
              for (int c = 0; c < 3; ++c) {
                DecodedVector value(*batch->childAt(c + int(grouped)));
                VELOX_CHECK(!value.isNullAt(r));
                result[c] = value.valueAt<int64_t>(r);
              }
              std::lock_guard lock(mutex);
              VELOX_CHECK(actual.emplace(key, result).second);
            }
            return {};
          });
        });
      });
      auto aggregate = folly::parseJson(R"PLAN({"pop":"hash-aggregate","@id":1,"keys":[],
        "exprs":[{"ref":"`s`","expr":"sum(`v` + 1)"},
                 {"ref":"`castsum`","expr":"sum(cast(cast(`v` as VARCHAR) as BIGINT))"},
                 {"ref":"`n`","expr":"count(cast(`v` as VARCHAR))"}],
        "child":{"pop":"test-scan","@id":2}})PLAN");
      if (grouped)
        aggregate["keys"].push_back(folly::dynamic::object("ref", "`g`")(
            "expr", "if(`v` < 500, 0, 1)"));
      auto plan = converter.convert(folly::dynamic::object("pop", "single-sender")(
          "@id", 0)("child", aggregate));
      auto task = runtime.task(grouped ? "grouped-arguments" : "global-arguments", plan, pool);
      task->start(4);
      task->taskCompletionFuture().wait();
      VELOX_CHECK_EQ(task->state(), exec::TaskState::kFinished);
      if (grouped) {
        VELOX_CHECK_EQ(actual.size(), 2);
        VELOX_CHECK((actual.at(0) == std::array<int64_t, 3>{125250, 124750, 500}));
        VELOX_CHECK((actual.at(1) == std::array<int64_t, 3>{375250, 374750, 500}));
      } else {
        VELOX_CHECK_EQ(actual.size(), 1);
        VELOX_CHECK((actual.at(0) == std::array<int64_t, 3>{500500, 499500, 1000}));
      }
      auto deleted = task->taskDeletionFuture();
      task.reset();
      deleted.wait();
    }
    for (auto kind :
         {SourceKind::NativeScan, SourceKind::Receiver, SourceKind::JniScan}) {
      auto pool = memory::memoryManager()->addRootPool(
          "pure-task-" + std::to_string(int(kind)));
      auto leaf = pool->addLeafChild("input-plan");
      auto rows = std::make_shared<Rows>(leaf.get(), 100);
      std::mutex mutex;
      std::vector<int64_t> actual;
      FragmentPlanConverter converter(
          leaf.get(),
          [&](const folly::dynamic &) {
            return SourceBinding{ROW({"v"}, {BIGINT()}), kind,
                                 [rows](memory::MemoryPool *) { return rows; }};
          },
          [&](const folly::dynamic &, const RowTypePtr &) {
            return SinkFactory([&](memory::MemoryPool *) {
              return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
                if (!batch)
                  return {};
                DecodedVector values(*batch->childAt(0));
                std::lock_guard lock(mutex);
                for (int r = 0; r < batch->size(); ++r)
                  actual.push_back(values.valueAt<int64_t>(r));
                return {};
              });
            });
          });
      auto plan = converter.convert(
          folly::parseJson(R"({"pop":"single-sender","@id":0,"child":{
          "pop":"project","@id":1,"exprs":[{"ref":"`out`","expr":"`v` + 1"}],"child":{
          "pop":"filter","@id":2,"expr":"`v` >= 500","child":{
          "pop":"unordered-receiver","@id":3}}}})"));
      auto task =
          runtime.task("pure-minor-" + std::to_string(int(kind)), plan, pool);
      task->start(4);
      task->taskCompletionFuture().wait();
      VELOX_CHECK_EQ(task->state(), exec::TaskState::kFinished);
      std::sort(actual.begin(), actual.end());
      VELOX_CHECK_EQ(actual.size(), 500);
      for (int r = 0; r < 500; ++r)
        VELOX_CHECK_EQ(actual[r], r + 501);
      VELOX_CHECK_GE(task->taskStats().pipelineStats.size(), 1);
      auto deleted = task->taskDeletionFuture();
      task.reset();
      deleted.wait();
    }
    VELOX_CHECK_GE(availableCpuCount(), 1);
    std::cout << "Original minor plan -> Velox Task: 3 source kinds, 4 "
                 "drivers, filter/project/native sender, exactly-once output "
                 "passed; no JVM.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
