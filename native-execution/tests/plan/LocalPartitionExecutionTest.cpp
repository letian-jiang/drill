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
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <velox/exec/HashPartitionFunction.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class Input final : public BatchSource {
public:
  Input(RowVectorPtr rows, std::shared_ptr<std::atomic<int>> next,
        memory::MemoryPool *pool)
      : rows_(std::move(rows)), next_(std::move(next)), pool_(pool) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    auto start = next_->fetch_add(33);
    if (start >= rows_->size())
      return std::nullopt;
    auto count = std::min<int>(33, rows_->size() - start);
    auto indices = AlignedBuffer::allocate<vector_size_t>(count, pool_);
    for (int i = 0; i < count; ++i)
      indices->asMutable<vector_size_t>()[i] = start + count - i - 1;
    std::vector<VectorPtr> columns;
    for (const auto &column : rows_->children())
      columns.push_back(
          BaseVector::wrapInDictionary(nullptr, indices, count, column));
    return std::make_shared<RowVector>(pool_, rows_->rowType(), nullptr, count,
                                       std::move(columns));
  }
  void cancel() override {}

private:
  RowVectorPtr rows_;
  std::shared_ptr<std::atomic<int>> next_;
  memory::MemoryPool *pool_;
};
std::optional<std::string> key(int id) {
  if (id % 17 == 0)
    return std::nullopt;
  return std::string("key-雪\0", 8) + std::to_string(id % 29);
}
void test(VeloxRuntime &runtime, int count, int drivers, bool grouped) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("partition-input");
  auto type =
      ROW({"id", "k", "payload"},
          {BIGINT(), VARCHAR(), ROW({"n", "text"}, {BIGINT(), VARCHAR()})});
  auto input = BaseVector::create<RowVector>(type, count, pool.get());
  auto payload = input->childAt(2)->as<RowVector>();
  std::map<std::optional<std::string>, int64_t> expected;
  for (int row = 0; row < count; ++row) {
    input->childAt(0)->as<FlatVector<int64_t>>()->set(row, row);
    auto k = key(row);
    if (k)
      input->childAt(1)->as<FlatVector<StringView>>()->set(row, StringView(*k));
    else
      input->childAt(1)->setNull(row, true);
    ++expected[k];
    payload->childAt(0)->as<FlatVector<int64_t>>()->set(row, -row);
    payload->childAt(0)->setNull(row, row % 11 == 0);
    auto text = std::string(100 + row % 23, 'a' + row % 26) + "雪🚀";
    payload->childAt(1)->as<FlatVector<StringView>>()->set(row,
                                                           StringView(text));
  }
  uint64_t vectors[2]{};
  for (int buffered = 0; buffered < 2; ++buffered) {
    auto next = std::make_shared<std::atomic<int>>(0);
    std::mutex mutex;
    std::vector<bool> seen(count);
    std::map<std::optional<std::string>, int64_t> actual;
    auto sink = SinkFactory([&](memory::MemoryPool *) {
      return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
        if (!batch)
          return {};
        std::lock_guard lock(mutex);
        if (grouped) {
          DecodedVector keys(*batch->childAt(0)), counts(*batch->childAt(1));
          for (int row = 0; row < batch->size(); ++row) {
            auto k = keys.isNullAt(row)
                         ? std::optional<std::string>{}
                         : std::optional<std::string>(
                               keys.valueAt<StringView>(row).str());
            VELOX_CHECK(actual.emplace(k, counts.valueAt<int64_t>(row)).second);
          }
        } else {
          DecodedVector ids(*batch->childAt(0));
          for (int row = 0; row < batch->size(); ++row) {
            auto id = ids.valueAt<int64_t>(row);
            VELOX_CHECK_GE(id, 0);
            VELOX_CHECK_LT(id, count);
            VELOX_CHECK(!seen[id]);
            seen[id] = true;
            VELOX_CHECK_EQ(batch->compare(input.get(), row, id, CompareFlags{}),
                           0, "Partition buffering changed a complete row");
          }
        }
        return {};
      });
    });
    FragmentPlanConverter converter(
        pool.get(),
        [&](const auto &) {
          return SourceBinding{type, SourceKind::Receiver,
                               [input, next](memory::MemoryPool *driver) {
                                 return std::make_shared<Input>(input, next,
                                                                driver);
                               }};
        },
        [&](const auto &, const auto &) { return sink; });
    folly::dynamic source =
        folly::dynamic::object("pop", "unordered-receiver")("@id", 2);
    core::PlanNodePtr plan;
    std::string partition;
    if (grouped) {
      folly::dynamic aggregate =
          folly::dynamic::object("pop", "hash-aggregate")("@id", 1)(
              "keys", folly::dynamic::array(
                          folly::dynamic::object("ref", "`k`")("expr", "`k`")))(
              "exprs", folly::dynamic::array(folly::dynamic::object(
                           "ref", "`n`")("expr", "count(1)")))("child", source);
      plan = converter.convert(folly::dynamic::object("pop", "single-sender")(
          "@id", 0)("child", aggregate));
      partition = "drill_1_partition";
    } else {
      auto child = converter.convert(source);
      partition = "test_partition";
      child = std::make_shared<core::LocalPartitionNode>(
          partition, core::LocalPartitionNode::Type::kRepartition, false,
          std::make_shared<exec::HashPartitionFunctionSpec>(
              type, std::vector<column_index_t>{1}),
          std::vector<core::PlanNodePtr>{child});
      plan = std::make_shared<DrillSenderNode>("test_sender", child, sink);
    }
    // An actual old-default Task is the control. The treatment uses precisely
    // the shared runtime used by both Java-hosted and native-hosted minors.
    auto ctx =
        core::QueryCtx::create(runtime.cpu(), core::QueryConfig({}), {},
                               nullptr, root, nullptr, "partition-control");
    auto task =
        buffered ? runtime.task("partition-buffered", plan, root)
                 : exec::Task::create(
                       "partition-control",
                       core::PlanFragment{
                           plan, core::ExecutionStrategy::kUngrouped, 1, {}},
                       0, std::move(ctx), exec::Task::ExecutionMode::kParallel,
                       exec::ConsumerSupplier{});
    auto complete = task->taskCompletionFuture();
    task->start(drivers);
    std::move(complete).get();
    auto error = task->error();
    auto stats = task->taskStats();
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (stats.numCompletedDrivers != stats.numTotalDrivers &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      stats = task->taskStats();
    }
    VELOX_CHECK_EQ(stats.numCompletedDrivers, stats.numTotalDrivers);
    auto deleted = task->taskDeletionFuture();
    task.reset();
    std::move(deleted).get();
    if (error)
      std::rethrow_exception(error);
    if (grouped)
      VELOX_CHECK(actual == expected);
    else
      VELOX_CHECK(
          std::all_of(seen.begin(), seen.end(), [](bool v) { return v; }));
    uint64_t rows = 0;
    for (const auto &p : stats.pipelineStats)
      for (const auto &op : p.operatorStats)
        if (op.planNodeId == partition && op.operatorType == "LocalExchange") {
          vectors[buffered] += op.outputVectors;
          rows += op.outputPositions;
        }
    VELOX_CHECK_EQ(rows, count, "Partition tails or NULL keys were lost");
  }
  if (count >= 16000)
    VELOX_CHECK_LT(vectors[1] * 4, vectors[0]);
  std::cout << "rows=" << count << " drivers=" << drivers
            << " grouped=" << grouped << " old vectors=" << vectors[0]
            << " buffered=" << vectors[1] << '\n';
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(16);
    for (bool grouped : {false, true})
      for (int drivers : {1, 4, 16}) {
        // A single partition intentionally remains unbuffered.
        if (drivers > 1)
          test(runtime, 16003, drivers, grouped);
        for (int count : {0, 1, 97})
          test(runtime, count, drivers, grouped);
      }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
