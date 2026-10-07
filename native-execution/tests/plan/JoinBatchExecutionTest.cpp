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
#include <thread>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class Input final : public BatchSource {
public:
  Input(RowVectorPtr input, std::shared_ptr<std::atomic<int>> next,
        memory::MemoryPool *pool)
      : input_(std::move(input)), next_(std::move(next)), pool_(pool) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    auto start = next_->fetch_add(8192);
    if (start >= input_->size())
      return std::nullopt;
    auto count = std::min<int>(8192, input_->size() - start);
    auto indices = AlignedBuffer::allocate<vector_size_t>(count, pool_);
    for (int i = 0; i < count; ++i)
      indices->asMutable<vector_size_t>()[i] = start + count - i - 1;
    std::vector<VectorPtr> columns;
    for (const auto &column : input_->children())
      columns.push_back(
          BaseVector::wrapInDictionary(nullptr, indices, count, column));
    return std::make_shared<RowVector>(pool_, input_->rowType(), nullptr, count,
                                       std::move(columns));
  }
  void cancel() override {}

private:
  RowVectorPtr input_;
  std::shared_ptr<std::atomic<int>> next_;
  memory::MemoryPool *pool_;
};
void test(VeloxRuntime &runtime, int count, int drivers, const char *join,
          bool emptyBuild = false) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("join-batch-input");
  auto leftType =
      ROW({"lid", "k", "payload", "nums", "amount"},
          {BIGINT(), BIGINT(), ROW({"n", "text"}, {BIGINT(), VARCHAR()}),
           ARRAY(BIGINT()), DECIMAL(18, 2)});
  auto rightType = ROW({"rid", "rk", "rs"}, {BIGINT(), BIGINT(), VARCHAR()});
  auto left = BaseVector::create<RowVector>(leftType, count, pool.get());
  auto right = BaseVector::create<RowVector>(rightType, emptyBuild ? 0 : 116,
                                             pool.get());
  auto payload = left->childAt(2)->as<RowVector>();
  auto nums = left->childAt(3)->as<ArrayVector>();
  nums->elements()->resize(count * 2);
  for (int i = 0; i < count; ++i) {
    left->childAt(0)->as<FlatVector<int64_t>>()->set(i, i);
    left->childAt(1)->as<FlatVector<int64_t>>()->set(i, i % 37);
    left->childAt(1)->setNull(i, i % 19 == 0);
    payload->childAt(0)->as<FlatVector<int64_t>>()->set(i, -i);
    payload->childAt(0)->setNull(i, i % 11 == 0);
    auto text = std::string(100 + i % 23, 'a' + i % 26) + "雪🚀";
    payload->childAt(1)->as<FlatVector<StringView>>()->set(i, StringView(text));
    nums->setOffsetAndSize(i, i * 2, i % 3);
    nums->setNull(i, i % 7 == 0);
    for (int j = 0; j < 2; ++j)
      nums->elements()->as<FlatVector<int64_t>>()->set(i * 2 + j, i * 2 + j);
    nums->elements()->setNull(i * 2 + 1, i % 5 == 0);
    left->childAt(4)->as<FlatVector<int64_t>>()->set(i, i - 8000);
  }
  for (int i = 0; i < right->size(); ++i) {
    right->childAt(0)->as<FlatVector<int64_t>>()->set(i, i);
    right->childAt(1)->as<FlatVector<int64_t>>()->set(i, i / 5);
    right->childAt(1)->setNull(i, i == 115);
    auto text = std::string("build-雪\0", 10) + std::to_string(i);
    right->childAt(2)->as<FlatVector<StringView>>()->set(i, StringView(text));
    right->childAt(2)->setNull(i, i % 13 == 0);
  }
  bool semi = std::string(join) == "SEMI", anti = std::string(join) == "ANTI";
  bool outer = std::string(join) == "LEFT";
  std::map<std::pair<int, int>, int> expected;
  // Independent bag oracle: five build rows for keys 0..22; NULL never joins.
  for (int i = 0; i < count; ++i) {
    bool match = !emptyBuild && i % 19 != 0 && i % 37 < 23;
    if (semi || anti) {
      if (semi == match)
        ++expected[{i, -1}];
    } else if (match) {
      for (int j = i % 37 * 5; j < i % 37 * 5 + 5; ++j)
        ++expected[{i, j}];
    } else if (outer)
      ++expected[{i, -1}];
  }
  uint64_t vectors[2]{};
  for (int treatment = 0; treatment < 2; ++treatment) {
    auto leftNext = std::make_shared<std::atomic<int>>(0);
    auto rightNext = std::make_shared<std::atomic<int>>(0);
    std::mutex mutex;
    std::map<std::pair<int, int>, int> actual;
    auto sink = SinkFactory([&](memory::MemoryPool *) {
      return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
        if (!batch)
          return {};
        std::lock_guard lock(mutex);
        auto leftOffset = semi || anti ? 0 : rightType->size();
        DecodedVector ids(*batch->childAt(leftOffset));
        for (int row = 0; row < batch->size(); ++row) {
          int i = ids.valueAt<int64_t>(row);
          VELOX_CHECK_GE(i, 0);
          VELOX_CHECK_LT(i, count);
          for (size_t c = 0; c < leftType->size(); ++c)
            VELOX_CHECK_EQ(
                batch->childAt(leftOffset + c)
                    ->compare(left->childAt(c).get(), row, i, CompareFlags{}),
                0, "Join batching changed a complete probe value");
          int j = -1;
          if (!semi && !anti) {
            DecodedVector buildIds(*batch->childAt(0));
            if (!buildIds.isNullAt(row)) {
              j = buildIds.valueAt<int64_t>(row);
              VELOX_CHECK_GE(j, 0);
              VELOX_CHECK_LT(j, right->size());
              for (size_t c = 0; c < rightType->size(); ++c)
                VELOX_CHECK_EQ(
                    batch->childAt(c)->compare(right->childAt(c).get(), row, j,
                                               CompareFlags{}),
                    0, "Join batching changed a complete build value");
            } else
              for (size_t c = 0; c < rightType->size(); ++c)
                VELOX_CHECK(batch->childAt(c)->isNullAt(row));
          }
          ++actual[{i, j}];
        }
        return {};
      });
    });
    FragmentPlanConverter converter(
        pool.get(),
        [&](const folly::dynamic &node) {
          bool probe = node["@id"].asInt() == 2;
          auto input = probe ? left : right;
          auto next = probe ? leftNext : rightNext;
          return SourceBinding{input->rowType(), SourceKind::Receiver,
                               [input, next](memory::MemoryPool *driver) {
                                 return std::make_shared<Input>(input, next,
                                                                driver);
                               }};
        },
        [&](const auto &, const auto &) { return sink; });
    folly::dynamic joined = folly::dynamic::object("pop", "hash-join")(
        "@id", 1)("joinType", join)(
        "conditions",
        folly::dynamic::array(folly::dynamic::object("left", "`k`")(
            "right", "`rk`")("relationship", "==")))(
        "left", folly::dynamic::object("pop", "unordered-receiver")("@id", 2))(
        "right", folly::dynamic::object("pop", "unordered-receiver")("@id", 3));
    auto plan = converter.convert(folly::dynamic::object(
        "pop", "single-sender")("@id", 0)("child", joined));
    // Preserve the previous partition-buffer setting, changing only the batch
    // preference. Treatment is the exact runtime used by both host languages.
    auto ctx = core::QueryCtx::create(
        runtime.cpu(),
        core::QueryConfig(std::unordered_map<std::string, std::string>{
            {core::QueryConfig::
                 kMinLocalExchangePartitionCountToUsePartitionBuffer,
             "2"}}),
        {}, nullptr, root, nullptr, "join-batch-control");
    auto task =
        treatment ? runtime.task("join-batch-treatment", plan, root)
                  : exec::Task::create(
                        "join-batch-control",
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
    VELOX_CHECK(actual == expected, "Join output differs from independent bag");
    uint64_t rows = 0, expectedRows = 0, probeDrivers = 0;
    for (const auto &entry : expected)
      expectedRows += entry.second;
    for (const auto &p : stats.pipelineStats)
      for (const auto &op : p.operatorStats)
        if (op.planNodeId == "drill_1" && op.operatorType == "HashProbe") {
          vectors[treatment] += op.outputVectors;
          rows += op.outputPositions;
          probeDrivers += op.numDrivers;
        }
    VELOX_CHECK_EQ(probeDrivers, drivers);
    VELOX_CHECK_EQ(rows, expectedRows, "Join tail or NULL-key rows were lost");
  }
  if (!emptyBuild && count >= 16000 && (std::string(join) == "INNER" || outer))
    VELOX_CHECK_LT(vectors[1] * 2, vectors[0]);
  std::cout << "rows=" << count << " drivers=" << drivers << " join=" << join
            << " emptyBuild=" << emptyBuild << " old vectors=" << vectors[0]
            << " treatment=" << vectors[1] << '\n';
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(16);
    for (const char *join : {"INNER", "LEFT", "SEMI", "ANTI"})
      for (int drivers : {1, 4, 16}) {
        for (int count : {0, 1, 2, 97, 16003})
          test(runtime, count, drivers, join);
        test(runtime, 97, drivers, join, true);
      }
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
