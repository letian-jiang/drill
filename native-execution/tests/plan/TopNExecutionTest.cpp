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
#include <mutex>
#include <numeric>
#include <thread>
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
    int start = next_->fetch_add(127);
    if (start >= rows_->size())
      return std::nullopt;
    int count = std::min<int>(127, rows_->size() - start);
    auto indices = AlignedBuffer::allocate<vector_size_t>(count, pool_);
    for (int i = 0; i < count; ++i)
      indices->asMutable<vector_size_t>()[i] = start + i;
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
std::optional<int64_t> number(int id) {
  return id % 11 == 0 ? std::nullopt : std::optional<int64_t>(id % 137 - 68);
}
std::optional<std::string> label(int id) {
  static const std::string labels[] = {"雪", "alpha", "🚀", "",
                                       std::string("a\0b", 3)};
  return id % 13 == 0 ? std::nullopt
                      : std::optional<std::string>(labels[id % 5]);
}
template <class T>
int compare(const std::optional<T> &a, const std::optional<T> &b,
            bool ascending, bool nullsFirst) {
  if (!a || !b)
    return !a && !b ? 0 : (!a == nullsFirst ? -1 : 1);
  int result = *a < *b ? -1 : (*a > *b ? 1 : 0);
  return ascending ? result : -result;
}
folly::dynamic order(const char *key, bool ascending, bool nullsFirst) {
  return folly::dynamic::object("expr", key)(
      "order", ascending ? "ASC" : "DESC")("nullDirection",
                                           nullsFirst ? "FIRST" : "LAST");
}
void test(VeloxRuntime &runtime, int mode, int count, int limit, int offset = 0,
          bool duplicate = false) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("topn-input");
  auto type = ROW({"id", "k", "label", "amount", "payload"},
                  {BIGINT(), BIGINT(), VARCHAR(), DECIMAL(18, 2),
                   ROW({"n", "text"}, {BIGINT(), VARCHAR()})});
  auto input = BaseVector::create<RowVector>(type, count, pool.get());
  auto payload = input->childAt(4)->as<RowVector>();
  for (int row = 0; row < count; ++row) {
    int id = count - row - 1;
    input->childAt(0)->as<FlatVector<int64_t>>()->set(row, id);
    if (auto n = number(id))
      input->childAt(1)->as<FlatVector<int64_t>>()->set(row, *n);
    else
      input->childAt(1)->setNull(row, true);
    if (auto text = label(id))
      input->childAt(2)->as<FlatVector<StringView>>()->set(row,
                                                           StringView(*text));
    else
      input->childAt(2)->setNull(row, true);
    input->childAt(3)->as<FlatVector<int64_t>>()->set(row, id % 23 - 11);
    payload->childAt(0)->as<FlatVector<int64_t>>()->set(row, -id);
    payload->childAt(0)->setNull(row, id % 7 == 0);
    auto text = std::string(100 + id % 31, 'a' + id % 26) + "雪🚀";
    payload->childAt(1)->as<FlatVector<StringView>>()->set(row,
                                                           StringView(text));
  }
  bool ascending = mode & 1, nullsFirst = mode & 2;
  std::vector<int64_t> expected(count);
  std::iota(expected.begin(), expected.end(), 0);
  std::sort(expected.begin(), expected.end(), [&](int a, int b) {
    if (int c = compare(number(a), number(b), ascending, nullsFirst))
      return c < 0;
    if (int c = compare(label(a), label(b), !ascending, !nullsFirst))
      return c < 0;
    if (a % 23 != b % 23)
      return a % 23 > b % 23;
    return a < b;
  });
  expected.resize(std::min(count, limit));
  expected.erase(expected.begin(),
                 expected.begin() + std::min<int>(offset, expected.size()));
  auto orders = folly::dynamic::array(order("`k`", ascending, nullsFirst));
  if (duplicate)
    orders.push_back(order("`k`", !ascending, !nullsFirst));
  orders.push_back(order("`label`", !ascending, !nullsFirst));
  orders.push_back(order("`amount`", false, false));
  orders.push_back(order("`id`", true, false));
  // Compare both implementations with an independent ordered oracle, including
  // full nested payloads. The old full-sort plan remains a useful control.
  for (bool topn : {false, true}) {
    auto next = std::make_shared<std::atomic<int>>(0);
    auto factories = std::make_shared<std::atomic<int>>(0);
    std::mutex mutex;
    std::vector<int64_t> actual;
    FragmentPlanConverter converter(
        pool.get(),
        [&](const auto &) {
          return SourceBinding{
              type, SourceKind::Receiver,
              [input, next, factories](memory::MemoryPool *driver) {
                ++*factories;
                return std::make_shared<Input>(input, next, driver);
              }};
        },
        [&](const auto &, const auto &) {
          return SinkFactory([&](memory::MemoryPool *) {
            return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
              if (!batch)
                return {};
              VELOX_CHECK(batch->rowType()->equivalent(*type));
              std::lock_guard lock(mutex);
              DecodedVector ids(*batch->childAt(0));
              for (int row = 0; row < batch->size(); ++row) {
                int id = ids.valueAt<int64_t>(row);
                VELOX_CHECK_EQ(batch->compare(input.get(), row, count - id - 1,
                                              CompareFlags{}),
                               0, "TopN changed a pass-through column");
                actual.push_back(id);
              }
              return {};
            });
          });
        });
    folly::dynamic sort = folly::dynamic::object("pop", topn ? "top-n"
                                                             : "external-sort")(
        "@id", 1)("orderings", orders)("limit", limit)(
        "child", folly::dynamic::object("pop", "unordered-receiver")("@id", 2));
    folly::dynamic selected = sort;
    if (!topn || offset)
      selected = folly::dynamic::object("pop", "limit")("@id", 3)(
          "first", offset)("last", limit)("child", selected);
    auto plan = converter.convert(folly::dynamic::object(
        "pop", "single-sender")("@id", 0)("child", selected));
    auto task = runtime.task("topn-" + std::to_string(mode) + "-" +
                                 std::to_string(topn),
                             plan, root);
    auto complete = task->taskCompletionFuture();
    task->start(4);
    std::move(complete).get();
    auto error = task->error();
    auto stats = task->taskStats();
    // Finishing the output pipeline may finish the Task before an idle partial
    // driver has closed. Wait for that driver's final stats before asserting
    // the exact parallelism, rather than sampling the asynchronous close.
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
    VELOX_CHECK(actual == expected,
                "Ordered TopN differs from independent reference");
    if (topn && limit > 0 && offset < limit) {
      int partialDrivers = 0, finalDrivers = 0;
      uint64_t inputs = 0, candidates = 0;
      for (const auto &p : stats.pipelineStats)
        for (const auto &op : p.operatorStats) {
          if (op.planNodeId == "drill_1_partial") {
            partialDrivers += op.numDrivers;
            inputs += op.inputPositions;
          } else if (op.planNodeId == "drill_1") {
            VELOX_CHECK_EQ(op.operatorType, "TopN");
            finalDrivers += op.numDrivers;
            candidates += op.inputPositions;
          }
        }
      VELOX_CHECK_EQ(partialDrivers, 4);
      VELOX_CHECK_EQ(finalDrivers, 1);
      VELOX_CHECK_EQ(inputs, count);
      VELOX_CHECK_LE(candidates, uint64_t(4) * limit);
      VELOX_CHECK_GE(candidates, std::min(count, limit));
      std::cout << "rows=" << count << " limit=" << limit
                << " final candidates=" << candidates << '\n';
    } else if (!limit) {
      VELOX_CHECK_EQ(factories->load(), 0,
                     "LIMIT 0 must not read source batches");
    }
    if (topn) {
      for (int64_t invalid : {int64_t(-1), int64_t(INT32_MAX) + 1}) {
        sort["limit"] = invalid;
        bool rejected = false;
        try {
          converter.convert(sort);
        } catch (const VeloxException &) {
          rejected = true;
        }
        VELOX_CHECK(rejected, "Invalid original TopN INT limit accepted");
      }
    }
  }
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(4);
    for (int mode = 0; mode < 4; ++mode)
      test(runtime, mode, 16003, 17, mode == 3 ? 7 : 0);
    test(runtime, 1, 16003, 17, 0, true);
    test(runtime, 0, 7, 100);
    test(runtime, 2, 0, 10);
    test(runtime, 3, 100, 0);
    test(runtime, 1, 257, 1);
    test(runtime, 0, 257, 10, 10);
    std::cout << "TopN: independent ordered oracle/full-sort control, 4 "
                 "partial drivers/1 final, "
                 "bounded candidates, "
                 "NULL/directions/Unicode/Decimal/dictionary/nested payload, "
                 "OFFSET, duplicate keys, empty input, LIMIT 0/1/above size "
                 "and invalid limit passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
