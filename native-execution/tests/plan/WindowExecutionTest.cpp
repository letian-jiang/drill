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
#include <cmath>
#include <iostream>
#include <mutex>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class Input final : public BatchSource {
public:
  Input(std::shared_ptr<memory::MemoryPool> owner, RowVectorPtr data,
        std::shared_ptr<std::atomic<int>> next, memory::MemoryPool *pool)
      : owner_(std::move(owner)), data_(std::move(data)),
        next_(std::move(next)), pool_(pool) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    int start = next_->fetch_add(2);
    if (start >= data_->size())
      return std::nullopt;
    int count = std::min<int>(2, data_->size() - start);
    auto indices = AlignedBuffer::allocate<vector_size_t>(count, pool_);
    for (int i = 0; i < count; ++i)
      indices->asMutable<vector_size_t>()[i] = start + i;
    std::vector<VectorPtr> columns;
    for (const auto &column : data_->children())
      columns.push_back(
          BaseVector::wrapInDictionary(nullptr, indices, count, column));
    return std::make_shared<RowVector>(pool_, data_->rowType(), nullptr, count,
                                       std::move(columns));
  }
  void cancel() override {}

private:
  std::shared_ptr<memory::MemoryPool> owner_;
  RowVectorPtr data_;
  std::shared_ptr<std::atomic<int>> next_;
  memory::MemoryPool *pool_;
};
std::optional<int64_t> value(int id) {
  return id % 3 == 2 || id % 7 == 0 ? std::nullopt
                                    : std::optional<int64_t>(id + 3);
}
void test(VeloxRuntime &runtime, int mode, int count = 15) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("window-input");
  auto type =
      ROW({"id", "p", "k", "v"}, {BIGINT(), INTEGER(), BIGINT(), BIGINT()});
  auto input = BaseVector::create<RowVector>(type, count, pool.get());
  for (int row = 0; row < count; ++row) {
    int id = count - row - 1;
    input->childAt(0)->as<FlatVector<int64_t>>()->set(row, id);
    input->childAt(1)->as<FlatVector<int32_t>>()->set(row, id % 3);
    input->childAt(1)->setNull(row, id % 3 == 2);
    input->childAt(2)->as<FlatVector<int64_t>>()->set(row, id / 6);
    auto v = value(id);
    if (v)
      input->childAt(3)->as<FlatVector<int64_t>>()->set(row, *v);
    else
      input->childAt(3)->setNull(row, true);
  }
  bool global = mode == 5, rows = mode == 1, peers = mode == 3,
       full = mode >= 4;
  folly::dynamic aggregates = folly::dynamic::array;
  auto add = [&](const char *name, const char *expr) {
    aggregates.push_back(folly::dynamic::object("ref", std::string("`") + name +
                                                           "`")("expr", expr));
  };
  if (mode == 0) {
    add("rank", "rank(1)");
    add("dense", "dense_rank(1)");
    add("percent", "percent_rank(1)");
    add("cume", "cume_dist(1)");
  } else {
    add("sum", "sum(`v`)");
    add("count", "count(`v`)");
    add("extra", "sum(add(`v`,3))");
    if (rows) {
      add("rownum", "row_number(1)");
      add("tile", "ntile(3)");
      add("lag", "lag(`v`)");
      add("lead", "lead(`v`,1)");
    }
    if (rows || mode == 4) {
      add("first", "first_value(`v`)");
      add("last", "last_value(`v`)");
    }
  }
  folly::dynamic window = folly::dynamic::object("pop", "window")("@id", 1)(
      "frameUnitsRows", rows)("start",
                              folly::dynamic::object("unbounded", !peers)(
                                  "offset", peers ? 0 : INT64_MIN))(
      "end", folly::dynamic::object("unbounded", full)("offset",
                                                       full ? INT64_MIN : 0))(
      "withins", global ? folly::dynamic::array
                        : folly::dynamic::array(folly::dynamic::object(
                              "ref", "`p`")("expr", "`p`")))(
      "orderings", global ? folly::dynamic::array
                          : folly::dynamic::array(folly::dynamic::object(
                                "order", "ASC")("nullDirection", "LAST")(
                                "expr", rows || mode == 4 ? "`id`" : "`k`")))(
      "aggregations", aggregates)(
      "child", folly::dynamic::object("pop", "unordered-receiver")("@id", 2));
  auto next = std::make_shared<std::atomic<int>>(0);
  std::mutex mutex;
  std::vector<int> seen(count);
  int received = 0;
  FragmentPlanConverter converter(
      pool.get(),
      [&](const auto &) {
        return SourceBinding{type, SourceKind::Receiver,
                             [pool, input, next](memory::MemoryPool *driver) {
                               return std::make_shared<Input>(pool, input, next,
                                                              driver);
                             }};
      },
      [&](const auto &, const auto &) {
        return SinkFactory([&](memory::MemoryPool *) {
          return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
            if (!batch)
              return {};
            std::lock_guard lock(mutex);
            VELOX_CHECK_EQ(batch->childrenSize(), 4 + aggregates.size());
            for (const auto &name : batch->rowType()->names())
              VELOX_CHECK(!name.starts_with("drill_"),
                          "Helper column escaped Window");
            DecodedVector ids(*batch->childAt(0));
            for (int row = 0; row < batch->size(); ++row) {
              int id = ids.valueAt<int64_t>(row);
              VELOX_CHECK_EQ(seen.at(id)++, 0);
              ++received;
              std::vector<int> partition;
              for (int i = 0; i < count; ++i)
                if (global || i % 3 == id % 3)
                  partition.push_back(i);
              auto position =
                  std::find(partition.begin(), partition.end(), id) -
                  partition.begin();
              size_t begin = 0, end = partition.size();
              if (rows)
                end = position + 1;
              else if (!full) {
                while (end && partition[end - 1] / 6 > id / 6)
                  --end;
                if (peers)
                  while (begin < end && partition[begin] / 6 < id / 6)
                    ++begin;
              }
              int64_t sum = 0, present = 0;
              for (size_t i = begin; i < end; ++i)
                if (auto v = value(partition[i])) {
                  sum += *v;
                  ++present;
                }
              for (size_t col = 4; col < batch->childrenSize(); ++col) {
                auto name = batch->rowType()->nameOf(col);
                DecodedVector result(*batch->childAt(col));
                if (name == "percent" || name == "cume") {
                  size_t rank = 1, peerEnd = 0;
                  for (int i : partition) {
                    if (i / 6 < id / 6)
                      ++rank;
                    if (i / 6 <= id / 6)
                      ++peerEnd;
                  }
                  double expected =
                      name == "cume" ? double(peerEnd) / partition.size()
                      : partition.size() == 1
                          ? 0
                          : double(rank - 1) / (partition.size() - 1);
                  VELOX_CHECK_LT(
                      std::abs(result.valueAt<double>(row) - expected), 1e-14);
                  continue;
                }
                std::optional<int64_t> expected;
                if (name == "rank") {
                  expected = 1;
                  for (int i : partition)
                    if (i / 6 < id / 6)
                      ++*expected;
                } else if (name == "dense")
                  expected = id / 6 + 1;
                else if (name == "sum")
                  expected =
                      present ? std::optional<int64_t>(sum) : std::nullopt;
                else if (name == "extra")
                  expected = present ? std::optional<int64_t>(sum + 3 * present)
                                     : std::nullopt;
                else if (name == "count")
                  expected = present;
                else if (name == "rownum")
                  expected = position + 1;
                else if (name == "tile") {
                  int bucket = 1;
                  size_t offset = 0;
                  for (int b = 0; b < 3; ++b) {
                    size_t width = partition.size() / 3 +
                                   (size_t(b) < partition.size() % 3);
                    if (size_t(position) < offset + width) {
                      bucket = b + 1;
                      break;
                    }
                    offset += width;
                  }
                  expected = bucket;
                } else if (name == "lag")
                  expected =
                      position ? value(partition[position - 1]) : std::nullopt;
                else if (name == "lead")
                  expected = size_t(position + 1) < partition.size()
                                 ? value(partition[position + 1])
                                 : std::nullopt;
                else if (name == "first")
                  expected = value(partition[begin]);
                else if (name == "last")
                  expected = value(partition[end - 1]);
                else
                  VELOX_FAIL("Unknown oracle column");
                VELOX_CHECK_EQ(result.isNullAt(row), !expected,
                               "{} id {} mode {}", name, id, mode);
                if (expected) {
                  auto actual = result.base()->typeKind() == TypeKind::INTEGER
                                    ? result.valueAt<int32_t>(row)
                                    : result.valueAt<int64_t>(row);
                  VELOX_CHECK_EQ(actual, *expected, "{} id {} mode {}", name,
                                 id, mode);
                }
              }
            }
            return {};
          });
        });
      });
  auto plan = converter.convert(folly::dynamic::object("pop", "single-sender")(
      "@id", 0)("child", window));
  auto task = runtime.task("window-" + std::to_string(mode), plan, root);
  auto complete = task->taskCompletionFuture();
  task->start(4);
  std::move(complete).get();
  auto error = task->error();
  auto stats = task->taskStats();
  auto deleted = task->taskDeletionFuture();
  task.reset();
  std::move(deleted).get();
  if (error)
    std::rethrow_exception(error);
  VELOX_CHECK_EQ(received, count);
  int windowDrivers = 0;
  for (const auto &p : stats.pipelineStats)
    for (const auto &op : p.operatorStats)
      if (op.operatorType == "Window")
        windowDrivers += op.numDrivers;
  VELOX_CHECK_EQ(windowDrivers, global ? 1 : 4, "Window partition scheduling");
  // The original finite-offset sentinel must fail before a Task is submitted.
  window["start"] =
      folly::dynamic::object("unbounded", false)("offset", INT64_MIN);
  bool rejected = false;
  try {
    converter.convert(window);
  } catch (const VeloxException &) {
    rejected = true;
  }
  VELOX_CHECK(rejected);
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(4);
    for (int mode = 0; mode < 6; ++mode)
      test(runtime, mode);
    test(runtime, 0, 1);
    test(runtime, 1, 0);
    std::cout << "Window: ROWS/RANGE peers/prefix/full, NULL "
                 "partitions/values, ranking/NTILE/lead/lag/first/last, scalar "
                 "arguments, helpers, 4 partition drivers/global gather and "
                 "finite-bound rejection passed\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
