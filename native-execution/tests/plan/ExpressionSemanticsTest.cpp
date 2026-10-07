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
#include "plan/ExpressionBinder.h"
#include "scan/iceberg/IcebergOutput.h"
#include <atomic>
#include <cmath>
#include <iostream>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
class Input final : public BatchSource {
public:
  Input(std::shared_ptr<memory::MemoryPool> pool, RowVectorPtr data,
        std::shared_ptr<std::atomic<bool>> claimed)
      : pool_(std::move(pool)), data_(std::move(data)),
        claimed_(std::move(claimed)) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    if (claimed_->exchange(true))
      return std::nullopt;
    return data_;
  }
  void cancel() override {}

private:
  std::shared_ptr<memory::MemoryPool> pool_;
  RowVectorPtr data_;
  std::shared_ptr<std::atomic<bool>> claimed_;
};
int main() {
  try {
    VeloxRuntime runtime(4);
    auto root =
        memory::memoryManager()->addRootPool("drill-expression-semantics");
    auto pool = root->addLeafChild("values");
    auto schema =
        ROW({"s", "n", "d", "a", "b"},
            {VARCHAR(), DOUBLE(), DOUBLE(), DECIMAL(38, 2), DECIMAL(38, 3)});
    auto input = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(schema, 3, pool.get()));
    auto strings = input->childAt(0)->as<FlatVector<StringView>>();
    strings->set(0, StringView("汉字abc"));
    strings->set(1, StringView("hello"));
    strings->setNull(2, true);
    for (int r = 0; r < 3; ++r) {
      input->childAt(1)->as<FlatVector<double>>()->set(r, r == 2 ? 0.0 : 1.0);
      input->childAt(2)->as<FlatVector<double>>()->set(r, r == 1 ? -0.0 : 0.0);
      input->childAt(3)->as<FlatVector<int128_t>>()->set(r, 12345);
      input->childAt(4)->as<FlatVector<int128_t>>()->set(r, 123450);
    }
    auto claimed = std::make_shared<std::atomic<bool>>(false);
    int rows = 0;
    FragmentPlanConverter converter(
        pool.get(),
        [&](const folly::dynamic &) {
          return SourceBinding{schema, SourceKind::Receiver,
                               [pool, input, claimed](memory::MemoryPool *) {
                                 return std::make_shared<Input>(pool, input,
                                                                claimed);
                               }};
        },
        [&](const folly::dynamic &, const RowTypePtr &) {
          return SinkFactory([&](memory::MemoryPool *) {
            return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
              if (!batch)
                return {};
              DecodedVector same(*batch->childAt(0)), ratio(*batch->childAt(1)),
                  prefix(*batch->childAt(2)), empty(*batch->childAt(3)),
                  discount(*batch->childAt(4));
              DecodedVector partitionHash(*batch->childAt(5)),
                  epoch(*batch->childAt(6)), year(*batch->childAt(7));
              VELOX_CHECK(batch->childAt(6)->type()->isDate());
              VELOX_CHECK(batch->childAt(7)->type()->isBigint());
              // Reference from Java HashHelper.hash32(123.45,
              // HashHelper.hash32(42.0, 1301011)); keep independent of C++
              // hash.
              constexpr int32_t expectedHash = 258144348;
              for (int r = 0; r < batch->size(); ++r) {
                VELOX_CHECK_EQ(partitionHash.valueAt<int32_t>(r), expectedHash);
                VELOX_CHECK_EQ(epoch.valueAt<int32_t>(r), 0);
                VELOX_CHECK_EQ(year.valueAt<int64_t>(r), 1970);
                VELOX_CHECK(same.valueAt<bool>(r));
                auto divide = ratio.valueAt<double>(r);
                VELOX_CHECK(r == 2 ? std::isnan(divide)
                                   : std::isinf(divide) &&
                                         (std::signbit(divide) == (r == 1)));
                VELOX_CHECK(r == 2 ? prefix.isNullAt(r)
                                   : prefix.valueAt<StringView>(r) ==
                                         StringView(r == 0 ? "汉字" : "he"));
                VELOX_CHECK(r == 2 ? empty.isNullAt(r)
                                   : empty.valueAt<StringView>(r).empty());
                VELOX_CHECK_EQ(discount.valueAt<int64_t>(r), 25);
                ++rows;
              }
              return {};
            });
          });
        });
    auto plan = converter.convert(
        folly::parseJson(R"PLAN({"pop":"single-sender","@id":0,"child":{
      "pop":"project","@id":1,"exprs":[
        {"ref":"`same`","expr":"equal(`a`,`b`)"},
        {"ref":"`ratio`","expr":"divide(`n`,`d`)"},
        {"ref":"`prefix`","expr":"substring(`s`,1,2)"},
        {"ref":"`empty`","expr":"substring(`s`,0,2)"},
        {"ref":"`discount`","expr":"subtract(1,cast('0.75' as VARDECIMAL(15,2)))"},
        {"ref":"`partition`","expr":"hash32AsDouble(`a`,hash32AsDouble(42,1301011))"},
        {"ref":"`epoch`","expr":"cast(0 as DATE)"},
        {"ref":"`year`","expr":"extractYear(cast(0 as DATE))"}],
      "child":{"pop":"unordered-receiver","@id":2}}})PLAN"));
    auto task = runtime.task("drill-expression-semantics", plan, root);
    auto complete = task->taskCompletionFuture();
    task->start(4);
    std::move(complete).get();
    if (task->error())
      std::rethrow_exception(task->error());
    VELOX_CHECK_EQ(rows, 3);
    auto temporalType = ROW({"ts", "n"}, {TIMESTAMP(), BIGINT()});
    auto temporal = std::dynamic_pointer_cast<RowVector>(BaseVector::create(temporalType, 4, pool.get()));
    auto *ts = temporal->childAt(0)->as<FlatVector<Timestamp>>();
    ts->set(0, Timestamp(-1,999999000)); ts->set(1, Timestamp(0,1000));
    ts->set(2, Timestamp(0,1001000)); ts->setNull(3, true);
    auto *integers = temporal->childAt(1)->as<FlatVector<int64_t>>();
    for (int row = 0; row < 4; ++row) integers->set(row, 0);
    integers->set(1, INT64_MIN); integers->set(2, INT64_MAX);
    auto temporalClaimed = std::make_shared<std::atomic<bool>>(false);
    std::vector<int64_t> temporalRows;
    FragmentPlanConverter temporalConverter(pool.get(),
        [&](const folly::dynamic &) {
          return SourceBinding{temporalType, SourceKind::NativeScan,
              [pool, temporal, temporalClaimed](memory::MemoryPool *) {
                return std::make_shared<Input>(pool, temporal, temporalClaimed);
              }, icebergOutput};
        }, [&](const folly::dynamic &, const RowTypePtr &) {
          return SinkFactory([&](memory::MemoryPool *) {
            return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
              if (batch) {
                DecodedVector values(*batch->childAt(0));
                DecodedVector castValue(*batch->childAt(1)), negative(*batch->childAt(2)), future(*batch->childAt(3));
                for (int row = 0; row < batch->size(); ++row) {
                  auto value = values.valueAt<Timestamp>(row);
                  VELOX_CHECK_EQ(value.getNanos() % 1000000, 0);
                  VELOX_CHECK_EQ(castValue.valueAt<Timestamp>(row).toMillis(), value.toMillis() == 0 ? INT64_MIN : INT64_MAX);
                  VELOX_CHECK_EQ(negative.valueAt<Timestamp>(row).toMillis(), -1001);
                  VELOX_CHECK_EQ(future.valueAt<Timestamp>(row).toMillis(), 1924992000123LL);
                  temporalRows.push_back(value.toMillis());
                }
              }
              return {};
            });
          });
        });
    auto temporalPlan = temporalConverter.convert(folly::parseJson(R"PLAN(
      {"pop":"single-sender","@id":0,"child":{"pop":"project","@id":2,"exprs":[
       {"ref":"`ts`","expr":"`ts`"},{"ref":"`cast`","expr":"cast(`n` as TIMESTAMP)"},
       {"ref":"`negative`","expr":"cast(-1001 as TIMESTAMP)"},
       {"ref":"`future`","expr":"cast(1924992000123 as TIMESTAMP)"}],
       "child":{"pop":"test-scan","@id":1,
       "nativeScan":{"filter":"greater_than(`ts`,cast('1970-01-01 00:00:00' as TIMESTAMP))",
       "outputFields":[{"name":"ts","minor":"TIMESTAMP","optional":true},
                       {"name":"n","minor":"BIGINT","optional":true}]}}}})PLAN"));
    auto temporalTask = runtime.task("drill-native-scan-precision", temporalPlan, root);
    auto temporalComplete = temporalTask->taskCompletionFuture();
    temporalTask->start(1);
    std::move(temporalComplete).get();
    if (temporalTask->error()) std::rethrow_exception(temporalTask->error());
    VELOX_CHECK(temporalRows == std::vector<int64_t>({0,1}),
        "Scan filter must see raw microseconds before output normalization");
    auto timeSchema = ROW({"t"}, {TIME_MICRO_UTC()});
    auto times = BaseVector::create<RowVector>(timeSchema, 9, pool.get());
    auto *timeValues = times->childAt(0)->as<FlatVector<int64_t>>();
    const std::vector<int64_t> rawTimes{1001000, 1001001, 1001999, 0, 0, 86399999999LL,
                                       1001499, 1001500, 1001501};
    for (vector_size_t row = 0; row < times->size(); ++row) timeValues->set(row, rawTimes[row]);
    timeValues->setNull(3, true);
    auto timeClaimed = std::make_shared<std::atomic<bool>>(false);
    std::vector<int64_t> actualTimes;
    FragmentPlanConverter timeConverter(pool.get(), [&](const folly::dynamic &) {
      return SourceBinding{timeSchema, SourceKind::NativeScan,
          [pool, times, timeClaimed](memory::MemoryPool *) {
            return std::make_shared<Input>(pool, times, timeClaimed);
          }, icebergOutput};
    }, [&](const folly::dynamic &, const RowTypePtr &output) {
      VELOX_CHECK(output->childAt(0)->equivalent(*TIME()));
      return SinkFactory([&](memory::MemoryPool *) {
        return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
          if (!batch) return {};
          DecodedVector t(*batch->childAt(0)), i(*batch->childAt(1)), l(*batch->childAt(2)),
              literal(*batch->childAt(3)), text(*batch->childAt(4));
          for (vector_size_t row = 0; row < batch->size(); ++row) {
            auto value = t.valueAt<int64_t>(row);
            VELOX_CHECK_EQ(i.valueAt<int32_t>(row), value);
            VELOX_CHECK_EQ(l.valueAt<int64_t>(row), value);
            VELOX_CHECK_EQ(literal.valueAt<int64_t>(row), 1001);
            VELOX_CHECK_EQ(text.valueAt<int64_t>(row), 1001);
            actualTimes.push_back(value);
          }
          return {};
        });
      });
    });
    auto timePlan = timeConverter.convert(folly::parseJson(R"PLAN(
      {"pop":"single-sender","@id":0,"child":{"pop":"project","@id":2,"exprs":[
       {"ref":"`t`","expr":"`t`"},{"ref":"`i`","expr":"cast(`t` as INT)"},
       {"ref":"`l`","expr":"cast(`t` as BIGINT)"},
       {"ref":"`literal`","expr":"cast(1001 as TIME)"},
       {"ref":"`text`","expr":"cast('00:00:01.001' as TIME)"}],
       "child":{"pop":"test-scan","@id":1,"nativeScan":{
       "filter":"greater_than(`t`,cast(1001 as TIME))",
       "outputFields":[{"name":"t","minor":"TIME","optional":true}]}}}})PLAN"));
    auto timeTask = runtime.task("drill-native-time-precision", timePlan, root);
    auto timeComplete = timeTask->taskCompletionFuture(); timeTask->start(4);
    std::move(timeComplete).get();
    if (timeTask->error()) std::rethrow_exception(timeTask->error());
    VELOX_CHECK(actualTimes == std::vector<int64_t>({1001,1002,86400000,1001,1002,1002}),
        "TIME scan filter must compare microseconds and output must expose milliseconds");
    const auto logicalTime = ROW({"t"}, {TIME()});
    VELOX_CHECK(bindExpression("min(`t`)", logicalTime)->type()->equivalent(*TIME()),
                 "MIN must retain the TIME logical type");
    VELOX_CHECK(bindExpression("max(`t`)", logicalTime)->type()->equivalent(*TIME()),
                 "MAX must retain the TIME logical type");
    std::cout << "Exact mixed-scale decimals, integer/decimal coercion, "
                 "UTF-8/NULL substring, IEEE divide, native scan filter/precision ordering passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
