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
// Minimal native scan module: assigned integer ranges, no Java/JNI calls.
#include "columnar/ColumnarBatch.h"
#include "scan/NativeScanRegistry.h"
#include <atomic>
#include <velox/vector/FlatVector.h>
namespace drill::nativeexec {
namespace {
struct Work {
  struct Range {
    int32_t start;
    int32_t rows;
  };
  std::vector<Range> ranges;
  std::atomic<size_t> next{0};
};
class RangeReader final : public BatchSource {
public:
  RangeReader(std::shared_ptr<Work> work, RowTypePtr schema,
              memory::MemoryPool *pool, folly::CancellationToken cancellation)
      : work_(std::move(work)), schema_(std::move(schema)),
        pool_(pool->shared_from_this()),
        cancellation_(std::move(cancellation)) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    if (cancelled_.load() || cancellation_.isCancellationRequested())
      return std::nullopt;
    auto n = work_->next.fetch_add(1);
    if (n >= work_->ranges.size())
      return std::nullopt;
    auto range = work_->ranges[n];
    auto column = BaseVector::create<FlatVector<int32_t>>(INTEGER(), range.rows,
                                                          pool_.get());
    for (int32_t i = 0; i < range.rows; ++i)
      column->set(i, range.start + i);
    return std::make_shared<RowVector>(pool_.get(), schema_, nullptr,
                                       range.rows,
                                       std::vector<VectorPtr>{column});
  }
  void cancel() override { cancelled_.store(true); }

private:
  std::shared_ptr<Work> work_;
  RowTypePtr schema_;
  std::shared_ptr<memory::MemoryPool> pool_;
  folly::CancellationToken cancellation_;
  std::atomic<bool> cancelled_{false};
};
class RangePlugin final : public NativeScanPlugin {
public:
  std::string_view provider() const override { return "example-range"; }
  uint32_t descriptorVersion() const override { return 1; }
  NativeScanBinding prepare(const NativeScanRequest &request,
                            const NativeScanContext &context) const override {
    auto schema = columnarType(request.descriptor["readFields"]);
    VELOX_USER_CHECK_EQ(schema->size(), 1);
    VELOX_USER_CHECK(schema->childAt(0)->isInteger(),
                     "Range scan requires one INT field");
    auto work = std::make_shared<Work>();
    for (const auto &split : request.descriptor["splits"]) {
      auto start = split["start"].asInt(), rows = split["rows"].asInt();
      VELOX_USER_CHECK(start >= INT32_MIN && rows >= 0 && rows <= INT32_MAX &&
                           start <= INT32_MAX - rows,
                       "Range scan work exceeds INT bounds");
      work->ranges.push_back({int32_t(start), int32_t(rows)});
    }
    return {schema,
            [work, schema,
             cancellation = context.cancellation](memory::MemoryPool *pool) {
              return std::make_shared<RangeReader>(work, schema, pool,
                                                   cancellation);
            },
            {}};
  }
};
} // namespace
} // namespace drill::nativeexec
extern "C" void drill_register_native_scan_plugins_v1(
    drill::nativeexec::NativeScanRegistry *registry) {
  registry->registerPlugin(std::make_shared<drill::nativeexec::RangePlugin>());
}
