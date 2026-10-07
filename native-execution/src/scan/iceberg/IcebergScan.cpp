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
#include "scan/iceberg/IcebergScan.h"
#include "scan/iceberg/IcebergArrowBatch.h"
#include "scan/iceberg/IcebergOutput.h"
#include "IcebergReaderAbi.h"
#include <atomic>
#include <deque>
#include <dlfcn.h>
#include <folly/json.h>
#include <velox/vector/arrow/Abi.h>
#include <velox/vector/arrow/Bridge.h>
namespace drill::nativeexec {
namespace {
struct Api {
  decltype(&drill_iceberg_open) open;
  decltype(&drill_iceberg_next) next;
  decltype(&drill_iceberg_close) close;
  Api() {
    auto path = std::getenv("DRILL_NATIVE_ICEBERG_LIBRARY");
    VELOX_USER_CHECK(
        path, "Set DRILL_NATIVE_ICEBERG_LIBRARY for native Iceberg Scan");
    auto module = dlopen(path, RTLD_NOW | RTLD_LOCAL | RTLD_DEEPBIND);
    VELOX_USER_CHECK(module, "Cannot load Iceberg SDK: {}", dlerror());
    open =
        reinterpret_cast<decltype(open)>(dlsym(module, "drill_iceberg_open"));
    next =
        reinterpret_cast<decltype(next)>(dlsym(module, "drill_iceberg_next"));
    close =
        reinterpret_cast<decltype(close)>(dlsym(module, "drill_iceberg_close"));
    VELOX_USER_CHECK(open && next && close, "Missing Iceberg C ABI functions");
    // Arrow release callbacks may outlive the reader; retain the SDK module.
  }
};
Api &api() {
  static Api api;
  return api;
}
struct Splits {
  std::vector<std::string> configs;
  std::atomic<size_t> next{0};
};
class Source final : public BatchSource,
                     public std::enable_shared_from_this<Source> {
public:
  Source(std::shared_ptr<Splits> splits, memory::MemoryPool *pool,
         folly::Executor *io)
      : splits_(std::move(splits)), pool_(pool->shared_from_this()), io_(io) {}
  ~Source() override {
    if (reader_)
      api().close(reader_);
  }
  std::optional<RowVectorPtr> next(ContinueFuture &future) override {
    std::lock_guard lock(mutex_);
    if (error_)
      std::rethrow_exception(error_);
    if (cancelled_ || finished_)
      return std::nullopt;
    if (ready_) {
      auto result = std::move(ready_);
      return result;
    }
    ContinuePromise promise;
    future = promise.getSemiFuture();
    waiters_.push_back(std::move(promise));
    if (!reading_) {
      reading_ = true;
      io_->add([self = shared_from_this()] { self->read(); });
    }
    return RowVectorPtr{};
  }
  void cancel() override {
    std::vector<ContinuePromise> wake;
    {
      std::lock_guard lock(mutex_);
      cancelled_ = true;
      ready_.reset();
      wake.swap(waiters_);
    }
    for (auto &promise : wake)
      promise.setValue();
  }

private:
  RowVectorPtr readNext() {
    char error[4096] = {};
    while (true) {
      if (!reader_) {
        auto index = splits_->next.fetch_add(1);
        if (index >= splits_->configs.size())
          return nullptr;
        VELOX_USER_CHECK_EQ(api().open(splits_->configs[index].c_str(),
                                       &reader_, error, sizeof(error)),
                            0, "Iceberg open: {}", error);
      }
      ArrowSchema schema{};
      ArrowArray array{};
      auto result = api().next(reader_, &schema, &array, error, sizeof(error));
      VELOX_USER_CHECK_GE(result, 0, "Iceberg read: {}", error);
      if (result == 0) {
        api().close(reader_);
        reader_ = nullptr;
        continue;
      }
      try {
        auto vector = importIcebergBatch(schema, array, pool_.get());
        VELOX_USER_CHECK(vector, "Iceberg SDK batch must be a struct");
        return vector;
      } catch (...) {
        if (array.release)
          array.release(&array);
        if (schema.release)
          schema.release(&schema);
        throw;
      }
    }
  }
  void read() {
    RowVectorPtr batch;
    std::exception_ptr error;
    try {
      batch = readNext();
    } catch (...) {
      error = std::current_exception();
    }
    std::vector<ContinuePromise> wake;
    {
      std::lock_guard lock(mutex_);
      reading_ = false;
      error_ = error;
      finished_ = !batch;
      if (!cancelled_)
        ready_ = std::move(batch);
      wake.swap(waiters_);
    }
    for (auto &promise : wake)
      promise.setValue();
  }
  std::shared_ptr<Splits> splits_;
  std::shared_ptr<memory::MemoryPool> pool_;
  folly::Executor *io_;
  void *reader_ = nullptr;
  std::mutex mutex_;
  std::vector<ContinuePromise> waiters_;
  RowVectorPtr ready_;
  bool reading_ = false, finished_ = false, cancelled_ = false;
  std::exception_ptr error_;
};
} // namespace
SourceFactory icebergSourceFactory(const folly::dynamic &configs,
                                   folly::Executor *io) {
  auto splits = std::make_shared<Splits>();
  for (auto &config : configs)
    splits->configs.push_back(folly::toJson(config));
  return [splits, io](memory::MemoryPool *pool) {
    return std::make_shared<Source>(splits, pool, io);
  };
}
namespace {
class IcebergScanPlugin final : public NativeScanPlugin {
public:
  std::string_view provider() const override { return "iceberg-parquet"; }
  uint32_t descriptorVersion() const override { return 1; }
  NativeScanBinding prepare(const NativeScanRequest &request,
                            const NativeScanContext &context) const override {
    const auto &scan = request.descriptor;
    VELOX_USER_CHECK(scan.count("splits") && scan["splits"].isArray(),
                     "Iceberg native scan requires assigned splits");
    return {icebergReadType(scan["readFields"]),
            icebergSourceFactory(scan["splits"], context.io), icebergOutput};
  }
};
}
std::shared_ptr<const NativeScanPlugin> icebergScanPlugin() {
  return std::make_shared<IcebergScanPlugin>();
}
} // namespace drill::nativeexec
