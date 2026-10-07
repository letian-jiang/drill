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
#include "scan/jni/JniPluginScan.h"
#include "columnar/ColumnarBatch.h"
#include <atomic>
#include <chrono>
#include <climits>
#include <deque>
#include <folly/ScopeGuard.h>
#include <folly/json.h>
#include <iostream>
#include <jni.h>
#include <sstream>
namespace drill::nativeexec {
namespace {
std::atomic<size_t> attachments{0}, detachments{0};
using Clock = std::chrono::steady_clock;
uint64_t elapsed(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                              start)
      .count();
}
struct Metrics {
  uint64_t works = 0;
  std::atomic<uint64_t> openCalls{0}, openNs{0}, closeCalls{0}, closeNs{0};
  std::atomic<uint64_t> readCalls{0}, readNs{0}, readNonImportNs{0},
      readErrors{0};
  std::atomic<uint64_t> importCalls{0}, importNs{0}, importErrors{0};
  std::atomic<uint64_t> importedBatches{0}, importedRows{0}, importedBytes{0};
  std::atomic<uint64_t> ioSubmissions{0}, ioQueueNs{0};
  folly::dynamic snapshot() const {
    return folly::dynamic::object("works", works)(
        "reader_open_calls", openCalls.load())("reader_open_ns", openNs.load())(
        "reader_close_calls", closeCalls.load())(
        "reader_close_ns", closeNs.load())("read_calls", readCalls.load())(
        "read_call_ns", readNs.load())("read_non_import_ns",
                                       readNonImportNs.load())(
        "read_errors", readErrors.load())("import_calls", importCalls.load())(
        "import_ns", importNs.load())("import_errors", importErrors.load())(
        "imported_batches", importedBatches.load())(
        "imported_rows", importedRows.load())("imported_buffer_bytes",
                                              importedBytes.load())(
        "io_submissions", ioSubmissions.load())("io_queue_wait_ns",
                                                ioQueueNs.load());
  }
};
class Timer {
public:
  explicit Timer(std::atomic<uint64_t> &counter, uint64_t *local = nullptr)
      : counter_(counter), local_(local), start_(Clock::now()) {}
  ~Timer() {
    auto nanos = elapsed(start_);
    counter_.fetch_add(nanos, std::memory_order_relaxed);
    if (local_)
      *local_ += nanos;
  }

private:
  std::atomic<uint64_t> &counter_;
  uint64_t *local_;
  Clock::time_point start_;
};
void scanLog(const std::string &line) {
  static std::mutex mutex;
  std::lock_guard lock(mutex);
  std::cout << line << std::endl;
}
struct ImportSlot {
  memory::MemoryPool *pool;
  const ColumnarBatchLayout &layout;
  std::vector<jlong> &addresses, &lengths;
  std::vector<BufferView> &buffers;
  RowVectorPtr batch;
  std::exception_ptr error;
  Metrics &metrics;
  uint64_t importNanos = 0;
};
std::string string(JNIEnv *env, jstring value) {
  const char *text = env->GetStringUTFChars(value, nullptr);
  VELOX_USER_CHECK(text, "Cannot read JNI string");
  std::string result(text);
  env->ReleaseStringUTFChars(value, text);
  return result;
}
void check(JNIEnv *env) {
  if (!env->ExceptionCheck())
    return;
  auto error = env->ExceptionOccurred();
  env->ExceptionClear();
  auto klass = env->GetObjectClass(error);
  auto toString = env->GetMethodID(klass, "toString", "()Ljava/lang/String;");
  auto message = static_cast<jstring>(env->CallObjectMethod(error, toString));
  auto text =
      message ? string(env, message) : std::string("Java scan exception");
  env->DeleteLocalRef(message);
  env->DeleteLocalRef(klass);
  env->DeleteLocalRef(error);
  VELOX_USER_FAIL("JNI plugin scan: {}", text);
}
jbyteArray bytes(JNIEnv *env, const std::string &value) {
  VELOX_USER_CHECK_LE(value.size(), size_t(INT_MAX),
                      "JNI scan descriptor is too large");
  auto result = env->NewByteArray(static_cast<jsize>(value.size()));
  check(env);
  VELOX_USER_CHECK(result, "Cannot allocate JNI scan descriptor");
  env->SetByteArrayRegion(result, 0, static_cast<jsize>(value.size()),
                          reinterpret_cast<const jbyte *>(value.data()));
  if (env->ExceptionCheck()) {
    env->DeleteLocalRef(result);
    check(env);
  }
  return result;
}
std::string bytes(JNIEnv *env, jbyteArray value) {
  VELOX_USER_CHECK(value, "Missing JNI scan schema");
  auto size = env->GetArrayLength(value);
  std::string result(size, '\0');
  env->GetByteArrayRegion(value, 0, size,
                          reinterpret_cast<jbyte *>(result.data()));
  check(env);
  return result;
}
void JNICALL import(JNIEnv *env, jclass, jlong pointer, jint rows,
                    jlongArray addresses, jlongArray lengths) {
  auto &slot = *reinterpret_cast<ImportSlot *>(pointer);
  ++slot.metrics.importCalls;
  Timer timer(slot.metrics.importNs, &slot.importNanos);
  try {
    auto count = env->GetArrayLength(addresses);
    VELOX_USER_CHECK_EQ(count, env->GetArrayLength(lengths));
    VELOX_USER_CHECK_EQ(static_cast<size_t>(count), slot.layout.bufferCount,
                        "Plugin buffer count changed");
    auto &pointers = slot.addresses;
    auto &sizes = slot.lengths;
    env->GetLongArrayRegion(addresses, 0, count, pointers.data());
    env->GetLongArrayRegion(lengths, 0, count, sizes.data());
    check(env);
    auto &buffers = slot.buffers;
    uint64_t bufferBytes = 0;
    for (int i = 0; i < count; ++i) {
      VELOX_USER_CHECK_GE(sizes[i], 0);
      VELOX_USER_CHECK(sizes[i] == 0 || pointers[i] != 0,
                       "Plugin returned a null data buffer");
      buffers[i] = {reinterpret_cast<const char *>(pointers[i]),
                    size_t(sizes[i])};
      bufferBytes += sizes[i];
    }
    slot.batch = decodeBatchBuffers(slot.layout, rows, buffers, slot.pool);
    ++slot.metrics.importedBatches;
    slot.metrics.importedRows += rows;
    slot.metrics.importedBytes += bufferBytes;
  } catch (...) {
    ++slot.metrics.importErrors;
    slot.error = std::current_exception();
  }
}
struct Host {
  JavaVM *vm = nullptr;
  jclass klass;
  jmethodID reserve, open, schema, read, cancel, close, active;
  Host(JNIEnv *env, jclass scanHost) {
    VELOX_USER_CHECK_EQ(env->GetJavaVM(&vm), JNI_OK);
    bind(env, scanHost);
  }
  void bind(JNIEnv *env, jclass scanHost) {
    klass = static_cast<jclass>(env->NewGlobalRef(scanHost));
    check(env);
    VELOX_USER_CHECK(klass, "Cannot retain ScanHost class");
    JNINativeMethod method{const_cast<char *>("importBatch"),
                           const_cast<char *>("(JI[J[J)V"),
                           reinterpret_cast<void *>(&import)};
    VELOX_USER_CHECK_EQ(env->RegisterNatives(klass, &method, 1), JNI_OK);
    reserve = env->GetStaticMethodID(klass, "reserveScan", "()J");
    open = env->GetStaticMethodID(klass, "initializeUtf8", "(J[B)V");
    schema = env->GetStaticMethodID(klass, "schemaUtf8", "(J)[B");
    read = env->GetStaticMethodID(klass, "readBulk", "(JJ)I");
    cancel = env->GetStaticMethodID(klass, "cancel", "(J)V");
    close = env->GetStaticMethodID(klass, "close", "(J)V");
    active = env->GetStaticMethodID(klass, "activeScans", "()I");
    check(env);
  }
};
std::mutex hostMutex;
std::unique_ptr<Host> processHost;
Host &host() {
  std::lock_guard lock(hostMutex);
  VELOX_USER_CHECK(processHost, "JNI scan host must be bound by the Java Drillbit before execution");
  return *processHost;
}
// One daemon attachment per native thread, detached only when that thread
// exits. Never call host() from TLS teardown; the JVM remains loaded for the
// worker lifetime and the stored VM pointer outlives its executor threads.
struct ThreadEnv {
  JNIEnv *env = nullptr;
  JavaVM *vm = nullptr;
  bool owned = false;
  ThreadEnv() : vm(host().vm) {
    auto result = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    if (result == JNI_EDETACHED) {
      VELOX_USER_CHECK_EQ(vm->AttachCurrentThreadAsDaemon(
                              reinterpret_cast<void **>(&env), nullptr),
                          JNI_OK);
      owned = true;
      ++attachments;
    } else {
      VELOX_USER_CHECK_EQ(result, JNI_OK);
    }
  }
  ~ThreadEnv() {
    if (owned) {
      vm->DetachCurrentThread();
      ++detachments;
    }
  }
};
struct Env {
  JNIEnv *env;
  Env() {
    thread_local ThreadEnv current;
    env = current.env;
  }
};
// A Java handle owns one plugin reader, including the first batch produced by
// buildSchema(). Keep the seed handle until a driver consumes it; opening a
// throwaway reader for schema discovery would read the first work twice.
class Reader {
public:
  explicit Reader(const folly::dynamic &descriptor,
                  const folly::CancellationToken &cancellation,
                  std::shared_ptr<Metrics> metrics)
      : metrics_(std::move(metrics)) {
    ++metrics_->openCalls;
    Timer timer(metrics_->openNs);
    Env current;
    auto env = current.env;
    auto &h = host();
    handle_ = env->CallStaticLongMethod(h.klass, h.reserve);
    check(env);
    try {
      VELOX_USER_CHECK_NE(handle_, 0, "Plugin returned an invalid scan handle");
      // Capture the immutable handle, not this: constructor cancellation and
      // partial cleanup must not access a half-constructed Reader.
      cancellation_.emplace(cancellation, [handle = handle_] {
        try {
          Env current;
          auto &bound = host();
          current.env->CallStaticVoidMethod(bound.klass, bound.cancel, handle);
          check(current.env);
        } catch (...) {
        }
      });
      auto text = bytes(env, folly::toJson(descriptor));
      env->CallStaticVoidMethod(h.klass, h.open, handle_, text);
      env->DeleteLocalRef(text);
      check(env);
      auto type = static_cast<jbyteArray>(
          env->CallStaticObjectMethod(h.klass, h.schema, handle_));
      check(env);
      try {
        layout_ = columnarLayout(folly::parseJson(bytes(env, type)));
        addresses_.resize(layout_.bufferCount);
        lengths_.resize(layout_.bufferCount);
        buffers_.resize(layout_.bufferCount);
      } catch (...) {
        env->DeleteLocalRef(type);
        throw;
      }
      env->DeleteLocalRef(type);
    } catch (...) {
      // Preserve the schema failure even if cleanup also raises an exception.
      cancellation_.reset();
      {
        ++metrics_->closeCalls;
        Timer closeTimer(metrics_->closeNs);
        env->CallStaticVoidMethod(h.klass, h.close, handle_);
      }
      env->ExceptionClear();
      handle_ = 0;
      throw;
    }
  }
  ~Reader() {
    cancellation_.reset();
    if (!handle_)
      return;
    ++metrics_->closeCalls;
    Timer timer(metrics_->closeNs);
    try {
      Env e;
      auto &h = host();
      e.env->CallStaticVoidMethod(h.klass, h.close, handle_);
      check(e.env);
      auto active = e.env->CallStaticIntMethod(h.klass, h.active);
      check(e.env);
      scanLog("JNI plugin scan closed; active scans=" + std::to_string(active));
    } catch (...) {
    }
  }
  RowTypePtr schema() const { return layout_.type; }
  RowVectorPtr read(memory::MemoryPool *pool) {
    ImportSlot slot{pool,     layout_, addresses_, lengths_,
                    buffers_, {},      {},         *metrics_};
    ++metrics_->readCalls;
    auto started = Clock::now();
    auto timer = folly::makeGuard([&] {
      auto nanos = elapsed(started);
      metrics_->readNs += nanos;
      metrics_->readNonImportNs +=
          nanos >= slot.importNanos ? nanos - slot.importNanos : 0;
    });
    Env e;
    auto &h = host();
    auto result = e.env->CallStaticIntMethod(h.klass, h.read, handle_,
                                             reinterpret_cast<jlong>(&slot));
    check(e.env);
    if (slot.error)
      std::rethrow_exception(slot.error);
    VELOX_USER_CHECK(result == 0 || slot.batch,
                     "Plugin returned DATA without a batch");
    return result == 0 ? nullptr : std::move(slot.batch);
  }
  void cancel() {
    try {
      Env e;
      auto &h = host();
      e.env->CallStaticVoidMethod(h.klass, h.cancel, handle_);
      check(e.env);
    } catch (...) {
    }
  }

private:
  std::shared_ptr<Metrics> metrics_;
  ColumnarBatchLayout layout_;
  std::vector<jlong> addresses_, lengths_;
  std::vector<BufferView> buffers_;
  std::optional<folly::CancellationCallback> cancellation_;
  jlong handle_ = 0;
};
struct Work {
  explicit Work(const folly::dynamic &descriptor,
                folly::CancellationToken token)
      : cancellation(std::move(token)) {
    // Only plugins which explicitly declare independent work may be split.
    // Opaque/cursor plugins keep their complete descriptor and one reader.
    auto key = descriptor.get_ptr("independentWorkList");
    if (key) {
      VELOX_USER_CHECK(key->isString(),
                       "independentWorkList must be a field name");
      auto &items = descriptor["scan"][key->asString()];
      VELOX_USER_CHECK(items.isArray(),
                       "Independent scan work must be an array");
      for (auto &item : items) {
        auto part = descriptor;
        part.erase("independentWorkList");
        part["scan"][key->asString()] = folly::dynamic::array(item);
        descriptors.push_back(std::move(part));
      }
    }
    // Empty scans still need the plugin's schema and normal EOS behavior.
    if (descriptors.empty())
      descriptors.push_back(descriptor);
    metrics->works = descriptors.size();
    seed = std::make_shared<Reader>(descriptors.front(), cancellation, metrics);
    schema = seed->schema();
  }
  ~Work() {
    seed.reset();
    std::ostringstream line;
    line << "JNI plugin scan work finished; works=" << descriptors.size()
         << "; readers=" << readers.load() << "; peak reads=" << peak.load()
         << "; batches=" << batches.load()
         << "; data wakes=" << dataWakes.load()
         << "; terminal wakes=" << terminalWakes.load()
         << "; queue peak=" << queuePeak.load()
         << "; attachments=" << attachments.load()
         << "; detachments=" << detachments.load();
    scanLog(line.str());
  }
  std::shared_ptr<Reader> claim(const folly::CancellationToken &source) {
    size_t index;
    {
      std::lock_guard lock(mutex);
      if (next == descriptors.size())
        return nullptr;
      index = next++;
      if (index == 0) {
        ++readers;
        return std::move(seed);
      }
    }
    auto reader = std::make_shared<Reader>(
        descriptors[index],
        folly::cancellation_token_merge(cancellation, source), metrics);
    VELOX_USER_CHECK(reader->schema()->equivalent(*schema),
                     "Independent plugin readers have different schemas");
    ++readers;
    return reader;
  }
  RowVectorPtr read(const std::shared_ptr<Reader> &reader,
                    memory::MemoryPool *pool) {
    auto count = ++active;
    auto previous = peak.load();
    while (previous < count && !peak.compare_exchange_weak(previous, count)) {
    }
    try {
      auto batch = reader->read(pool);
      --active;
      return batch;
    } catch (...) {
      ++metrics->readErrors;
      --active;
      throw;
    }
  }
  std::vector<folly::dynamic> descriptors;
  folly::CancellationToken cancellation;
  RowTypePtr schema;
  std::mutex mutex;
  size_t next = 0;
  std::shared_ptr<Reader> seed;
  std::atomic<size_t> readers{0}, active{0}, peak{0};
  std::atomic<size_t> batches{0}, dataWakes{0}, terminalWakes{0}, queuePeak{0};
  std::shared_ptr<Metrics> metrics = std::make_shared<Metrics>();
};
class Source final : public BatchSource,
                     public std::enable_shared_from_this<Source> {
public:
  Source(std::shared_ptr<Work> work, memory::MemoryPool *pool,
         folly::Executor *io)
      : work_(std::move(work)), pool_(pool->shared_from_this()), io_(io) {
    // Prefetch imports while CPU drivers can release earlier owned batches.
    VELOX_CHECK(pool_->threadSafe(),
                "JNI prefetch requires a thread-safe pool");
  }
  std::optional<RowVectorPtr> next(ContinueFuture &future) override {
    std::lock_guard lock(mutex_);
    if (cancelled_)
      return std::nullopt;
    if (error_)
      std::rethrow_exception(error_);
    if (!ready_.empty()) {
      auto batch = std::move(ready_.front());
      ready_.pop_front();
      startReadLocked();
      return batch;
    }
    if (finished_)
      return std::nullopt;
    ContinuePromise promise;
    future = promise.getSemiFuture();
    waiters_.push_back(std::move(promise));
    startReadLocked();
    return RowVectorPtr{};
  }
  void cancel() override {
    std::vector<ContinuePromise> wake;
    std::shared_ptr<Reader> reader;
    {
      std::lock_guard lock(mutex_);
      if (cancelled_)
        return;
      cancelled_ = true;
      ready_.clear();
      reader = reader_;
      // A pending read holds self and closes its handle when it returns.
      if (!reading_)
        reader_.reset();
      while (!waiters_.empty()) {
        wake.push_back(std::move(waiters_.front()));
        waiters_.pop_front();
      }
      work_->terminalWakes += wake.size();
    }
    // This also cancels a claimed reader whose constructor has not returned,
    // when reader_ cannot yet be published under mutex_.
    cancellation_.requestCancellation();
    if (reader)
      reader->cancel();
    for (auto &promise : wake)
      promise.setValue();
  }

private:
  // mutex_ serializes submission and queue access. One read per source remains
  // in flight; at most two fully owned batches wait for CPU consumption.
  void startReadLocked() {
    if (reading_ || finished_ || cancelled_ || error_ ||
        ready_.size() >= kPrefetchBatches)
      return;
    reading_ = true;
    auto submitted = Clock::now();
    ++work_->metrics->ioSubmissions;
    io_->add([self = shared_from_this(), submitted] {
      self->work_->metrics->ioQueueNs += elapsed(submitted);
      self->read();
    });
  }
  RowVectorPtr readNext() {
    while (true) {
      std::shared_ptr<Reader> reader;
      {
        std::lock_guard lock(mutex_);
        if (cancelled_)
          return nullptr;
        reader = reader_;
      }
      if (!reader) {
        // Java reader creation runs on the shared I/O executor, never on the
        // driver CPU pool. Each driver imports into its own operator pool.
        reader = work_->claim(cancellation_.getToken());
        if (!reader)
          return nullptr;
        {
          std::lock_guard lock(mutex_);
          if (cancelled_)
            return nullptr;
          reader_ = reader;
        }
      }
      auto batch = work_->read(reader, pool_.get());
      if (batch)
        return batch;
      {
        std::lock_guard lock(mutex_);
        reader_.reset();
      }
      // EOF closes this reader before claiming another work. Other drivers
      // continue independently; normal close must not cancel their readers.
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
    std::shared_ptr<Reader> close;
    {
      std::lock_guard lock(mutex_);
      reading_ = false;
      error_ = error;
      finished_ = !batch;
      if (error_ || cancelled_)
        ready_.clear();
      else if (batch) {
        ready_.push_back(std::move(batch));
        ++work_->batches;
        auto peak = work_->queuePeak.load();
        while (peak < ready_.size() &&
               !work_->queuePeak.compare_exchange_weak(peak, ready_.size())) {
        }
      }
      if (cancelled_ || error_ || finished_) {
        close = std::move(reader_);
        while (!waiters_.empty()) {
          wake.push_back(std::move(waiters_.front()));
          waiters_.pop_front();
        }
        work_->terminalWakes += wake.size();
      } else if (!waiters_.empty()) {
        wake.push_back(std::move(waiters_.front()));
        waiters_.pop_front();
        ++work_->dataWakes;
      }
      startReadLocked();
    }
    close.reset();
    for (auto &promise : wake)
      promise.setValue();
  }
  std::shared_ptr<Work> work_;
  std::shared_ptr<memory::MemoryPool> pool_;
  folly::Executor *io_;
  std::mutex mutex_;
  folly::CancellationSource cancellation_;
  std::shared_ptr<Reader> reader_;
  static constexpr size_t kPrefetchBatches = 2;
  std::deque<ContinuePromise> waiters_;
  std::deque<RowVectorPtr> ready_;
  std::exception_ptr error_;
  bool reading_ = false, finished_ = false, cancelled_ = false;
};
} // namespace
void bindJniScanHost(JNIEnv *env, jclass scanHost) {
  std::lock_guard lock(hostMutex);
  JavaVM *vm = nullptr;
  VELOX_USER_CHECK_EQ(env->GetJavaVM(&vm), JNI_OK);
  if (!processHost) {
    processHost = std::make_unique<Host>(env, scanHost);
    return;
  }
  VELOX_USER_CHECK(processHost->vm == vm &&
                       env->IsSameObject(processHost->klass, scanHost),
                   "Native engines in one process must share the same JVM "
                   "and ScanHost class loader");
}
JniScanBinding openJniPluginScan(const folly::dynamic &descriptor,
                                 memory::MemoryPool *pool, folly::Executor *io,
                                 folly::CancellationToken cancellation) {
  auto work = std::make_shared<Work>(descriptor, std::move(cancellation));
  auto statistics = [metrics = work->metrics] { return metrics->snapshot(); };
  if (work->descriptors.size() == 1) {
    // Preserve batch distribution across drivers for an opaque/single work.
    auto source = std::make_shared<Source>(work, pool, io);
    return {work->schema, [source](memory::MemoryPool *) { return source; },
            std::move(statistics)};
  }
  return {work->schema,
          [work, io](memory::MemoryPool *pool) {
            return std::make_shared<Source>(work, pool, io);
          },
          std::move(statistics)};
}
} // namespace drill::nativeexec
