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
#include "execution/NativeEngine.h"
#include "execution/VeloxRuntime.h"
#include "protocol/Protobuf.h"
#include <condition_variable>
#include <dlfcn.h>
#include <folly/base64.h>
#include <folly/json.h>
#include <future>
#include <iostream>
#include <jni.h>
#include <set>
#include <thread>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
void startTestJvm() {
  auto module = dlopen(std::getenv("DRILL_NATIVE_JVM_LIBRARY"), RTLD_NOW | RTLD_GLOBAL);
  VELOX_CHECK(module);
  auto create = reinterpret_cast<jint (*)(JavaVM **, void **, void *)>(
      dlsym(module, "JNI_CreateJavaVM"));
  VELOX_CHECK(create);
  std::string classpath = std::string("-Djava.class.path=") +
      std::getenv("DRILL_NATIVE_SCAN_CLASSPATH");
  std::string opens = "--add-opens=java.base/java.nio=ALL-UNNAMED";
  JavaVMOption options[] = {{classpath.data(), nullptr}, {opens.data(), nullptr}};
  JavaVMInitArgs args{};
  args.version = JNI_VERSION_1_8;
  args.nOptions = 2;
  args.options = options;
  JavaVM *vm = nullptr;
  JNIEnv *env = nullptr;
  VELOX_CHECK_EQ(create(&vm, reinterpret_cast<void **>(&env), &args), JNI_OK);
  auto klass = env->FindClass("org/apache/drill/exec/nativeexecution/scan/ScanHost");
  VELOX_CHECK(klass);
  bindJniScanHost(env, klass);
  env->DeleteLocalRef(klass);
  vm->DetachCurrentThread();
}
int metric(const char *name) {
  // Inspect the isolated test host, without extending the production JNI API.
  auto module = dlopen(std::getenv("DRILL_NATIVE_JVM_LIBRARY"), RTLD_NOW);
  VELOX_CHECK(module);
  auto get = reinterpret_cast<jint (*)(JavaVM **, jsize, jsize *)>(
      dlsym(module, "JNI_GetCreatedJavaVMs"));
  JavaVM *vm;
  jsize count;
  VELOX_CHECK_EQ(get(&vm, 1, &count), JNI_OK);
  VELOX_CHECK_EQ(count, 1);
  JNIEnv *env;
  bool owned = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8) ==
               JNI_EDETACHED;
  if (owned)
    VELOX_CHECK_EQ(vm->AttachCurrentThreadAsDaemon(
                       reinterpret_cast<void **>(&env), nullptr),
                   JNI_OK);
  auto klass =
      env->FindClass("org/apache/drill/exec/nativeexecution/scan/ScanHost");
  VELOX_CHECK(klass);
  auto method = env->GetStaticMethodID(klass, name, "()I");
  VELOX_CHECK(method);
  int result = env->CallStaticIntMethod(klass, method);
  VELOX_CHECK(!env->ExceptionCheck());
  env->DeleteLocalRef(klass);
  if (owned)
    vm->DetachCurrentThread();
  dlclose(module);
  return result;
}
folly::dynamic descriptor(const std::string &mode, bool independent = true) {
  folly::dynamic value = folly::dynamic::object("provider", "test")("mode",
                                                                    mode)(
      "scan",
      folly::dynamic::object("workList", folly::dynamic::array(0, 1, 2, 3, 4)));
  if (independent)
    value["independentWorkList"] = "workList";
  return value;
}
std::vector<int64_t> consume(const std::shared_ptr<BatchSource> &source) {
  std::vector<int64_t> values;
  while (true) {
    ContinueFuture wait;
    auto result = source->next(wait);
    if (!result)
      break;
    if (!*result) {
      VELOX_CHECK(wait.valid());
      std::move(wait).get(std::chrono::seconds(10));
      continue;
    }
    values.push_back(
        (*result)->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0));
  }
  source->cancel();
  return values;
}
void drained() {
  for (int i = 0; i < 200 && metric("activeScans"); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  VELOX_CHECK_EQ(metric("activeScans"), 0);
}
void unicode(VeloxRuntime &runtime) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("unicode");
  for (const auto &mode : {"unicode-path", "unicode-schema"}) {
    auto plan = descriptor(mode, false);
    plan["scan"]["path"] = "/读取/🚀/part.parquet";
    auto binding = openJniPluginScan(plan, pool.get(), runtime.io());
    VELOX_CHECK_EQ(binding.schema->nameOf(0),
                   std::string(mode) == "unicode-schema" ? "列🚀" : "v");
    VELOX_CHECK_EQ(consume(binding.factory(pool.get())).size(), 15);
  }
  drained();
}
void parallel(VeloxRuntime &runtime, bool independent) {
  auto root = memory::memoryManager()->addRootPool();
  auto seed = root->addLeafChild("seed");
  auto p1 = root->addLeafChild("reader-1"), p2 = root->addLeafChild("reader-2");
  std::function<folly::dynamic()> snapshot;
  {
    auto binding = openJniPluginScan(
        descriptor(independent ? "barrier" : "serial", independent), seed.get(),
        runtime.io());
    snapshot = binding.statistics;
    auto one = binding.factory(p1.get()), two = binding.factory(p2.get());
    VELOX_CHECK_EQ(one == two, !independent);
    auto f1 = std::async(std::launch::async, consume, one);
    auto f2 = std::async(std::launch::async, consume, two);
    auto values = f1.get();
    auto others = f2.get();
    values.insert(values.end(), others.begin(), others.end());
    std::set<int64_t> expected;
    for (int work = 0; work < 5; ++work)
      for (int batch = 0; batch < 3; ++batch)
        expected.insert(work * 10 + batch);
    VELOX_CHECK_EQ(values.size(), expected.size());
    VELOX_CHECK(std::set<int64_t>(values.begin(), values.end()) == expected);
    if (independent)
      VELOX_CHECK_GE(metric("peakReads"), 2);
    auto stats = snapshot();
    VELOX_CHECK_EQ(stats["works"].asInt(), independent ? 5 : 1);
    VELOX_CHECK_EQ(stats["reader_open_calls"].asInt(), independent ? 5 : 1);
    VELOX_CHECK_EQ(stats["read_calls"].asInt(), independent ? 20 : 16);
    VELOX_CHECK_EQ(stats["import_calls"].asInt(), 15);
    VELOX_CHECK_EQ(stats["imported_batches"].asInt(), 15);
    VELOX_CHECK_EQ(stats["imported_rows"].asInt(), 15);
    VELOX_CHECK_EQ(stats["imported_buffer_bytes"].asInt(), 120);
    VELOX_CHECK_EQ(stats["read_errors"].asInt(), 0);
    VELOX_CHECK_EQ(stats["import_errors"].asInt(), 0);
    VELOX_CHECK_GT(stats["reader_open_ns"].asInt(), 0);
    VELOX_CHECK_GT(stats["import_ns"].asInt(), 0);
    VELOX_CHECK_EQ(stats["read_call_ns"].asInt(),
                   stats["read_non_import_ns"].asInt() +
                       stats["import_ns"].asInt());
    VELOX_CHECK_GE(stats["io_submissions"].asInt(), 15);
    VELOX_CHECK_GT(stats["io_queue_wait_ns"].asInt(), 0);
  }
  drained();
  // Retaining metrics after the binding/sources die must not retain a seed,
  // reader, vector tree, JNI service or producer pool.
  VELOX_CHECK_EQ(snapshot()["reader_close_calls"].asInt(), independent ? 5 : 1);
}
void failure(VeloxRuntime &runtime, const std::string &mode) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild(mode);
  {
    auto binding =
        openJniPluginScan(descriptor(mode), pool.get(), runtime.io());
    auto source = binding.factory(pool.get());
    bool failed = false;
    try {
      consume(source);
    } catch (const std::exception &e) {
      std::string text = e.what();
      failed = text.find(mode == "failure" ? "injected work read failure"
                         : mode == "badBuffers"
                             ? "buffer count changed"
                             : "different schemas") != std::string::npos;
    }
    VELOX_CHECK(failed);
    source->cancel();
    auto stats = binding.statistics();
    VELOX_CHECK_EQ(stats["import_errors"].asInt(),
                   mode == "badBuffers" ? 1 : 0);
    VELOX_CHECK_EQ(stats["read_errors"].asInt(), mode == "mismatch" ? 0 : 1);
  }
  drained();
}
void prefetch(VeloxRuntime &runtime) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("prefetch");
  {
    auto binding =
        openJniPluginScan(descriptor("gated", false), pool.get(), runtime.io());
    auto source = binding.factory(pool.get());
    ContinueFuture waits[3];
    for (auto &wait : waits) {
      auto result = source->next(wait);
      VELOX_CHECK(result && !*result && wait.valid());
    }
    metric("releaseGate");
    for (int i = 0; i < 200 && metric("prefetchData") < 2; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    VELOX_CHECK_EQ(metric("prefetchData"), 2,
                   "Prefetch must stop at two queued batches");
    VELOX_CHECK(waits[0].isReady() && waits[1].isReady());
    VELOX_CHECK(!waits[2].isReady(),
                "DATA must wake only one waiter per batch");
    source->cancel();
    for (auto &wait : waits)
      std::move(wait).get(std::chrono::seconds(5));
  }
  drained();
}
void cancelled(VeloxRuntime &runtime, bool started) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("cancel");
  std::function<folly::dynamic()> snapshot;
  {
    auto binding =
        openJniPluginScan(descriptor("blocked"), pool.get(), runtime.io());
    snapshot = binding.statistics;
    if (started) {
      auto source = binding.factory(pool.get());
      ContinueFuture wait;
      auto result = source->next(wait);
      VELOX_CHECK(result && !*result && wait.valid());
      for (int i = 0; i < 200 && !metric("activeReads"); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      VELOX_CHECK_GT(metric("activeReads"), 0);
      source->cancel();
      std::move(wait).get(std::chrono::seconds(5));
      ContinueFuture unused;
      VELOX_CHECK(!source->next(unused));
    }
    // Without a driver, destroying the binding must close its unused seed.
  }
  drained();
  VELOX_CHECK_EQ(snapshot()["reader_open_calls"].asInt(), 1);
  VELOX_CHECK_EQ(snapshot()["reader_close_calls"].asInt(), 1);
}
void waitForOpen() {
  for (int i = 0; i < 200 && !metric("activeOpens"); ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  VELOX_CHECK_EQ(metric("activeOpens"), 1);
}
void opening(VeloxRuntime &runtime, bool before) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("opening");
  folly::CancellationSource cancelled;
  if (before)
    cancelled.requestCancellation();
  auto future = std::async(std::launch::async, [&] {
    bool rejected = false;
    try {
      auto binding = openJniPluginScan(descriptor("openBlocked"), pool.get(),
                                       runtime.io(), cancelled.getToken());
    } catch (const std::exception &) {
      rejected = true;
    }
    return rejected;
  });
  if (!before) {
    waitForOpen();
    cancelled.requestCancellation();
  }
  VELOX_CHECK(future.wait_for(std::chrono::seconds(5)) ==
              std::future_status::ready);
  VELOX_CHECK(future.get());
  drained();
}
void openingWork(VeloxRuntime &runtime) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("opening-work");
  {
    auto binding = openJniPluginScan(descriptor("laterOpenBlocked"), pool.get(),
                                     runtime.io());
    auto source = binding.factory(pool.get());
    for (int row = 0; row < 3; ++row) {
      while (true) {
        ContinueFuture wait;
        auto batch = source->next(wait);
        VELOX_CHECK(batch.has_value());
        if (!*batch) {
          std::move(wait).get(std::chrono::seconds(5));
          continue;
        }
        VELOX_CHECK_EQ(
            (*batch)->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0), row);
        break;
      }
    }
    waitForOpen();
    ContinueFuture wait;
    auto batch = source->next(wait);
    VELOX_CHECK(batch && !*batch && wait.valid());
    source->cancel();
    std::move(wait).get(std::chrono::seconds(5));
    ContinueFuture unused;
    VELOX_CHECK(!source->next(unused));
  }
  drained();
}
void openingFailure(VeloxRuntime &runtime) {
  auto root = memory::memoryManager()->addRootPool();
  auto pool = root->addLeafChild("opening-failure");
  bool rejected = false;
  try {
    openJniPluginScan(descriptor("openFailure"), pool.get(), runtime.io());
  } catch (const std::exception &error) {
    rejected = std::string(error.what()).find("injected plugin open failure") !=
               std::string::npos;
  }
  VELOX_CHECK(rejected);
  drained();
}
void openingMinor(int action) {
  using protocol::Message;
  using protocol::Writer;
  auto endpoint =
      Writer().bytes(1, "127.0.0.1").integer(3, 63001).integer(4, 63002).take();
  auto query = Writer().fixed64(1, 31).fixed64(2, 41 + action).take();
  auto handle = Writer().bytes(1, query).integer(2, 1).integer(3, 0).take();
  std::mutex mutex;
  std::condition_variable changed;
  uint32_t terminal = 0;
  EngineCallbacks callbacks;
  callbacks.status = [&](std::string bytes) {
    auto state = Message(Message(bytes).bytes(1)).integer(1);
    if (state >= 3) {
      std::lock_guard lock(mutex);
      terminal = state;
      changed.notify_all();
    }
  };
  callbacks.rootBatch = [](std::string, std::string) -> ContinueFuture {
    VELOX_FAIL("Cancelled opening minor must not send data");
  };
  NativeEngine engine(endpoint, 4, std::move(callbacks));
  folly::dynamic scan = folly::dynamic::object("pop", "test-scan")("@id", 1)(
      "jniScan", descriptor("openBlocked"));
  folly::dynamic plan = folly::dynamic::object("pop", "single-sender")(
      "@id", 0)("receiver-major-fragment", 0)("receiver-minor-fragment", 0)(
      "destination", folly::base64Encode(endpoint))("child", scan);
  auto fragment = Writer()
                      .bytes(1, handle)
                      .bytes(8, folly::toJson(plan))
                      .bytes(10, endpoint)
                      .bytes(11, endpoint)
                      .take();
  engine.submitFragments(Writer().bytes(1, fragment).take());
  waitForOpen();
  if (action == 0)
    engine.cancel(handle);
  else if (action == 1) {
    auto receiver = Writer().bytes(1, query).integer(2, 0).integer(3, 0).take();
    engine.receiverFinished(
        Writer().bytes(1, receiver).bytes(2, handle).take());
  } else {
    auto closing = std::async(std::launch::async, [&] { engine.close(); });
    VELOX_CHECK(closing.wait_for(std::chrono::seconds(5)) ==
                std::future_status::ready);
    closing.get();
  }
  if (action != 2) {
    std::unique_lock lock(mutex);
    VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return terminal != 0; }));
    VELOX_CHECK_EQ(terminal, action == 1 ? 3 : 4);
  }
  engine.close();
  drained();
}
} // namespace
int main() {
  try {
    startTestJvm();
    VeloxRuntime runtime(4);
    unicode(runtime);
    parallel(runtime, true);
    parallel(runtime, false);
    failure(runtime, "failure");
    failure(runtime, "mismatch");
    failure(runtime, "badBuffers");
    prefetch(runtime);
    cancelled(runtime, true);
    cancelled(runtime, false);
    opening(runtime, true);
    opening(runtime, false);
    openingWork(runtime);
    openingFailure(runtime);
    for (int action : {0, 1, 2})
      openingMinor(action);
    {
      auto root = memory::memoryManager()->addRootPool();
      auto pool = root->addLeafChild("after-cancel");
      auto binding = openJniPluginScan(descriptor("checkInterrupt", false),
                                       pool.get(), runtime.io());
      VELOX_CHECK_EQ(consume(binding.factory(pool.get())).size(), 15);
    }
    drained();
    VELOX_CHECK_LE(metric("readThreadCount"), 4,
                   "I/O threads must retain their JNI attachment");
    std::cout << "JNI readers: concurrent reads, exactly-once work claiming, "
                 "shared opaque reader, schema/read/open failures, "
                 "active/opening cancel, minor cancel/early finish/shutdown "
                 "and unused seed cleanup passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
