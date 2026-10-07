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
#include "execution/NativeEngine.h"
#include "scan/jni/JniPluginScan.h"
#include <climits>
#include <iostream>
#include <jni.h>
#include <stdexcept>
#include <unordered_map>
namespace drill::nativeexec {
namespace {
void check(JNIEnv *env) {
  if (!env->ExceptionCheck())
    return;
  auto error = env->ExceptionOccurred();
  env->ExceptionClear();
  auto klass = env->GetObjectClass(error);
  auto method = env->GetMethodID(klass, "toString", "()Ljava/lang/String;");
  auto message = static_cast<jstring>(env->CallObjectMethod(error, method));
  const char *chars =
      message ? env->GetStringUTFChars(message, nullptr) : nullptr;
  std::string text = chars ? chars : "Java native engine callback failed";
  if (chars)
    env->ReleaseStringUTFChars(message, chars);
  env->DeleteLocalRef(message);
  env->DeleteLocalRef(klass);
  env->DeleteLocalRef(error);
  env->ExceptionClear();
  throw std::runtime_error(text);
}
struct ThreadEnv {
  JavaVM *vm;
  JNIEnv *env = nullptr;
  bool owned = false;
  explicit ThreadEnv(JavaVM *value) : vm(value) {
    auto result = vm->GetEnv(reinterpret_cast<void **>(&env), JNI_VERSION_1_8);
    if (result == JNI_EDETACHED) {
      VELOX_USER_CHECK_EQ(vm->AttachCurrentThreadAsDaemon(
                              reinterpret_cast<void **>(&env), nullptr),
                          JNI_OK);
      owned = true;
    } else
      VELOX_USER_CHECK_EQ(result, JNI_OK);
  }
  ~ThreadEnv() {
    if (owned)
      vm->DetachCurrentThread();
  }
};
JNIEnv *environment(JavaVM *vm) {
  thread_local ThreadEnv current(vm);
  VELOX_CHECK(current.vm == vm);
  return current.env;
}
struct LocalFrame {
  JNIEnv *env;
  explicit LocalFrame(JNIEnv *value) : env(value) {
    VELOX_USER_CHECK_EQ(env->PushLocalFrame(16), JNI_OK);
  }
  ~LocalFrame() { env->PopLocalFrame(nullptr); }
};
std::string bytes(JNIEnv *env, jbyteArray value) {
  VELOX_USER_CHECK(value, "Missing protobuf input");
  auto length = env->GetArrayLength(value);
  std::string result(length, '\0');
  env->GetByteArrayRegion(value, 0, length,
                          reinterpret_cast<jbyte *>(result.data()));
  check(env);
  return result;
}
jbyteArray bytes(JNIEnv *env, const std::string &value) {
  VELOX_USER_CHECK_LE(value.size(), size_t(INT_MAX));
  auto result = env->NewByteArray(value.size());
  check(env);
  VELOX_USER_CHECK(result, "Cannot allocate protobuf output");
  env->SetByteArrayRegion(result, 0, value.size(),
                          reinterpret_cast<const jbyte *>(value.data()));
  check(env);
  return result;
}
class JavaBridge {
public:
  JavaBridge(JNIEnv *env, jobject callbacks) {
    VELOX_USER_CHECK(callbacks, "Missing native engine callbacks");
    VELOX_USER_CHECK_EQ(env->GetJavaVM(&vm_), JNI_OK);
    callbacks_ = env->NewGlobalRef(callbacks);
    check(env);
    VELOX_USER_CHECK(callbacks_, "Cannot retain engine callbacks");
    auto klass = env->GetObjectClass(callbacks);
    status_ = env->GetMethodID(klass, "onStatus", "([B)V");
    root_ =
        env->GetMethodID(klass, "onRootBatch", "(J[BLjava/nio/ByteBuffer;)Z");
    env->DeleteLocalRef(klass);
    check(env);
  }
  ~JavaBridge() { environment(vm_)->DeleteGlobalRef(callbacks_); }
  void status(std::string body) {
    auto env = environment(vm_);
    LocalFrame frame(env);
    env->CallVoidMethod(callbacks_, status_, bytes(env, body));
    check(env);
  }
  ContinueFuture root(std::string header, std::string payload) {
    auto batch = std::make_shared<Batch>(std::move(header), std::move(payload));
    auto future = batch->ack.getSemiFuture();
    uint64_t id;
    {
      std::lock_guard lock(mutex_);
      id = ++next_;
      pending_.emplace(id, batch);
    }
    try {
      auto env = environment(vm_);
      LocalFrame frame(env);
      auto view = env->NewDirectByteBuffer(batch->payload.data(),
                                           batch->payload.size());
      check(env);
      auto accepted = env->CallBooleanMethod(callbacks_, root_, jlong(id),
                                             bytes(env, batch->header), view);
      check(env);
      if (!accepted)
        complete(id, 1);
    } catch (...) {
      fail(id, std::current_exception());
    }
    return future;
  }
  void complete(uint64_t id, int result) {
    VELOX_USER_CHECK(result >= 0 && result <= 3,
                     "Invalid root delivery result");
    std::exception_ptr error;
    if (result >= 2)
      error = std::make_exception_ptr(
          std::runtime_error(result == 2 ? "Java root batch delivery failed"
                                         : "Java root was cancelled"));
    fail(id, error);
  }
  void drain() {
    std::unique_lock lock(mutex_);
    drained_.wait(lock, [&] { return pending_.empty() && completing_ == 0; });
  }

private:
  struct Batch {
    std::string header, payload;
    ContinuePromise ack{"Java root batch delivery"};
    Batch(std::string h, std::string p)
        : header(std::move(h)), payload(std::move(p)) {}
  };
  void fail(uint64_t id, std::exception_ptr error) {
    std::shared_ptr<Batch> batch;
    {
      std::lock_guard lock(mutex_);
      auto it = pending_.find(id);
      VELOX_USER_CHECK(it != pending_.end(),
                       "Unknown or completed root batch {}", id);
      batch = std::move(it->second);
      pending_.erase(it);
      ++completing_;
    }
    if (error)
      batch->ack.setException(folly::exception_wrapper(error));
    else
      batch->ack.setValue();
    {
      std::lock_guard lock(mutex_);
      --completing_;
    }
    drained_.notify_all();
  }
  JavaVM *vm_ = nullptr;
  jobject callbacks_ = nullptr;
  jmethodID status_, root_;
  std::mutex mutex_;
  std::condition_variable drained_;
  uint64_t next_ = 0;
  size_t completing_ = 0;
  std::unordered_map<uint64_t, std::shared_ptr<Batch>> pending_;
};
class LifecycleBridge {
public:
  LifecycleBridge(JNIEnv *env, jobject value) {
    VELOX_USER_CHECK(value, "Missing native lifecycle listener");
    VELOX_USER_CHECK_EQ(env->GetJavaVM(&vm_), JNI_OK);
    value_ = env->NewGlobalRef(value);
    auto klass = env->GetObjectClass(value);
    method_ = env->GetMethodID(klass, "onFailure", "([B)V");
    env->DeleteLocalRef(klass);
    check(env);
  }
  ~LifecycleBridge() { environment(vm_)->DeleteGlobalRef(value_); }
  void failure(const std::string &error) {
    auto env = environment(vm_);
    LocalFrame frame(env);
    env->CallVoidMethod(value_, method_, bytes(env, error));
    check(env);
  }

private:
  JavaVM *vm_ = nullptr;
  jobject value_ = nullptr;
  jmethodID method_;
};
struct Instance {
  std::shared_ptr<JavaBridge> bridge;
  std::shared_ptr<LifecycleBridge> lifecycle;
  std::shared_ptr<std::atomic<uint64_t>> jniCalls =
      std::make_shared<std::atomic<uint64_t>>(0);
  NativeEngine engine;
  std::mutex closing;
  std::unique_ptr<protocol::RpcServer> controlServer, dataServer;
  std::atomic<bool> ready{false};
  bool closed = false;
  std::string endpoint;
  Instance(std::string endpoint, unsigned threads,
           std::shared_ptr<JavaBridge> value)
      : bridge(std::move(value)),
        engine(std::move(endpoint), threads,
               EngineCallbacks{
                   {},
                   [b = bridge](auto status) { b->status(std::move(status)); },
                   [b = bridge](auto header, auto payload) {
                     return b->root(std::move(header), std::move(payload));
                   }}) {}
  Instance(std::string canonical, unsigned threads, std::string bindAddress,
           std::string advertisedAddress,
           std::shared_ptr<LifecycleBridge> listener)
      : lifecycle(std::move(listener)),
        engine(canonical, threads,
               EngineCallbacks{[calls = jniCalls] {
                                 return folly::dynamic::object("transport",
                                                               "rpc")(
                                     "jni_engine_calls",
                                     calls->load())("jni_status_callbacks",
                                                    0)("jni_root_callbacks", 0);
                               },
                               {},
                               {}}) {
    auto failure = [this](std::string error) {
      engine.fail(error);
      lifecycle->failure(error);
    };
    controlServer = std::make_unique<protocol::RpcServer>(
        bindAddress, 0,
        [this](const auto &request) {
          VELOX_USER_CHECK(ready.load(), "Native service is not ready");
          return engine.control(request);
        },
        failure);
    dataServer = std::make_unique<protocol::RpcServer>(
        bindAddress, 0,
        [this](const auto &request) {
          VELOX_USER_CHECK(ready.load(), "Native service is not ready");
          return engine.data(request);
        },
        failure);
    auto capability = protocol::Writer()
                          .bytes(1, advertisedAddress)
                          .integer(3, 2)
                          .integer(5, controlServer->port())
                          .integer(6, dataServer->port())
                          .take();
    endpoint = canonical + protocol::Writer().bytes(9, capability).take();
    engine.setEndpoint(endpoint);
    ready.store(true);
  }
  void close() {
    if (closed)
      return;
    // Keep data listeners and status connections alive while tasks drain.
    if (controlServer)
      controlServer->stop();
    engine.close();
    if (bridge)
      bridge->drain();
    if (dataServer)
      dataServer->stop();
    closed = true;
    if (controlServer)
      std::cout << "Native RPC service drained." << std::endl;
  }
  ~Instance() { close(); }
};
std::mutex instancesMutex;
uint64_t nextInstance = 0;
std::unordered_map<uint64_t, std::shared_ptr<Instance>> instances;
std::shared_ptr<Instance> instance(jlong id) {
  std::lock_guard lock(instancesMutex);
  auto it = instances.find(id);
  VELOX_USER_CHECK(it != instances.end(), "Unknown native engine {}", id);
  return it->second;
}
template <typename F>
auto guard(JNIEnv *env, F function) -> decltype(function()) {
  try {
    return function();
  } catch (const std::exception &error) {
    if (!env->ExceptionCheck()) {
      auto klass = env->FindClass("java/lang/IllegalStateException");
      if (klass)
        env->ThrowNew(klass, error.what());
      env->DeleteLocalRef(klass);
    }
  }
  return decltype(function())();
}
} // namespace
} // namespace drill::nativeexec
using namespace drill::nativeexec;
extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_createNative(
    JNIEnv *env, jclass, jbyteArray endpoint, jobject callbacks,
    jclass scanHost, jint threads) {
  return guard(env, [&]() -> jlong {
    VELOX_USER_CHECK_GE(threads, 0);
    VELOX_USER_CHECK(scanHost, "Missing ScanHost class");
    bindJniScanHost(env, scanHost);
    auto bridge = std::make_shared<JavaBridge>(env, callbacks);
    auto value = std::make_shared<Instance>(
        bytes(env, endpoint), threads ? unsigned(threads) : availableCpuCount(),
        std::move(bridge));
    std::lock_guard lock(instancesMutex);
    auto id = ++nextInstance;
    instances.emplace(id, std::move(value));
    return id;
  });
}
extern "C" JNIEXPORT jlong JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_createRpcNative(
    JNIEnv *env, jclass, jbyteArray endpoint, jclass scanHost, jint threads,
    jbyteArray bindAddress, jbyteArray advertisedAddress, jobject lifecycle) {
  return guard(env, [&]() -> jlong {
    VELOX_USER_CHECK_GE(threads, 0);
    bindJniScanHost(env, scanHost);
    auto value = std::make_shared<Instance>(
        bytes(env, endpoint), threads ? unsigned(threads) : availableCpuCount(),
        bytes(env, bindAddress), bytes(env, advertisedAddress),
        std::make_shared<LifecycleBridge>(env, lifecycle));
    std::lock_guard lock(instancesMutex);
    auto id = ++nextInstance;
    instances.emplace(id, std::move(value));
    return id;
  });
}
extern "C" JNIEXPORT jbyteArray JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_endpointNative(
    JNIEnv *env, jclass, jlong id) {
  return guard(env, [&] { return bytes(env, instance(id)->endpoint); });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_submitNative(
    JNIEnv *env, jclass, jlong id, jbyteArray fragments) {
  guard(env, [&] {
    auto value = instance(id);
    ++*value->jniCalls;
    value->engine.submitFragments(bytes(env, fragments));
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_cancelNative(
    JNIEnv *env, jclass, jlong id, jbyteArray handle) {
  guard(env, [&] {
    auto value = instance(id);
    ++*value->jniCalls;
    value->engine.cancel(bytes(env, handle));
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_receiverFinishedNative(
    JNIEnv *env, jclass, jlong id, jbyteArray finished) {
  guard(env, [&] {
    auto value = instance(id);
    ++*value->jniCalls;
    value->engine.receiverFinished(bytes(env, finished));
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_acceptNative(
    JNIEnv *env, jclass, jlong id, jbyteArray header, jobject payload) {
  guard(env, [&] {
    std::string raw;
    if (payload) {
      auto size = env->GetDirectBufferCapacity(payload);
      auto address = env->GetDirectBufferAddress(payload);
      VELOX_USER_CHECK(size >= 0 && (size == 0 || address),
                       "Invalid direct payload");
      if (size)
        raw.assign(static_cast<const char *>(address), size);
    }
    auto value = instance(id);
    ++*value->jniCalls;
    value->engine.acceptRecordBatch(bytes(env, header), std::move(raw));
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_acceptBuffersNative(
    JNIEnv *env, jclass, jlong id, jbyteArray header, jlongArray addresses,
    jlongArray lengths) {
  guard(env, [&] {
    VELOX_USER_CHECK(addresses && lengths, "Missing buffers");
    auto count = env->GetArrayLength(addresses);
    VELOX_USER_CHECK_EQ(count, env->GetArrayLength(lengths));
    std::vector<jlong> pointers(count), sizes(count);
    env->GetLongArrayRegion(addresses, 0, count, pointers.data());
    env->GetLongArrayRegion(lengths, 0, count, sizes.data());
    check(env);
    std::string raw;
    for (int i = 0; i < count; ++i) {
      VELOX_USER_CHECK(sizes[i] >= 0 && (sizes[i] == 0 || pointers[i]),
                       "Invalid buffer");
      VELOX_USER_CHECK_LE(uint64_t(sizes[i]), uint64_t(INT_MAX) - raw.size());
      if (sizes[i])
        raw.append(reinterpret_cast<const char *>(pointers[i]), sizes[i]);
    }
    auto value = instance(id);
    ++*value->jniCalls;
    value->engine.acceptRecordBatch(bytes(env, header), std::move(raw));
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_completeNative(
    JNIEnv *env, jclass, jlong id, jlong batchId, jint result) {
  guard(env, [&] {
    auto value = instance(id);
    ++*value->jniCalls;
    VELOX_USER_CHECK(value->bridge, "RPC engines have no JNI root callback");
    value->bridge->complete(batchId, result);
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_failNative(
    JNIEnv *env, jclass, jlong id, jbyteArray reason) {
  guard(env, [&] { instance(id)->engine.fail(bytes(env, reason)); });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_quiesceNative(
    JNIEnv *env, jclass, jlong id) {
  guard(env, [&] { instance(id)->engine.quiesce(); });
}
extern "C" JNIEXPORT jboolean JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_awaitIdleNative(
    JNIEnv *env, jclass, jlong id, jlong timeout) {
  return guard(env, [&]() -> jboolean {
    VELOX_USER_CHECK_GE(timeout, 0);
    return instance(id)->engine.awaitIdle(timeout);
  });
}
extern "C" JNIEXPORT void JNICALL
Java_org_apache_drill_exec_nativeexecution_NativeEngine_closeNative(JNIEnv *env,
                                                                    jclass,
                                                                    jlong id) {
  guard(env, [&] {
    auto value = instance(id);
    std::lock_guard close(value->closing);
    value->close();
    std::lock_guard lock(instancesMutex);
    instances.erase(id);
  });
}
