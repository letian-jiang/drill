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
#include "columnar/DrillBatchMetadata.h"
#include "execution/NativeEngine.h"
#include <condition_variable>
#include <folly/base64.h>
#include <folly/json.h>
#include <iostream>
#include <map>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
using protocol::Message;
using protocol::Writer;
int main() {
  try {
    // Intentionally no RPC listener. Both native exchange and host callbacks
    // must work even when the advertised local ports cannot be connected to.
    auto endpoint = Writer().bytes(1, "127.0.0.1").integer(3, 63001)
                        .integer(4, 63002).take();
    auto query = Writer().fixed64(1, 17).fixed64(2, 19).take();
    auto assignedEndpoint = endpoint + Writer().integer(7, 1).take();
    auto handle = [&](uint32_t major, uint32_t minor) {
      return Writer().bytes(1, query).integer(2, major).integer(3, minor).take();
    };
    std::mutex mutex;
    std::condition_variable changed;
    std::map<std::pair<uint32_t, uint32_t>, uint32_t> terminal;
    std::string error;
    bool dataAccepted = false;
    int schemas = 0, eos = 0;
    int64_t sum = 0;
    auto deferredAck = std::make_shared<ContinuePromise>("Java root ACK");
    std::shared_ptr<memory::MemoryPool> root, pool;
    EngineCallbacks callbacks;
    callbacks.status = [&](std::string bytes) {
      Message status(bytes), profile(status.bytes(1)), h(status.bytes(2));
      VELOX_CHECK(profile.bytes(9) == assignedEndpoint,
                  "Status must preserve the original assignment endpoint");
      if (profile.integer(1) < 3)
        return;
      std::lock_guard lock(mutex);
      terminal[{h.integer(2), h.integer(3)}] = profile.integer(1);
      if (profile.integer(1) == 5)
        error = Message(profile.bytes(2)).bytes(4);
      changed.notify_all();
    };
    callbacks.rootBatch = [&](std::string bytes,
                               std::string payload) -> ContinueFuture {
      Message batch(bytes);
      VELOX_CHECK_EQ(batch.integer(2), 0);
      auto header = batchHeaderFromDefinition(batch.bytes(6));
      std::lock_guard lock(mutex);
      if (batch.integer(7)) {
        VELOX_CHECK(payload.empty());
        VELOX_CHECK(header["fields"].empty());
        ++eos;
      } else if (header["rows"].asInt()) {
        auto rows = decodeBatch({header, payload, {}}, pool.get());
        VELOX_CHECK_EQ(rows->size(), 1);
        DecodedVector values(*rows->childAt(0));
        sum = values.valueAt<int64_t>(0);
        dataAccepted = true;
        changed.notify_all();
        return deferredAck->getSemiFuture();
      } else {
        ++schemas;
      }
      return {};
    };
    NativeEngine engine(endpoint, 4, std::move(callbacks));
    root = memory::memoryManager()->addRootPool("embedded-test-root");
    pool = root->addLeafChild("java-adapter");
    auto node = folly::base64Encode(endpoint);
    auto sender = [&](uint32_t major, uint32_t minor,
                       folly::dynamic child) -> folly::dynamic {
      return folly::dynamic::object("pop", "single-sender")("@id", 0)(
          "receiver-major-fragment", major)("receiver-minor-fragment", minor)(
          "destination", node)("child", std::move(child));
    };
    auto receiver = [&](uint32_t major, unsigned count) -> folly::dynamic {
      folly::dynamic senders = folly::dynamic::array;
      for (unsigned i = 0; i < count; ++i)
        senders.push_back(folly::dynamic::object("minorFragmentId", i)(
            "endpoint", node));
      return folly::dynamic::object("pop", "unordered-receiver")("@id", 4)(
          "sender-major-fragment", major)("senders", std::move(senders));
    };
    folly::dynamic aggregate = folly::dynamic::object("pop", "streaming-aggregate")(
        "@id", 1)("keys", folly::dynamic::array)(
        "exprs", folly::dynamic::array(
                     folly::dynamic::object("ref", "`s`")("expr", "sum(`out`)")))(
        "child", receiver(2, 2));
    folly::dynamic filtered = folly::dynamic::object("pop", "filter")("@id", 3)(
        "expr", "greater_than_or_equal_to(`v`,50)")("child", receiver(3, 1));
    folly::dynamic projected = folly::dynamic::object("pop", "project")("@id", 2)(
        "exprs", folly::dynamic::array(
                     folly::dynamic::object("ref", "`out`")("expr", "add(`v`,1)")))(
        "child", std::move(filtered));
    auto fragment = [&](uint32_t major, uint32_t minor,
                         const folly::dynamic &plan) {
      return Writer().bytes(1, handle(major, minor))
          .bytes(8, folly::toJson(plan)).bytes(10, assignedEndpoint)
          .bytes(11, endpoint).take();
    };
    Writer initial;
    initial.bytes(1, fragment(1, 0, sender(0, 0, aggregate)));
    for (uint32_t i = 0; i < 2; ++i)
      initial.bytes(1, fragment(2, i, sender(1, 0, projected)));
    engine.submitFragments(initial.take());
    auto type = ROW({"v"}, {BIGINT()});
    auto fields = fieldsFromType(type);
    for (uint32_t i = 0; i < 2; ++i) {
      auto column = BaseVector::create(BIGINT(), 50, pool.get());
      for (int r = 0; r < 50; ++r)
        column->as<FlatVector<int64_t>>()->set(r, i * 50 + r);
      auto rows = std::make_shared<RowVector>(pool.get(), type, nullptr, 50,
                                             std::vector<VectorPtr>{column});
      auto batch = encodeBatch(rows, fields);
      engine.acceptRecordBatch(
          Writer().bytes(1, query).integer(2, 2).integer(3, i)
              .integer(4, 3).integer(5, 0).bytes(6, batchDefinition(batch))
              .integer(7, 1).take(), batch.data);
    }
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return dataAccepted || !error.empty(); }));
      VELOX_CHECK(error.empty(), "Native task failed: {}", error);
      VELOX_CHECK_EQ(sum, 3775);
      VELOX_CHECK_EQ(schemas, 1);
      VELOX_CHECK_EQ(eos, 0, "EOS overtook the Java root ACK");
      VELOX_CHECK(!terminal.count({1, 0}), "Task finished before root ACK");
    }
    deferredAck->setValue();
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(10),
                                   [&] { return terminal.size() == 3; }));
      for (auto &[h, state] : terminal)
        VELOX_CHECK_EQ(state, 3, "Native task failed: {}", error);
      VELOX_CHECK_EQ(eos, 1);
    }
    // The core rejects root execution even when called through a local host.
    bool rejected = false;
    try {
      engine.submitFragments(Writer().bytes(1, fragment(0, 0, sender(0, 0, aggregate))).take());
    } catch (const std::exception &) {
      rejected = true;
    }
    VELOX_CHECK(rejected);
    engine.close();
    engine.close();
    std::cout << "NativeEngine: Java adapter -> two local native minors -> "
                 "native aggregate -> host root, delayed ACK/EOS, root "
                 "rejection and idempotent close passed; no sockets/JVM.\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
