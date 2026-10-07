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
    std::unique_ptr<NativeEngine> engineA, engineB;
    protocol::RpcServer controlA(
        "127.0.0.1", 0, [&](const auto &r) { return engineA->control(r); });
    protocol::RpcServer dataA("127.0.0.1", 0,
                              [&](const auto &r) { return engineA->data(r); });
    protocol::RpcServer controlB(
        "127.0.0.1", 0, [&](const auto &r) { return engineB->control(r); });
    protocol::RpcServer dataB("127.0.0.1", 0,
                              [&](const auto &r) { return engineB->data(r); });
    auto endpoint = std::string{};
    auto query = Writer().fixed64(1, 17).fixed64(2, 19).take();
    auto assignedEndpoint = std::string{};
    auto endpointB = std::string{};
    auto handle = [&](uint32_t major, uint32_t minor) {
      return Writer()
          .bytes(1, query)
          .integer(2, major)
          .integer(3, minor)
          .take();
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
    auto ack = [](int type) {
      return protocol::RpcMessage{protocol::RpcMode::Response,
                                  0,
                                  type,
                                  Writer().integer(1, 1).take(),
                                  {}};
    };
    protocol::RpcServer javaControl(
        "127.0.0.1", 0, [&](const protocol::RpcMessage &request) {
          if (request.type == 0)
            return protocol::RpcMessage{protocol::RpcMode::Response,
                                        0,
                                        0,
                                        Writer().integer(1, 3).take(),
                                        {}};
          VELOX_CHECK_EQ(request.type, 8);
          auto bytes = request.body;
          Message status(bytes), profile(status.bytes(1)), h(status.bytes(2));
          VELOX_CHECK(profile.bytes(9) ==
                          (h.integer(2) == 1 ? endpointB : assignedEndpoint),
                      "Status must preserve the original assignment endpoint");
          if (profile.integer(1) < 3)
            return ack(1);
          std::lock_guard lock(mutex);
          terminal[{h.integer(2), h.integer(3)}] = profile.integer(1);
          if (profile.integer(1) == 5)
            error = Message(profile.bytes(2)).bytes(4);
          changed.notify_all();
          return ack(1);
        });
    protocol::RpcServer javaData(
        "127.0.0.1", 0, [&](const protocol::RpcMessage &request) {
          if (request.type == 0)
            return protocol::RpcMessage{protocol::RpcMode::Response,
                                        0,
                                        0,
                                        Writer().integer(1, 4).take(),
                                        {}};
          VELOX_CHECK_EQ(request.type, 3);
          auto bytes = request.body;
          auto payload = request.raw;
          Message batch(bytes);
          VELOX_CHECK_EQ(batch.integer(2), 0);
          auto header = batchHeaderFromDefinition(batch.bytes(6));
          std::unique_lock lock(mutex);
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
            lock.unlock();
            deferredAck->getSemiFuture().get();
            return ack(6);
          } else {
            ++schemas;
          }
          return ack(6);
        });
    auto makeEndpoint = [&](int javaC, int javaD, int nativeC, int nativeD) {
      return Writer()
          .bytes(1, "127.0.0.1")
          .integer(3, javaC)
          .integer(4, javaD)
          .integer(7, 1)
          .bytes(9, Writer()
                        .bytes(1, "127.0.0.1")
                        .integer(3, 2)
                        .integer(5, nativeC)
                        .integer(6, nativeD)
                        .take())
          .take();
    };
    endpoint = makeEndpoint(javaControl.port(), javaData.port(),
                            controlA.port(), dataA.port());
    assignedEndpoint = endpoint;
    endpointB = makeEndpoint(63001, 63002, controlB.port(), dataB.port());
    engineA = std::make_unique<NativeEngine>(endpoint, 4);
    engineB = std::make_unique<NativeEngine>(endpointB, 4);
    auto connection = [&](uint16_t port, bool control) {
      return std::make_unique<protocol::RpcClient>(
          "127.0.0.1", port,
          protocol::RpcMessage{protocol::RpcMode::Request,
                               0,
                               0,
                               Writer()
                                   .integer(1, control ? 3 : 4)
                                   .integer(2, control ? 0 : 1)
                                   .take(),
                               {}});
    };
    auto submitA = connection(controlA.port(), true);
    auto submitB = connection(controlB.port(), true);
    auto input = connection(dataA.port(), false);
    root = memory::memoryManager()->addRootPool("embedded-test-root");
    pool = root->addLeafChild("java-adapter");
    auto node = folly::base64Encode(endpoint);
    auto nodeB = folly::base64Encode(endpointB);
    auto sender = [&](uint32_t major, uint32_t minor,
                      folly::dynamic child) -> folly::dynamic {
      return folly::dynamic::object("pop", "single-sender")("@id", 0)(
          "receiver-major-fragment", major)("receiver-minor-fragment", minor)(
          "destination", major == 1 ? nodeB : node)("child", std::move(child));
    };
    auto receiver = [&](uint32_t major, unsigned count) -> folly::dynamic {
      folly::dynamic senders = folly::dynamic::array;
      for (unsigned i = 0; i < count; ++i)
        senders.push_back(
            folly::dynamic::object("minorFragmentId", i)("endpoint", node));
      return folly::dynamic::object("pop", "unordered-receiver")("@id", 4)(
          "sender-major-fragment", major)("senders", std::move(senders));
    };
    folly::dynamic aggregate = folly::dynamic::object(
        "pop", "streaming-aggregate")("@id", 1)("keys", folly::dynamic::array)(
        "exprs", folly::dynamic::array(folly::dynamic::object("ref", "`s`")(
                     "expr", "sum(`out`)")))("child", receiver(2, 2));
    folly::dynamic filtered = folly::dynamic::object("pop", "filter")("@id", 3)(
        "expr", "greater_than_or_equal_to(`v`,50)")("child", receiver(3, 1));
    folly::dynamic projected = folly::dynamic::object("pop", "project")(
        "@id", 2)("exprs",
                  folly::dynamic::array(folly::dynamic::object("ref", "`out`")(
                      "expr", "add(`v`,1)")))("child", std::move(filtered));
    auto fragment = [&](uint32_t major, uint32_t minor,
                        const folly::dynamic &plan) {
      return Writer()
          .bytes(1, handle(major, minor))
          .bytes(8, folly::toJson(plan))
          .bytes(10, major == 1 ? endpointB : assignedEndpoint)
          .bytes(11, endpoint)
          .take();
    };
    Writer routes;
    routes.bytes(1, handle(0, 0));
    auto route = [&](uint32_t major, uint32_t minor) {
      auto assigned = major == 1 ? endpointB : endpoint;
      Message node(assigned), capability(node.bytes(9));
      routes.bytes(
          2,
          Writer()
              .bytes(1, handle(major, minor))
              .bytes(2, assigned)
              .integer(3, major == 0 ? 0 : 1)
              .bytes(4, "127.0.0.1")
              .integer(5, major == 0 ? node.integer(3) : capability.integer(5))
              .integer(6, major == 0 ? node.integer(4) : capability.integer(6))
              .take());
    };
    route(0, 0);
    route(1, 0);
    route(2, 0);
    route(2, 1);
    route(3, 0);
    auto context = routes.take();
    submitB->request(3, Writer()
                            .bytes(2, context)
                            .bytes(1, fragment(1, 0, sender(0, 0, aggregate)))
                            .take());
    Writer initial;
    initial.bytes(2, context);
    for (uint32_t i = 0; i < 2; ++i)
      initial.bytes(1, fragment(2, i, sender(1, 0, projected)));
    submitA->request(3, initial.take());
    auto type = ROW({"v"}, {BIGINT()});
    auto fields = fieldsFromType(type);
    for (uint32_t i = 0; i < 2; ++i) {
      auto column = BaseVector::create(BIGINT(), 50, pool.get());
      for (int r = 0; r < 50; ++r)
        column->as<FlatVector<int64_t>>()->set(r, i * 50 + r);
      auto rows = std::make_shared<RowVector>(pool.get(), type, nullptr, 50,
                                              std::vector<VectorPtr>{column});
      auto batch = encodeBatch(rows, fields);
      input->request(3,
                     Writer()
                         .bytes(1, query)
                         .integer(2, 2)
                         .integer(3, i)
                         .integer(4, 3)
                         .integer(5, 0)
                         .bytes(6, batchDefinition(batch))
                         .integer(7, 1)
                         .take(),
                     batch.data);
    }
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(10), [&] {
        return dataAccepted || !error.empty();
      }));
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
      engineA->submitFragments(
          Writer()
              .bytes(2, context)
              .bytes(1, fragment(0, 0, sender(0, 0, aggregate)))
              .take());
    } catch (const std::exception &) {
      rejected = true;
    }
    VELOX_CHECK(rejected);
    // Missing routes and duplicate/cross-query routes fail before creating
    // work.
    auto rejects = [](auto action) {
      bool rejected = false;
      try {
        action();
      } catch (const std::exception &) {
        rejected = true;
      }
      VELOX_CHECK(rejected);
    };
    rejects([&] {
      engineA->submitFragments(
          Writer().bytes(1, fragment(2, 0, sender(1, 0, projected))).take());
    });
    rejects([&] {
      protocol::FragmentRoutes invalid(
          context +
          Writer().bytes(2, Message(context).messages(2).front()).take());
    });
    // Cancellation arriving before initialize must stay cancelled without
    // schema.
    auto cancelledHandle = handle(2, 9);
    Writer extra;
    extra.bytes(1, handle(0, 0));
    for (auto routeBytes : Message(context).messages(2))
      extra.bytes(2, routeBytes);
    Message capability(Message(endpoint).bytes(9));
    extra.bytes(2, Writer()
                       .bytes(1, cancelledHandle)
                       .bytes(2, endpoint)
                       .integer(3, 1)
                       .bytes(4, "127.0.0.1")
                       .integer(5, capability.integer(5))
                       .integer(6, capability.integer(6))
                       .take());
    submitA->request(6, cancelledHandle);
    submitA->request(3, Writer()
                            .bytes(2, extra.take())
                            .bytes(1, fragment(2, 9, sender(1, 0, projected)))
                            .take());
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(10), [&] {
        return terminal.count({2, 9});
      }));
      VELOX_CHECK_EQ(terminal.at({2, 9}), 4);
    }
    Writer failureRoutes;
    failureRoutes.bytes(1, handle(0, 0));
    for (auto routeBytes : Message(context).messages(2))
      failureRoutes.bytes(2, routeBytes);
    failureRoutes.bytes(2, Writer()
                               .bytes(1, handle(2, 10))
                               .bytes(2, endpoint)
                               .integer(3, 1)
                               .bytes(4, "127.0.0.1")
                               .integer(5, capability.integer(5))
                               .integer(6, capability.integer(6))
                               .take());
    submitA->request(3, Writer()
                            .bytes(2, failureRoutes.take())
                            .bytes(1, fragment(2, 10, sender(1, 0, projected)))
                            .take());
    engineA->fail("controlled native listener failure");
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(10), [&] {
        return terminal.count({2, 10});
      }));
      VELOX_CHECK_EQ(terminal.at({2, 10}), 5);
      VELOX_CHECK_EQ(error, "controlled native listener failure");
    }
    engineA->quiesce();
    engineB->quiesce();
    VELOX_CHECK(engineA->awaitIdle(1000));
    VELOX_CHECK(engineB->awaitIdle(1000));
    rejects([&] {
      engineA->submitFragments(
          Writer()
              .bytes(2, context)
              .bytes(1, fragment(2, 0, sender(1, 0, projected)))
              .take());
    });
    engineA->close();
    engineB->close();
    std::cout << "Homogeneous RPC: canonical assignment, frozen routes, local "
                 "and cross-engine native exchange, "
                 "loopback status/root, DATA ACK before EOS, missing/root "
                 "routes rejected, cancel-before-submit and native "
                 "failure/withdrawal passed.\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
