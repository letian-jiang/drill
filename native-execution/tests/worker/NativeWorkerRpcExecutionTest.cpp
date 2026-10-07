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
#include "worker/MinorTaskRegistry.h"
#include <condition_variable>
#include <folly/base64.h>
#include <folly/json.h>
#include <iostream>
#include <map>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
using protocol::Message;
using protocol::RpcMessage;
using protocol::RpcMode;
using protocol::Writer;
int main() {
  try {
    VeloxRuntime runtime(4);
    std::mutex mutex;
    std::condition_variable complete;
    uint32_t terminal = 0;
    std::map<uint64_t, uint32_t> terminals;
    int upstreamFinished = 0, announcedSchemas = 0;
    std::string error;
    int64_t actual = 0;
    int outputRows = 0, eos = 0;
    auto root = memory::memoryManager()->addRootPool("rpc-test-root");
    auto outputPool = root->addLeafChild("receiver");
    protocol::RpcServer foreman("127.0.0.1", 0, [&](const RpcMessage &request) {
      if (request.type == 0)
        return RpcMessage{
            RpcMode::Response, 0, 0, Writer().integer(1, 3).take(), {}};
      if (request.type == 7) {
        Message finished(request.body);
        VELOX_CHECK_EQ(Message(finished.bytes(2)).integer(2), 2);
        std::lock_guard lock(mutex);
        ++upstreamFinished;
        complete.notify_all();
        return RpcMessage{
            RpcMode::Response, 0, 1, Writer().integer(1, 1).take(), {}};
      }
      VELOX_CHECK_EQ(request.type, 8);
      Message status(request.body), profile(status.bytes(1));
      auto state = profile.integer(1);
      if (state >= 3) {
        std::lock_guard lock(mutex);
        terminal = state;
        auto id = Message(Message(status.bytes(2)).bytes(1)).integer(1);
        terminals[id] = state;
        error = std::string(Message(profile.bytes(2)).bytes(4));
        complete.notify_all();
      }
      return RpcMessage{
          RpcMode::Response, 0, 1, Writer().integer(1, 1).take(), {}};
    });
    protocol::RpcServer rootData(
        "127.0.0.1", 0, [&](const RpcMessage &request) {
          if (request.type == 0)
            return RpcMessage{
                RpcMode::Response, 0, 0, Writer().integer(1, 4).take(), {}};
          Message batch(request.body);
          auto header = batchHeaderFromDefinition(batch.bytes(6));
          VELOX_CHECK_EQ(batch.integer(2), 0);
          VELOX_CHECK(batch.repeatedIntegers(3) == std::vector<uint32_t>{0});
          std::lock_guard lock(mutex);
          if (header["rows"].asInt() == 0 && !header["fields"].empty())
            ++announcedSchemas;
          if (header["rows"].asInt() > 0) {
            auto vector = decodeBatch(WireBatch{header, request.raw, {}},
                                      outputPool.get());
            DecodedVector values(*vector->childAt(0));
            for (int r = 0; r < vector->size(); ++r) {
              if (!values.isNullAt(r))
                actual += values.valueAt<int64_t>(r);
              ++outputRows;
            }
          }
          if (batch.integer(7)) {
            VELOX_CHECK(request.raw.empty(),
                        "EOS leaked a Java receive buffer");
            VELOX_CHECK(header["fields"].empty(), "EOS must have no schema");
            ++eos;
          }
          return RpcMessage{
              RpcMode::Response, 0, 6, Writer().integer(1, 1).take(), {}};
        });
    auto endpoint = Writer()
                        .bytes(1, "127.0.0.1")
                        .integer(3, foreman.port())
                        .integer(4, rootData.port())
                        .take();
    std::unique_ptr<MinorTaskRegistry> registry;
    protocol::RpcServer control("127.0.0.1", 0, [&](const auto &request) {
      return registry->control(request);
    });
    protocol::RpcServer data("127.0.0.1", 0, [&](const auto &request) {
      return registry->data(request);
    });
    auto workerEndpoint = Writer().bytes(1, "127.0.0.1")
                              .integer(3, control.port())
                              .integer(4, data.port()).take();
    registry = std::make_unique<MinorTaskRegistry>(runtime, workerEndpoint);
    auto query = Writer().fixed64(1, 7).fixed64(2, 9).take();
    auto handle = Writer().bytes(1, query).integer(2, 1).integer(3, 0).take();
    std::string plan =
        R"PLAN({"pop":"single-sender","@id":0,"receiver-major-fragment":0,"receiver-minor-fragment":0,
      "destination":{"address":"127.0.0.1","dataPort":)PLAN" +
        std::to_string(rootData.port()) + R"PLAN(},"child":{
      "pop":"streaming-aggregate","@id":1,"keys":[],"exprs":[{"ref":"`s`","expr":"sum(`out`)"}],"child":{
      "pop":"project","@id":2,"exprs":[{"ref":"`out`","expr":"add(`v`,1)"}],"child":{
      "pop":"filter","@id":3,"expr":"greater_than_or_equal_to(`v`,50)","child":{
      "pop":"unordered-receiver","@id":4,"sender-major-fragment":2,"senders":[{"minorFragmentId":0},{"minorFragmentId":1}]}}}}})PLAN";
    // Exercise the actual Jackson protobuf endpoint representation.
    auto parsedPlan = folly::parseJson(plan);
    parsedPlan["destination"] = folly::base64Encode(endpoint);
    auto &senders = parsedPlan["child"]["child"]["child"]["child"]["senders"];
    for (auto &sender : senders)
      sender["endpoint"] = folly::base64Encode(endpoint);
    plan = folly::toJson(parsedPlan);
    auto fragment =
        Writer().bytes(1, handle).bytes(8, plan).bytes(11, endpoint).take();
    protocol::RpcClient controlClient(
        "127.0.0.1", control.port(),
        {RpcMode::Request,
         0,
         0,
         Writer().integer(1, 3).integer(2, 0).bytes(3, endpoint).take(),
         {}});
    controlClient.request(3, Writer().bytes(1, fragment).take());
    protocol::RpcClient dataClient("127.0.0.1", data.port(),
                                   {RpcMode::Request,
                                    0,
                                    0,
                                    Writer().integer(1, 4).integer(2, 1).take(),
                                    {}});
    auto type = ROW({"v"}, {BIGINT()});
    auto fields = fieldsFromType(type);
    for (int sender = 0; sender < 2; ++sender) {
      auto column = BaseVector::create(BIGINT(), 50, outputPool.get());
      for (int r = 0; r < 50; ++r)
        column->as<FlatVector<int64_t>>()->set(r, sender * 50 + r);
      auto rows = std::make_shared<RowVector>(
          outputPool.get(), type, nullptr, 50, std::vector<VectorPtr>{column});
      auto batch = encodeBatch(rows, fields);
      auto body = Writer()
                      .bytes(1, query)
                      .integer(2, 1)
                      .integer(3, 0)
                      .integer(4, 2)
                      .integer(5, sender)
                      .bytes(6, batchDefinition(batch))
                      .integer(7, 1)
                      .take();
      auto ack = dataClient.request(3, body, batch.data);
      VELOX_CHECK_EQ(ack.type, 6);
    }
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(complete.wait_for(lock, std::chrono::seconds(10),
                                    [&] { return terminal != 0; }),
                  "Native task did not finish");
      VELOX_CHECK_EQ(terminal, 3, "Native task failed: {}", error);
      VELOX_CHECK_EQ(outputRows, 1);
      VELOX_CHECK_EQ(actual, 3775);
      VELOX_CHECK_EQ(eos, 1);
    }
    auto submit = [&](uint64_t id) {
      auto q = Writer().fixed64(1, id).fixed64(2, 9).take();
      auto h = Writer().bytes(1, q).integer(2, 1).integer(3, 0).take();
      auto f = Writer().bytes(1, h).bytes(8, plan).bytes(11, endpoint).take();
      controlClient.request(3, Writer().bytes(1, f).take());
      return std::pair<std::string, std::string>{q, h};
    };
    auto sendSchema = [&](const std::string &q) {
      dataClient.request(3, Writer()
                                .bytes(1, q)
                                .integer(2, 1)
                                .integer(3, 0)
                                .integer(4, 2)
                                .integer(5, 0)
                                .bytes(6, schemaDefinition(fields))
                                .integer(7, 0)
                                .take());
    };
    auto waitTerminal = [&](uint64_t id, uint32_t expected = 3) {
      std::unique_lock lock(mutex);
      VELOX_CHECK(complete.wait_for(lock, std::chrono::seconds(10),
                                    [&] { return terminals.count(id); }),
                  "Minor {} did not finish", id);
      VELOX_CHECK_EQ(terminals.at(id), expected, "Minor {} failed: {}", id,
                     error);
    };
    // Zero input still publishes the output schema and a buffer-free EOS.
    auto [emptyQuery, emptyHandle] = submit(8);
    sendSchema(emptyQuery);
    for (uint32_t sender = 0; sender < 2; ++sender)
      dataClient.request(3, Writer()
                                .bytes(1, emptyQuery)
                                .integer(2, 1)
                                .integer(3, 0)
                                .integer(4, 2)
                                .integer(5, sender)
                                .bytes(6, Writer().integer(1, 0).take())
                                .integer(7, 1)
                                .take());
    waitTerminal(8);
    {
      std::lock_guard lock(mutex);
      VELOX_CHECK_EQ(eos, 2);
      VELOX_CHECK_EQ(announcedSchemas, 2);
    }
    auto finishReceiver = [&](const std::string &q, const std::string &h) {
      auto receiver = Writer().bytes(1, q).integer(2, 0).integer(3, 0).take();
      controlClient.request(7, Writer().bytes(1, receiver).bytes(2, h).take());
    };
    // Limit can close the receiver before the sender's input schema arrives.
    auto [earlyQuery, earlyHandle] = submit(10);
    finishReceiver(earlyQuery, earlyHandle);
    waitTerminal(10);
    // It can also close a running Task blocked on a receiver without EOS.
    auto [runningQuery, runningHandle] = submit(11);
    sendSchema(runningQuery);
    finishReceiver(runningQuery, runningHandle);
    waitTerminal(11);
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(
          complete.wait_for(lock, std::chrono::seconds(10),
                            [&] { return upstreamFinished == 4; }),
          "Receiver closure did not propagate to both upstream senders");
      VELOX_CHECK_EQ(eos, 2, "Closed root receivers must not receive EOS");
    }
    auto cancelQuery = Writer().fixed64(1, 12).fixed64(2, 9).take();
    auto cancelHandle =
        Writer().bytes(1, cancelQuery).integer(2, 1).integer(3, 0).take();
    // Separate control connections can deliver cancel before initialization.
    controlClient.request(6, cancelHandle);
    submit(12);
    waitTerminal(12, 4);
    auto [cancelRunningQuery, cancelRunningHandle] = submit(13);
    sendSchema(cancelRunningQuery);
    controlClient.request(6, cancelRunningHandle);
    waitTerminal(13, 4);
    {
      std::unique_lock lock(mutex);
      VELOX_CHECK(complete.wait_for(lock, std::chrono::seconds(10),
                                    [&] { return upstreamFinished == 8; }),
                  "Cancel did not stop upstream");
    }
    control.stop();
    data.stop();
    registry->stop();
    std::cout << "Original BitControl submit -> native receiver -> four "
                 "drivers -> filter/project/aggregate -> BitData result + EOS "
                 "+ Foreman status passed; no JVM.\n";
    return 0;
  } catch (const std::exception &e) {
    std::cerr << e.what() << '\n';
    return 1;
  }
}
