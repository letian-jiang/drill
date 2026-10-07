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
#include "scan/jni/JniPluginScan.h"
#include <condition_variable>
#include <folly/base64.h>
#include <folly/json.h>
#include <fstream>
#include <iostream>
#include <map>
#include <velox/vector/DecodedVector.h>
using namespace drill::nativeexec;
using protocol::Message;
using protocol::RpcMessage;
using protocol::RpcMode;
using protocol::Writer;
int main(int argc, char **argv) {
  try {
    VELOX_CHECK(argc == 2 || argc == 4,
                "Supply original SubScan manifest [external control/data ports]");
    std::ifstream input(argv[1]);
    VELOX_CHECK(input.good());
    auto scans = folly::parseJson(std::string(std::istreambuf_iterator<char>(input), {}));
    std::mutex mutex;
    std::condition_variable changed;
    std::map<uint64_t, uint32_t> terminal;
    std::map<uint64_t, folly::dynamic> results;
    std::string error;
    std::shared_ptr<memory::MemoryPool> root, pool;
    protocol::RpcServer control("127.0.0.1", 0, [&](const RpcMessage &request) {
      if (request.type == 0)
        return RpcMessage{RpcMode::Response, 0, 0, Writer().integer(1, 3).take(), {}};
      VELOX_CHECK_EQ(request.type, 8);
      Message status(request.body), profile(status.bytes(1));
      if (profile.integer(1) >= 3) {
        std::lock_guard lock(mutex);
        auto query = Message(Message(status.bytes(2)).bytes(1)).integer(1);
        terminal[query] = profile.integer(1);
        if (profile.integer(1) != 3) error = Message(profile.bytes(2)).bytes(4);
        changed.notify_all();
      }
      return RpcMessage{RpcMode::Response, 0, 1, Writer().integer(1, 1).take(), {}};
    });
    protocol::RpcServer data("127.0.0.1", 0, [&](const RpcMessage &request) {
      if (request.type == 0)
        return RpcMessage{RpcMode::Response, 0, 0, Writer().integer(1, 4).take(), {}};
      Message batch(request.body);
      VELOX_CHECK_EQ(batch.integer(2), 0);
      auto header = batchHeaderFromDefinition(batch.bytes(6));
      if (header["rows"].asInt()) {
        auto rows = decodeBatch({header, request.raw, {}}, pool.get());
        VELOX_CHECK_EQ(rows->size(), 1);
        folly::dynamic result = folly::dynamic::object;
        for (size_t i = 0; i < rows->childrenSize(); ++i) {
          DecodedVector value(*rows->childAt(i));
          VELOX_CHECK(!value.isNullAt(0));
          const auto &name = rows->rowType()->nameOf(i);
          switch (rows->childAt(i)->typeKind()) {
          case TypeKind::BIGINT: result[name] = value.valueAt<int64_t>(0); break;
          case TypeKind::TIMESTAMP: result[name] = value.valueAt<Timestamp>(0).toMillis(); break;
          case TypeKind::VARCHAR: result[name] = value.valueAt<StringView>(0).str(); break;
          case TypeKind::VARBINARY: result[name] = folly::base64Encode(value.valueAt<StringView>(0).str()); break;
          default: VELOX_FAIL("Unexpected original plugin aggregate result type");
          }
        }
        std::lock_guard lock(mutex);
        results[Message(batch.bytes(1)).integer(1)] = std::move(result);
      }
      return RpcMessage{RpcMode::Response, 0, 6, Writer().integer(1, 1).take(), {}};
    });
    auto foreman = Writer().bytes(1, "127.0.0.1").integer(3, control.port()).integer(4, data.port()).take();
    std::unique_ptr<NativeEngine> engine;
    std::unique_ptr<protocol::RpcServer> workerControl, workerData;
    uint16_t controlPort, dataPort;
    if (argc == 4) {
      auto controlValue = std::stoi(argv[2]), dataValue = std::stoi(argv[3]);
      VELOX_CHECK(controlValue > 0 && controlValue <= 65535 &&
                  dataValue > 0 && dataValue <= 65535 && controlValue != dataValue);
      controlPort = controlValue;
      dataPort = dataValue;
      initializeVelox();
    } else {
      workerControl = std::make_unique<protocol::RpcServer>("127.0.0.1", 0,
          [&](const auto &request) { return engine->control(request); });
      workerData = std::make_unique<protocol::RpcServer>("127.0.0.1", 0,
          [&](const auto &request) { return engine->data(request); });
      controlPort = workerControl->port();
      dataPort = workerData->port();
    }
    auto worker = Writer().bytes(1, "127.0.0.1").integer(3, controlPort).integer(4, dataPort).take();
    if (argc == 2) engine = std::make_unique<NativeEngine>(worker, 4);
    root = memory::memoryManager()->addRootPool("generic-plugin-rpc");
    pool = root->addLeafChild("root-input");
    protocol::RpcClient client("127.0.0.1", controlPort,
        {RpcMode::Request, 0, 0, Writer().integer(1, 3).integer(2, 0).bytes(3, foreman).take(), {}});
    uint64_t index = 100;
    for (const auto &entry : scans) {
      auto query = Writer().fixed64(1, ++index).fixed64(2, 103).take();
      auto handle = Writer().bytes(1, query).integer(2, 1).integer(3, 0).take();
      auto scan = entry["scan"];
      scan["jniScan"] = folly::dynamic::object("provider", "drill-java-subscan")("scan", scan);
      auto textColumn = entry.getDefault("textColumn", "w").asString();
      auto field = "`" + textColumn + "`";
      folly::dynamic aggregate = folly::dynamic::object("pop", "streaming-aggregate")("@id", 1)(
          "keys", folly::dynamic::array)("exprs", folly::dynamic::array(
              folly::dynamic::object("ref", "`n`")("expr", "count(`v`)"),
              folly::dynamic::object("ref", "`s`")("expr", "sum(cast(`v` as BIGINT))"),
              folly::dynamic::object("ref", "`strings`")("expr", "count(" + field + ")"),
              folly::dynamic::object("ref", "`first`")("expr", "min(" + field + ")"),
              folly::dynamic::object("ref", "`last`")("expr", "max(" + field + ")")))("child", scan);
      if (entry.count("aggregates")) aggregate["exprs"] = entry["aggregates"];
      folly::dynamic plan = folly::dynamic::object("pop", "single-sender")("@id", 0)(
          "receiver-major-fragment", 0)("receiver-minor-fragment", 0)(
          "destination", folly::base64Encode(foreman))("child", aggregate);
      auto fragment = Writer().bytes(1, handle).bytes(8, folly::toJson(plan))
          .bytes(10, worker).bytes(11, foreman)
          .bytes(14, Writer().bytes(1, "scan-test-user").take()).bytes(15, "[]").take();
      client.request(3, Writer().bytes(1, fragment).take());
      {
        std::unique_lock lock(mutex);
        VELOX_CHECK(changed.wait_for(lock, std::chrono::seconds(45), [&] { return terminal.count(index); }));
        VELOX_CHECK_EQ(terminal.at(index), 3, "{}: {}", entry["name"].asString(), error);
        VELOX_CHECK(results.at(index) == entry["expected"],
                    "Original plugin aggregate mismatch: {} vs {}",
                    folly::toJson(results.at(index)), folly::toJson(entry["expected"]));
      }
      std::cout << entry["name"].asString()
                << ": original SubScan -> JNI -> native aggregates -> BitData root passed (Unicode paths/column names, counts, numeric sum, nullable UTF-8 min/max)\n";
    }
    if (engine) {
      workerControl->stop();
      workerData->stop();
      engine->close();
      closeStandaloneJniScanHost();
      std::cout << "C++ RPC worker generic plugins passed; all original readers and scan host resources closed\n";
    } else {
      std::cout << "External C++ worker original plugin values passed; caller must verify process shutdown\n";
    }
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
