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
#include "execution/VeloxRuntime.h"
#include "plan/FragmentPlanConverter.h"
#include "scan/NativeScanRegistry.h"
#include "scan/iceberg/IcebergScan.h"
#include <folly/json.h>
#include <iostream>
#include <mutex>
#include <velox/vector/DecodedVector.h>
using namespace drill::nativeexec;
int main() {
  try {
    // CTest sets DRILL_NATIVE_SCAN_PLUGINS to the separately built example .so.
    VeloxRuntime runtime(4);
    auto pool = memory::memoryManager()->addRootPool("scan-plugin");
    auto leaf = pool->addLeafChild("plan");
    folly::CancellationSource cancellation;
    auto scan = folly::parseJson(R"({"provider":"example-range","version":1,
      "readFields":[{"name":"id","minor":"INT","optional":false}],
      "outputFields":[{"name":"id","minor":"INT","optional":false}],
      "splits":[{"start":0,"rows":1000},{"start":1000,"rows":1000},
                {"start":2000,"rows":1000},{"start":3000,"rows":1000}]})");
    NativeScanRegistry isolated;
    isolated.registerPlugin(icebergScanPlugin());
    bool duplicateRejected = false;
    try {
      isolated.registerPlugin(icebergScanPlugin());
    } catch (const std::exception &) {
      duplicateRejected = true;
    }
    VELOX_CHECK(duplicateRejected);
    auto &registry = NativeScanRegistry::instance();
    auto rejects = [&](folly::dynamic invalid, std::string_view message) {
      try {
        registry.bind(invalid,
                      {leaf.get(), runtime.io(), cancellation.getToken()});
      } catch (const std::exception &error) {
        VELOX_CHECK(std::string_view(error.what()).find(message) !=
                    std::string_view::npos);
        return;
      }
      VELOX_FAIL("Invalid scan descriptor was accepted");
    };
    auto invalid = scan;
    invalid["provider"] = "missing";
    rejects(invalid, "not registered");
    invalid = scan;
    invalid["version"] = 2;
    rejects(invalid, "version");
    invalid = scan;
    invalid.erase("outputFields");
    rejects(invalid, "outputFields");
    auto binding = registry.bind(
        scan, {leaf.get(), runtime.io(), cancellation.getToken()});
    auto reader = binding.factory(leaf.get());
    cancellation.requestCancellation();
    ContinueFuture future;
    VELOX_CHECK(!reader->next(future).has_value());
    std::mutex mutex;
    std::vector<int32_t> rows;
    FragmentPlanConverter converter(
        leaf.get(),
        [&](const folly::dynamic &node) {
          auto result =
              registry.bind(node["nativeScan"], {leaf.get(), runtime.io(), {}});
          return SourceBinding{result.schema, SourceKind::NativeScan,
                               result.factory, result.normalizeOutput};
        },
        [&](const folly::dynamic &, const RowTypePtr &schema) {
          VELOX_CHECK(!fieldsFromType(schema)[0]["optional"].asBool());
          return SinkFactory([&](memory::MemoryPool *) {
            return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
              if (!batch)
                return {};
              DecodedVector values(*batch->childAt(0));
              std::lock_guard lock(mutex);
              for (int32_t i = 0; i < batch->size(); ++i)
                rows.push_back(values.valueAt<int32_t>(i));
              return {};
            });
          });
        });
    folly::dynamic node = folly::dynamic::object("pop", "single-sender")(
        "@id", 0)("child", folly::dynamic::object("pop", "external-range-scan")(
                               "@id", 1)("nativeScan", scan));
    auto plan = converter.convert(node);
    auto task = runtime.task("external-scan-module", plan, pool, leaf);
    task->start(4);
    task->taskCompletionFuture().wait();
    VELOX_CHECK_EQ(task->state(), exec::TaskState::kFinished);
    std::sort(rows.begin(), rows.end());
    VELOX_CHECK_EQ(rows.size(), 4000);
    for (int32_t i = 0; i < 4000; ++i)
      VELOX_CHECK_EQ(rows[i], i);
    auto deleted = task->taskDeletionFuture();
    task.reset();
    deleted.wait();
    std::cout << "Loaded external scan module; 4 drivers, exactly-once work, "
                 "cancellation and descriptor validation passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
