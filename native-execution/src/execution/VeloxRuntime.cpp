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
#include "execution/VeloxRuntime.h"
#include "scan/NativeScanRegistry.h"
#include "exchange/DrillHash.h"
#include "velox/functions/DrillDecimal.h"
#include "velox/functions/DrillDict.h"
#include "velox/functions/DrillNumeric.h"
#include "velox/plan/DrillPlanNodes.h"
#include <algorithm>
#include <folly/executors/InlineExecutor.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>
#include <mutex>
#include <sched.h>
#include <thread>
#include <velox/expression/RegisterSpecialForm.h>
#include <velox/functions/lib/window/RegistrationFunctions.h>
#include <velox/functions/prestosql/Map.h>
#include <velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h>
#include <velox/functions/prestosql/window/WindowFunctionsRegistration.h>
#include <velox/functions/sparksql/aggregates/Register.h>
#include <velox/functions/sparksql/registration/Register.h>
namespace drill::nativeexec {
unsigned availableCpuCount() {
  cpu_set_t cpus;
  if (sched_getaffinity(0, sizeof(cpus), &cpus) == 0)
    return std::max(1, CPU_COUNT(&cpus));
  return std::max(1u, std::thread::hardware_concurrency());
}
void initializeVelox() {
  static std::once_flag once;
  std::call_once(once, [] {
    memory::MemoryManager::Options options;
    memory::MemoryManager::initialize(options);
    functions::sparksql::registerFunctions();
    aggregate::prestosql::registerAllAggregateFunctions("", true, false, true);
    functions::aggregate::sparksql::registerAggregateFunctions("", true, true);
    window::prestosql::registerAllWindowFunctions();
    // Drill ranking functions return BIGINT, but NTILE's INT argument/result
    // is the Spark variant, as in WindowFunction.Ntile.
    functions::window::registerNtileInteger("ntile");
    exec::registerFunctionCallToSpecialForms();
    registerDrillOperators();
    initializeNativeScanPlugins();
    registerDecimalFunctions();
    registerHashFunctions();
    registerNumericFunctions();
    registerDictFunctions();
    functions::registerMapFunction("drill_dict", true);
  });
}
VeloxRuntime::VeloxRuntime(unsigned threads)
    : threads_(std::max(1u, threads)),
      cpu_(threads_,
           std::make_shared<folly::NamedThreadFactory>("drill-native-cpu")),
      io_(threads_,
          std::make_shared<folly::NamedThreadFactory>("drill-native-io")) {
  initializeVelox();
}
std::shared_ptr<exec::Task>
VeloxRuntime::task(std::string id, core::PlanNodePtr plan,
                   const std::shared_ptr<memory::MemoryPool> &pool,
                   const std::shared_ptr<memory::MemoryPool> &planPool) {
  // Drill minors normally run with fewer than Velox's default 33 partitions.
  // Without buffered partitioning, a small post-filter batch is split once per
  // driver, sending tiny vectors through every following aggregate/expression.
  // Reuse Velox's partition buffers for any parallel repartition. Gather and
  // eager-flush exchanges retain their existing behavior and EOS flushes tails.
  auto ctx = core::QueryCtx::create(
      &cpu_,
      core::QueryConfig(std::unordered_map<std::string, std::string>{
          {core::QueryConfig::
               kMinLocalExchangePartitionCountToUsePartitionBuffer,
           "2"},
          // Original scan batches are commonly 8192 rows. The 1024-row Velox
          // default splits join output repeatedly before filters and sender
          // hash expressions. Keep vector work at that scan batch scale; the
          // existing byte limit and maximum output rows still apply.
          {core::QueryConfig::kPreferredOutputBatchRows, "8192"}}),
      {}, nullptr, pool, nullptr, id);
  core::PlanFragment fragment{
      std::move(plan), core::ExecutionStrategy::kUngrouped, 1, {}};
  auto task = exec::Task::create(id, std::move(fragment), 0, std::move(ctx),
                                 exec::Task::ExecutionMode::kParallel,
                                 exec::ConsumerSupplier{});
  if (planPool) {
    // Plan constants and source factories can outlive run() and Task
    // completion. Velox fulfills deletion promises after destroying the plan
    // and QueryCtx. Keep both pools until then, releasing the leaf before its
    // parent root.
    task->taskDeletionFuture()
        .via(&folly::InlineExecutor::instance())
        .thenValue([root = pool, leaf = planPool](auto) mutable {
          leaf.reset();
          root.reset();
        });
  }
  return task;
}
} // namespace drill::nativeexec
