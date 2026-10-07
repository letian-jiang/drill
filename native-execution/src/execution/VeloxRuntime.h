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
#pragma once
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <velox/exec/Task.h>
namespace drill::nativeexec {
using namespace facebook::velox;
unsigned availableCpuCount();
void initializeVelox();
class VeloxRuntime {
public:
  explicit VeloxRuntime(unsigned threads = availableCpuCount());
  folly::Executor *cpu() { return &cpu_; }
  folly::Executor *io() { return &io_; }
  unsigned threads() const { return threads_; }
  std::shared_ptr<exec::Task>
  task(std::string id, core::PlanNodePtr plan,
       const std::shared_ptr<memory::MemoryPool> &pool,
       const std::shared_ptr<memory::MemoryPool> &planPool = nullptr);

private:
  unsigned threads_;
  folly::CPUThreadPoolExecutor cpu_, io_;
};
} // namespace drill::nativeexec
