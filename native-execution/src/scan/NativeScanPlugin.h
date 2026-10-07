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
#include "scan/ScanReader.h"
#include <folly/CancellationToken.h>
#include <folly/Executor.h>
#include <folly/dynamic.h>
#include <velox/core/Expressions.h>
namespace drill::nativeexec {
// Each minor owns its immutable descriptor, including assigned work/splits.
// The provider controls its payload; readFields/outputFields/filter are common.
struct NativeScanRequest {
  folly::dynamic descriptor;
};
struct NativeScanContext {
  memory::MemoryPool *planPool;
  folly::Executor *io;
  folly::CancellationToken cancellation;
};
using NormalizeScanOutput = std::function<core::TypedExprPtr(
    core::TypedExprPtr, const folly::dynamic &, memory::MemoryPool *)>;
struct NativeScanBinding {
  RowTypePtr schema;
  // Called once per driver. Factories must share a work queue when parallel
  // drivers must claim each assigned split exactly once.
  SourceFactory factory;
  // Optional provider-specific conversion to Drill's output representation.
  // Common filter/projection execute natively above the source.
  NormalizeScanOutput normalizeOutput;
};
class NativeScanPlugin {
public:
  virtual ~NativeScanPlugin() = default;
  virtual std::string_view provider() const = 0;
  virtual uint32_t descriptorVersion() const = 0;
  // Must own anything captured by the returned factory; request/context are
  // borrowed only during prepare. Reader pools belong to the driver. Async
  // operations must keep their buffers/pools alive until completion.
  virtual NativeScanBinding prepare(const NativeScanRequest &request,
                                    const NativeScanContext &context) const = 0;
};
} // namespace drill::nativeexec
