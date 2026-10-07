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
#include <functional>
#include <optional>
#include <velox/common/future/VeloxPromise.h>
#include <velox/vector/ComplexVector.h>
namespace drill::nativeexec {
using namespace facebook::velox;
// nullopt is EOS; a present null vector means WAIT with a valid future.
class BatchSource {
public:
  virtual ~BatchSource() = default;
  virtual std::optional<RowVectorPtr> next(ContinueFuture &future) = 0;
  virtual void cancel() = 0;
};
using SourceFactory =
    std::function<std::shared_ptr<BatchSource>(memory::MemoryPool *)>;
// A null batch flushes the final partial output. A valid future waits for ACKs.
using BatchSink = std::function<ContinueFuture(RowVectorPtr)>;
// Each driver owns its routing buffers in its operator memory pool.
using SinkFactory = std::function<BatchSink(memory::MemoryPool *)>;
} // namespace drill::nativeexec
