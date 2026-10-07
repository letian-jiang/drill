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
namespace drill::nativeexec {
// One buffer per driver and destination. Copies own strings and values, so an
// upstream operator can immediately reuse its vector after addInput returns.
class SenderBuffer {
public:
  SenderBuffer(RowTypePtr type, memory::MemoryPool *pool,
               vector_size_t batchRows = 4096,
               std::shared_ptr<void> poolOwner = nullptr)
      : type_(std::move(type)), pool_(pool), poolOwner_(std::move(poolOwner)),
        batchRows_(batchRows) {
    VELOX_CHECK_NOT_NULL(pool_);
    VELOX_CHECK_GT(batchRows_, 0);
    VELOX_CHECK_LE(batchRows_, 65535);
  }
  std::vector<RowVectorPtr>
  append(const RowVectorPtr &input,
         const std::vector<vector_size_t> *selection = nullptr);
  RowVectorPtr flush();
  void discard() {
    buffer_.reset();
    size_ = 0;
  }

private:
  RowTypePtr type_;
  memory::MemoryPool *pool_;
  // Declared before buffer_: its allocator owner outlives buffered values.
  std::shared_ptr<void> poolOwner_;
  vector_size_t batchRows_, size_ = 0;
  RowVectorPtr buffer_;
};
} // namespace drill::nativeexec
