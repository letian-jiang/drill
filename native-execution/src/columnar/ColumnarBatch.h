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
#include <folly/json.h>
#include <velox/vector/ComplexVector.h>
namespace drill::nativeexec {
using namespace facebook::velox;
struct WireBatch {
  folly::dynamic header;
  std::string data;
  RowVectorPtr localVector;
  size_t sizeBytes() const {
    return localVector ? localVector->estimateFlatSize() : data.size();
  }
};
struct BufferView {
  const char *data;
  size_t size;
};
// Immutable schema and buffer shape compiled once for a plugin reader.
struct ColumnarBatchLayout;
struct ColumnarFieldLayout {
  TypePtr type;
  std::string minor;
  bool optional;
  size_t bufferCount;
  std::shared_ptr<const ColumnarBatchLayout> children;
  size_t arrayPrefix = 0;
};
struct ColumnarBatchLayout {
  RowTypePtr type;
  std::vector<ColumnarFieldLayout> fields;
  size_t bufferCount = 0;
};
ColumnarBatchLayout columnarLayout(const folly::dynamic &fields);
RowVectorPtr decodeBatchBuffers(const ColumnarBatchLayout &, int64_t rows,
                                const std::vector<BufferView> &,
                                memory::MemoryPool *);
RowTypePtr columnarType(const folly::dynamic &fields);
void validateLocalBatch(const RowVectorPtr &, const folly::dynamic &fields);
// Copy into an inbox or receiving Task pool. No buffer retains the sending Task.
RowVectorPtr copyLocalBatch(const RowVectorPtr &, const RowTypePtr &,
                            const folly::dynamic &fields, memory::MemoryPool *,
                            const std::vector<vector_size_t> *rows = nullptr);
RowVectorPtr decodeBatchBuffers(const folly::dynamic &header,
                                const std::vector<BufferView> &,
                                memory::MemoryPool *);
RowVectorPtr decodeBatch(const WireBatch &, memory::MemoryPool *);
WireBatch encodeBatch(const RowVectorPtr &, const folly::dynamic &fields,
                      const std::vector<vector_size_t> *rows = nullptr);
} // namespace drill::nativeexec
