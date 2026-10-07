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
#include "exchange/SenderBuffer.h"
namespace drill::nativeexec {
std::vector<RowVectorPtr>
SenderBuffer::append(const RowVectorPtr &input,
                     const std::vector<vector_size_t> *selection) {
  std::vector<RowVectorPtr> result;
  const vector_size_t count = selection ? selection->size() : input->size();
  vector_size_t offset = 0;
  while (offset < count) {
    if (!buffer_)
      buffer_ = std::dynamic_pointer_cast<RowVector>(
          BaseVector::create(type_, batchRows_, pool_));
    auto rows = std::min(batchRows_ - size_, count - offset);
    if (selection) {
      std::vector<BaseVector::CopyRange> ranges;
      for (vector_size_t i = 0; i < rows; ++i) {
        auto source = (*selection)[offset + i];
        if (!ranges.empty() &&
            ranges.back().sourceIndex + ranges.back().count == source)
          ++ranges.back().count;
        else
          ranges.push_back({source, size_ + i, 1});
      }
      buffer_->copyRanges(input.get(),
                          folly::Range(ranges.data(), ranges.size()));
    } else {
      buffer_->copy(input.get(), size_, offset, rows);
    }
    size_ += rows;
    offset += rows;
    if (size_ == batchRows_)
      result.push_back(flush());
  }
  return result;
}
RowVectorPtr SenderBuffer::flush() {
  if (!size_)
    return nullptr;
  auto result = std::move(buffer_);
  result->resize(size_);
  size_ = 0;
  return result;
}
} // namespace drill::nativeexec
