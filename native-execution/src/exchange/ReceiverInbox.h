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
#include "columnar/ColumnarBatch.h"
#include "scan/ScanReader.h"
#include <deque>
#include <mutex>
#include <set>
namespace drill::nativeexec {
class SenderBuffer;
class ReceiverInbox : public std::enable_shared_from_this<ReceiverInbox> {
public:
  void senders(std::set<uint32_t> expected);
  void push(WireBatch batch, uint32_t sender, bool last);
  void pushLocal(const RowVectorPtr &batch, const folly::dynamic &fields,
                 uint32_t sender);
  // Consume a SenderBuffer snapshot whose strings and values are owned.
  // Exclusive flat data transfers pools; shared/borrowed buffers are copied.
  bool pushOwnedLocal(RowVectorPtr batch, const folly::dynamic &fields,
                      uint32_t sender);
  // Local destinations can coalesce in the inbox's independent allocator.
  // The buffer retains the inbox owner until its pending tail is released.
  std::shared_ptr<SenderBuffer> senderBuffer(const RowTypePtr &type,
                                             vector_size_t batchRows = 4096);
  void end(uint32_t sender);
  RowTypePtr schema();
  folly::dynamic statistics();
  SourceFactory factory();
  void cancel();
  void onEarlyFinish(std::function<void()> callback);

private:
  friend class InboxSource;
  std::optional<RowVectorPtr> next(memory::MemoryPool *, ContinueFuture &);
  std::shared_ptr<memory::MemoryPool> localPool();
  bool finished() const;
  std::mutex mutex_;
  std::shared_ptr<memory::MemoryPool> localRoot_, localPool_;
  std::deque<WireBatch> queue_;
  RowTypePtr schema_;
  std::vector<ContinuePromise> waiters_;
  std::set<uint32_t> expected_, ended_;
  std::function<void()> earlyFinish_;
  uint64_t dataBatches_ = 0, dataRows_ = 0, dataWakes_ = 0, terminalWakes_ = 0,
           queuePeak_ = 0;
  bool assigned_ = false, cancelled_ = false;
};
} // namespace drill::nativeexec
