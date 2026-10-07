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
#include "exchange/ReceiverInbox.h"
#include "exchange/SenderBuffer.h"
#include <velox/vector/FlatVector.h>
namespace drill::nativeexec {
namespace {
bool exclusiveBuffer(const BufferPtr &buffer, memory::MemoryPool *pool) {
  return !buffer || (buffer->isMutable() && buffer->pool() == pool);
}
bool exclusiveTree(const VectorPtr &vector, memory::MemoryPool *pool) {
  if (!vector || vector.use_count() != 1 || vector->pool() != pool ||
      !exclusiveBuffer(vector->nulls(), pool))
    return false;
  if (vector->encoding() == VectorEncoding::Simple::ROW) {
    for (const auto &child : vector->as<RowVector>()->children())
      if (!exclusiveTree(child, pool)) return false;
    return true;
  }
  if (vector->encoding() == VectorEncoding::Simple::ARRAY) {
    auto *array = vector->as<ArrayVector>();
    return exclusiveBuffer(array->offsets(), pool) && exclusiveBuffer(array->sizes(), pool)
        && exclusiveTree(array->elements(), pool);
  }
  if (vector->encoding() == VectorEncoding::Simple::MAP) {
    auto *map = vector->as<MapVector>();
    return exclusiveBuffer(map->offsets(), pool) && exclusiveBuffer(map->sizes(), pool)
        && exclusiveTree(map->mapKeys(), pool) && exclusiveTree(map->mapValues(), pool);
  }
  if (vector->encoding() != VectorEncoding::Simple::FLAT ||
      !exclusiveBuffer(vector->values(), pool)) return false;
  if (vector->type()->isVarchar() || vector->type()->isVarbinary())
    for (const auto &buffer : vector->as<FlatVector<StringView>>()->stringBuffers())
      if (!exclusiveBuffer(buffer, pool))
        return false;
  return true;
}
bool exclusiveFlatBatch(const RowVectorPtr &batch) {
  if (batch.use_count() != 1 || !exclusiveBuffer(batch->nulls(), batch->pool())) return false;
  for (const auto &child : batch->children())
    if (!exclusiveTree(child, batch->pool())) return false;
  return true;
}
void resizeOwnedTree(BaseVector *value, const TypePtr &type, vector_size_t count) {
  value->setType(type);
  value->resize(count);
  if (type->isRow()) {
    auto *row = value->as<RowVector>();
    for (size_t i = 0; i < row->childrenSize(); ++i)
      resizeOwnedTree(row->childAt(i).get(), type->childAt(i), count);
  } else if (type->isMap()) {
    auto *map = value->as<MapVector>();
    resizeOwnedTree(map->mapKeys().get(), type->childAt(0), map->mapKeys()->size());
    resizeOwnedTree(map->mapValues().get(), type->childAt(1), map->mapValues()->size());
    for (vector_size_t row = 0; row < count; ++row)
      if (map->isNullAt(row)) { map->setOffsetAndSize(row, 0, 0); map->setNull(row, false); }
  } else if (type->isArray()) {
    auto *array = value->as<ArrayVector>();
    // Element counts are independent of the visible outer row count.
    resizeOwnedTree(array->elements().get(), type->childAt(0), array->elements()->size());
  }
}
} // namespace
class InboxSource final : public BatchSource {
public:
  InboxSource(std::shared_ptr<ReceiverInbox> inbox, memory::MemoryPool *pool)
      : inbox_(std::move(inbox)), pool_(pool->shared_from_this()) {}
  std::optional<RowVectorPtr> next(ContinueFuture &future) override {
    return inbox_->next(pool_.get(), future);
  }
  void cancel() override { inbox_->cancel(); }

private:
  std::shared_ptr<ReceiverInbox> inbox_;
  std::shared_ptr<memory::MemoryPool> pool_;
};
void ReceiverInbox::senders(std::set<uint32_t> expected) {
  std::lock_guard lock(mutex_);
  expected_ = std::move(expected);
  assigned_ = true;
}
bool ReceiverInbox::finished() const {
  return cancelled_ ||
         (assigned_ && std::includes(ended_.begin(), ended_.end(),
                                     expected_.begin(), expected_.end()));
}
RowTypePtr ReceiverInbox::schema() {
  std::lock_guard lock(mutex_);
  return schema_;
}
folly::dynamic ReceiverInbox::statistics() {
  std::lock_guard lock(mutex_);
  return folly::dynamic::object("data_batches", dataBatches_)(
      "data_rows", dataRows_)("data_wakes", dataWakes_)(
      "terminal_wakes", terminalWakes_)("queue_peak", queuePeak_);
}
void ReceiverInbox::push(WireBatch batch, uint32_t sender, bool last) {
  std::vector<ContinuePromise> wake;
  {
    std::lock_guard lock(mutex_);
    if (cancelled_)
      return;
    auto type = columnarType(batch.header["fields"]);
    if (!schema_)
      schema_ = type;
    else
      VELOX_USER_CHECK(schema_->equivalent(*type),
                       "Drill receiver schema changed during a Task");
    auto rows = batch.header["rows"].asInt();
    if (rows > 0) {
      queue_.push_back(std::move(batch));
      ++dataBatches_;
      dataRows_ += rows;
      queuePeak_ = std::max(queuePeak_, uint64_t(queue_.size()));
    }
    if (last) {
      ended_.insert(sender);
      terminalWakes_ += waiters_.size();
      wake.swap(waiters_);
    } else if (rows > 0 && !waiters_.empty()) {
      // One batch makes one blocked driver runnable. Wake all only for EOS
      // or cancellation, so idle drivers do not contend for the same input.
      wake.push_back(std::move(waiters_.front()));
      waiters_.erase(waiters_.begin());
      ++dataWakes_;
    }
  }
  for (auto &promise : wake)
    promise.setValue();
}
void ReceiverInbox::pushLocal(const RowVectorPtr &batch,
                              const folly::dynamic &fields, uint32_t sender) {
  auto pool = localPool();
  if (!pool)
    return;
  auto owned = copyLocalBatch(batch, columnarType(fields), fields, pool.get());
  push(
      WireBatch{folly::dynamic::object("rows", batch->size())("fields", fields),
                {},
                std::move(owned)},
      sender, false);
}
std::shared_ptr<memory::MemoryPool> ReceiverInbox::localPool() {
  {
    std::lock_guard lock(mutex_);
    if (cancelled_)
      return nullptr;
    if (!localPool_) {
      // Inbox ownership permits data to arrive before Task initialization.
      // Owned sender snapshots transfer here; borrowed/shared data is copied.
      // Dequeue subsequently transfers to the receiving Task.
      localRoot_ = memory::memoryManager()->addRootPool();
      localPool_ = localRoot_->addLeafChild("local-exchange");
    }
    return localPool_;
  }
}
std::shared_ptr<SenderBuffer>
ReceiverInbox::senderBuffer(const RowTypePtr &type, vector_size_t batchRows) {
  auto pool = localPool();
  if (!pool)
    return nullptr;
  return std::make_shared<SenderBuffer>(type, pool.get(), batchRows,
                                        shared_from_this());
}
bool ReceiverInbox::pushOwnedLocal(RowVectorPtr batch,
                                   const folly::dynamic &fields,
                                   uint32_t sender) {
  if (!exclusiveFlatBatch(batch)) {
    pushLocal(batch, fields, sender);
    return false;
  }
  auto pool = localPool();
  if (!pool)
    return false;
  auto type = columnarType(fields);
  VELOX_CHECK(batch->type()->equivalent(*type), "Local exchange type mismatch");
  VELOX_CHECK_LE(batch->size(), 65535);
  // SenderBuffer truncates the row vector for its tail, leaving children at
  // the allocation capacity. Preserve the old copy route's visible shape;
  // trimming exclusive children retains their existing backing buffers.
  resizeOwnedTree(batch.get(), type, batch->size());
  validateLocalBatch(batch, fields);
  if (batch->pool() != pool.get())
    batch->transferOrCopyTo(pool.get());
  const auto rows = batch->size();
  push(WireBatch{folly::dynamic::object("rows", rows)("fields", fields),
                 {},
                 std::move(batch)},
       sender, false);
  return true;
}
void ReceiverInbox::end(uint32_t sender) {
  std::vector<ContinuePromise> wake;
  {
    std::lock_guard lock(mutex_);
    ended_.insert(sender);
    terminalWakes_ += waiters_.size();
    wake.swap(waiters_);
  }
  for (auto &promise : wake)
    promise.setValue();
}
std::optional<RowVectorPtr> ReceiverInbox::next(memory::MemoryPool *pool,
                                                ContinueFuture &future) {
  WireBatch batch;
  {
    std::lock_guard lock(mutex_);
    if (cancelled_)
      return std::nullopt;
    if (!queue_.empty()) {
      batch = std::move(queue_.front());
      queue_.pop_front();
    } else {
      if (finished())
        return std::nullopt;
      ContinuePromise promise;
      future = promise.getSemiFuture();
      waiters_.push_back(std::move(promise));
      return RowVectorPtr{};
    }
  }
  if (batch.localVector) {
    // Local enqueue creates an exclusive flat snapshot owned by the inbox.
    // Transfer its buffers/accounting instead of copying the rows again.
    // Velox copies only buffers from a different allocator or BufferViews.
    // All descendants then belong to the receiving operator's pool, so the
    // inbox and sending Task may be destroyed while downstream retains data.
    batch.localVector->transferOrCopyTo(pool);
    return std::move(batch.localVector);
  }
  return decodeBatch(batch, pool);
}
SourceFactory ReceiverInbox::factory() {
  return [self = shared_from_this()](memory::MemoryPool *pool) {
    return std::make_shared<InboxSource>(self, pool);
  };
}
void ReceiverInbox::onEarlyFinish(std::function<void()> callback) {
  std::lock_guard lock(mutex_);
  earlyFinish_ = std::move(callback);
}
void ReceiverInbox::cancel() {
  std::vector<ContinuePromise> wake;
  std::function<void()> callback;
  {
    std::lock_guard lock(mutex_);
    if (cancelled_)
      return;
    if (!finished())
      callback = earlyFinish_;
    cancelled_ = true;
    queue_.clear();
    terminalWakes_ += waiters_.size();
    wake.swap(waiters_);
  }
  if (callback)
    callback();
  for (auto &promise : wake)
    promise.setValue();
}
} // namespace drill::nativeexec
