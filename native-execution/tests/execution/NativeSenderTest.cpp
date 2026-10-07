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
#include "exchange/ReceiverInbox.h"
#include "execution/VeloxRuntime.h"
#include "velox/plan/DrillPlanNodes.h"
#include <condition_variable>
#include <iostream>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
using namespace drill::nativeexec;
namespace {
class OneBatch final : public BatchSource {
public:
  explicit OneBatch(memory::MemoryPool *pool) : pool_(pool) {}
  std::optional<RowVectorPtr> next(ContinueFuture &) override {
    if (read_)
      return std::nullopt;
    read_ = true;
    auto vector = std::dynamic_pointer_cast<RowVector>(
        BaseVector::create(ROW({"v"}, {BIGINT()}), 1, pool_));
    vector->childAt(0)->as<FlatVector<int64_t>>()->set(0, 42);
    return vector;
  }
  void cancel() override {}

private:
  memory::MemoryPool *pool_;
  bool read_ = false;
};
void buffers(memory::MemoryPool *pool) {
  auto type = ROW({"v", "s"}, {BIGINT(), VARCHAR()});
  SenderBuffer buffer(type, pool, 4);
  auto input =
      std::dynamic_pointer_cast<RowVector>(BaseVector::create(type, 2, pool));
  std::vector<RowVectorPtr> output;
  std::vector<vector_size_t> selection{1, 0};
  // Reuse and mutate source vectors: buffered strings/nulls must be owned.
  for (int b = 0; b < 5; ++b) {
    for (int r = 0; r < 2; ++r) {
      input->childAt(0)->as<FlatVector<int64_t>>()->set(r, b * 2 + r);
      auto text =
          "a long string owned by buffered output " + std::to_string(b * 2 + r);
      input->childAt(1)->as<FlatVector<StringView>>()->set(r, StringView(text));
      input->childAt(1)->setNull(r, b == 2 && r == 1);
    }
    for (auto &batch : buffer.append(input, &selection))
      output.push_back(std::move(batch));
  }
  output.push_back(buffer.flush());
  VELOX_CHECK(!buffer.flush());
  VELOX_CHECK_EQ(output.size(), 3);
  int index = 0;
  for (auto &batch : output) {
    VELOX_CHECK_EQ(batch->size(), index == 8 ? 2 : 4);
    DecodedVector values(*batch->childAt(0)), strings(*batch->childAt(1));
    for (int r = 0; r < batch->size(); ++r, ++index) {
      int expected = (index / 2) * 2 + (1 - index % 2);
      VELOX_CHECK_EQ(values.valueAt<int64_t>(r), expected);
      VELOX_CHECK_EQ(strings.isNullAt(r), expected == 5);
      if (expected != 5)
        VELOX_CHECK_EQ(strings.valueAt<StringView>(r).str(),
                       "a long string owned by buffered output " +
                           std::to_string(expected));
    }
  }
  buffer.append(input);
  buffer.discard();
  VELOX_CHECK(!buffer.flush());
}
void inboxBufferLifetime(memory::MemoryPool *pool) {
  auto type = ROW({"v", "s"}, {BIGINT(), VARCHAR()});
  auto input = BaseVector::create<RowVector>(type, 1, pool);
  input->childAt(0)->as<FlatVector<int64_t>>()->set(0, 42);
  const std::string text(8192, 'x');
  input->childAt(1)->as<FlatVector<StringView>>()->set(0, StringView(text));
  auto inbox = std::make_shared<ReceiverInbox>();
  std::weak_ptr<ReceiverInbox> owner = inbox;
  auto buffer = inbox->senderBuffer(type, 4);
  VELOX_CHECK(buffer && buffer->append(input).empty());
  inbox.reset();
  VELOX_CHECK(!owner.expired(), "Pending tail lost its independent allocator");
  buffer.reset();
  VELOX_CHECK(owner.expired());
  inbox = std::make_shared<ReceiverInbox>();
  owner = inbox;
  buffer = inbox->senderBuffer(type, 4);
  VELOX_CHECK(buffer && buffer->append(input).empty());
  inbox->cancel();
  VELOX_CHECK(!inbox->senderBuffer(type, 4));
  inbox.reset();
  VELOX_CHECK(!owner.expired());
  buffer->discard();
  buffer.reset();
  VELOX_CHECK(owner.expired());
}
void driver(VeloxRuntime &runtime,
            const std::shared_ptr<memory::MemoryPool> &root) {
  std::mutex mutex;
  std::condition_variable condition;
  bool added = false, flushed = false;
  ContinuePromise dataAck("test DATA ACK"), tailAck("test tail ACK");
  auto source = std::make_shared<DrillSourceNode>(
      "source", ROW({"v"}, {BIGINT()}), SourceKind::Receiver,
      [](memory::MemoryPool *pool) {
        return std::make_shared<OneBatch>(pool);
      });
  auto sender = std::make_shared<DrillSenderNode>(
      "sender", source, [&](memory::MemoryPool *) {
        return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
          std::lock_guard lock(mutex);
          if (batch) {
            added = true;
            condition.notify_all();
            return dataAck.getSemiFuture();
          }
          flushed = true;
          condition.notify_all();
          return tailAck.getSemiFuture();
        });
      });
  auto task = runtime.task("async-sender", sender, root);
  task->start(1);
  {
    std::unique_lock lock(mutex);
    VELOX_CHECK(condition.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return added; }));
  }
  // A second task completes on the single CPU while first waits on its ACK.
  auto siblingRoot = memory::memoryManager()->addRootPool("sibling");
  auto sibling = std::make_shared<DrillSenderNode>(
      "sibling-sender", source, [](memory::MemoryPool *) {
        return BatchSink([](RowVectorPtr) -> ContinueFuture { return {}; });
      });
  auto second = runtime.task("sibling", sibling, siblingRoot);
  second->start(1);
  VELOX_CHECK(second->taskCompletionFuture().wait(std::chrono::seconds(5)));
  VELOX_CHECK_EQ(second->state(), exec::TaskState::kFinished);
  VELOX_CHECK_EQ(task->state(), exec::TaskState::kRunning);
  dataAck.setValue(folly::Unit{});
  {
    std::unique_lock lock(mutex);
    VELOX_CHECK(condition.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return flushed; }));
  }
  // EOS/FINISHED cannot precede the final partial batch ACK.
  VELOX_CHECK_EQ(task->state(), exec::TaskState::kRunning);
  tailAck.setValue(folly::Unit{});
  std::move(task->taskCompletionFuture()).get();
  VELOX_CHECK_EQ(task->state(), exec::TaskState::kFinished);
}
void failedOrCancelledAck(VeloxRuntime &runtime, bool cancel) {
  auto root =
      memory::memoryManager()->addRootPool(cancel ? "cancel-ack" : "fail-ack");
  std::mutex mutex;
  std::condition_variable condition;
  bool added = false;
  ContinuePromise ack("test failed/cancelled ACK");
  auto source = std::make_shared<DrillSourceNode>(
      "source", ROW({"v"}, {BIGINT()}), SourceKind::Receiver,
      [](memory::MemoryPool *pool) {
        return std::make_shared<OneBatch>(pool);
      });
  auto sender = std::make_shared<DrillSenderNode>(
      "sender", source, [&](memory::MemoryPool *) {
        return BatchSink([&](RowVectorPtr batch) -> ContinueFuture {
          if (!batch)
            return {};
          std::lock_guard lock(mutex);
          auto future = ack.getSemiFuture();
          added = true;
          condition.notify_all();
          return future;
        });
      });
  auto task = runtime.task(cancel ? "cancel-ack" : "fail-ack", sender, root);
  task->start(1);
  {
    std::unique_lock lock(mutex);
    VELOX_CHECK(condition.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return added; }));
  }
  if (cancel) {
    task->requestCancel();
    VELOX_CHECK(task->taskCompletionFuture().wait(std::chrono::seconds(5)));
    VELOX_CHECK_EQ(task->state(), exec::TaskState::kCanceled);
    ack.setValue(folly::Unit{});
  } else {
    ack.setException(std::runtime_error("injected DATA ACK failure"));
    VELOX_CHECK(task->taskCompletionFuture().wait(std::chrono::seconds(5)));
    VELOX_CHECK_EQ(task->state(), exec::TaskState::kFailed);
    VELOX_CHECK_NOT_NULL(task->error());
  }
}
} // namespace
int main() {
  try {
    VeloxRuntime runtime(1);
    auto root = memory::memoryManager()->addRootPool("native-sender-test");
    auto pool = root->addLeafChild("buffers");
    buffers(pool.get());
    inboxBufferLifetime(pool.get());
    driver(runtime, root);
    failedOrCancelledAck(runtime, false);
    failedOrCancelledAck(runtime, true);
    std::cout << "Sender coalescing: selected row order, owned strings, nulls, "
                 "tail/discard, ACK yields CPU and final ACK drains before "
                 "finish passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
