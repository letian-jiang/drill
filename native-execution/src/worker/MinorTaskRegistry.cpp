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
#include "worker/MinorTaskRegistry.h"
#include "columnar/DrillBatchMetadata.h"
#include "exchange/SenderBuffer.h"
#include "plan/FragmentPlanConverter.h"
#include "scan/iceberg/IcebergArrowBatch.h"
#include "scan/NativeScanRegistry.h"
#include "scan/jni/JniPluginScan.h"
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <folly/base64.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/json.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <velox/vector/DecodedVector.h>
namespace drill::nativeexec {
using protocol::Message;
using protocol::RpcMessage;
using protocol::RpcMode;
using protocol::Writer;
namespace {
std::string queryKey(std::string_view bytes) {
  Message query(bytes);
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(16) << query.integer(1)
      << std::setw(16) << query.integer(2);
  return out.str();
}
std::string taskKey(std::string_view bytes) {
  Message handle(bytes);
  return queryKey(handle.bytes(1)) + "." + std::to_string(handle.integer(2)) +
         "." + std::to_string(handle.integer(3));
}
uint64_t now() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}
uint64_t elapsedNanos(std::chrono::steady_clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::steady_clock::now() - start)
      .count();
}
struct SenderAcknowledgements {
  explicit SenderAcknowledgements(size_t count)
      : remaining(count), promise("Drill Sender ACKs") {}
  void finish(std::exception_ptr failure) {
    std::exception_ptr result;
    {
      std::lock_guard lock(mutex);
      if (failure && !error)
        error = failure;
      if (--remaining)
        return;
      result = error;
    }
    if (result)
      promise.setException(folly::exception_wrapper(result));
    else
      promise.setValue(folly::Unit{});
  }
  std::mutex mutex;
  size_t remaining;
  std::exception_ptr error;
  ContinuePromise promise;
};
void visit(const folly::dynamic &node,
           const std::function<void(const folly::dynamic &)> &function) {
  if (!node.isObject() || !node.count("pop"))
    return;
  function(node);
  for (auto name : {"child", "left", "right"})
    if (node.count(name))
      visit(node[name], function);
  if (node.count("children"))
    for (auto &child : node["children"])
      visit(child, function);
}
} // namespace
struct MinorTaskRegistry::Destination {
  std::string address;
  uint16_t port;
  uint32_t major, minor;
};
struct MinorTaskRegistry::Minor {
  struct SenderMetrics {
    std::atomic<uint64_t> vectors{0}, rows{0}, batches{0}, wireRows{0},
        bytes{0};
    std::atomic<uint64_t> partitionNs{0}, encodeNs{0}, requestNs{0};
    std::atomic<uint64_t> lockNs{0}, queueNs{0}, sendNs{0}, responseNs{0};
    std::atomic<uint64_t> smallBatches{0};
    std::atomic<uint64_t> localBatches{0}, localRows{0}, localCopyNs{0};
    std::atomic<uint64_t> localAdoptedBatches{0}, localAdoptedRows{0};
  } sender;
  std::mutex mutex;
  std::condition_variable condition;
  std::string id, handle, query, foreman, assignment, scanContext;
  folly::dynamic plan;
  std::shared_ptr<const protocol::FragmentRoutes> routes;
  std::unordered_map<uint32_t, std::shared_ptr<ReceiverInbox>> sources;
  std::vector<Destination> destinations;
  std::unordered_map<std::string, std::function<folly::dynamic()>> jniScanStats;
  folly::dynamic outputFields;
  std::shared_ptr<memory::MemoryPool> pool;
  std::shared_ptr<exec::Task> task;
  folly::CancellationSource scanCancellation;
  uint64_t started = now();
  std::atomic<uint64_t> taskStarted{0};
  std::set<std::pair<uint32_t, uint32_t>> expectedReceivers, closedReceivers;
  bool scheduled = false, cancelled = false, terminal = false,
       earlyComplete = false;
};
MinorTaskRegistry::MinorTaskRegistry(VeloxRuntime &runtime,
                                     std::string endpoint,
                                     EngineCallbacks callbacks)
    : runtime_(runtime), endpoint_(std::move(endpoint)),
      callbacks_(std::move(callbacks)) {}
MinorTaskRegistry::~MinorTaskRegistry() { stop(); }
void MinorTaskRegistry::setEndpoint(std::string endpoint) {
  std::lock_guard lock(mutex_);
  VELOX_USER_CHECK(tasks_.empty() && clients_.empty() && inboxes_.empty(),
                   "Cannot change an active engine endpoint");
  endpoint_ = std::move(endpoint);
}
void MinorTaskRegistry::quiesce() {
  std::lock_guard lock(mutex_);
  admitting_ = false;
}
void MinorTaskRegistry::fail(std::string error) {
  std::vector<std::string> handles;
  {
    std::lock_guard lock(mutex_);
    if (stopped_)
      return;
    admitting_ = false;
    if (failure_.empty())
      failure_ = std::move(error);
    for (auto &[id, minor] : tasks_)
      handles.push_back(minor->handle);
  }
  for (auto &handle : handles)
    control({RpcMode::Request, 0, 6, std::move(handle), {}});
}
bool MinorTaskRegistry::awaitIdle(uint64_t timeoutMillis) {
  std::vector<std::shared_ptr<Minor>> tasks;
  {
    std::lock_guard lock(mutex_);
    VELOX_USER_CHECK(!admitting_, "Quiesce before waiting for native tasks");
    for (auto &[id, minor] : tasks_)
      tasks.push_back(minor);
  }
  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(timeoutMillis);
  for (auto &minor : tasks) {
    std::unique_lock lock(minor->mutex);
    if (timeoutMillis) {
      if (!minor->condition.wait_until(lock, deadline,
                                       [&] { return minor->terminal; }))
        return false;
    } else
      minor->condition.wait(lock, [&] { return minor->terminal; });
  }
  std::unique_lock lock(mutex_);
  if (timeoutMillis)
    return notificationsFinished_.wait_until(
        lock, deadline, [&] { return pendingNotifications_ == 0; });
  notificationsFinished_.wait(lock, [&] { return pendingNotifications_ == 0; });
  return true;
}
bool MinorTaskRegistry::localData(const Destination &destination) const {
  Message endpoint(endpoint_);
  Message capability(endpoint.bytes(9));
  if (capability.integer(3) == 2)
    return destination.address == capability.bytes(1) &&
           destination.port == capability.integer(6);
  return destination.address == endpoint.bytes(1) &&
         destination.port == endpoint.integer(4);
}
RpcMessage MinorTaskRegistry::sendControl(std::string_view bytes, uint32_t type,
                                          std::string body) {
  Message endpoint(bytes), local(endpoint_);
  if (endpoint.bytes(1) == local.bytes(1) &&
      endpoint.integer(3) ==
          (type != 8 && Message(local.bytes(9)).integer(3) == 2
               ? Message(local.bytes(9)).integer(5)
               : local.integer(3))) {
    if (type == 8 && callbacks_.status) {
      callbacks_.status(std::move(body));
      return {RpcMode::Response, 0, 1, Writer().integer(1, 1).take(), {}};
    }
    if (type != 8)
      return control({RpcMode::Request, 0, int32_t(type), std::move(body), {}});
  }
  return client(std::string(endpoint.bytes(1)), endpoint.integer(3), true)
      ->request(type, std::move(body));
}
ContinueFuture MinorTaskRegistry::sendData(const Destination &destination,
                                           std::string body, std::string raw) {
  if (localData(destination)) {
    if (destination.major != 0) {
      data({RpcMode::Request, 0, 3, std::move(body), std::move(raw)});
      return {};
    }
    if (callbacks_.rootBatch)
      return callbacks_.rootBatch(std::move(body), std::move(raw));
  }
  auto ack = std::make_shared<SenderAcknowledgements>(1);
  auto future = ack->promise.getSemiFuture();
  client(destination.address, destination.port, false)
      ->requestAsync(3, std::move(body), std::move(raw),
                     [ack](auto error, auto, auto) { ack->finish(error); });
  return future;
}
void MinorTaskRegistry::acceptLocal(const Destination &destination,
                                    const std::shared_ptr<Minor> &minor,
                                    RowVectorPtr vector) {
  Message sender(minor->handle);
  auto id = queryKey(minor->query) + "." + std::to_string(destination.major) +
            "." + std::to_string(destination.minor);
  auto source = inbox(id, sender.integer(2));
  auto start = std::chrono::steady_clock::now();
  auto rows = vector->size();
  if (source->pushOwnedLocal(std::move(vector), minor->outputFields,
                             sender.integer(3))) {
    ++minor->sender.localAdoptedBatches;
    minor->sender.localAdoptedRows += rows;
  }
  ++minor->sender.localBatches;
  minor->sender.localRows += rows;
  minor->sender.localCopyNs += elapsedNanos(start);
  std::shared_ptr<Minor> receiver;
  {
    std::lock_guard lock(mutex_);
    auto it = tasks_.find(id);
    if (it != tasks_.end())
      receiver = it->second;
  }
  if (receiver)
    prepare(receiver);
}
std::shared_ptr<ReceiverInbox> MinorTaskRegistry::inbox(const std::string &task,
                                                        uint32_t major) {
  std::lock_guard lock(mutex_);
  auto &slot = inboxes_[task + "/" + std::to_string(major)];
  if (!slot)
    slot = std::make_shared<ReceiverInbox>();
  return slot;
}
std::shared_ptr<protocol::RpcClient>
MinorTaskRegistry::client(std::string address, uint16_t port, bool control) {
  auto key = address + ":" + std::to_string(port) + (control ? "c" : "d");
  {
    std::lock_guard lock(mutex_);
    VELOX_USER_CHECK(!stopped_, "Native worker is stopping");
    auto it = clients_.find(key);
    if (it != clients_.end())
      return it->second;
  }
  Writer handshake;
  handshake.integer(1, control ? 3 : 4).integer(2, control ? 0 : 1);
  if (control)
    handshake.bytes(
        3, Message(Message(endpoint_).bytes(9)).integer(3) == 2
               ? endpoint_ +
                     Writer()
                         .integer(
                             3, Message(Message(endpoint_).bytes(9)).integer(5))
                         .integer(
                             4, Message(Message(endpoint_).bytes(9)).integer(6))
                         .take()
               : endpoint_);
  auto value = std::make_shared<protocol::RpcClient>(
      address, port, RpcMessage{RpcMode::Request, 0, 0, handshake.take(), {}},
      control ? protocol::RpcClient::Handler([this](const auto &request) {
        return this->control(request);
      })
              : protocol::RpcClient::Handler{});
  std::lock_guard lock(mutex_);
  VELOX_USER_CHECK(!stopped_, "Native worker is stopping");
  auto [it, inserted] = clients_.emplace(key, value);
  return it->second;
}
void MinorTaskRegistry::status(const std::shared_ptr<Minor> &minor,
                               uint32_t state, std::string error) {
  {
    std::lock_guard lock(mutex_);
    if (stopped_)
      return;
    if (!failure_.empty() && state >= 3) {
      state = 5;
      error = failure_;
    }
  }
  Writer profile;
  Message handle(minor->handle);
  profile.integer(1, state)
      .integer(3, handle.integer(3))
      .integer(5, minor->started)
      .bytes(9, minor->assignment)
      .integer(10, now())
      .integer(11, now());
  if (state >= 3)
    profile.integer(6, now());
  if (!error.empty())
    profile.bytes(2, Writer().integer(3, 11).bytes(4, error).take());
  if (minor->task && state >= 3) {
    auto stats = minor->task->taskStats();
    struct OperatorProfile {
      std::string name;
      uint64_t cpu = 0, blocked = 0;
      std::vector<std::pair<uint64_t, uint64_t>> streams;
    };
    std::map<uint32_t, OperatorProfile> operators;
    for (auto &pipeline : stats.pipelineStats)
      for (auto &op : pipeline.operatorStats) {
        if (!op.planNodeId.starts_with("drill_"))
          continue;
        auto id = op.planNodeId.substr(6);
        if (id.empty() ||
            id.find_first_not_of("0123456789") != std::string::npos)
          continue;
        auto &entry = operators[std::stoul(id)];
        auto type = "VELOX_" + op.operatorType;
        if (entry.name.empty())
          entry.name = type;
        else if (entry.name != type)
          entry.name = "VELOX_HashJoin";
        entry.cpu += op.addInputTiming.cpuNanos + op.getOutputTiming.cpuNanos +
                     op.finishTiming.cpuNanos;
        entry.blocked += op.blockedWallNanos;
        if (op.operatorType == "HashBuild") {
          if (entry.streams.size() < 2)
            entry.streams.resize(2);
          entry.streams[1] = {op.inputPositions, op.inputVectors};
        } else if (op.operatorType == "HashProbe") {
          if (entry.streams.empty())
            entry.streams.resize(1);
          entry.streams[0] = {op.inputPositions, op.inputVectors};
        } else
          entry.streams.emplace_back(op.inputPositions, op.inputVectors);
      }
    for (auto &[id, entry] : operators) {
      Writer operatorProfile;
      operatorProfile.integer(3, id)
          .bytes(10, entry.name)
          .integer(5, 0)
          .integer(6, entry.cpu)
          .integer(9, entry.blocked);
      for (auto &[positions, vectors] : entry.streams)
        operatorProfile.bytes(
            1, Writer().integer(1, positions).integer(2, vectors).take());
      profile.bytes(4, operatorProfile.take());
    }
    if (const char *directory = std::getenv("DRILL_NATIVE_STATS_DIR")) {
      folly::dynamic report = folly::dynamic::object("task", minor->id)(
          "total_drivers", stats.numTotalDrivers)("completed_drivers",
                                                  stats.numCompletedDrivers)(
          "terminated_drivers", stats.numTerminatedDrivers)(
          "running_drivers", stats.numRunningDrivers)("queued_drivers",
                                                      stats.numQueuedDrivers)(
          "native_task_created", true)("status_state", state)(
          "task_state", exec::taskStateString(minor->task->state()))(
          "threads", runtime_.threads())("submitted_ms", minor->started)(
          "task_start_ms", minor->taskStarted.load())("finished_ms", now())(
          "pipelines", folly::dynamic::array);
      {
        std::lock_guard lock(minor->mutex);
        report["early_complete"] = minor->earlyComplete;
        report["cancelled"] = minor->cancelled;
        report["jni_scans"] = folly::dynamic::object;
        for (const auto &[node, snapshot] : minor->jniScanStats)
          report["jni_scans"][node] = snapshot();
      }
      if (callbacks_.diagnostics)
        report["host"] = callbacks_.diagnostics();
      report["execution_transport"] = minor->routes ? "native-rpc" : "legacy";
      auto &sender = minor->sender;
      report["sender"] =
          folly::dynamic::object("input_vectors", sender.vectors.load())(
              "input_rows", sender.rows.load())(
              "wire_batches", sender.batches.load())("wire_rows",
                                                     sender.wireRows.load())(
              "wire_bytes", sender.bytes.load())("batches_le_64_rows",
                                                 sender.smallBatches.load())(
              "partition_ns",
              sender.partitionNs.load())("encode_ns", sender.encodeNs.load())(
              "request_ns", sender.requestNs.load())("connection_lock_wait_ns",
                                                     sender.lockNs.load())(
              "writer_queue_wait_ns", sender.queueNs.load())(
              "send_ns", sender.sendNs.load())("response_wait_ns",
                                               sender.responseNs.load())(
              "local_batches", sender.localBatches.load())(
              "local_rows", sender.localRows.load())("local_copy_ns",
                                                     sender.localCopyNs.load())(
              "local_enqueue_ns", sender.localCopyNs.load())(
              "local_adopted_batches", sender.localAdoptedBatches.load())(
              "local_adopted_rows", sender.localAdoptedRows.load());
      report["receivers"] = folly::dynamic::array;
      for (auto &[major, source] : minor->sources) {
        auto receiver = source->statistics();
        receiver["sender_major"] = major;
        report["receivers"].push_back(std::move(receiver));
      }
      for (auto &pipeline : stats.pipelineStats) {
        folly::dynamic entry = folly::dynamic::object(
            "input", pipeline.inputPipeline)("output", pipeline.outputPipeline)(
            "operators", folly::dynamic::array);
        for (auto &op : pipeline.operatorStats)
          entry["operators"].push_back(folly::dynamic::object(
              "node", op.planNodeId)("operator", op.operatorType)(
              "drivers", op.numDrivers)("input_rows", op.inputPositions)(
              "output_rows", op.outputPositions)("input_vectors",
                                                 op.inputVectors)(
              "output_vectors",
              op.outputVectors)("cpu_ns", op.addInputTiming.cpuNanos +
                                              op.getOutputTiming.cpuNanos +
                                              op.finishTiming.cpuNanos)(
              "wall_ns", op.addInputTiming.wallNanos +
                             op.getOutputTiming.wallNanos +
                             op.finishTiming.wallNanos)("blocked_ns",
                                                        op.blockedWallNanos));
        report["pipelines"].push_back(entry);
      }
      std::filesystem::create_directories(directory);
      std::ofstream(std::filesystem::path(directory) / (minor->id + ".json"))
          << folly::toPrettyJson(report);
    }
  }
  if (!minor->task && state >= 3) {
    if (const char *directory = std::getenv("DRILL_NATIVE_STATS_DIR")) {
      folly::dynamic report = folly::dynamic::object("task", minor->id)(
          "native_task_created", false)("status_state", state)(
          "task_state", "NotStarted")("total_drivers", 0)("completed_drivers",
                                                          0)(
          "threads", runtime_.threads())("pipelines", folly::dynamic::array);
      {
        std::lock_guard lock(minor->mutex);
        report["early_complete"] = minor->earlyComplete;
        report["cancelled"] = minor->cancelled;
      }
      std::filesystem::create_directories(directory);
      std::ofstream(std::filesystem::path(directory) / (minor->id + ".json"))
          << folly::toPrettyJson(report);
    }
  }
  if (minor->foreman.empty())
    return;
  sendControl(minor->foreman, 8,
              Writer().bytes(1, profile.data()).bytes(2, minor->handle).take());
}
void MinorTaskRegistry::initialize(
    std::string_view bytes,
    std::shared_ptr<const protocol::FragmentRoutes> routes) {
  Message fragment(bytes);
  auto minor = std::make_shared<Minor>();
  minor->routes = std::move(routes);
  minor->handle = fragment.bytes(1);
  minor->id = taskKey(minor->handle);
  Message handle(minor->handle);
  VELOX_USER_CHECK_NE(handle.integer(2), 0,
                      "Root fragments execute on the Java Foreman");
  minor->query = handle.bytes(1);
  minor->foreman = fragment.bytes(11);
  minor->assignment = fragment.bytes(10);
  if (minor->assignment.empty())
    minor->assignment = endpoint_;
  Message assigned(minor->assignment), local(endpoint_);
  VELOX_USER_CHECK(assigned.bytes(1) == local.bytes(1) &&
                       assigned.integer(3) == local.integer(3) &&
                       assigned.integer(4) == local.integer(4),
                   "Minor fragment is assigned to another Drillbit");
  if (minor->routes) {
    const auto &route = minor->routes->get(minor->handle);
    VELOX_USER_CHECK(route.native && !minor->routes->root(minor->handle),
                     "Root fragments execute on Java");
    VELOX_USER_CHECK(route.assignment == minor->assignment,
                     "Route assignment differs from original plan");
  }
  minor->plan = folly::parseJson(fragment.bytes(8));
  // Only reader resources cross into Java, never the native computation tree.
  minor->scanContext = folly::base64Encode(Writer()
                                               .bytes(1, minor->handle)
                                               .bytes(10, minor->assignment)
                                               .bytes(11, minor->foreman)
                                               .bytes(14, fragment.bytes(14))
                                               .bytes(15, fragment.bytes(15))
                                               .bytes(16, fragment.bytes(16))
                                               .take());
  if (const char *directory = std::getenv("DRILL_NATIVE_PLAN_DIR")) {
    std::filesystem::create_directories(directory);
    std::ofstream(std::filesystem::path(directory) / (minor->id + ".json"))
        << folly::toPrettyJson(minor->plan);
  }
  auto receiverMajor = uint32_t(minor->plan["receiver-major-fragment"].asInt());
  if (minor->plan["pop"] == "single-sender")
    minor->expectedReceivers.emplace(
        receiverMajor, minor->plan["receiver-minor-fragment"].asInt());
  else
    for (auto &receiver : minor->plan["destinations"])
      minor->expectedReceivers.emplace(receiverMajor,
                                       receiver["minorFragmentId"].asInt());
  visit(minor->plan, [&](const folly::dynamic &node) {
    auto pop = node["pop"].asString();
    if (pop != "unordered-receiver" && pop != "merging-receiver")
      return;
    auto major = node["sender-major-fragment"].asInt();
    auto source = inbox(minor->id, major);
    std::set<uint32_t> senders;
    for (auto &sender : node["senders"])
      senders.insert(sender["minorFragmentId"].asInt());
    source->senders(std::move(senders));
    auto targets = node["senders"];
    auto query = minor->query;
    auto receiver = minor->handle;
    source->onEarlyFinish(
        [this, targets, query, receiver, major, routes = minor->routes] {
          receiverFinished(targets, query, receiver, major, routes);
        });
    minor->sources.emplace(major, source);
  });
  {
    std::lock_guard lock(mutex_);
    VELOX_USER_CHECK(!stopped_, "Native worker is stopping");
    VELOX_USER_CHECK(admitting_, "Native service is quiescent");
    minor->cancelled = cancelledBeforeSubmit_.erase(minor->id) != 0;
    VELOX_USER_CHECK(tasks_.emplace(minor->id, minor).second,
                     "Duplicate minor task {}", minor->id);
  }
  prepare(minor);
}
void MinorTaskRegistry::receiverFinished(
    folly::dynamic targets, std::string query, std::string receiver,
    uint32_t major, std::shared_ptr<const protocol::FragmentRoutes> routes) {
  {
    std::lock_guard lock(mutex_);
    if (stopped_)
      return;
    ++pendingNotifications_;
  }
  try {
    runtime_.io()->add(
        [this, targets = std::move(targets), query = std::move(query),
         receiver = std::move(receiver), major, routes = std::move(routes)] {
          for (auto &target : targets) {
            try {
              auto bytes = folly::base64Decode(target["endpoint"].asString());
              auto sender = Writer()
                                .bytes(1, query)
                                .integer(2, major)
                                .integer(3, target["minorFragmentId"].asInt())
                                .take();
              if (routes) {
                const auto &route = routes->get(sender);
                bytes = Writer()
                            .bytes(1, route.address)
                            .integer(3, route.control)
                            .take();
              }
              sendControl(bytes, 7,
                          Writer().bytes(1, receiver).bytes(2, sender).take());
            } catch (const std::exception &) {
              // The query may have been cancelled and its sender already
              // closed.
            }
          }
          std::lock_guard lock(mutex_);
          --pendingNotifications_;
          notificationsFinished_.notify_all();
        });
  } catch (...) {
    std::lock_guard lock(mutex_);
    --pendingNotifications_;
    notificationsFinished_.notify_all();
    throw;
  }
}
void MinorTaskRegistry::prepare(const std::shared_ptr<Minor> &minor) {
  std::lock_guard lock(minor->mutex);
  if (minor->scheduled || minor->terminal)
    return;
  if (!minor->cancelled && !minor->earlyComplete)
    for (auto &[major, source] : minor->sources)
      if (!source->schema())
        return;
  minor->scheduled = true;
  runtime_.io()->add([this, minor] { run(minor); });
}
void MinorTaskRegistry::run(const std::shared_ptr<Minor> &minor) {
  try {
    bool cancelled, earlyComplete;
    {
      std::lock_guard lock(minor->mutex);
      cancelled = minor->cancelled;
      earlyComplete = minor->earlyComplete;
    }
    if (cancelled || earlyComplete) {
      for (auto &[major, source] : minor->sources)
        source->cancel();
      status(minor, cancelled ? 4 : 3);
      std::lock_guard lock(minor->mutex);
      minor->terminal = true;
      minor->condition.notify_all();
      return;
    }
    auto ownedPool = memory::memoryManager()->addRootPool(minor->id);
    minor->pool = ownedPool;
    auto leaf = minor->pool->addLeafChild("plan");
    FragmentPlanConverter converter(
        leaf.get(),
        [&](const folly::dynamic &node) {
          auto pop = node["pop"].asString();
          if (pop == "unordered-receiver" || pop == "merging-receiver") {
            auto source =
                minor->sources.at(node["sender-major-fragment"].asInt());
            return SourceBinding{source->schema(), SourceKind::Receiver,
                                 source->factory()};
          }
          if (node.count("jniScan")) {
            auto descriptor = node["jniScan"];
            descriptor["fragmentContext"] = minor->scanContext;
            auto binding =
                openJniPluginScan(descriptor, leaf.get(), runtime_.io(),
                                  minor->scanCancellation.getToken());
            {
              std::lock_guard lock(minor->mutex);
              minor->jniScanStats["drill_" +
                                  std::to_string(node["@id"].asInt())] =
                  std::move(binding.statistics);
            }
            return SourceBinding{binding.schema, SourceKind::JniScan,
                                 std::move(binding.factory)};
          }
          VELOX_USER_CHECK(node.count("nativeScan"),
                           "Plugin {} has no native scan descriptor; JNI "
                           "ScanHost is required",
                           pop);
          auto &scan = node["nativeScan"];
          auto binding = NativeScanRegistry::instance().bind(scan,
              {leaf.get(), runtime_.io(), minor->scanCancellation.getToken()});
          return SourceBinding{binding.schema, SourceKind::NativeScan,
                               std::move(binding.factory),
                               std::move(binding.normalizeOutput)};
        },
        [&](const folly::dynamic &node, const RowTypePtr &schema) {
          minor->outputFields = fieldsFromType(schema);
          auto major = node["receiver-major-fragment"].asInt();
          auto pop = node["pop"].asString();
          VELOX_USER_CHECK(pop == "single-sender" ||
                               pop == "broadcast-sender" ||
                               pop == "hash-partition-sender",
                           "Unsupported Drill sender routing: {}", pop);
          if (pop == "single-sender") {
            auto &endpoint = node["destination"];
            std::string address;
            uint16_t port;
            if (endpoint.isString()) {
              auto bytes = folly::base64Decode(endpoint.asString());
              Message parsed(bytes);
              address = parsed.bytes(1);
              port = parsed.integer(4);
            } else {
              address = endpoint["address"].asString();
              port = endpoint["dataPort"].asInt();
            }
            minor->destinations.push_back(
                {address, port, uint32_t(major),
                 uint32_t(node["receiver-minor-fragment"].asInt())});
          } else
            for (auto &destination : node["destinations"]) {
              auto &endpoint = destination["endpoint"];
              std::string address;
              uint16_t port;
              if (endpoint.isString()) {
                auto bytes = folly::base64Decode(endpoint.asString());
                Message parsed(bytes);
                address = parsed.bytes(1);
                port = parsed.integer(4);
              } else {
                address = endpoint["address"].asString();
                port = endpoint["dataPort"].asInt();
              }
              minor->destinations.push_back(
                  {address, port, uint32_t(major),
                   uint32_t(destination["minorFragmentId"].asInt())});
            }
          if (minor->routes)
            for (auto &destination : minor->destinations) {
              const auto &route =
                  minor->routes->get(destination.major, destination.minor);
              destination.address = route.address;
              destination.port = route.data;
            }
          bool partitioned = pop == "hash-partition-sender";
          return SinkFactory([this, minor, schema,
                              partitioned](memory::MemoryPool *pool) {
            std::vector<std::shared_ptr<SenderBuffer>> buffers;
            auto count = partitioned ? minor->destinations.size() : 1;
            for (size_t i = 0; i < count; ++i) {
              std::shared_ptr<SenderBuffer> buffer;
              // A hash destination or single-recipient sender owns its local
              // snapshot independently of the producer Task. Multi-recipient
              // broadcast keeps the shared producer snapshot and copy guards.
              if (partitioned || minor->destinations.size() == 1) {
                const auto &destination = minor->destinations[i];
                if (localData(destination) && destination.major != 0) {
                  auto id = queryKey(minor->query) + "." +
                            std::to_string(destination.major) + "." +
                            std::to_string(destination.minor);
                  Message handle(minor->handle);
                  buffer = inbox(id, handle.integer(2))->senderBuffer(schema);
                }
              }
              if (!buffer)
                buffer = std::make_shared<SenderBuffer>(schema, pool);
              buffers.push_back(std::move(buffer));
            }
            std::vector<std::shared_ptr<protocol::RpcClient>> connections;
            for (auto &destination : minor->destinations)
              connections.push_back(
                  localData(destination) &&
                          (destination.major != 0 || callbacks_.rootBatch)
                      ? nullptr
                      : client(destination.address, destination.port, false));
            return BatchSink([this, minor, schema, partitioned,
                              buffers = std::move(buffers),
                              connections = std::move(connections)](
                                 RowVectorPtr vector) -> ContinueFuture {
              std::vector<bool> open(minor->destinations.size());
              {
                std::lock_guard lock(minor->mutex);
                for (size_t i = 0; i < open.size(); ++i) {
                  auto &destination = minor->destinations[i];
                  open[i] = !minor->cancelled &&
                            !minor->closedReceivers.count(
                                {destination.major, destination.minor});
                }
              }
              std::vector<std::pair<size_t, RowVectorPtr>> output;
              auto partitionStart = std::chrono::steady_clock::now();
              if (vector) {
                ++minor->sender.vectors;
                minor->sender.rows += vector->size();
              }
              if (partitioned) {
                std::vector<std::vector<vector_size_t>> partitions(open.size());
                if (vector) {
                  VELOX_USER_CHECK(!partitions.empty());
                  DecodedVector hashes(*vector->children().back());
                  for (vector_size_t row = 0; row < vector->size(); ++row) {
                    VELOX_USER_CHECK(!hashes.isNullAt(row),
                                     "NULL partition hash");
                    auto bucket =
                        std::abs(int64_t(hashes.valueAt<int32_t>(row)) %
                                 int64_t(partitions.size()));
                    if (open[bucket])
                      partitions[bucket].push_back(row);
                  }
                  auto columns = vector->children();
                  columns.pop_back();
                  vector = std::make_shared<RowVector>(vector->pool(), schema,
                                                       nullptr, vector->size(),
                                                       std::move(columns));
                }
                for (size_t i = 0; i < buffers.size(); ++i) {
                  if (!open[i]) {
                    buffers[i]->discard();
                    continue;
                  }
                  if (vector) {
                    for (auto &batch :
                         buffers[i]->append(vector, &partitions[i]))
                      output.emplace_back(i, std::move(batch));
                  } else if (auto tail = buffers[i]->flush()) {
                    output.emplace_back(i, std::move(tail));
                  }
                }
              } else {
                if (std::none_of(open.begin(), open.end(),
                                 [](bool value) { return value; })) {
                  buffers[0]->discard();
                  return {};
                }
                std::vector<RowVectorPtr> batches;
                if (vector)
                  batches = buffers[0]->append(vector);
                else if (auto tail = buffers[0]->flush())
                  batches.push_back(std::move(tail));
                // Broadcast copies encoded bytes, not the input vectors.
                for (auto &batch : batches)
                  for (size_t i = 0; i < open.size(); ++i)
                    if (open[i])
                      output.emplace_back(i, batch);
              }
              minor->sender.partitionNs += elapsedNanos(partitionStart);
              if (output.empty())
                return {};
              auto ack =
                  std::make_shared<SenderAcknowledgements>(output.size());
              auto future = ack->promise.getSemiFuture();
              Message handle(minor->handle);
              RowVectorPtr previous;
              WireBatch encoded;
              for (auto &[i, batch] : output) {
                auto requestStart = std::chrono::steady_clock::now();
                try {
                  auto &destination = minor->destinations[i];
                  if (!connections[i] && destination.major != 0) {
                    acceptLocal(destination, minor, std::move(batch));
                    ack->finish(nullptr);
                    continue;
                  }
                  auto encodeStart = std::chrono::steady_clock::now();
                  if (previous != batch) {
                    encoded = encodeBatch(batch, minor->outputFields);
                    previous = batch;
                  }
                  auto body = Writer()
                                  .bytes(1, minor->query)
                                  .integer(2, destination.major)
                                  .integer(3, destination.minor)
                                  .integer(4, handle.integer(2))
                                  .integer(5, handle.integer(3))
                                  .bytes(6, batchDefinition(encoded))
                                  .integer(7, 0)
                                  .take();
                  minor->sender.encodeNs += elapsedNanos(encodeStart);
                  ++minor->sender.batches;
                  minor->sender.wireRows += batch->size();
                  minor->sender.bytes += encoded.data.size();
                  if (batch->size() <= 64)
                    ++minor->sender.smallBatches;
                  if (!connections[i]) {
                    auto delivered =
                        sendData(destination, std::move(body), encoded.data);
                    if (!delivered.valid())
                      ack->finish(nullptr);
                    else
                      std::move(delivered)
                          .via(&folly::InlineExecutor::instance())
                          .thenTry([ack](folly::Try<folly::Unit> result) {
                            try {
                              result.throwUnlessValue();
                              ack->finish(nullptr);
                            } catch (...) {
                              ack->finish(std::current_exception());
                            }
                          });
                    continue;
                  }
                  connections[i]->requestAsync(
                      3, std::move(body), encoded.data,
                      [minor, ack,
                       requestStart](auto error, RpcMessage,
                                     protocol::RequestTiming timing) {
                        minor->sender.requestNs += elapsedNanos(requestStart);
                        minor->sender.lockNs += timing.lockWaitNanos;
                        minor->sender.queueNs += timing.queueWaitNanos;
                        minor->sender.sendNs += timing.sendNanos;
                        minor->sender.responseNs += timing.responseWaitNanos;
                        ack->finish(error);
                      });
                } catch (...) {
                  ack->finish(std::current_exception());
                }
              }
              return future;
            });
          });
        });
    auto plan = converter.convert(minor->plan);
    if (const char *directory = std::getenv("DRILL_NATIVE_PLAN_DIR"))
      std::ofstream(std::filesystem::path(directory) /
                    (minor->id + ".velox.txt"))
          << plan->toString(true, true);
    // Drill announces schema even when the sender produces no rows. Termination
    // is a separate empty RecordBatchDef, without buffers or SerializedFields.
    Message senderHandle(minor->handle);
    for (auto &destination : minor->destinations) {
      {
        std::lock_guard lock(minor->mutex);
        if (minor->closedReceivers.count(
                {destination.major, destination.minor}))
          continue;
      }
      auto body = Writer()
                      .bytes(1, minor->query)
                      .integer(2, destination.major)
                      .integer(3, destination.minor)
                      .integer(4, senderHandle.integer(2))
                      .integer(5, senderHandle.integer(3))
                      .bytes(6, schemaDefinition(minor->outputFields))
                      .integer(7, 0)
                      .take();
      auto delivered = sendData(destination, std::move(body));
      if (delivered.valid())
        std::move(delivered).get();
    }
    auto task = runtime_.task(minor->id, plan, minor->pool, leaf);
    {
      std::lock_guard lock(minor->mutex);
      minor->task = task;
      if (minor->cancelled)
        task->requestCancel();
    }
    status(minor, 2);
    std::cout << "Native minor running: " << minor->id << std::endl;
    task->taskCompletionFuture()
        .via(runtime_.cpu())
        .thenValue([this, minor](auto) {
          try {
            // A Task aborted before start never creates source operators.
            // Close its inboxes explicitly so early finish reaches upstream.
            for (auto &[major, source] : minor->sources)
              source->cancel();
            auto state = minor->task->state();
            bool earlyComplete;
            {
              std::lock_guard lock(minor->mutex);
              earlyComplete = minor->earlyComplete && !minor->cancelled;
            }
            if (state == exec::TaskState::kFinished ||
                (state == exec::TaskState::kAborted && earlyComplete)) {
              auto def = Writer().integer(1, 0).take();
              Message handle(minor->handle);
              for (auto &destination : minor->destinations) {
                {
                  std::lock_guard lock(minor->mutex);
                  if (minor->closedReceivers.count(
                          {destination.major, destination.minor}))
                    continue;
                }
                Writer body;
                body.bytes(1, minor->query)
                    .integer(2, destination.major)
                    .integer(3, destination.minor)
                    .integer(4, handle.integer(2))
                    .integer(5, handle.integer(3))
                    .bytes(6, def)
                    .integer(7, 1);
                auto delivered = sendData(destination, body.take());
                if (delivered.valid())
                  std::move(delivered).get();
              }
              status(minor, 3);
            } else if (state == exec::TaskState::kCanceled ||
                       state == exec::TaskState::kAborted)
              status(minor, 4);
            else {
              auto error = minor->task->error();
              std::string message = "Native task failed";
              if (error) {
                try {
                  std::rethrow_exception(error);
                } catch (const std::exception &e) {
                  message = e.what();
                }
              }
              status(minor, 5, message);
            }
          } catch (const std::exception &e) {
            try {
              status(minor, 5, e.what());
            } catch (...) {
            }
          }
          {
            std::lock_guard lock(minor->mutex);
            minor->terminal = true;
            minor->task.reset();
            minor->pool.reset();
            minor->condition.notify_all();
          }
        });
    {
      std::lock_guard lock(minor->mutex);
      if (minor->earlyComplete)
        task->requestAbort();
    }
    minor->taskStarted = now();
    task->start(runtime_.threads());
  } catch (const std::exception &error) {
    uint32_t state;
    {
      std::lock_guard lock(minor->mutex);
      state = minor->cancelled ? 4 : minor->earlyComplete ? 3 : 5;
    }
    std::cerr << "Native minor stopped: " << minor->id << ": " << error.what()
              << std::endl;
    try {
      status(minor, state, state == 5 ? error.what() : "");
    } catch (...) {
    }
    std::lock_guard lock(minor->mutex);
    minor->terminal = true;
    minor->task.reset();
    minor->pool.reset();
    minor->condition.notify_all();
  }
}
RpcMessage MinorTaskRegistry::control(const RpcMessage &request) {
  if (request.type == 0) {
    VELOX_USER_CHECK_EQ(Message(request.body).integer(1), 3);
    return {RpcMode::Response,
            request.coordination,
            0,
            Writer().integer(1, 3).integer(2, 0).bytes(3, endpoint_).take(),
            {}};
  }
  if (request.type == 3) {
    Message message(request.body);
    std::shared_ptr<const protocol::FragmentRoutes> routes;
    if (!message.bytes(2).empty())
      routes = std::make_shared<protocol::FragmentRoutes>(message.bytes(2));
    VELOX_USER_CHECK(routes ||
                         Message(Message(endpoint_).bytes(9)).integer(3) != 2,
                     "Native RPC fragments require frozen query routes");
    // Validate the entire submission before admitting any task.
    for (auto fragment : message.messages(1)) {
      Message plan(fragment);
      if (routes) {
        const auto &route = routes->get(plan.bytes(1));
        VELOX_USER_CHECK(route.native && route.assignment == plan.bytes(10),
                         "Invalid native fragment route");
      }
    }
    for (auto fragment : message.messages(1))
      initialize(fragment, routes);
  } else if (request.type == 6 || request.type == 16) {
    auto id = taskKey(request.body);
    std::shared_ptr<Minor> minor;
    {
      std::lock_guard lock(mutex_);
      auto it = tasks_.find(id);
      if (it != tasks_.end())
        minor = it->second;
      else if (request.type == 6)
        cancelledBeforeSubmit_.insert(id);
    }
    if (minor && request.type == 6) {
      {
        std::lock_guard lock(minor->mutex);
        minor->cancelled = true;
        for (auto &[major, source] : minor->sources)
          source->cancel();
        if (minor->task)
          minor->task->requestCancel();
      }
      minor->scanCancellation.requestCancellation();
      prepare(minor);
    }
  } else if (request.type == 7) {
    Message finished(request.body);
    auto id = taskKey(finished.bytes(2));
    Message receiver(finished.bytes(1));
    std::shared_ptr<Minor> minor;
    {
      std::lock_guard lock(mutex_);
      auto it = tasks_.find(id);
      if (it != tasks_.end())
        minor = it->second;
    }
    if (minor) {
      bool cancelScan = false;
      {
        std::lock_guard lock(minor->mutex);
        VELOX_USER_CHECK(receiver.bytes(1) == minor->query,
                         "FinishedReceiver belongs to another query");
        auto target = std::pair<uint32_t, uint32_t>{receiver.integer(2),
                                                    receiver.integer(3)};
        VELOX_USER_CHECK(minor->expectedReceivers.count(target),
                         "FinishedReceiver has an unassigned destination");
        minor->closedReceivers.insert(target);
        if (minor->closedReceivers == minor->expectedReceivers &&
            !minor->terminal) {
          minor->earlyComplete = true;
          cancelScan = true;
          if (minor->task)
            minor->task->requestAbort();
        }
      }
      if (cancelScan)
        minor->scanCancellation.requestCancellation();
      // A receiver can finish before the sender receives its first schema.
      // Schedule the terminal notification even though conversion cannot start.
      prepare(minor);
    }
  } else
    VELOX_USER_FAIL("Unsupported BitControl RPC {}", request.type);
  return {RpcMode::Response,
          request.coordination,
          1,
          Writer().integer(1, 1).take(),
          {}};
}
RpcMessage MinorTaskRegistry::data(const RpcMessage &request) {
  if (request.type == 0) {
    VELOX_USER_CHECK_EQ(Message(request.body).integer(1), 4);
    return {RpcMode::Response,
            request.coordination,
            0,
            Writer().integer(1, 4).take(),
            {}};
  }
  VELOX_USER_CHECK_EQ(request.type, 3, "Unsupported BitData RPC");
  Message body(request.body);
  auto query = queryKey(body.bytes(1));
  auto major = body.integer(2);
  auto sender = body.integer(5);
  auto last = body.integer(7);
  auto header = batchHeaderFromDefinition(body.bytes(6));
  for (auto receiver : body.repeatedIntegers(3)) {
    auto id =
        query + "." + std::to_string(major) + "." + std::to_string(receiver);
    auto source = inbox(id, body.integer(4));
    if (last && header["rows"].asInt() == 0 && header["fields"].empty()) {
      VELOX_USER_CHECK(request.raw.empty(), "EOS must have no buffers");
      source->end(sender);
    } else {
      source->push(WireBatch{header, request.raw, {}}, sender, last);
    }
    std::shared_ptr<Minor> minor;
    {
      std::lock_guard lock(mutex_);
      auto it = tasks_.find(id);
      if (it != tasks_.end())
        minor = it->second;
    }
    if (minor)
      prepare(minor);
  }
  return {RpcMode::Response,
          request.coordination,
          6,
          Writer().integer(1, 1).take(),
          {}};
}
void MinorTaskRegistry::stop() {
  std::vector<std::shared_ptr<Minor>> tasks;
  std::vector<std::shared_ptr<protocol::RpcClient>> clients;
  {
    std::lock_guard lock(mutex_);
    if (stopped_)
      return;
    stopped_ = true;
    for (auto &[id, minor] : tasks_)
      tasks.push_back(minor);
    for (auto &[id, client] : clients_)
      clients.push_back(client);
  }
  for (auto &client : clients)
    client->shutdown();
  for (auto &minor : tasks) {
    {
      std::lock_guard lock(minor->mutex);
      minor->cancelled = true;
      for (auto &[major, source] : minor->sources)
        source->cancel();
      if (minor->task)
        minor->task->requestCancel();
      if (!minor->scheduled) {
        minor->terminal = true;
        minor->condition.notify_all();
      }
    }
    minor->scanCancellation.requestCancellation();
  }
  for (auto &minor : tasks) {
    std::unique_lock lock(minor->mutex);
    minor->condition.wait(lock, [&] { return minor->terminal; });
  }
  std::unique_lock lock(mutex_);
  notificationsFinished_.wait(lock, [&] { return pendingNotifications_ == 0; });
}
} // namespace drill::nativeexec
