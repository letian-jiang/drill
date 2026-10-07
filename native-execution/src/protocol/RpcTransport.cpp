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
#include "protocol/RpcTransport.h"
#include <cerrno>
#include <cstring>
#include <future>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>
#include <unordered_map>
namespace drill::nativeexec::protocol {
namespace {
void fail(const char *operation) {
  throw std::runtime_error(std::string(operation) + ": " +
                           std::strerror(errno));
}
void sendAll(int fd, std::string_view bytes) {
  while (!bytes.empty()) {
    auto n = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      fail("Drill RPC send");
    bytes.remove_prefix(n);
  }
}
RpcMessage receive(int fd, FrameDecoder &decoder) {
  while (true) {
    if (auto result = decoder.next())
      return std::move(*result);
    char bytes[65536];
    auto n = ::recv(fd, bytes, sizeof(bytes), 0);
    if (n < 0 && errno == EINTR)
      continue;
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
      continue;
    if (n <= 0)
      throw std::runtime_error("Drill RPC peer closed the connection");
    decoder.append(std::string_view(bytes, n));
  }
}
int openSocket(const std::string &address, uint16_t port, bool server) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = server ? AI_PASSIVE : 0;
  addrinfo *addresses = nullptr;
  auto code = getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints,
                          &addresses);
  if (code)
    throw std::runtime_error(gai_strerror(code));
  int fd = -1;
  for (auto a = addresses; a; a = a->ai_next) {
    fd = ::socket(a->ai_family, a->ai_socktype | SOCK_CLOEXEC, a->ai_protocol);
    if (fd < 0)
      continue;
    timeval timeout{30, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    int value = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
    if (server)
      setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value));
    if ((server ? ::bind(fd, a->ai_addr, a->ai_addrlen)
                : ::connect(fd, a->ai_addr, a->ai_addrlen)) == 0)
      break;
    ::close(fd);
    fd = -1;
  }
  freeaddrinfo(addresses);
  if (fd < 0)
    fail(server ? "Drill RPC bind" : "Drill RPC connect");
  return fd;
}
} // namespace
struct RpcClient::PendingRequest {
  Completion completion;
  std::chrono::steady_clock::time_point queued, sent, received;
  RequestTiming timing;
  bool written = false;
  std::optional<RpcMessage> response;
};
RpcClient::RpcClient(const std::string &address, uint16_t port,
                     RpcMessage handshake, Handler handler)
    : handler_(std::move(handler)) {
  socket_ = openSocket(address, port, false);
  try {
    handshake.coordination = nextId_++;
    send(handshake);
    auto response = receive(socket_, decoder_);
    if (response.mode != RpcMode::Response || response.type != 0 ||
        response.coordination != handshake.coordination)
      throw std::runtime_error("Unexpected Drill RPC handshake response");
    reader_ = std::thread([this] { readLoop(); });
    writer_ = std::thread([this] { writeLoop(); });
  } catch (...) {
    shutdown();
    if (reader_.joinable())
      reader_.join();
    ::close(socket_);
    socket_ = -1;
    throw;
  }
}
RpcClient::~RpcClient() {
  shutdown();
  if (reader_.joinable())
    reader_.join();
  if (writer_.joinable())
    writer_.join();
  if (socket_ >= 0)
    ::close(socket_);
}
void RpcClient::failPending(std::exception_ptr error) {
  decltype(pending_) pending;
  {
    std::lock_guard lock(pendingMutex_);
    if (!failure_)
      failure_ = error;
    stopped_ = true;
    pending.swap(pending_);
    outgoing_.clear();
  }
  if (socket_ >= 0)
    ::shutdown(socket_, SHUT_RDWR);
  condition_.notify_all();
  for (auto &[id, request] : pending) {
    // Cancellation and connection failure settle every waiter exactly once.
    try {
      request->completion(error, {}, request->timing);
    } catch (...) {
    }
  }
}
void RpcClient::shutdown() {
  failPending(std::make_exception_ptr(
      std::runtime_error("Drill RPC connection is closing")));
}
void RpcClient::send(const RpcMessage &message) {
  auto frame = encodeFrame(message);
  std::lock_guard lock(sendMutex_);
  sendAll(socket_, frame);
}
void RpcClient::complete(std::shared_ptr<PendingRequest> pending,
                         RpcMessage response) {
  pending->timing.responseWaitNanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::max(pending->received, pending->sent) - pending->sent)
          .count();
  std::exception_ptr error;
  try {
    if (response.mode == RpcMode::Failure)
      throw std::runtime_error("Drill RPC failed: " +
                               std::string(Message(response.body).bytes(4)));
    if (response.mode != RpcMode::Response)
      throw std::runtime_error("Expected Drill RPC response");
  } catch (...) {
    // Malformed failure protobufs must also settle this request's waiter.
    error = std::current_exception();
  }
  // User callbacks never execute with the pending map or socket lock held.
  try {
    pending->completion(error, std::move(response), pending->timing);
  } catch (...) {
  }
}
void RpcClient::readLoop() {
  try {
    while (!stopped_) {
      auto message = receive(socket_, decoder_);
      if (message.mode == RpcMode::Pong)
        continue;
      if (message.mode == RpcMode::Ping) {
        message.mode = RpcMode::Pong;
        send(message);
        continue;
      }
      if (message.mode == RpcMode::Request) {
        RpcMessage reply;
        try {
          if (!handler_)
            throw std::runtime_error("Unexpected incoming Drill RPC request");
          reply = handler_(message);
          reply.mode = RpcMode::Response;
        } catch (const std::exception &error) {
          reply = {RpcMode::Failure,
                   message.coordination,
                   message.type,
                   Writer().bytes(4, error.what()).take(),
                   {}};
        }
        reply.coordination = message.coordination;
        send(reply);
        continue;
      }
      std::shared_ptr<PendingRequest> pending;
      {
        std::lock_guard lock(pendingMutex_);
        auto it = pending_.find(message.coordination);
        if (it == pending_.end()) {
          if (stopped_)
            return;
          throw std::runtime_error("Unexpected Drill RPC coordination ID");
        }
        pending = it->second;
        pending->received = std::chrono::steady_clock::now();
        // The peer can ACK before the writer records the end of sendAll.
        if (!pending->written) {
          pending->response = std::move(message);
          continue;
        }
        pending_.erase(it);
      }
      complete(std::move(pending), std::move(message));
      condition_.notify_all();
    }
  } catch (...) {
    failPending(std::current_exception());
  }
}
void RpcClient::writeLoop() {
  try {
    while (true) {
      RpcMessage message;
      std::shared_ptr<PendingRequest> pending;
      {
        std::unique_lock lock(pendingMutex_);
        while (true) {
          if (stopped_)
            return;
          auto deadline = std::chrono::steady_clock::time_point::max();
          for (auto &[id, request] : pending_)
            deadline =
                std::min(deadline, request->queued + std::chrono::seconds(30));
          if (deadline <= std::chrono::steady_clock::now())
            throw std::runtime_error(
                "Timed out waiting for Drill RPC response");
          if (!outgoing_.empty())
            break;
          if (pending_.empty())
            condition_.wait(lock);
          else
            condition_.wait_until(lock, deadline);
        }
        message = std::move(outgoing_.front());
        outgoing_.pop_front();
        pending = pending_.at(message.coordination);
      }
      auto start = std::chrono::steady_clock::now();
      send(message);
      std::optional<RpcMessage> response;
      {
        std::lock_guard lock(pendingMutex_);
        auto it = pending_.find(message.coordination);
        if (it == pending_.end())
          continue; // shutdown has already completed this request.
        pending->sent = std::chrono::steady_clock::now();
        pending->timing.queueWaitNanos =
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                start - pending->queued)
                .count() -
            pending->timing.lockWaitNanos;
        pending->timing.sendNanos =
            std::chrono::duration_cast<std::chrono::nanoseconds>(pending->sent -
                                                                 start)
                .count();
        pending->written = true;
        response = std::move(pending->response);
        if (response)
          pending_.erase(it);
      }
      if (response)
        complete(std::move(pending), std::move(*response));
    }
  } catch (...) {
    failPending(std::current_exception());
  }
}
void RpcClient::requestAsync(int32_t type, std::string body, std::string raw,
                             Completion completion) {
  auto pending = std::make_shared<PendingRequest>();
  pending->queued = std::chrono::steady_clock::now();
  pending->completion = std::move(completion);
  std::exception_ptr error;
  {
    std::lock_guard lock(pendingMutex_);
    pending->timing.lockWaitNanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - pending->queued)
            .count();
    error = failure_;
    if (!error) {
      while (true) {
        if (nextId_ == INT32_MAX)
          nextId_ = 1;
        auto id = nextId_++;
        if (!pending_.count(id)) {
          pending_.emplace(id, pending);
          outgoing_.push_back(
              {RpcMode::Request, id, type, std::move(body), std::move(raw)});
          break;
        }
      }
    }
  }
  if (error)
    pending->completion(error, {}, {});
  else
    condition_.notify_one();
}
RpcMessage RpcClient::request(int32_t type, std::string body, std::string raw,
                              RequestTiming *timing) {
  auto promise = std::make_shared<std::promise<RpcMessage>>();
  auto future = promise->get_future();
  requestAsync(
      type, std::move(body), std::move(raw),
      [promise, timing](auto error, RpcMessage response, RequestTiming stats) {
        if (timing)
          *timing = stats;
        if (error)
          promise->set_exception(error);
        else
          promise->set_value(std::move(response));
      });
  return future.get();
}
RpcServer::RpcServer(const std::string &address, uint16_t port, Handler handler,
                     std::function<void(std::string)> onFailure)
    : onFailure_(std::move(onFailure)), port_(port),
      handler_(std::move(handler)) {
  listener_ = openSocket(address, port, true);
  try {
    if (::listen(listener_, 128) < 0)
      fail("Drill RPC listen");
    sockaddr_storage actual{};
    socklen_t length = sizeof(actual);
    if (getsockname(listener_, reinterpret_cast<sockaddr *>(&actual), &length) <
        0)
      fail("Drill RPC getsockname");
    port_ = actual.ss_family == AF_INET
                ? ntohs(reinterpret_cast<sockaddr_in *>(&actual)->sin_port)
                : ntohs(reinterpret_cast<sockaddr_in6 *>(&actual)->sin6_port);
    thread_ = std::thread([this] { serve(); });
  } catch (...) {
    ::close(listener_);
    listener_ = -1;
    throw;
  }
}
RpcServer::~RpcServer() { stop(); }
void RpcServer::stop() {
  stopped_ = true;
  if (thread_.joinable())
    thread_.join();
  if (listener_ >= 0) {
    ::close(listener_);
    listener_ = -1;
  }
}
void RpcServer::serve() {
  std::unordered_map<int, FrameDecoder> clients;
  while (!stopped_) {
    std::vector<pollfd> descriptors{{listener_, POLLIN, 0}};
    for (auto &[fd, decoder] : clients)
      descriptors.push_back({fd, POLLIN, 0});
    auto code = ::poll(descriptors.data(), descriptors.size(), 100);
    if (code < 0) {
      if (errno == EINTR)
        continue;
      break;
    }
    if (descriptors[0].revents & (POLLERR | POLLNVAL | POLLHUP))
      break;
    if (descriptors[0].revents & POLLIN) {
      auto fd = ::accept4(listener_, nullptr, nullptr, SOCK_CLOEXEC);
      if (fd >= 0) {
        int value = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value));
        clients.emplace(fd, FrameDecoder{});
      }
    }
    for (size_t i = 1; i < descriptors.size(); ++i) {
      auto entry = descriptors[i];
      if (!entry.revents)
        continue;
      bool closed = false;
      try {
        if (entry.revents & POLLIN) {
          char bytes[65536];
          auto n = ::recv(entry.fd, bytes, sizeof(bytes), 0);
          if (n <= 0)
            closed = true;
          else {
            auto &decoder = clients.at(entry.fd);
            decoder.append(std::string_view(bytes, n));
            while (auto request = decoder.next()) {
              if (request->mode == RpcMode::Pong)
                continue;
              RpcMessage response;
              if (request->mode == RpcMode::Ping) {
                response = *request;
                response.mode = RpcMode::Pong;
              } else {
                try {
                  response = handler_(*request);
                  response.mode = RpcMode::Response;
                } catch (const std::exception &error) {
                  response = {RpcMode::Failure,
                              request->coordination,
                              request->type,
                              Writer().bytes(4, error.what()).take(),
                              {}};
                }
              }
              response.coordination = request->coordination;
              sendAll(entry.fd, encodeFrame(response));
            }
          }
        }
        if (entry.revents & (POLLHUP | POLLERR | POLLNVAL))
          closed = true;
      } catch (const std::exception &) {
        closed = true;
      }
      if (closed) {
        ::close(entry.fd);
        clients.erase(entry.fd);
      }
    }
  }
  for (auto &[fd, decoder] : clients)
    ::close(fd);
  if (!stopped_ && onFailure_) {
    try {
      onFailure_("Native RPC listener event loop failed");
    } catch (...) {
    }
  }
}
} // namespace drill::nativeexec::protocol
