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
#include <future>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
using namespace drill::nativeexec::protocol;
namespace {
void check(bool value) {
  if (!value)
    throw std::runtime_error("RPC multiplex assertion failed");
}
RpcMessage read(int fd, FrameDecoder &decoder) {
  while (true) {
    if (auto message = decoder.next())
      return std::move(*message);
    char bytes[65536];
    auto n = recv(fd, bytes, sizeof(bytes), 0);
    if (n <= 0)
      throw std::runtime_error("Test peer closed or timed out");
    decoder.append(std::string_view(bytes, n));
  }
}
void write(int fd, const RpcMessage &message) {
  auto frame = encodeFrame(message);
  std::string_view bytes(frame);
  while (!bytes.empty()) {
    auto n = send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (n <= 0)
      throw std::runtime_error("Test peer send failed");
    bytes.remove_prefix(n);
  }
}
void scenario(int mode) {
  constexpr int count = 64;
  int listener = socket(AF_INET, SOCK_STREAM, 0);
  check(listener >= 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  check(bind(listener, reinterpret_cast<sockaddr *>(&address),
             sizeof(address)) == 0);
  socklen_t size = sizeof(address);
  check(getsockname(listener, reinterpret_cast<sockaddr *>(&address), &size) ==
        0);
  check(listen(listener, 1) == 0);
  std::promise<void> received;
  auto allReceived = received.get_future();
  auto peer = std::async(std::launch::async, [&] {
    int fd = accept(listener, nullptr, nullptr);
    check(fd >= 0);
    try {
      timeval timeout{5, 0};
      setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      FrameDecoder decoder;
      auto handshake = read(fd, decoder);
      write(fd, {RpcMode::Response, handshake.coordination, 0, {}, {}});
      std::vector<RpcMessage> requests;
      // No ACK until ALL requests arrive: a one-inflight client cannot pass.
      for (int i = 0; i < count; ++i) {
        auto message = read(fd, decoder);
        check(message.type == 3 && message.raw == std::string("data\0", 5));
        requests.push_back(std::move(message));
      }
      received.set_value();
      if (mode == 0) {
        write(fd, {RpcMode::Request, 9001, 7, {}, {}});
        write(fd, {RpcMode::Ping, 9002, 0, {}, {}});
        auto reverse = read(fd, decoder);
        auto pong = read(fd, decoder);
        check(reverse.mode == RpcMode::Response &&
              reverse.coordination == 9001);
        check(pong.mode == RpcMode::Pong && pong.coordination == 9002);
        // Reverse-order ACKs, with one request-level failure.
        for (auto it = requests.rbegin(); it != requests.rend(); ++it) {
          auto value = Message(it->body).integer(1);
          bool failed = value == 7 || value == 8;
          write(fd, {failed ? RpcMode::Failure : RpcMode::Response,
                     it->coordination,
                     6,
                     value == 8 ? std::string(1, char(0x80))
                     : failed   ? Writer().bytes(4, "injected failure").take()
                                : it->body,
                     {}});
        }
      } else if (mode == 2) {
        // Client shutdown must settle waiters even when the peer never ACKs.
        char byte;
        check(recv(fd, &byte, 1, 0) == 0);
      }
      shutdown(fd, SHUT_RDWR);
      close(fd);
    } catch (...) {
      shutdown(fd, SHUT_RDWR);
      close(fd);
      throw;
    }
  });
  RpcClient client("127.0.0.1", ntohs(address.sin_port),
                   {RpcMode::Request, 0, 0, {}, {}},
                   [](const RpcMessage &request) {
                     check(request.type == 7);
                     return RpcMessage{RpcMode::Response, 0, 1, {}, {}};
                   });
  std::vector<std::shared_ptr<std::promise<int>>> promises;
  std::vector<std::future<int>> futures;
  for (int i = 0; i < count; ++i) {
    auto promise = std::make_shared<std::promise<int>>();
    futures.push_back(promise->get_future());
    promises.push_back(promise);
    client.requestAsync(
        3, Writer().integer(1, i).take(), std::string("data\0", 5),
        [promise](auto error, RpcMessage response, RequestTiming) {
          if (error)
            promise->set_exception(error);
          else
            promise->set_value(Message(response.body).integer(1));
        });
  }
  if (mode == 2) {
    check(allReceived.wait_for(std::chrono::seconds(5)) ==
          std::future_status::ready);
    client.shutdown();
  }
  for (int i = 0; i < count; ++i) {
    check(futures[i].wait_for(std::chrono::seconds(5)) ==
          std::future_status::ready);
    bool failed = false;
    try {
      check(futures[i].get() == i);
    } catch (const std::exception &) {
      failed = true;
    }
    check(failed == (mode != 0 || i == 7 || i == 8));
  }
  peer.get();
  close(listener);
}
} // namespace
int main() {
  try {
    scenario(0);
    scenario(1); // Peer disconnect with 64 outstanding requests.
    scenario(2); // Local shutdown with 64 outstanding requests.
    std::cout
        << "Multiplexed RPC: 64 in flight, reverse ACKs, reverse control, "
           "Ping/Pong, request failure, disconnect and shutdown passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
