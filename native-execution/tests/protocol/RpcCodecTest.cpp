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
#include "protocol/RpcMessage.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
using namespace drill::nativeexec::protocol;
void check(bool value) {
  if (!value)
    throw std::runtime_error("RPC codec assertion failed");
}
int main(int argc, char **argv) {
  try {
    // PlanFragment and QueryId field numbers match BitControl/ExecutionProtos.
    auto query = Writer()
                     .fixed64(1, 0xfedcba9876543210ULL)
                     .fixed64(2, 0x123456789abcdef0ULL)
                     .take();
    auto handle = Writer().bytes(1, query).integer(2, 1).integer(3, 2).take();
    auto fragment = Writer()
                        .bytes(1, handle)
                        .bytes(8, R"({"pop":"single-sender","@id":0})")
                        .integer(9, 1)
                        .take();
    RpcMessage original{RpcMode::Request, 37, 3,
                        Writer().bytes(1, fragment).take(),
                        std::string("raw\0body", 8)};
    auto frame = encodeFrame(original);
    for (size_t cut = 0; cut < frame.size(); ++cut) {
      FrameDecoder decoder;
      decoder.append(std::string_view(frame).substr(0, cut));
      check(!decoder.next());
      decoder.append(std::string_view(frame).substr(cut));
      auto value = decoder.next();
      check(value && value->coordination == 37 && value->type == 3 &&
            value->body == original.body && value->raw == original.raw);
      check(!decoder.next());
    }
    FrameDecoder multiple;
    multiple.append(frame + frame);
    check(bool(multiple.next()));
    check(bool(multiple.next()));
    check(!multiple.next());
    auto fields = Message(original.body).messages(1);
    check(fields.size() == 1);
    Message parsedFragment(fields[0]);
    Message parsedHandle(parsedFragment.bytes(1));
    Message parsedQuery(parsedHandle.bytes(1));
    check(parsedQuery.integer(1) == 0xfedcba9876543210ULL);
    check(parsedHandle.integer(2) == 1);
    check(parsedHandle.integer(3) == 2);
    auto packed =
        Writer().integer(2, 3).bytes(2, std::string("\x05\x07", 2)).take();
    check(Message(packed).repeatedIntegers(2) ==
          std::vector<uint32_t>({3, 5, 7}));
    bool invalid = false;
    try {
      FrameDecoder d;
      d.append(std::string(6, '\xff'));
      d.next();
    } catch (const std::exception &) {
      invalid = true;
    }
    check(invalid);
    if (argc == 2) {
      std::ofstream out(argv[1], std::ios::binary);
      out.write(frame.data(), frame.size());
    }
    std::cout
        << "Drill RPC framing, every split boundary, concatenated frames, "
           "original fragment fields and malformed length passed.\n";
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
