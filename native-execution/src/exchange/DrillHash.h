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
#include <folly/dynamic.h>
#include <velox/vector/ComplexVector.h>
#include <velox/vector/DecodedVector.h>
namespace drill::nativeexec {
using namespace facebook::velox;
struct HashKey {
  TypePtr type;
  std::unique_ptr<DecodedVector> decoded;
};
uint32_t hashRow(const std::vector<HashKey> &keys, int row, bool prehashed,
                 uint32_t seed = 1301011);
std::vector<HashKey> decodeHashKeys(const RowVectorPtr &,
                                    const folly::dynamic &keys);
void registerHashFunctions();
} // namespace drill::nativeexec
