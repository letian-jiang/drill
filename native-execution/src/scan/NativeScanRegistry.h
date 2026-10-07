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
#include "scan/NativeScanPlugin.h"
#include <mutex>
#include <unordered_map>
namespace drill::nativeexec {
class NativeScanRegistry {
public:
  void registerPlugin(std::shared_ptr<const NativeScanPlugin> plugin);
  NativeScanBinding bind(const folly::dynamic &descriptor,
                         const NativeScanContext &context) const;
  static NativeScanRegistry &instance();

private:
  mutable std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<const NativeScanPlugin>>
      plugins_;
};
// Called once during native runtime startup. Registers built-ins then loads
// modules from colon-separated DRILL_NATIVE_SCAN_PLUGINS. Modules export the
// versioned entrypoint below and remain loaded for all readers/buffer owners.
void initializeNativeScanPlugins();
using RegisterNativeScanPluginsV1 = void (*)(NativeScanRegistry *);
} // namespace drill::nativeexec
