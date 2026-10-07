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
#include "scan/NativeScanRegistry.h"
#include "scan/iceberg/IcebergScan.h"
#include <cstdlib>
#include <dlfcn.h>
#include <sstream>
namespace drill::nativeexec {
NativeScanRegistry &NativeScanRegistry::instance() {
  static NativeScanRegistry registry;
  return registry;
}
void NativeScanRegistry::registerPlugin(
    std::shared_ptr<const NativeScanPlugin> plugin) {
  VELOX_USER_CHECK(plugin && !plugin->provider().empty() &&
                       plugin->descriptorVersion() > 0,
                   "Invalid native scan provider registration");
  auto name = std::string(plugin->provider());
  std::lock_guard lock(mutex_);
  VELOX_USER_CHECK(plugins_.emplace(name, std::move(plugin)).second,
                   "Duplicate native scan provider {}", name);
}
NativeScanBinding
NativeScanRegistry::bind(const folly::dynamic &descriptor,
                         const NativeScanContext &context) const {
  VELOX_USER_CHECK(descriptor.isObject(),
                   "Native scan descriptor must be an object");
  // Retain historical Iceberg descriptors; new descriptors always use provider.
  auto name =
      descriptor.getDefault("provider", descriptor.getDefault("format", ""))
          .asString();
  auto version = descriptor.getDefault("version", 1).asInt();
  std::shared_ptr<const NativeScanPlugin> plugin;
  {
    std::lock_guard lock(mutex_);
    auto found = plugins_.find(name);
    VELOX_USER_CHECK(found != plugins_.end(),
                     "Native scan provider {} is not registered", name);
    plugin = found->second;
  }
  VELOX_USER_CHECK_EQ(
      version, plugin->descriptorVersion(),
      "Unsupported descriptor version for native scan provider {}", name);
  VELOX_USER_CHECK(descriptor.count("readFields") &&
                       descriptor.count("outputFields"),
                   "Native scan {} requires readFields and outputFields", name);
  auto result = plugin->prepare({descriptor}, context);
  VELOX_USER_CHECK(result.schema && result.factory,
                   "Native scan {} returned an invalid binding", name);
  // Retain the provider for factories and readers that capture provider state.
  auto factory = std::move(result.factory);
  result.factory = [plugin,
                    factory = std::move(factory)](memory::MemoryPool *pool) {
    auto reader = factory(pool);
    VELOX_USER_CHECK(reader, "Native scan {} returned no reader",
                     plugin->provider());
    // An aliasing shared_ptr keeps both the reader and its provider alive.
    struct Owner {
      std::shared_ptr<const NativeScanPlugin> plugin;
      std::shared_ptr<BatchSource> reader;
    };
    auto owner = std::make_shared<Owner>(Owner{plugin, reader});
    return std::shared_ptr<BatchSource>(std::move(owner), reader.get());
  };
  return result;
}
void initializeNativeScanPlugins() {
  static std::once_flag once;
  std::call_once(once, [] {
    auto &registry = NativeScanRegistry::instance();
    registry.registerPlugin(icebergScanPlugin());
    if (auto paths = std::getenv("DRILL_NATIVE_SCAN_PLUGINS")) {
      std::istringstream stream(paths);
      std::string path;
      while (std::getline(stream, path, ':')) {
        if (path.empty())
          continue;
        auto module = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
        VELOX_USER_CHECK(module, "Cannot load native scan module {}: {}", path,
                         dlerror());
        auto entry = reinterpret_cast<RegisterNativeScanPluginsV1>(
            dlsym(module, "drill_register_native_scan_plugins_v1"));
        VELOX_USER_CHECK(
            entry, "Native scan module {} has no v1 registration entrypoint",
            path);
        // Intentionally never dlclose: Arrow/vector deleters may outlive Tasks.
        entry(&registry);
      }
    }
  });
}
} // namespace drill::nativeexec
