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
#include "scan/ScanReader.h"
#include <folly/CancellationToken.h>
#include <folly/dynamic.h>
#include <functional>
#include <jni.h>
namespace drill::nativeexec {
struct JniScanBinding {
  RowTypePtr schema;
  SourceFactory factory;
  // A cumulative snapshot of counters only: retaining this callback must not
  // retain readers, vectors, pools or a JNI host.
  std::function<folly::dynamic()> statistics;
};
// Called from the Java host before any Task starts. Borrows its existing JVM
// and the exact ScanHost class/loader instead of creating a second JVM.
void bindJniScanHost(JNIEnv *env, jclass scanHost);
// Standalone worker shutdown, after Tasks/readers drain. A borrowed JVM and
// Java Drillbit services are never closed by this entrypoint.
void closeStandaloneJniScanHost();
// Opens a seed plugin reader for schema discovery on the shared Scan/I/O
// executor. Independent work uses per-driver sources/pools and is claimed
// once. Single/opaque work shares one source across drivers.
JniScanBinding openJniPluginScan(const folly::dynamic &descriptor,
                                 memory::MemoryPool *pool, folly::Executor *io,
                                 folly::CancellationToken cancellation = {});
} // namespace drill::nativeexec
