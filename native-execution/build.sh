#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
set -euo pipefail
TASK_SOURCE_DIR=$(cd "$(dirname "$0")" && pwd -P)
TASK_REPO=$(cd "$TASK_SOURCE_DIR/.." && pwd)
TASK_TOOLS=${DRILL_NATIVE_TOOLS:-$TASK_REPO/.tools}
TASK_PREFIX=${1:-${DRILL_NATIVE_DEPENDENCY_PREFIX:-}}
python3 "$TASK_SOURCE_DIR/prepare.py" --cache "$TASK_TOOLS/native-runtime"
cmake -S "$TASK_SOURCE_DIR" -B "$TASK_TOOLS/native-runtime/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DDRILL_NATIVE_CACHE="$TASK_TOOLS/native-runtime" \
  -DDRILL_NATIVE_DEPENDENCY_PREFIX="$TASK_PREFIX" \
  -DCMAKE_PREFIX_PATH="${DRILL_NATIVE_CMAKE_PREFIX:-$TASK_PREFIX}"
cmake --build "$TASK_TOOLS/native-runtime/build" -j "${DRILL_NATIVE_BUILD_JOBS:-2}"
printf '%s\n' "Native engine: $TASK_TOOLS/native-runtime/build/libdrill_native_engine.so"
