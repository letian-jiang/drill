#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0 .
set -euo pipefail
TASK_SOURCE_DIR=$(cd "$(dirname "$0")" && pwd -P)
TASK_REPO=$(cd "$TASK_SOURCE_DIR/../.." && pwd)
TASK_CACHE=${DRILL_NATIVE_TOOLS:-$TASK_REPO/.tools}
TASK_TOOLS="$TASK_CACHE/iceberg-toolchain"
TASK_ENV="$TASK_TOOLS/env"
TASK_COMMIT=3c5715c66fb4d4306c54eec4aac327e2897f800b
mkdir -p "$TASK_TOOLS"
if [ ! -x "$TASK_TOOLS/bin/micromamba" ]; then
  curl -fLsS https://micro.mamba.pm/api/micromamba/linux-64/latest -o "$TASK_TOOLS/micromamba.tar.bz2"
  tar -xjf "$TASK_TOOLS/micromamba.tar.bz2" -C "$TASK_TOOLS" bin/micromamba
fi
if [ ! -x "$TASK_ENV/bin/x86_64-conda-linux-gnu-g++" ]; then
  "$TASK_TOOLS/bin/micromamba" create -y -r "$TASK_TOOLS/mamba" -p "$TASK_ENV" -c conda-forge \
    gxx_linux-64=14.4.0 zlib=1.3.2 snappy=1.2.2 zstd=1.5.7
fi
if [ ! -d "$TASK_CACHE/iceberg-cpp/.git" ]; then
  git clone https://github.com/apache/iceberg-cpp.git "$TASK_CACHE/iceberg-cpp"
fi
test "$(git -C "$TASK_CACHE/iceberg-cpp" rev-parse HEAD)" = "$TASK_COMMIT" || \
  git -C "$TASK_CACHE/iceberg-cpp" checkout --detach "$TASK_COMMIT"
if git -C "$TASK_CACHE/iceberg-cpp" apply --check "$TASK_SOURCE_DIR/sdk.patch" 2>/dev/null; then
  git -C "$TASK_CACHE/iceberg-cpp" apply "$TASK_SOURCE_DIR/sdk.patch"
else
  git -C "$TASK_CACHE/iceberg-cpp" apply --reverse --check "$TASK_SOURCE_DIR/sdk.patch"
fi
"$TASK_TOOLS/bin/micromamba" list -p "$TASK_ENV" --explicit > "$TASK_TOOLS/packages.lock"
TASK_PYTHON=${DRILL_NATIVE_PYTHON:-python3}
TASK_ARROW_DIR=$("$TASK_PYTHON" -c 'import pyarrow; from pathlib import Path; assert pyarrow.__version__ == "25.0.1"; print(Path(pyarrow.__file__).parent)')
ICEBERG_AVRO_URL=https://codeload.github.com/apache/avro/tar.gz/209a3735ec330679790824da54b7558db55e4e7f \
cmake -S "$TASK_SOURCE_DIR" -B "$TASK_CACHE/iceberg-build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$TASK_ENV" \
  -DDRILL_ICEBERG_ARROW_DIR="$TASK_ARROW_DIR" \
  -DDRILL_ICEBERG_SOURCE="$TASK_CACHE/iceberg-cpp" \
  -DCMAKE_CXX_COMPILER="$TASK_ENV/bin/x86_64-conda-linux-gnu-g++" \
  -DCMAKE_C_COMPILER="$TASK_ENV/bin/x86_64-conda-linux-gnu-gcc"
cmake --build "$TASK_CACHE/iceberg-build" --target drill_iceberg_reader -j "${DRILL_ICEBERG_BUILD_JOBS:-4}"
