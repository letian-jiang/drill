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
"""Repository-local test paths, with explicit support for existing dependency caches."""
import os
from pathlib import Path
import shutil

REPO_ROOT = Path(__file__).resolve().parents[2]
MODULE_ROOT = REPO_ROOT / 'native-execution'
BENCHMARK = MODULE_ROOT / 'benchmark'
_local_tools = REPO_ROOT / '.tools'
_workspace_tools = REPO_ROOT.parent / '.tools'
TOOLS = Path(os.environ.get('DRILL_NATIVE_TOOLS', str(
    _workspace_tools if not _local_tools.exists() and (_workspace_tools / 'native-runtime').exists()
    else _local_tools))).resolve()
_legacy_data = REPO_ROOT.parent / 'benchmark/data'
DEFAULT_DATA = _legacy_data if (_legacy_data / 'manifest.json').exists() else BENCHMARK / 'data'

def maven():
    executable = os.environ.get('MVN') or shutil.which('mvn')
    if executable:
        return executable
    cached = TOOLS / 'apache-maven-3.9.11/bin/mvn'
    if not cached.is_file():
        raise FileNotFoundError('Install Maven on PATH or set MVN to its executable')
    return str(cached)
