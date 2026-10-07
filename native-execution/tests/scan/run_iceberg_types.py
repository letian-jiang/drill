#!/usr/bin/env python3
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
"""Compare typed Iceberg rows across Java/JNI/SDK in both host deployments."""
import argparse
from collections import Counter
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_paths import REPO_ROOT, MODULE_ROOT, BENCHMARK, TOOLS, DEFAULT_DATA, maven
ROOT = REPO_ROOT
sys.path.insert(0, str(BENCHMARK))
from validate_worker_topology import validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--queries', default='1,2,3,4,6')
    args = parser.parse_args()
    numbers = list(map(int, args.queries.split(',')))
    dataset = args.dataset.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    expected = json.loads((dataset / 'expected.json').read_text())
    manifests = []
    summary = []
    for deployment in ('single', 'distributed'):
        baseline = None
        for backend in ('java', 'native-jni', 'native-sdk'):
            run = output / f'{deployment}-{backend}'
            command = [sys.executable, str(MODULE_ROOT / 'tests/run_iceberg_integration.py'),
                       '--deployment', deployment, '--worker-mode',
                       'java' if backend == 'java' else 'native', '--dataset', str(dataset),
                       '--queries', args.queries, '--query-timeout', '90', '--timeout', str(max(600, len(numbers) * 100)),
                       '--output', str(run)]
            if backend == 'native-jni':
                command.append('--force-jni-scan')
            print('Running', run.name, flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            validate(run)
            manifests.append(json.loads((run / 'run.json').read_text()))
            if backend == 'java':
                baseline = run
            checks = []
            for query in numbers:
                value = json.loads((run / f'q{query:02}.json').read_text())
                java = json.loads((baseline / f'q{query:02}.json').read_text())
                rows = Counter(map(tuple, value['rows']))
                if rows != Counter(map(tuple, java['rows'])):
                    raise ValueError(f'{run.name}/q{query:02}: Java row mismatch')
                independent = str(query) in expected
                if independent and rows != Counter(map(tuple, expected[str(query)])):
                    raise ValueError(f'{run.name}/q{query:02}: independent row mismatch')
                checks.append({'query': query, 'row_count': len(value['rows']),
                               'same_round_java': True, 'independent_oracle': independent})
            (run / 'correctness.json').write_text(json.dumps(checks, indent=2))
            summary.append({'mode': run.name, 'checks': checks})
    for key in ('dataset_manifest_sha256', 'schema_file_sha256', 'queries_sha256'):
        if any(manifest[key] != manifests[0][key] for manifest in manifests):
            raise ValueError(f'Input changed between runs: {key}')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('Validated six typed Iceberg configurations:', output, flush=True)


if __name__ == '__main__':
    main()
