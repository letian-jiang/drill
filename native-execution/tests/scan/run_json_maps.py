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
"""Original JSON MAPs through real Java Foreman/root in both native topologies."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_paths import REPO_ROOT, MODULE_ROOT, BENCHMARK, TOOLS, DEFAULT_DATA, maven
ROOT = REPO_ROOT
sys.path.insert(0, str(BENCHMARK))
from validate_worker_topology import validate


def compact(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def generate(dataset):
    dataset.mkdir(parents=True, exist_ok=False)
    warehouse = dataset / 'warehouse' / 'maps'
    warehouse.mkdir(parents=True)
    queries = dataset / 'queries'
    queries.mkdir()
    count = 130003
    records = []
    for i in range(count):
        n = None if i % 29 == 0 else i
        s = None if i % 31 == 0 else f'{i:06d}:雪🚀\0' + ('x' * 20000 if i % 997 == 0 else '')
        records.append({'id': i, 'm': {'n': n, 'inner': {'s': s}}, 'literal.dot': f'quoted{i}'})
    for part, rows in enumerate((records[:65003], records[65003:])):
        (warehouse / f'part{part}.json').write_text(''.join(compact(row) + '\n' for row in rows), encoding='utf-8')
    # Empty DDL: schema is inferred by the original JSON reader.
    (dataset / 'schema.sql').write_text('')
    sql = {
        1: 'SELECT id, m, t.`literal.dot` AS quoted FROM maps t ORDER BY id LIMIT 128',
        2: 'SELECT COUNT(*) AS n, COUNT(t.m.n) AS numbers, SUM(t.m.n) AS total, '
           'COUNT(t.m.`inner`.s) AS strings, MIN(t.m.`inner`.s) AS first, MAX(t.m.`inner`.s) AS last FROM maps t',
        3: 'SELECT t.m.`inner`.s AS s, COUNT(*) AS n FROM maps t WHERE id < 128 GROUP BY t.m.`inner`.s ORDER BY s',
        4: 'SELECT a.id AS id, a.m.`inner`.s AS s, b.m AS matched FROM maps a JOIN maps b '
           'ON a.m.n = b.m.n WHERE a.id < 128 AND b.id < 128 ORDER BY id',
        6: 'SELECT id, m FROM maps t WHERE t.m.n >= 129900 ORDER BY id LIMIT 8',
    }
    for query, text in sql.items():
        (queries / f'q{query:02}.sql').write_text(text + ';\n')
    numbers = [row['m']['n'] for row in records if row['m']['n'] is not None]
    strings = [row['m']['inner']['s'] for row in records if row['m']['inner']['s'] is not None]
    map_value = lambda row: compact(row['m'])
    groups = Counter(row['m']['inner']['s'] for row in records[:128])
    expected = {
        '1': [[str(row['id']), map_value(row), row['literal.dot']] for row in records[:128]],
        '2': [[str(count), str(len(numbers)), str(sum(numbers)), str(len(strings)), min(strings), max(strings)]],
        '3': [[s, str(n)] for s, n in groups.items()],
        '4': [[str(row['id']), row['m']['inner']['s'], map_value(row)]
              for row in records[:128] if row['m']['n'] is not None],
        '6': [[str(row['id']), map_value(row)]
              for row in records if row['m']['n'] is not None and row['m']['n'] >= 129900][:8],
    }
    (dataset / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    (dataset / 'manifest.json').write_text(json.dumps({
        'rows': count, 'schema': 'id BIGINT, m MAP<n BIGINT,inner MAP<s VARCHAR>>, literal.dot VARCHAR',
        'files': {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in warehouse.glob('*.json')},
        'oracle': 'Independent Python values, explicit NULLs and named nested MAP children; no engine output'}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--generate', action='store_true')
    args = parser.parse_args()
    dataset = args.dataset.resolve()
    if args.generate:
        generate(dataset)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    expected = json.loads((dataset / 'expected.json').read_text())
    summary, manifests = [], []
    for deployment in ('single', 'distributed'):
        baseline = None
        for backend in ('java', 'native-jni'):
            run = output / f'{deployment}-{backend}'
            command = [sys.executable, str(MODULE_ROOT / 'tests/run_iceberg_integration.py'),
                       '--format', 'json', '--dataset', str(dataset), '--deployment', deployment,
                       '--worker-mode', 'java' if backend == 'java' else 'native', '--force-jni-scan',
                       '--queries', '1,2,3,4,6', '--output', str(run), '--query-timeout', '90', '--timeout', '900']
            print('Running', run.name, flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            validate(run)
            if backend == 'java':
                baseline = run
            checks = []
            for query, oracle in expected.items():
                rows = json.loads((run / f'q{int(query):02}.json').read_text())['rows']
                java = json.loads((baseline / f'q{int(query):02}.json').read_text())['rows']
                if Counter(map(tuple, rows)) != Counter(map(tuple, oracle)):
                    raise ValueError(f'{run.name}/q{query}: independent MAP row mismatch')
                if Counter(map(tuple, rows)) != Counter(map(tuple, java)):
                    raise ValueError(f'{run.name}/q{query}: Java MAP row mismatch')
                checks.append({'query': int(query), 'rows': len(rows), 'same_round_java': True, 'independent_oracle': True})
            (run / 'correctness.json').write_text(json.dumps(checks, indent=2))
            summary.append({'mode': run.name, 'checks': checks})
            manifests.append(json.loads((run / 'run.json').read_text()))
    for key in ('dataset_manifest_sha256', 'schema_file_sha256', 'queries_sha256'):
        if any(value[key] != manifests[0][key] for value in manifests):
            raise ValueError(f'Input changed between runs: {key}')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('Validated original JSON MAPs in four configurations:', output, flush=True)


if __name__ == '__main__':
    main()
