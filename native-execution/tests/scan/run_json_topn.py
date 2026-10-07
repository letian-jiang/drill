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
"""Original JSON plugin TopN: ordered independent oracle and both native hosts."""
import argparse
from functools import cmp_to_key
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
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def generate(dataset):
    dataset.mkdir(parents=True, exist_ok=False)
    warehouse = dataset / 'warehouse' / 'events'
    warehouse.mkdir(parents=True)
    queries = dataset / 'queries'
    queries.mkdir()
    labels = ['雪', 'alpha', '🚀', '', 'a\0b']
    records = []
    for i in range(130003):
        v = None if i % 31 == 0 else i % 997 - 400
        s = None if i % 37 == 0 else labels[i % len(labels)]
        records.append({'id': i, 'k': None if i % 29 == 0 else i % 11,
                        'v': v, 's': s, 'ns': [] if i % 4 == 0 else [i, i + 1],
                        'm': {'v': v, 'txt': s}})
    reversed_rows = records[::-1]
    for part, rows in enumerate((reversed_rows[:65003], reversed_rows[65003:])):
        (warehouse / f'part{part}.json').write_text(''.join(compact(row) + '\n' for row in rows), encoding='utf-8')
    (dataset / 'schema.sql').write_text('')
    # Each tuple is field, ascending, nullsFirst. id breaks all remaining ties.
    definitions = {
        1: ([('k', True, False), ('v', False, True), ('id', True, False)], 17, 0, None),
        2: ([('k', False, True), ('v', True, False), ('id', False, False)], 107, 7, None),
        3: ([('s', True, True), ('id', True, False)], 1030, 0, None),
        4: ([('amount', False, False), ('id', True, False)], 31, 0, None),
        6: ([('k', True, True), ('k', False, False), ('id', True, False)], 1, 0, None),
        7: ([('id', False, False)], 100, 0, 37),
        8: ([('k', False, False), ('id', True, False)], 17, 65533, None),
        9: ([('s', False, False), ('id', False, False)], 17, 8190, None),
    }
    expected = {}
    for query, (keys, limit, offset, below) in definitions.items():
        ordering = ', '.join(f'{key} {"ASC" if asc else "DESC"} NULLS {"FIRST" if first else "LAST"}' for key, asc, first in keys)
        sql = ('SELECT id, k, v, s, CAST(v AS DECIMAL(18,2)) AS amount, ns, m FROM events '
               + (f'WHERE id < {below} ' if below is not None else '')
               + f'ORDER BY {ordering} LIMIT {limit} OFFSET {offset};\n')
        (queries / f'q{query:02}.sql').write_text(sql)

        def compare(a, b):
            for key, ascending, nulls_first in keys:
                key = 'v' if key == 'amount' else key
                x, y = a[key], b[key]
                if x is None or y is None:
                    result = 0 if x is None and y is None else (-1 if (x is None) == nulls_first else 1)
                else:
                    result = ((x > y) - (x < y)) * (1 if ascending else -1)
                if result:
                    return result
            return 0

        selected = [r for r in records if below is None or r['id'] < below]
        selected.sort(key=cmp_to_key(compare))
        expected[str(query)] = [[str(r['id']), None if r['k'] is None else str(r['k']),
            None if r['v'] is None else str(r['v']), r['s'],
            # The benchmark serializes BigDecimal with stripTrailingZeros.
            None if r['v'] is None else str(r['v']), compact(r['ns']), compact(r['m'])]
            for r in selected[offset:offset + limit]]
    (dataset / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    (dataset / 'manifest.json').write_text(json.dumps({'rows': len(records),
        'schema': 'NULL BIGINT/string, DECIMAL expression, required MAP and repeated BIGINT payload',
        'files': {str(p.relative_to(dataset)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in [*warehouse.glob('*.json'), *queries.glob('*.sql'), dataset / 'schema.sql', dataset / 'expected.json']},
        'oracle': 'Independent Python lexicographic sort with explicit directions/NULLs and unique id ties'}, indent=2))


def normalize(rows):
    return [row[:5] + [compact(json.loads(value)) for value in row[5:]] for row in rows]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--generate', action='store_true')
    args = parser.parse_args()
    dataset, output = args.dataset.resolve(), args.output.resolve()
    if args.generate:
        generate(dataset)
    manifest = json.loads((dataset / 'manifest.json').read_text())
    for name, sha in manifest['files'].items():
        assert hashlib.sha256((dataset / name).read_bytes()).hexdigest() == sha, name
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
                '--queries', ','.join(expected), '--output', str(run), '--query-timeout', '120', '--timeout', '1200']
            print('Running', run.name, flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            validate(run)
            if backend == 'java':
                baseline = run
            checks = []
            for number, oracle in expected.items():
                result = json.loads((run / f'q{int(number):02}.json').read_text())
                rows = normalize(result['rows'])
                assert rows == oracle, (run.name, number, 'ordered independent oracle mismatch')
                java = normalize(json.loads((baseline / f'q{int(number):02}.json').read_text())['rows'])
                assert rows == java, (run.name, number, 'ordered same-round Java mismatch')
                operators = []
                if backend != 'java':
                    prefix = result['query_id'].replace('-', '')
                    for path in (run / 'stats').glob(prefix + '.*.json'):
                        stats = json.loads(path.read_text())
                        operators.extend(op for pipeline in stats['pipelines'] for op in pipeline['operators'] if op['operator'] == 'TopN')
                    assert operators, f'{run.name}/q{number}: TopN did not execute in native'
                checks.append({'query': int(number), 'rows': len(rows), 'ordered_same_round_java': True,
                               'ordered_independent_oracle': True, 'native_topn_operators': operators})
            (run / 'correctness.json').write_text(json.dumps(checks, indent=2))
            summary.append({'mode': run.name, 'checks': checks})
            manifests.append(json.loads((run / 'run.json').read_text()))
    for key in ('dataset_manifest_sha256', 'schema_file_sha256', 'queries_sha256'):
        assert all(value[key] == manifests[0][key] for value in manifests), f'Inputs changed: {key}'
    (output / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('Validated original JSON TopN in four configurations:', output, flush=True)


if __name__ == '__main__':
    main()
