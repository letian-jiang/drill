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
"""Real original JSON arrays/Flatten, Java and native hosts, independent oracle."""
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
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def generate(dataset):
    dataset.mkdir(parents=True, exist_ok=False)
    warehouse = dataset / 'warehouse' / 'arrays'
    warehouse.mkdir(parents=True)
    queries = dataset / 'queries'
    queries.mkdir()
    records = []
    for i in range(130003):
        records.append({'id': i, 'ns': [i * 10 + j for j in range(i % 4)],
                        'ss': [f'{i:06d}_{j}:雪🚀\0' + ('x' * 20000 if i % 997 == 0 else '') for j in range(i % 3)],
                        'ms': [{'v': None if i % 29 == 0 else i + j, 'tags': [f'tag{i}_{j}', '雪\0']}
                               for j in range(i % 3)],
                        # Explicit strings: the original JSON reader commits
                        # an initially empty inner list to VARCHAR before
                        # seeing later numeric values. Keep the source typed.
                        'nested': [[] if j % 3 == 0 else [str(i + j), str(j)] for j in range(i % 5)]})
    for part, rows in enumerate((records[:65003], records[65003:])):
        (warehouse / f'part{part}.json').write_text(''.join(compact(row) + '\n' for row in rows), encoding='utf-8')
    (dataset / 'schema.sql').write_text('')
    sql = {
        1: 'SELECT id, ns, ss, ms, nested FROM arrays ORDER BY id LIMIT 128',
        2: 'SELECT id, t.ns[0] AS n, t.ns[99] AS outside, t.ss[0] AS s, t.ms[0].v AS v, '
           't.ms[0].tags[1] AS tag, t.nested[1][0] AS inner_value, repeated_count(ns) AS length '
           'FROM arrays t WHERE id < 128 ORDER BY id',
        3: 'SELECT COUNT(*) AS n, SUM(repeated_count(ns)) AS numbers, SUM(repeated_count(ms)) AS maps, '
           'COUNT(t.ns[0]) AS present, MIN(t.ss[0]) AS first, MAX(t.ss[0]) AS last FROM arrays t',
        4: 'SELECT t.ns[0] AS n, COUNT(*) AS cnt FROM arrays t WHERE id < 128 GROUP BY t.ns[0] ORDER BY n',
        6: 'SELECT a.id AS id, b.ms AS matched, a.nested AS nested FROM arrays a JOIN arrays b '
           'ON a.ns[0] = b.ns[0] WHERE a.id < 128 AND b.id < 128 ORDER BY id',
        7: 'SELECT id, FLATTEN(ns) AS n FROM arrays WHERE id < 128 ORDER BY id, n',
        8: 'SELECT COUNT(*) AS n, SUM(t.v.v) AS total, SUM(repeated_count(t.v.tags)) AS tags '
           'FROM (SELECT FLATTEN(ms) AS v FROM arrays WHERE id < 128) t',
        9: 'SELECT COUNT(*) AS n, SUM(v) AS total FROM '
           '(SELECT FLATTEN(a) AS v FROM (SELECT FLATTEN(nested) AS a FROM arrays WHERE id < 128) t) u',
        10: 'SELECT id, FLATTEN(ns) AS n, FLATTEN(ss) AS s FROM arrays WHERE id < 128 ORDER BY id, n, s',
    }
    for query, text in sql.items():
        (queries / f'q{query:02}.sql').write_text(text + ';\n')
    string = lambda x: None if x is None else str(x)
    groups = Counter(row['ns'][0] if row['ns'] else None for row in records[:128])
    first_strings = [row['ss'][0] for row in records if row['ss']]
    maps = [m for row in records[:128] for m in row['ms']]
    flattened = [v for row in records[:128] for a in row['nested'] for v in a]
    expected = {
        '1': [[str(row['id'])] + [compact(row[col]) for col in ('ns', 'ss', 'ms', 'nested')] for row in records[:128]],
        '2': [[str(row['id']), string(row['ns'][0] if row['ns'] else None), None,
               row['ss'][0] if row['ss'] else None, string(row['ms'][0]['v'] if row['ms'] else None),
               row['ms'][0]['tags'][1] if row['ms'] else None,
               string(row['nested'][1][0] if len(row['nested']) > 1 and row['nested'][1] else None),
               str(len(row['ns']))] for row in records[:128]],
        '3': [[str(len(records)), str(sum(len(row['ns']) for row in records)),
               str(sum(len(row['ms']) for row in records)), str(sum(bool(row['ns']) for row in records)),
               min(first_strings), max(first_strings)]],
        '4': [[string(n), str(count)] for n, count in groups.items()],
        '6': [[str(row['id']), compact(row['ms']), compact(row['nested'])] for row in records[:128] if row['ns']],
        '7': [[str(row['id']), str(n)] for row in records[:128] for n in row['ns']],
        '8': [[str(len(maps)), str(sum(m['v'] for m in maps if m['v'] is not None)), str(sum(len(m['tags']) for m in maps))]],
        '9': [[str(len(flattened)), str(sum(int(v) for v in flattened))]],
        '10': [[str(row['id']), str(n), s] for row in records[:128] for n in row['ns'] for s in row['ss']],
    }
    (dataset / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    (dataset / 'manifest.json').write_text(json.dumps({'rows': len(records),
        'schema': 'Repeated BIGINT/VARCHAR/MAP and nested VARCHAR LIST; NULL map leaves, empty arrays and long UTF-8/NUL',
        'files': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in warehouse.glob('*.json')},
        'oracle': 'Independent Python values, named MAP fields, ordered arrays and sequential Flatten Cartesian expansion'}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--generate', action='store_true')
    args = parser.parse_args()
    dataset, output = args.dataset.resolve(), args.output.resolve()
    if args.generate:
        generate(dataset)
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
                       '--queries', ','.join(expected), '--output', str(run), '--query-timeout', '90', '--timeout', '900']
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
                    raise ValueError(f'{run.name}/q{query}: independent array/Flatten mismatch')
                if Counter(map(tuple, rows)) != Counter(map(tuple, java)):
                    raise ValueError(f'{run.name}/q{query}: Java array/Flatten mismatch')
                checks.append({'query': int(query), 'rows': len(rows), 'same_round_java': True, 'independent_oracle': True})
            (run / 'correctness.json').write_text(json.dumps(checks, indent=2))
            summary.append({'mode': run.name, 'checks': checks})
            manifests.append(json.loads((run / 'run.json').read_text()))
    for key in ('dataset_manifest_sha256', 'schema_file_sha256', 'queries_sha256'):
        if any(value[key] != manifests[0][key] for value in manifests):
            raise ValueError(f'Input changed between runs: {key}')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('Validated original JSON arrays and Flatten in four configurations:', output, flush=True)


if __name__ == '__main__':
    main()
