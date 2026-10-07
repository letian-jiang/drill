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
"""Original JSON Window queries: four host configurations and independent values."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
import math
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
    records = []
    for i in range(130003):
        # Interleave the large NULL partition with typed keys in every file.
        # An original JSON reader starting with an all-NULL file otherwise
        # commits that column to VARCHAR, changing its schema on the next file.
        p = None if i % 13 < 7 else i % 7
        v = None if p == 6 or i % 31 == 0 else i % 997 - 400
        s = None if i % 37 == 0 else f'{i:06d}:雪🚀\0'
        records.append({'id': i, 'p': p, 'k': None if i % 29 == 0 else i % 11,
                        'v': v, 's': s, 'm': {'v': v, 'txt': s},
                        'ns': [] if i % 4 == 0 else [i, i + 1]})
    reversed_rows = records[::-1]
    for part, rows in enumerate((reversed_rows[:65003], reversed_rows[65003:])):
        (warehouse / f'part{part}.json').write_text(''.join(compact(row) + '\n' for row in rows), encoding='utf-8')
    (dataset / 'schema.sql').write_text('')
    filters = 'id < 32 OR id BETWEEN 4090 AND 4106 OR id BETWEEN 8190 AND 8206 OR ' \
              'id BETWEEN 65520 AND 65545 OR id BETWEEN 69995 AND 70012 OR id >= 129970'
    selected = {row['id'] for row in records if row['id'] < 32 or 4090 <= row['id'] <= 4106
                or 8190 <= row['id'] <= 8206 or 65520 <= row['id'] <= 65545
                or 69995 <= row['id'] <= 70012 or row['id'] >= 129970}
    by_id = 'PARTITION BY p ORDER BY id'
    by_key = 'PARTITION BY p ORDER BY k NULLS LAST'
    rows = by_id + ' ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'
    full = by_id + ' RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING'
    peer = by_key + ' RANGE BETWEEN CURRENT ROW AND CURRENT ROW'
    definitions = {
        1: ('id, RANK() OVER w AS r, DENSE_RANK() OVER w AS d, PERCENT_RANK() OVER w AS pr, CUME_DIST() OVER w AS cd', by_key),
        2: ('id, ROW_NUMBER() OVER w AS n, NTILE(3) OVER w AS tile', by_id),
        3: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi, '
            'AVG(v) OVER w AS average, SUM(v + 3) OVER w AS extra, COUNT(*) OVER w AS total', rows),
        4: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi', by_key),
        6: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi', peer),
        7: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi, AVG(v) OVER w AS average', 'PARTITION BY p'),
        8: ('id, LAG(v) OVER w AS previous_v, LEAD(v, 1) OVER w AS next_v, LAG(s, 1) OVER w AS previous_s, LEAD(s) OVER w AS next_s', by_id),
        9: ('id, FIRST_VALUE(v) OVER w AS first_v, LAST_VALUE(v) OVER w AS last_v', rows),
        10: ('id, FIRST_VALUE(k) OVER w AS first_k, LAST_VALUE(k) OVER w AS last_k', peer),
        11: ('id, FIRST_VALUE(v) OVER w AS first_v, LAST_VALUE(v) OVER w AS last_v, '
             'FIRST_VALUE(s) OVER w AS first_s, LAST_VALUE(s) OVER w AS last_s', full),
        12: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n, MIN(v) OVER w AS lo, MAX(v) OVER w AS hi', ''),
        13: ('id, SUM(t.m.v + 3) OVER w AS extra FROM events t', rows),
        15: ('id, ns, m, ROW_NUMBER() OVER w AS n', by_id),
        16: ('id, ROW_NUMBER() OVER w AS n, SUM(v) OVER w AS s, LAG(v) OVER w AS previous_v, '
             'LEAD(v) OVER w AS next_v, FIRST_VALUE(v) OVER w AS first_v, LAST_VALUE(v) OVER w AS last_v',
             'PARTITION BY p ORDER BY id DESC'),
        17: ('id, SUM(v) OVER w AS s, COUNT(v) OVER w AS n', 'ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW'),
    }
    sql = {}
    for query, (columns, window) in definitions.items():
        source = columns if query == 13 else columns + ' FROM events'
        sql[query] = f'SELECT * FROM (SELECT {source} WINDOW w AS ({window})) t WHERE {filters} ORDER BY id'
    sql[14] = ('SELECT * FROM (SELECT id, running, SUM(running) OVER (PARTITION BY p ORDER BY id '
              'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS second_sum FROM '
              '(SELECT id, p, SUM(v) OVER (PARTITION BY p ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM events) a) t '
              f'WHERE {filters} ORDER BY id')
    for query, text in sql.items():
        (queries / f'q{query:02}.sql').write_text(text + ';\n')
    groups = defaultdict(list)
    for record in records:
        groups[record['p']].append(record)
    expected = {str(query): {} for query in sql}
    string = lambda value: None if value is None else str(value)

    def save(query, record, values):
        if record['id'] in selected:
            expected[str(query)][record['id']] = [str(record['id'])] + [string(v) for v in values]

    def numeric(values):
        present = [v for v in values if v is not None]
        return [sum(present) if present else None, len(present), min(present) if present else None, max(present) if present else None]

    for partition in groups.values():
        length = len(partition)
        totals = numeric(row['v'] for row in partition)
        prefix_sum = prefix_count = second_sum = second_count = 0
        minimum = maximum = None
        for index, row in enumerate(partition):
            v = row['v']
            if v is not None:
                prefix_sum += v
                prefix_count += 1
                minimum = v if minimum is None else min(minimum, v)
                maximum = v if maximum is None else max(maximum, v)
            if prefix_count:
                second_sum += prefix_sum
                second_count += 1
            large, small = length // 3 + 1, length // 3
            boundary = length % 3 * large
            bucket = index // large + 1 if index < boundary else length % 3 + (index - boundary) // small + 1
            running = prefix_sum if prefix_count else None
            save(2, row, [index + 1, bucket])
            save(3, row, [running, prefix_count, minimum, maximum, prefix_sum / prefix_count if prefix_count else None,
                          prefix_sum + 3 * prefix_count if prefix_count else None, index + 1])
            save(7, row, totals + [totals[0] / totals[1] if totals[1] else None])
            previous = partition[index - 1] if index else {'v': None, 's': None}
            following = partition[index + 1] if index + 1 < length else {'v': None, 's': None}
            save(8, row, [previous['v'], following['v'], previous['s'], following['s']])
            save(9, row, [partition[0]['v'], v])
            save(11, row, [partition[0]['v'], partition[-1]['v'], partition[0]['s'], partition[-1]['s']])
            save(13, row, [prefix_sum + 3 * prefix_count if prefix_count else None])
            save(14, row, [running, second_sum if second_count else None])
            save(15, row, [compact(row['ns']), compact(row['m']), index + 1])
        peers = defaultdict(list)
        for row in partition:
            peers[row['k']].append(row)
        seen = present_count = present_sum = 0
        minimum = maximum = None
        for dense, key in enumerate(sorted(peers, key=lambda k: (k is None, k or 0)), 1):
            peer_rows = peers[key]
            peer_values = numeric(row['v'] for row in peer_rows)
            if peer_values[1]:
                present_count += peer_values[1]
                present_sum += peer_values[0]
                minimum = peer_values[2] if minimum is None else min(minimum, peer_values[2])
                maximum = peer_values[3] if maximum is None else max(maximum, peer_values[3])
            for row in peer_rows:
                save(1, row, [seen + 1, dense, seen / (length - 1) if length > 1 else 0.0, (seen + len(peer_rows)) / length])
                save(4, row, [present_sum if present_count else None, present_count, minimum, maximum])
                save(6, row, peer_values)
                save(10, row, [key, key])
            seen += len(peer_rows)
        reverse = partition[::-1]
        total = present = 0
        for index, row in enumerate(reverse):
            if row['v'] is not None:
                total += row['v']; present += 1
            save(16, row, [index + 1, total if present else None,
                reverse[index - 1]['v'] if index else None,
                reverse[index + 1]['v'] if index + 1 < length else None, reverse[0]['v'], row['v']])
    global_values = numeric(row['v'] for row in records)
    total = present = 0
    for row in records:
        save(12, row, global_values)
        if row['v'] is not None:
            total += row['v']; present += 1
        save(17, row, [total if present else None, present])
    output = {query: [values[i] for i in sorted(values)] for query, values in expected.items()}
    (dataset / 'expected.json').write_text(json.dumps(output, ensure_ascii=False, indent=2))
    (dataset / 'manifest.json').write_text(json.dumps({'rows': len(records), 'selected_rows': len(selected),
        'schema': 'Nullable BIGINT partition/order/value; UTF-8/NUL strings; required MAP and REPEATED BIGINT',
        'large_null_partition_rows': 70003, 'all_null_value_partition': 6,
        'files': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in warehouse.glob('*.json')},
        'oracle': 'Independent Python partition/peer/rank/frame values; no engine output'}, indent=2))


def validate_rows(query, rows, oracle):
    assert len(rows) == len(oracle), (query, len(rows), len(oracle))
    actual = {int(row[0]): row for row in rows}
    assert len(actual) == len(rows), f'q{query}: duplicate rows'
    floats = {1: {3, 4}, 3: {5}, 7: {5}}.get(query, set())
    for reference in oracle:
        row = actual[int(reference[0])]
        assert len(row) == len(reference)
        for column, value in enumerate(reference):
            observed = row[column]
            if column in floats and value is not None:
                assert observed is not None and math.isclose(float(observed), float(value), rel_tol=1e-14, abs_tol=1e-15), (query, row[0], column, observed, value)
            else:
                assert observed == value, (query, row[0], column, observed, value)


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
                '--queries', ','.join(expected), '--output', str(run), '--query-timeout', '120', '--timeout', '1200']
            print('Running', run.name, flush=True)
            subprocess.run(command, cwd=ROOT, check=True)
            validate(run)
            if backend == 'java':
                baseline = run
            checks = []
            for number, oracle in expected.items():
                query = int(number)
                result = json.loads((run / f'q{query:02}.json').read_text())
                rows = result['rows']
                validate_rows(query, rows, oracle)
                java = json.loads((baseline / f'q{query:02}.json').read_text())['rows']
                assert Counter(map(tuple, rows)) == Counter(map(tuple, java)), f'{run.name}/q{query}: same-round Java mismatch'
                operators = []
                if backend != 'java':
                    prefix = result['query_id'].replace('-', '')
                    for path in (run / 'stats').glob(prefix + '.*.json'):
                        stats = json.loads(path.read_text())
                        operators.extend(op for pipeline in stats['pipelines'] for op in pipeline['operators'] if op['operator'] == 'Window')
                    assert operators, f'{run.name}/q{query}: Window did not execute in native'
                checks.append({'query': query, 'rows': len(rows), 'same_round_java': True,
                               'independent_oracle': True, 'native_window_operators': len(operators),
                               'native_window_drivers': [op['drivers'] for op in operators]})
            (run / 'correctness.json').write_text(json.dumps(checks, indent=2))
            summary.append({'mode': run.name, 'checks': checks})
            manifests.append(json.loads((run / 'run.json').read_text()))
    for key in ('dataset_manifest_sha256', 'schema_file_sha256', 'queries_sha256'):
        assert all(value[key] == manifests[0][key] for value in manifests), f'Inputs changed: {key}'
    (output / 'summary.json').write_text(json.dumps(summary, indent=2))
    print('Validated original JSON Windows in four configurations:', output, flush=True)


if __name__ == '__main__':
    main()
