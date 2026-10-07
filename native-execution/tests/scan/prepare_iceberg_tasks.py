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
"""Seal real Java-written Iceberg task fixtures with independent input-based oracles."""
import argparse
from collections import defaultdict
import datetime
import hashlib
import json
from pathlib import Path
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset', type=Path, required=True)
    args = parser.parse_args()
    dataset = args.dataset.resolve()
    receipts = json.loads((dataset / 'java-files.json').read_text())
    assert len(receipts) == 9
    assert not (dataset / 'manifest.json').exists()
    rows = 130003
    live = []
    for i in range(rows):
        part, pos = divmod(i, 32501)
        length = min(32501, rows - part * 32501)
        key = None if i % 29 == 0 else i % 37
        if pos not in (0, 8191, 8192, length - 1) and key not in (None, 5, 17):
            live.append((i, '雪' if part % 2 == 0 else None, key))
    selected = lambda i: i < 128 or 4090 <= i <= 4106 or 8190 <= i <= 8206 or 65000 <= i <= 65016 or i >= 129970
    text = lambda v: None if v is None else str(v)
    groups = defaultdict(list)
    for id, p, key in live:
        groups[p].append(id)
    keys = defaultdict(list)
    for id, p, key in live:
        keys[key].append(id)
    sql = {
        1: 'SELECT count(*),sum(id),count(p) FROM identity_tasks',
        2: 'SELECT p,count(*),sum(id),min(id),max(id) FROM identity_tasks GROUP BY p ORDER BY p NULLS FIRST',
        3: 'SELECT id,p FROM identity_tasks WHERE id<128 OR id BETWEEN 4090 AND 4106 OR id BETWEEN 8190 AND 8206 OR id BETWEEN 65000 AND 65016 OR id>=129970 ORDER BY id',
        4: "SELECT count(*),sum(id) FROM identity_tasks WHERE p='雪'",
        6: 'SELECT count(*),sum(id) FROM transform_tasks WHERE k=3',
        7: 'SELECT d,count(*),sum(id) FROM transform_tasks GROUP BY d ORDER BY d',
        8: 'SELECT count(*),sum(id),min(t.m.p),max(t.m.p) FROM nested_tasks t',
        9: 'SELECT p,count(*) FROM constant_tasks GROUP BY p',
        10: 'SELECT id,t.m.p,t.m.x FROM nested_tasks t WHERE id<128 ORDER BY id',
        11: 'SELECT p FROM constant_tasks LIMIT 17 OFFSET 8190',
        12: 'SELECT count(*),sum(id) FROM identity_tasks WHERE p IS NULL',
        13: 'SELECT k,count(*),sum(id) FROM identity_tasks GROUP BY k ORDER BY k',
    }
    snow = [id for id, p, _ in live if p is not None]
    null = [id for id, p, _ in live if p is None]
    expected = {
        '1': [[str(len(live)), str(sum(r[0] for r in live)), str(len(snow))]],
        '2': [[p, str(len(ids)), str(sum(ids)), str(min(ids)), str(max(ids))] for p, ids in groups.items()],
        '3': [[str(id), p] for id, p, key in live if selected(id)],
        '4': [[str(len(snow)), str(sum(snow))]],
        '6': [['17003', str(sum(range(17003)))]],
        '7': [[str((datetime.date(2020, 1, 1) + datetime.timedelta(days=part)
                   - datetime.date(1970, 1, 1)).days * 86400000), '17003',
               str(sum(range(part * 17003, (part + 1) * 17003)))] for part in range(3)],
        '8': [[str(rows), str(sum(range(rows))), 'part雪', 'part雪']],
        '9': [['constant雪', str(rows)]],
        '10': [[str(i), 'part雪', str(i % 47)] for i in range(128)],
        '11': [['constant雪'] for _ in range(17)],
        '12': [[str(len(null)), str(sum(null))]],
        '13': [[str(k), str(len(ids)), str(sum(ids))] for k, ids in keys.items()],
    }
    queries = dataset / 'queries'
    queries.mkdir(exist_ok=False)
    for q, text in sql.items():
        (queries / f'q{q:02}.sql').write_text(text + ';\n')
    (dataset / 'schema.sql').write_text(
        'CREATE OR REPLACE SCHEMA (id BIGINT NOT NULL,p VARCHAR,k BIGINT) FOR TABLE dfs.tpch.identity_tasks;\n'
        'CREATE OR REPLACE SCHEMA (id BIGINT NOT NULL,k BIGINT,d DATE) FOR TABLE dfs.tpch.transform_tasks;\n'
        'CREATE OR REPLACE SCHEMA (id BIGINT NOT NULL,m STRUCT<x BIGINT,p VARCHAR NOT NULL>) FOR TABLE dfs.tpch.nested_tasks;\n'
        'CREATE OR REPLACE SCHEMA (p VARCHAR NOT NULL) FOR TABLE dfs.tpch.constant_tasks;\n')
    (dataset / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    files = []
    row_groups = 0
    for file in dataset.rglob('*'):
        if file.is_file():
            files.append(file)
            if file.suffix == '.parquet':
                row_groups += pq.ParquetFile(file).metadata.num_row_groups
    manifest = {'identity_rows_before_delete': rows, 'identity_live_rows': len(live),
        'native_cases': 'identity/NULL/nested/only constants; bucket/day partitions; Parquet+Avro position/equality deletes',
        'planned_input_receipts': len(receipts), 'parquet_row_groups': row_groups,
        'oracle': 'Independent Python input IDs/partition values and exact position/equality delete predicates',
        'files': {str(p.relative_to(dataset)): hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        'queries': list(sql)}
    (dataset / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(dataset, len(live), 'identity live rows;', row_groups, 'Parquet row groups;', len(sql), 'queries')

if __name__ == '__main__':
    main()
