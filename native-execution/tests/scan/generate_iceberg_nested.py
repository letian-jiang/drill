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
"""Immutable Iceberg struct/list fixture, with original Drill reader semantics."""
import argparse
import base64
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil
import struct
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--large-array', action='store_true', help='Also test a 70003-element scalar array; original Java wide-row readers may reject it')
    parser.add_argument('--optional-struct', action='store_true', help='Nullable top/nested struct, with original inferred OPTIONAL MAP schema and no provided DDL')
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    count = 130003
    inner = pa.struct([pa.field('flag', pa.bool_())])
    member = pa.struct([pa.field('n', pa.int64()), pa.field('txt', pa.string()),
                        pa.field('ts', pa.timestamp('us')), pa.field('t', pa.time64('us')),
                        pa.field('bytes', pa.binary()), pa.field('fixed', pa.binary(4)),
                        pa.field('inner', inner, nullable=args.optional_struct)])
    item = pa.struct([pa.field('n', pa.int64()), pa.field('tag', pa.string()),
                      pa.field('ts', pa.timestamp('us')), pa.field('t', pa.time64('us'))])
    schema = pa.schema([pa.field('id', pa.int64(), nullable=False),
                        pa.field('m', member, nullable=args.optional_struct),
                        pa.field('ns', pa.list_(pa.int64())), pa.field('ss', pa.list_(pa.string())),
                        pa.field('ms', pa.list_(item)), pa.field('nested', pa.list_(pa.list_(pa.string())))])
    instants = [-1001, -1, 0, 999, 1001, 1001001, 1924992000123456]
    times = [0, 1, 1000999, 1001000, 1001001, 1001500, 86399999999]
    records, normalized = [], []
    b64 = lambda value: None if value is None else base64.b64encode(value).decode('ascii')
    for i in range(count):
        text = None if i % 29 == 0 else f'{i:06d}:雪🚀\0' + ('x' * 20000 if i % 4093 == 0 else '')
        ts = None if i % 31 == 0 else instants[i % len(instants)]
        t = None if i % 37 == 0 else times[i % len(times)]
        n = None if i % 23 == 0 else i % 997 - 400
        payload = None if i % 41 == 0 else b'\0\xff\x80' + str(i % 19).encode()
        fixed = None if i % 43 == 0 else struct.pack('<I', i)
        m = {'n': n, 'txt': text, 'ts': ts, 't': t, 'bytes': payload, 'fixed': fixed,
             'inner': {'flag': None if i % 17 == 0 else bool(i % 2)}}
        if args.optional_struct:
            if i % 7 == 0: m['inner'] = None
            if i % 11 == 0: m = None
        ns = list(range(70003)) if args.large_array and i == 0 else None if i % 11 == 0 else [None if j == 1 else i * 10 + j for j in range(i % 4)]
        ss = None if i % 13 == 0 else [None if j == 1 else f'{i:06d}_{j}:雪🚀\0' for j in range(i % 4)]
        ms = None if i % 19 == 0 else [None if j == 1 else {'n': n, 'tag': text, 'ts': ts, 't': t} for j in range(i % 4)]
        nested = None if i % 7 == 0 else [None if j == 1 else [str(i), None, str(j)] for j in range(i % 3)]
        records.append({'id': i, 'm': m, 'ns': ns, 'ss': ss, 'ms': ms, 'nested': nested})
        # Original OPTIONAL MapVector has no parent NULL bit. No writes leave
        # NULL scalar children and an always-present nested MapVector.
        if m is None:
            norm = {key: None for key in ['n','txt','ts','t','bytes','fixed']}
            norm['inner'] = {'flag': None}
        else:
            norm = dict(m, ts=None if ts is None else ts // 1000,
                        t=None if t is None else (t + 500) // 1000, bytes=b64(payload), fixed=b64(fixed))
            if norm['inner'] is None: norm['inner'] = {'flag': None}
        # ScalarArrayWriter.save is a no-op, so NULL scalar items add no entry.
        # ObjectArrayWriter.save retains NULL struct/list items as empty members.
        normalized.append({'id': i, 'm': norm, 'ns': [v for v in ns or [] if v is not None],
                           'ss': [v for v in ss or [] if v is not None],
                           'ms': [{'n': None, 'tag': None, 'ts': None, 't': None} if v is None else
                                  dict(v, ts=None if ts is None else ts // 1000,
                                       t=None if t is None else (t + 500) // 1000) for v in ms or []],
                           'nested': [[v for v in child or [] if v is not None] for child in nested or []]})
    table = pa.Table.from_pylist(records, schema=schema)
    location = out / 'warehouse' / 'nested_types'
    catalog = load_catalog('nested', type='sql', uri=f'sqlite:///{out / "catalog.db"}',
                           warehouse=(out / 'warehouse').as_uri())
    catalog.create_namespace('nested')
    iceberg = catalog.create_table(('nested', 'nested_types'), schema=schema,
                                   location=location.as_uri(), properties={'format-version': '1'})
    for start in range(0, count, 32501):
        iceberg.append(table.slice(start, 32501))
    metadata = Path(iceberg.metadata_location.removeprefix('file://'))
    pointer = location / 'metadata' / 'v1.metadata.json'
    shutil.copyfile(metadata, pointer)
    (pointer.parent / 'version-hint.text').write_text('1\n')
    (out / 'schema.sql').write_text(
        'CREATE OR REPLACE SCHEMA (id BIGINT NOT NULL, '
        'm STRUCT<n BIGINT,txt VARCHAR,ts TIMESTAMP,t TIME,bytes VARBINARY,fixed VARBINARY, '
        '`inner` STRUCT<flag BOOLEAN>>, ns ARRAY<BIGINT>, ss ARRAY<VARCHAR>, '
        'ms ARRAY<STRUCT<n BIGINT,tag VARCHAR,ts TIMESTAMP,t TIME>>, nested ARRAY<ARRAY<VARCHAR>>) '
        'FOR TABLE dfs.tpch.nested_types;\n')
    if args.optional_struct:
        # The common benchmark normally requires a provided schema file.
        # This fixture deliberately exercises the original inferred schema.
        (out / 'schema.sql').write_text('ALTER SESSION SET `store.table.use_schema_file` = false;\n')
    sql = {
        1: 'SELECT id, m, ns, ss, ms, nested FROM nested_types WHERE id < 32 ORDER BY id',
        2: 'SELECT id,t.m.n AS n,t.m.ts AS ts,t.m.t AS tm,t.m.bytes AS payload,t.m.fixed AS fixed,t.m.`inner`.flag AS flag,t.ns[0] AS first_n,t.ns[69999] AS large_n,t.ms[1].ts AS second_ts,t.nested[0][1] AS inner_value FROM nested_types t WHERE id < 128 ORDER BY id',
        3: 'SELECT count(*) AS n,sum(t.m.n) AS total,sum(repeated_count(ns)) AS numbers,sum(repeated_count(ms)) AS maps,min(t.m.t) AS lo,max(t.m.t) AS hi FROM nested_types t',
        4: 'SELECT t.m.t AS tm,count(*) AS n FROM nested_types t GROUP BY t.m.t ORDER BY tm NULLS FIRST',
        6: 'SELECT id,t.m.n AS n FROM nested_types t WHERE t.m.n > 0 AND id < 128 ORDER BY id',
        7: 'SELECT count(*) AS n,sum(v) AS total FROM (SELECT flatten(ns) AS v FROM nested_types WHERE id < 32) t',
        8: 'SELECT id,x.v.n AS n,x.v.ts AS ts,x.v.t AS tm FROM (SELECT id,flatten(ms) AS v FROM nested_types WHERE id < 32) x ORDER BY id,n,ts,tm',
        9: 'SELECT count(*) AS n,sum(cast(v AS BIGINT)) AS total FROM (SELECT flatten(a) AS v FROM (SELECT flatten(nested) AS a FROM nested_types WHERE id < 128) x) y',
        10: 'SELECT a.id AS id,b.ms AS matched FROM nested_types a JOIN nested_types b ON a.m.n=b.m.n WHERE a.id < 128 AND b.id < 128 ORDER BY id',
        11: "SELECT count(*) FROM nested_types t WHERE t.m.ts > TIMESTAMP '2031-01-01 00:00:00.123'",
        12: "SELECT count(*) FROM nested_types t WHERE t.m.t > TIME '00:00:01.001'",
        13: 'SELECT t.m.`inner`.flag AS flag,count(*) AS n FROM nested_types t GROUP BY t.m.`inner`.flag ORDER BY flag NULLS FIRST',
    }
    if args.optional_struct:
        sql[14] = 'SELECT id,t.m IS NULL AS absent,t.m.`inner` IS NULL AS absent_inner FROM nested_types t WHERE id < 128 ORDER BY id'
        sql[15] = 'SELECT count(t.m) AS n,count(t.m.`inner`) AS inner_n FROM nested_types t'
    queries = out / 'queries';queries.mkdir()
    for query, text in sql.items(): (queries / f'q{query:02}.sql').write_text(text + ';\n')
    string = lambda value: None if value is None else str(value).lower() if isinstance(value, bool) else str(value)
    prefix = normalized[:128]
    groups = Counter(row['m']['t'] for row in normalized)
    flags = Counter(row['m']['inner']['flag'] for row in normalized)
    numbers = [v for row in normalized[:32] for v in row['ns']]
    flattened = [v for row in prefix for child in row['nested'] for v in child]
    expected = {
        '1': [[str(row['id'])] + [compact(row[key]) for key in ['m','ns','ss','ms','nested']] for row in normalized[:32]],
        '2': [[str(row['id'])] + [string(row['m'][key]) for key in ['n','ts','t','bytes','fixed']] +
              [string(row['m']['inner']['flag']),string(row['ns'][0] if row['ns'] else None),
               string(row['ns'][69999] if len(row['ns'])>69999 else None),
               string(row['ms'][1]['ts'] if len(row['ms'])>1 else None),
               string(row['nested'][0][1] if row['nested'] and len(row['nested'][0])>1 else None)] for row in prefix],
        '3': [[str(count),str(sum(row['m']['n'] for row in normalized if row['m']['n'] is not None)),
               str(sum(len(row['ns']) for row in normalized)),str(sum(len(row['ms']) for row in normalized)),
               str(min(v for v in groups if v is not None)),str(max(v for v in groups if v is not None))]],
        '4': [[string(v),str(n)] for v,n in groups.items()],
        '6': [[str(row['id']),str(row['m']['n'])] for row in prefix if row['m']['n'] is not None and row['m']['n']>0],
        '7': [[str(len(numbers)),str(sum(numbers))]],
        '8': [[str(row['id']),string(v['n']),string(v['ts']),string(v['t'])] for row in normalized[:32] for v in row['ms']],
        '9': [[str(len(flattened)),str(sum(int(v) for v in flattened))]],
        '10': [[str(row['id']),compact(row['ms'])] for row in prefix if row['m']['n'] is not None],
        # Native pushed predicates see raw Iceberg precision, as does Java.
        '11': [[str(sum(row['m'] is not None and row['m']['ts'] is not None and row['m']['ts'] > 1924992000123000 for row in records))]],
        '12': [[str(sum(row['m'] is not None and row['m']['t'] is not None and row['m']['t'] > 1001000 for row in records))]],
        '13': [[string(flag),str(n)] for flag,n in flags.items()],
    }
    if args.optional_struct:
        expected['14'] = [[str(row['id']),'false','false'] for row in prefix]
        expected['15'] = [[str(count),str(count)]]
    (out / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    files=list(location.glob('data/*.parquet'))
    (out / 'manifest.json').write_text(json.dumps({'rows':count,'files':len(files),
        'schema':('OPTIONAL struct and nested OPTIONAL struct, inferred schema without provided DDL; ' if args.optional_struct else 'Required struct, nested required struct; ') + 'optional primitive/list/struct/list lists; TIME/TIMESTAMP/BINARY/FIXED leaves',
        'optional_struct':args.optional_struct,
        'null_structs':sum(row['m'] is None for row in records),
        'null_semantics':'NULL lists -> empty; scalar NULL items skipped; NULL struct/list entries retained as empty members',
        'max_scalar_array_elements':70003 if args.large_array else 3,'oracle':'Independent Python values before any engine run',
        'metadata_sha256':hashlib.sha256(pointer.read_bytes()).hexdigest(),
        'data_sha256':{str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        'expected_sha256':hashlib.sha256((out/'expected.json').read_bytes()).hexdigest()},indent=2))
    print(out,count,'rows',len(files),'files',flush=True)


if __name__ == '__main__':main()
