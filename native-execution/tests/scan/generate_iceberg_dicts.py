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
"""Seal an Iceberg map fixture and independent original-Drill value oracles."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import shutil

import pyarrow as pa
from pyiceberg.catalog import load_catalog


def compact(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def entries(value):
    # The benchmark walks DictReader as its original array of key/value tuples.
    return [{'key': key, 'value': item} for key, item in (value or {}).items()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    count = 130003
    member = pa.struct([pa.field('n', pa.int64()), pa.field('tags', pa.list_(pa.string()))])
    scalar = pa.map_(pa.string(), pa.int64())
    schema = pa.schema([
        pa.field('id', pa.int64(), nullable=False),
        pa.field('d', scalar), pa.field('s', pa.map_(pa.string(), pa.string())),
        pa.field('m', pa.map_(pa.string(), member)),
        pa.field('ts', pa.map_(pa.string(), pa.timestamp('us'))),
        pa.field('nested', pa.map_(pa.string(), pa.map_(pa.int32(), pa.string()))),
        pa.field('r', pa.list_(scalar)),
    ])
    records, normalized = [], []
    instants = [-1001, -1, 0, 999, 1001, 1001001, 1924992000123456]
    for i in range(count):
        absent = i % 11 == 0
        null = i % 13 == 0
        d = None if absent else {'a': None if null else i % 17 - 8}
        s = None if absent else {'雪🚀': None if null else f'{i:06d}:雪🚀\0' + ('x' * 20000 if i % 4093 == 0 else '')}
        value = None if null else {'n': None if i % 7 == 0 else i % 997,
                                   'tags': [str(i), None, '雪🚀']}
        m = None if absent else {'k': value}
        ts = None if absent else {'a': None if null else instants[i % len(instants)]}
        nested = None if absent else {'a': None if null else {7: None if i % 7 == 0 else str(i)}}
        r = None if i % 19 == 0 else [None, {'a': None if null else i}] if i % 3 else []
        records.append({'id': i, 'd': d, 's': s, 'm': m, 'ts': ts, 'nested': nested, 'r': r})
        # IcebergColumnConverterFactory uses REQUIRED map values. A NULL
        # scalar becomes the original writer default; a NULL struct/map/list
        # becomes empty children, rather than a nullable native value.
        nd = {} if absent else {'a': 0 if null else d['a']}
        ns = {} if absent else {'雪🚀': '' if null else s['雪🚀']}
        nm = {} if absent else {'k': {'n': None, 'tags': []} if value is None else
                              {'n': value['n'], 'tags': [str(i), '雪🚀']}}
        nt = {} if absent else {'a': 0 if null else ts['a'] // 1000}
        nn = {} if absent else {'a': {} if null else {7: '' if i % 7 == 0 else str(i)}}
        nr = [{} if child is None else {'a': 0 if null else i} for child in r or []]
        normalized.append({'id': i, 'd': nd, 's': ns, 'm': nm, 'ts': nt, 'nested': nn, 'r': nr})
    table = pa.Table.from_pylist(records, schema=schema)
    location = out / 'warehouse' / 'dict_types'
    catalog = load_catalog('dicts', type='sql', uri=f'sqlite:///{out / "catalog.db"}',
                           warehouse=(out / 'warehouse').as_uri())
    catalog.create_namespace('dicts')
    iceberg = catalog.create_table(('dicts', 'dict_types'), schema=schema,
                                   location=location.as_uri(), properties={'format-version': '1'})
    for start in range(0, count, 32501):
        iceberg.append(table.slice(start, 32501))
    pointer = location / 'metadata' / 'v1.metadata.json'
    shutil.copyfile(Path(iceberg.metadata_location.removeprefix('file://')), pointer)
    (pointer.parent / 'version-hint.text').write_text('1\n')
    (out / 'schema.sql').write_text(
        'CREATE OR REPLACE SCHEMA (id BIGINT NOT NULL, d MAP<VARCHAR,BIGINT NOT NULL>, '
        's MAP<VARCHAR,VARCHAR NOT NULL>, m MAP<VARCHAR,STRUCT<n BIGINT,tags ARRAY<VARCHAR>>>, '
        'ts MAP<VARCHAR,TIMESTAMP NOT NULL>, nested MAP<VARCHAR,MAP<INT,VARCHAR NOT NULL>>, '
        'r ARRAY<MAP<VARCHAR,BIGINT NOT NULL>>) FOR TABLE dfs.tpch.dict_types;\n')
    sql = {
        1: 'SELECT id,t.d.a AS a,t.d.missing AS missing,t.s.`雪🚀` AS txt,t.m.k.n AS n,t.m.k.tags[0] AS tag,t.ts.a AS ts,t.nested.a[7] AS inner_value,t.r[1].a AS repeated FROM dict_types t WHERE id < 128 ORDER BY id',
        2: 'SELECT count(*) AS n,sum(t.d.a) AS total,sum(repeated_count(r)) AS entries,min(t.ts.a) AS lo,max(t.ts.a) AS hi FROM dict_types t',
        3: 'SELECT t.d.a AS a,count(*) AS n FROM dict_types t GROUP BY t.d.a ORDER BY a NULLS FIRST',
        4: 'SELECT id,t.d.a AS a FROM dict_types t WHERE t.d.a > 0 AND id < 128 ORDER BY id',
        6: 'SELECT a.id AS a,b.id AS b,a.m.k.n AS n,b.s.`雪🚀` AS txt FROM dict_types a JOIN dict_types b ON a.d.a=b.d.a WHERE a.id < 32 AND b.id < 32 ORDER BY a,b',
        7: 'SELECT id,x.v.a AS a FROM (SELECT id,flatten(r) AS v FROM dict_types WHERE id < 128) x ORDER BY id,a NULLS FIRST',
        8: 'SELECT id,d,s,m,ts,nested,r FROM dict_types WHERE id < 32 ORDER BY id',
    }
    queries = out / 'queries'; queries.mkdir()
    for number, text in sql.items():
        (queries / f'q{number:02}.sql').write_text(text + ';\n')
    string = lambda value: None if value is None else str(value)
    prefix = normalized[:128]
    groups = Counter(row['d'].get('a') for row in normalized)
    times = [row['ts']['a'] for row in normalized if row['ts']]
    expected = {
        '1': [[str(row['id']),string(row['d'].get('a')),None,string(row['s'].get('雪🚀')),
               string(row['m'].get('k', {}).get('n')),
               string((row['m'].get('k', {}).get('tags') or [None])[0]),
               string(row['ts'].get('a')),string(row['nested'].get('a', {}).get(7)),
               string(row['r'][1].get('a') if len(row['r']) > 1 else None)] for row in prefix],
        '2': [[str(count),str(sum(row['d'].get('a', 0) for row in normalized)),
               str(sum(len(row['r']) for row in normalized)),str(min(times)),str(max(times))]],
        '3': [[string(value),str(n)] for value,n in groups.items()],
        '4': [[str(row['id']),str(row['d']['a'])] for row in prefix if row['d'].get('a', 0)>0],
        '6': [[str(a['id']),str(b['id']),string(a['m'].get('k', {}).get('n')),string(b['s'].get('雪🚀'))]
              for a in normalized[:32] for b in normalized[:32]
              if a['d'].get('a') is not None and a['d'].get('a') == b['d'].get('a')],
        '7': [[str(row['id']),string(child.get('a'))] for row in prefix for child in row['r']],
        '8': [[str(row['id']),compact(entries(row['d'])),compact(entries(row['s'])),
               compact(entries({key:dict(value, tags=value['tags']) for key,value in row['m'].items()})),
               compact(entries(row['ts'])),compact(entries({key:entries(value) for key,value in row['nested'].items()})),
               compact([entries(child) for child in row['r']])] for row in normalized[:32]],
    }
    (out / 'expected.json').write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    files = list(location.glob('data/*.parquet'))
    (out / 'manifest.json').write_text(json.dumps({
        'rows':count,'files':len(files), 'schema':'DICT scalar, struct, timestamp, nested DICT, and REPEATED DICT',
        'null_semantics':'Original Iceberg REQUIRED map values: scalar default, empty complex; NULL map -> empty',
        'oracle':'Independent Python values sealed before any engine run',
        'metadata_sha256':hashlib.sha256(pointer.read_bytes()).hexdigest(),
        'data_sha256':{str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest() for p in files},
        'expected_sha256':hashlib.sha256((out/'expected.json').read_bytes()).hexdigest(),
    }, indent=2))
    print(out,count,'rows',len(files),'files',flush=True)


if __name__ == '__main__':
    main()
