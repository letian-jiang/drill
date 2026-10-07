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
"""Immutable typed Iceberg fixture and independent millisecond/binary oracles."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import shutil
import struct
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def instant(millis):
    # The benchmark RowSet accessor exposes TIMESTAMP as epoch milliseconds.
    return None if millis is None else str(millis)


def binary(value):
    return None if value is None else base64.b64encode(value).decode('ascii')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--include-time', action='store_true')
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    count = 130003
    edges = [-1001001, -1001000, -1000999, -1001, -1000, -999, -1,
             0, 1, 999, 1000, 1001, 864000000001, 1924992000123456]
    raw = [edges[i % len(edges)] for i in range(count)]
    ts = [None if i % 29 == 0 else value for i, value in enumerate(raw)]
    utc = [None if i % 41 == 0 else value for i, value in enumerate(raw)]
    payload = [None if i % 31 == 0 else b'\0\xff\x80' * 8192 if i % 4093 == 0 else
               b'' if i % 17 == 0 else b'\xff\x80\0' if i % 11 == 0 else
               f'blob{i % 19}'.encode() for i in range(count)]
    fixed = [None if i % 37 == 0 else struct.pack('<I', i) for i in range(count)]
    table = pa.table({'id': pa.array(range(count), type=pa.int64()),
                      'ts': pa.array(ts, type=pa.timestamp('us')),
                      'utc_ts': pa.array(utc, type=pa.timestamp('us', tz='UTC')),
                      'payload': pa.array(payload, type=pa.binary()),
                      'fixed': pa.array(fixed, type=pa.binary(4))})
    time_edges = [0, 1, 999, 1000, 1001, 1000999, 1001000, 1001001,
                  1001999, 43200000123, 86399999000, 86399999999]
    time_raw = [None if i % 23 == 0 else time_edges[i % len(time_edges)] for i in range(count)]
    if args.include_time:
        table = table.append_column('t', pa.array(time_raw, type=pa.time64('us')))
    location = out / 'warehouse' / 'native_types'
    catalog = load_catalog('typed', type='sql', uri=f'sqlite:///{out / "catalog.db"}',
                           warehouse=(out / 'warehouse').as_uri())
    catalog.create_namespace('typed')
    iceberg = catalog.create_table(('typed', 'native_types'), schema=table.schema,
                                   location=location.as_uri(), properties={'format-version': '1'})
    for start in range(0, count, 32501):
        iceberg.append(table.slice(start, 32501))
    metadata = Path(iceberg.metadata_location.removeprefix('file://'))
    pointer = location / 'metadata' / 'v1.metadata.json'
    shutil.copyfile(metadata, pointer)
    (pointer.parent / 'version-hint.text').write_text('1\n')
    (out / 'schema.sql').write_text('CREATE OR REPLACE SCHEMA (id BIGINT, ts TIMESTAMP, '
        'utc_ts TIMESTAMP, payload VARBINARY, fixed VARBINARY'
        + (', t TIME' if args.include_time else '') + ') FOR TABLE dfs.tpch.native_types;\n')
    queries = out / 'queries'
    queries.mkdir()
    sql = {
        1: 'SELECT id,ts,utc_ts,payload,fixed FROM native_types WHERE id < 128 ORDER BY id',
        2: 'SELECT ts,utc_ts,count(*) AS n,min(payload) AS lo,max(payload) AS hi,min(fixed) AS f_lo,max(fixed) AS f_hi FROM native_types GROUP BY ts,utc_ts ORDER BY ts NULLS FIRST,utc_ts NULLS FIRST',
        3: 'SELECT count(*),count(ts),count(utc_ts),count(payload),sum(hash32asdouble(ts,1301011)),sum(hash32asdouble(payload,1301011)),sum(hash32asdouble(fixed,1301011)) FROM native_types',
        4: "SELECT id,ts,utc_ts,payload,fixed FROM native_types WHERE id < 128 AND ts > TIMESTAMP '1970-01-01 00:00:00' ORDER BY id",
        6: "SELECT count(*) FROM native_types WHERE ts > TIMESTAMP '2031-01-01 00:00:00.123'",
    }
    if args.include_time:
        sql[1] = 'SELECT id,ts,utc_ts,payload,fixed,t FROM native_types WHERE id < 128 ORDER BY id'
        sql[2] = 'SELECT t,count(*) AS n,min(t) AS lo,max(t) AS hi,min(ts) AS ts_lo,max(ts) AS ts_hi FROM native_types GROUP BY t ORDER BY t NULLS FIRST'
        sql[3] = 'SELECT count(*),count(t),sum(hash32asdouble(t,1301011)) FROM native_types'
        sql[7] = "SELECT count(*) FROM native_types WHERE t > TIME '00:00:01.001'"
        sql[8] = "SELECT id,t FROM native_types WHERE id < 128 AND t > TIME '00:00:01.001' ORDER BY id"
        sql[9] = 'SELECT a.t,count(*) AS n FROM native_types a JOIN native_types b ON a.t=b.t WHERE a.id<128 AND b.id<128 GROUP BY a.t ORDER BY a.t'
        sql[10] = 'SELECT min(t),max(t),count(t),count(*) FROM native_types'
    for query, text in sql.items():
        (queries / f'q{query:02}.sql').write_text(text + ';\n')
    normalized = [(None if ts[i] is None else ts[i] // 1000,
                   None if utc[i] is None else utc[i] // 1000) for i in range(count)]
    projected = [[str(i), instant(normalized[i][0]), instant(normalized[i][1]),
                  binary(payload[i]), binary(fixed[i])] for i in range(128)]
    groups = {}
    for i, key in enumerate(normalized):
        entry = groups.setdefault(key, [0, [], []])
        entry[0] += 1
        if payload[i] is not None: entry[1].append(payload[i])
        if fixed[i] is not None: entry[2].append(fixed[i])
    grouped = [[instant(key[0]), instant(key[1]), str(values[0]),
                binary(min(values[1])) if values[1] else None,
                binary(max(values[1])) if values[1] else None,
                binary(min(values[2])) if values[2] else None,
                binary(max(values[2])) if values[2] else None] for key, values in groups.items()]
    # This plan retains a Drill Filter above scan: compare normalized milliseconds.
    selected = [row for i, row in enumerate(projected) if normalized[i][0] is not None and normalized[i][0] > 0]
    # This count plan pushes the complete predicate into the plugin (raw microseconds).
    later = [[str(sum(value is not None and value > 1924992000123000 for value in ts))]]
    expected = {'1': projected, '2': grouped, '4': selected, '6': later}
    if args.include_time:
        # RowSet exposes TIME as its original integer millisecond payload.
        # DateUtilities.toDrillTime rounds half up, including 86400000 at the
        # day boundary; preserve the original payload rather than wrap to 0.
        time_ms = [None if value is None else (value + 500) // 1000 for value in time_raw]
        time_text = lambda value: None if value is None else str(value)
        expected['1'] = [row + [time_text(time_ms[i])] for i, row in enumerate(projected)]
        time_groups = {}
        for i, key in enumerate(time_ms):
            entry = time_groups.setdefault(key, [0, []])
            entry[0] += 1
            if normalized[i][0] is not None: entry[1].append(normalized[i][0])
        expected['2'] = [[time_text(key), str(n), time_text(key), time_text(key),
            instant(min(values)) if values else None, instant(max(values)) if values else None]
            for key, (n, values) in time_groups.items()]
        expected['7'] = [[str(sum(value is not None and value > 1001000 for value in time_raw))]]
        expected['8'] = [[str(i), str(time_ms[i])] for i in range(128)
                         if time_ms[i] is not None and time_ms[i] > 1001]
        from collections import Counter
        prefix_counts = Counter(value for value in time_ms[:128] if value is not None)
        expected['9'] = [[str(key), str(n * n)] for key, n in prefix_counts.items()]
        valid_times = [value for value in time_ms if value is not None]
        expected['10'] = [[str(min(valid_times)), str(max(valid_times)), str(len(valid_times)), str(count)]]
    (out / 'expected.json').write_text(json.dumps(expected, indent=2))
    files = list(location.glob('data/*.parquet'))
    (out / 'manifest.json').write_text(json.dumps({'rows': count, 'files': len(files),
        'raw_timestamp_unit': 'microseconds', 'drill_timestamp_unit': 'milliseconds',
        'types': ['BIGINT', 'TIMESTAMP', 'TIMESTAMPTZ', 'BINARY', 'FIXED[4]'] + (['TIME'] if args.include_time else []),
        'queries': sorted(sql),
        'metadata_sha256': hashlib.sha256(pointer.read_bytes()).hexdigest(),
        'expected_sha256': hashlib.sha256((out / 'expected.json').read_bytes()).hexdigest()}, indent=2))
    print(out, count, 'rows,', len(files), 'files', flush=True)


if __name__ == '__main__':
    main()
