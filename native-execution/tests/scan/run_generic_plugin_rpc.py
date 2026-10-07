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
"""Original CSV/JSON/Parquet readers in a C++ RPC worker, with no Java Drillbit."""
import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import pyarrow as pa
import pyarrow.parquet as pq

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_paths import REPO_ROOT, MODULE_ROOT, BENCHMARK, TOOLS, DEFAULT_DATA, maven
ROOT = REPO_ROOT
TEXT_COLUMN = '文本🚀'

def text_value(index, nullable):
    if nullable and index % 29 == 0:
        return None
    return f'value{index}:雪🚀' if index % 17 == 0 else f'value{index}'

def expected_values(counts, nullable):
    values = [text_value(i, nullable) for count in counts for i in range(count)]
    strings = [value for value in values if value is not None]
    return {'n': sum(counts), 's': sum(count * (count - 1) // 2 for count in counts),
            'strings': len(strings), 'first': min(strings), 'last': max(strings)}

def available_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--classpath-file', type=Path, required=True, help='A foreman-ready.json from a completed integration run')
    parser.add_argument('--executable', type=Path, default=TOOLS / 'native-runtime/build/native_generic_plugin_rpc_test')
    args = parser.parse_args()
    environment = dict(os.environ)
    metadata = json.loads(args.classpath_file.read_text())
    environment['DRILL_NATIVE_SCAN_CLASSPATH'] = metadata['classpath']
    environment['DRILL_NATIVE_JVM_LIBRARY'] = str(Path(shutil.which('java')).resolve().parents[1] / 'lib/server/libjvm.so')
    with tempfile.TemporaryDirectory(prefix='drill-generic-plugin-') as directory:
        directory = Path(directory) / '原始-reader-🚀'
        directory.mkdir()
        scans = []
        for format_name, format_config in (
                ('csv', {'type': 'text', 'extensions': ['csv'], 'extractHeader': True}),
                ('json', {'type': 'json'})):
            # Original HeaderBuilder rewrites both UTF-16 surrogate units of
            # the CSV header emoji to underscores. Keep that plugin behavior.
            text_column = '文本__' if format_name == 'csv' else TEXT_COLUMN
            files = []
            counts = (10003, 157)
            for part, count in enumerate(counts):
                path = directory / f'part{part}.{format_name}'
                if format_name == 'csv':
                    path.write_text(f'v,{TEXT_COLUMN}\n' + ''.join(f'{i},{text_value(i, False)}\n' for i in range(count)), encoding='utf-8')
                else:
                    path.write_text(''.join(json.dumps({'v': i, TEXT_COLUMN: text_value(i, True)}, ensure_ascii=False) + '\n' for i in range(count)), encoding='utf-8')
                files.append({'path': str(path), 'start': 0, 'length': path.stat().st_size})
            scans.append({'name': format_name, 'textColumn': text_column, 'expected': expected_values(counts, format_name == 'json'), 'scan': {
                'pop': 'fs-sub-scan', '@id': 7, 'userName': 'scan-test-user', 'files': files,
                'storage': {'type': 'file', 'connection': 'file:///', 'enabled': True,
                            'formats': {format_name: format_config}}, 'format': format_config,
                'columns': ['`v`', f'`{text_column}`'], 'selectionRoot': str(directory), 'partitionDepth': 0, 'limit': -1}})
        path = directory / 'original.parquet'
        pq.write_table(pa.table({'v': pa.array(range(130003), type=pa.int64()),
                                TEXT_COLUMN: pa.array([text_value(i, True) for i in range(130003)])}), path, row_group_size=20000)
        parquet = pq.ParquetFile(path)
        entries = [{'path': str(path), 'start': 0, 'length': path.stat().st_size,
                    'rowGroupIndex': i, 'numRecordsToRead': parquet.metadata.row_group(i).num_rows}
                   for i in range(parquet.metadata.num_row_groups)]
        scans.append({'name': 'parquet', 'textColumn': TEXT_COLUMN, 'expected': expected_values((130003,), True), 'scan': {
            'pop': 'parquet-row-group-scan', '@id': 7, 'userName': 'scan-test-user',
            'storageConfig': {'type': 'file', 'connection': 'file:///', 'enabled': True,
                              'formats': {'parquet': {'type': 'parquet'}}},
            'formatConfig': {'type': 'parquet'}, 'rowGroupReadEntries': entries,
            'columns': ['`v`', f'`{TEXT_COLUMN}`'], 'selectionRoot': str(directory), 'schema': None}})
        count = 130003
        timestamps = [None if i % 29 == 0 else (i - 65000) * 1001 + 17 for i in range(count)]
        binary = [None if i % 31 == 0 else b'' if i % 17 == 0 else
                  b'\xff\x80\0' + i.to_bytes(4, 'little') if i % 11 == 0 else
                  f'blob{i}'.encode('ascii') for i in range(count)]
        path = directory / 'timestamp-binary.parquet'
        pq.write_table(pa.table({'v': pa.array(range(count), type=pa.int64()),
                                'ts': pa.array(timestamps, type=pa.timestamp('ms')),
                                'binary': pa.array(binary, type=pa.binary())}), path, row_group_size=20000)
        parquet = pq.ParquetFile(path)
        temporal_scan = dict(scans[-1]['scan'])
        temporal_scan['rowGroupReadEntries'] = [
            {'path': str(path), 'start': 0, 'length': path.stat().st_size,
             'rowGroupIndex': i, 'numRecordsToRead': parquet.metadata.row_group(i).num_rows}
            for i in range(parquet.metadata.num_row_groups)]
        temporal_scan['columns'] = ['`v`', '`ts`', '`binary`']
        present_ts = [value for value in timestamps if value is not None]
        present_binary = [value for value in binary if value is not None]
        scans.append({'name': 'parquet-timestamp-binary', 'scan': temporal_scan,
                      'aggregates': [{'ref': f'`{name}`', 'expr': expr} for name, expr in (
                          ('n', 'count(`v`)'), ('ts_count', 'count(`ts`)'),
                          ('ts_min', 'min(`ts`)'), ('ts_max', 'max(`ts`)'),
                          ('binary_count', 'count(`binary`)'), ('binary_min', 'min(`binary`)'),
                          ('binary_max', 'max(`binary`)'))],
                      'expected': {'n': count, 'ts_count': len(present_ts),
                                   'ts_min': min(present_ts), 'ts_max': max(present_ts),
                                   'binary_count': len(present_binary),
                                   'binary_min': base64.b64encode(min(present_binary)).decode('ascii'),
                                   'binary_max': base64.b64encode(max(present_binary)).decode('ascii')}})
        # Original JSON reader supplies required MapVector parents, nullable
        # leaves and flattened child buffers. Cross several original batches.
        count = 130003
        numbers = [None if i % 29 == 0 else i for i in range(count)]
        strings = [None if i % 31 == 0 else f'{i:06d}:雪🚀\0' for i in range(count)]
        path = directory / 'nested-map.json'
        path.write_text(''.join(json.dumps({'m': {'n': numbers[i], 'inner': {'s': strings[i]}},
                                             'literal.dot': f'quoted{i}'}, ensure_ascii=False) + '\n'
                                for i in range(count)), encoding='utf-8')
        scan = dict(scans[1]['scan'])
        scan['files'] = [{'path': str(path), 'start': 0, 'length': path.stat().st_size}]
        scan['columns'] = ['`m`', '`literal.dot`']
        present_numbers = [value for value in numbers if value is not None]
        present_strings = [value for value in strings if value is not None]
        scans.append({'name': 'json-nested-map', 'scan': scan,
                      'aggregates': [{'ref': f'`{name}`', 'expr': expr} for name, expr in (
                          ('n', 'count(`literal.dot`)'), ('number_count', 'count(`m`.`n`)'),
                          ('number_sum', 'sum(cast(`m`.`n` as BIGINT))'),
                          ('string_count', 'count(`m`.`inner`.`s`)'),
                          ('string_min', 'min(`m`.`inner`.`s`)'),
                          ('string_max', 'max(`m`.`inner`.`s`)'))],
                      'expected': {'n': count, 'number_count': len(present_numbers),
                                   'number_sum': sum(present_numbers), 'string_count': len(present_strings),
                                   'string_min': min(present_strings), 'string_max': max(present_strings)}})
        manifest = directory / 'scans.json'
        manifest.write_text(json.dumps(scans, ensure_ascii=False), encoding='utf-8')
        subprocess.run([str(args.executable.resolve()), str(manifest)], env=environment, cwd=ROOT, check=True, timeout=180)

if __name__ == '__main__':
    main()
