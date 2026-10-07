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
"""Actual Iceberg C ABI: identity constants, delete unions, ranges and ownership."""
import argparse
import ctypes as ct
import datetime as dt
from decimal import Decimal
import gc
import json
import os
from pathlib import Path
import struct
import tempfile
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

class ArrowSchema(ct.Structure):
    pass
class ArrowArray(ct.Structure):
    pass
ArrowSchema._fields_ = [('format', ct.c_char_p), ('name', ct.c_char_p), ('metadata', ct.c_void_p),
    ('flags', ct.c_int64), ('n_children', ct.c_int64), ('children', ct.POINTER(ct.POINTER(ArrowSchema))),
    ('dictionary', ct.POINTER(ArrowSchema)), ('release', ct.c_void_p), ('private_data', ct.c_void_p)]
ArrowArray._fields_ = [(n, ct.c_int64) for n in ('length', 'null_count', 'offset', 'n_buffers', 'n_children')] + [
    ('buffers', ct.POINTER(ct.c_void_p)), ('children', ct.POINTER(ct.POINTER(ArrowArray))),
    ('dictionary', ct.POINTER(ArrowArray)), ('release', ct.c_void_p), ('private_data', ct.c_void_p)]

def field(name, type, id, required=False):
    return pa.field(name, type, nullable=not required, metadata={b'PARQUET:field_id': str(id).encode()})

def schema(fields):
    return {'type': 'struct', 'schema-id': 0, 'fields': fields}

def f(id, name, type, required=False):
    return {'id': id, 'name': name, 'required': required, 'type': type}

class Library:
    def __init__(self, path):
        self.lib = ct.CDLL(str(path.resolve()), mode=os.RTLD_NOW | os.RTLD_LOCAL | os.RTLD_DEEPBIND)
        self.lib.drill_iceberg_open.argtypes = [ct.c_char_p, ct.POINTER(ct.c_void_p), ct.c_void_p, ct.c_size_t]
        self.lib.drill_iceberg_next.argtypes = [ct.c_void_p, ct.POINTER(ArrowSchema), ct.POINTER(ArrowArray), ct.c_void_p, ct.c_size_t]
        self.lib.drill_iceberg_close.argtypes = [ct.c_void_p]
        self.lib.drill_iceberg_memory_bytes.restype = ct.c_longlong
    def read(self, config):
        reader = ct.c_void_p()
        error = ct.create_string_buffer(4096)
        result = self.lib.drill_iceberg_open(json.dumps(config).encode(), ct.byref(reader), error, len(error))
        if result: raise RuntimeError('open: ' + error.value.decode())
        batches = []
        try:
            while True:
                array, schema = ArrowArray(), ArrowSchema()
                result = self.lib.drill_iceberg_next(reader, ct.byref(schema), ct.byref(array), error, len(error))
                if result < 0: raise RuntimeError('next: ' + error.value.decode())
                if not result: break
                batches.append(pa.RecordBatch._import_from_c(ct.addressof(array), ct.addressof(schema)))
        finally:
            self.lib.drill_iceberg_close(reader)
        # Every exported batch remains usable after the original reader closes.
        return batches
    def rows(self, config):
        batches = self.read(config)
        rows = [row for batch in batches for row in batch.to_pylist()]
        del batches
        gc.collect()
        return rows


def test(lib, directory):
    count = 17003
    fields = [f(1, 'id', 'long', True), f(2, 'part', 'string'), f(3, 'key', 'long'),
              f(4, 'amount', 'decimal(18, 2)'), f(7, 'm', {'type': 'struct', 'fields': [
                  f(8, 'x', 'long'), f(9, 'part', 'string', True)]}, True)]
    full = schema(fields)
    parquet_schema = pa.schema([field('id', pa.int64(), 1, True), field('key', pa.int64(), 3),
        field('amount', pa.decimal128(18, 2), 4), field('m', pa.struct([
            field('x', pa.int64(), 8), field('part', pa.string(), 9, True)]), 7, True)])
    records = [{'id': i, 'key': None if i % 29 == 0 else i % 37,
                'amount': Decimal(i % 101 - 50) / 100,
                'm': {'x': i % 47, 'part': 'deliberately wrong file value'}} for i in range(count)]
    path = directory / 'data file.parquet'
    table = pa.Table.from_pylist(records, schema=parquet_schema)
    pq.write_table(table, path, row_group_size=4096)
    del table
    gc.collect()
    base = {'path': str(path), 'dataFilePath': path.as_uri(), 'fileSize': path.stat().st_size,
            'start': 0, 'length': path.stat().st_size, 'schema': full, 'tableSchema': full,
            'tableSchemas': [full], 'batchSize': 1024, 'constants': [
                {'fieldId': 2, 'type': 'string', 'value': list('雪🚀\0'.encode())},
                {'fieldId': 9, 'type': 'string', 'value': list('native partition'.encode())}]}
    expected = [dict(r, part='雪🚀\0', m=dict(r['m'], part='native partition')) for r in records]
    memory = lib.lib.drill_iceberg_memory_bytes()
    assert lib.rows(base) == expected, 'Identity constants must override stored values and missing columns'
    assert lib.lib.drill_iceberg_memory_bytes() == memory, 'Identity reader/batch leaked Arrow buffers'
    # A projection containing only constants still obtains correct row counts.
    constants_only = dict(base, schema=schema([fields[1]]))
    assert lib.rows(constants_only) == [{'part': '雪🚀\0'} for _ in records]
    null_constant = dict(constants_only, constants=[{'fieldId': 2, 'type': 'string', 'value': None}])
    assert lib.rows(null_constant) == [{'part': None} for _ in records]
    retained = lib.read(base)
    child = retained[0].column(4).field(1)
    del retained
    gc.collect()
    assert child[0].as_py() == 'native partition'
    del child
    gc.collect()
    assert lib.lib.drill_iceberg_memory_bytes() == memory, 'Retained child ownership leaked'

    # Use Iceberg single-value encodings, with independent typed Arrow values.
    # All columns except the row-count anchor are absent from the physical file.
    typed = [
        ('boolean', [1], True),
        ('int', list(struct.pack('<i', -2147483648)), -2147483648),
        ('long', list(struct.pack('<q', -9223372036854775808)), -9223372036854775808),
        ('float', list(struct.pack('<f', -1.25)), -1.25),
        ('double', list(struct.pack('<d', 1.0 / 8)), 1.0 / 8),
        ('decimal(18, 2)', list((-12345).to_bytes(2, 'big', signed=True)), Decimal('-123.45')),
        ('date', list(struct.pack('<i', -1)), dt.date(1969, 12, 31)),
        ('time', list(struct.pack('<q', 86399999999)), dt.time(23, 59, 59, 999999)),
        ('timestamp', list(struct.pack('<q', -1234567)), dt.datetime(1969, 12, 31, 23, 59, 58, 765433)),
        ('timestamptz', list(struct.pack('<q', -1234567)),
         dt.datetime(1969, 12, 31, 23, 59, 58, 765433, tzinfo=dt.timezone.utc)),
        ('binary', [0, 255, 128, 0], bytes([0, 255, 128, 0])),
        ('fixed[4]', [0, 255, 128, 0], bytes([0, 255, 128, 0])),
        ('uuid', list(range(16)), uuid.UUID(bytes=bytes(range(16)))),
    ]
    typed_schema = schema([f(100 + i, 'c' + str(i), type) for i, (type, _, _) in enumerate(typed)])
    typed_config = dict(base, schema=typed_schema, constants=[
        {'fieldId': 100 + i, 'type': type, 'value': encoded}
        for i, (type, encoded, _) in enumerate(typed)])
    typed_expected = {'c' + str(i): value for i, (_, _, value) in enumerate(typed)}
    typed_rows = lib.rows(typed_config)
    assert len(typed_rows) == count and all(row == typed_expected for row in typed_rows), (
        'Typed identity constant mismatch', len(typed_rows), typed_rows[:1], typed_expected)
    assert lib.lib.drill_iceberg_memory_bytes() == memory, 'Typed constant ownership leaked'

    positions = {0, 1023, 1024, 4095, 4096, 8191, 8192, count - 1}
    pos_path = directory / 'position.parquet'
    pos_schema = pa.schema([field('file_path', pa.string(), 2147483546, True),
                            field('pos', pa.int64(), 2147483545, True)])
    deletes = [{'file_path': path.as_uri(), 'pos': p} for p in sorted(positions)]
    deletes += [{'file_path': (directory / 'unrelated.parquet').as_uri(), 'pos': 10}]
    pq.write_table(pa.Table.from_pylist(deletes, schema=pos_schema), pos_path)
    eq_path = directory / 'equality.parquet'
    eq_schema = pa.schema([field('key', pa.int64(), 3)])
    pq.write_table(pa.Table.from_pylist([{'key': v} for v in [None, 5, 17]], schema=eq_schema), eq_path)
    composite = directory / 'composite.parquet'
    composite_schema = pa.schema([field('amount', pa.decimal128(18, 2), 4),
        field('m', pa.struct([field('x', pa.int64(), 8)]), 7, True)])
    pq.write_table(pa.Table.from_pylist([{'amount': Decimal('0.07'), 'm': {'x': 3}}], schema=composite_schema), composite)
    def delete(file, content, ids):
        return {'path': str(file), 'format': 'PARQUET', 'content': content,
                'fileSize': file.stat().st_size, 'recordCount': pq.ParquetFile(file).metadata.num_rows, 'equalityIds': ids}
    all_deletes = [delete(pos_path, 1, []), delete(eq_path, 2, [3]), delete(composite, 2, [4, 8])]
    def alive(row, selected):
        return not ((0 in selected and row['id'] in positions)
            or (1 in selected and row['key'] in [None, 5, 17])
            or (2 in selected and row['amount'] == Decimal('0.07') and row['m']['x'] == 3))
    for selected in ([0], [1], [2], [0, 1, 2]):
        config = dict(base, deletes=[all_deletes[i] for i in selected])
        oracle = [r for r in expected if alive(r, selected)]
        assert lib.rows(config) == oracle, ('Delete union mismatch', selected)
        # Delete-only keys and _pos must never appear in an output projection.
        minimal = dict(config, schema=schema([fields[0], fields[1]]))
        assert lib.rows(minimal) == [{'id': r['id'], 'part': r['part']} for r in oracle]
        # Split ownership and global _pos must remain identical to Foreman.
        metadata = pq.ParquetFile(path).metadata
        rows = []
        for group in range(metadata.num_row_groups):
            column = metadata.row_group(group).column(0)
            start = min(x for x in (column.dictionary_page_offset, column.data_page_offset) if x is not None and x >= 0)
            if group + 1 < metadata.num_row_groups:
                next_column = metadata.row_group(group + 1).column(0)
                end = min(x for x in (next_column.dictionary_page_offset, next_column.data_page_offset) if x is not None and x >= 0)
            else:
                end = path.stat().st_size
            rows.extend(lib.rows(dict(config, start=start, length=end - start)))
        assert rows == oracle, ('Global position deletes across splits', selected)
        assert lib.lib.drill_iceberg_memory_bytes() == memory, ('Delete/split owner leak', selected)
    # SDK schema history must recover an equality key dropped from the table.
    history = dict(base, schema=schema([fields[0]]), tableSchema=dict(schema([fields[0]]), **{'schema-id': 1}),
                   deletes=[all_deletes[1]], constants=[])
    assert lib.rows(history) == [{'id': r['id']} for r in expected if alive(r, [1])]
    # Delete an entire leading batch and a complete file: loop until live data/EOS.
    entire = directory / 'all.parquet'
    pq.write_table(pa.Table.from_pylist([{'file_path': path.as_uri(), 'pos': i} for i in range(count)], schema=pos_schema), entire)
    assert lib.rows(dict(base, deletes=[delete(entire, 1, [])])) == []
    malformed = dict(base, constants=[{'fieldId': 3, 'type': 'long', 'value': [1]}])
    rejected = False
    try: lib.rows(malformed)
    except RuntimeError: rejected = True
    assert rejected, 'Malformed Iceberg single-value bytes accepted'
    assert lib.lib.drill_iceberg_memory_bytes() == memory, 'Failed open leaked buffers'
    print('Iceberg C ABI: 13 typed constants/nested constants/NULL/only constants, position/equality/composite/mixed deletes, '
          'unprojected/dropped keys, nonzero split positions, fully deleted input, retained children and errors passed', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', type=Path, required=True)
    args = parser.parse_args()
    library = Library(args.library)
    with tempfile.TemporaryDirectory(prefix='drill-iceberg-tasks-') as directory:
        test(library, Path(directory))

if __name__ == '__main__':
    main()
