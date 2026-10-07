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
"""Generate immutable TPC-H Iceberg v1 tables with bounded Arrow batches."""
import argparse, gzip, hashlib, json, shutil, subprocess
from pathlib import Path
import duckdb
import pyarrow as pa
from pyiceberg.catalog import load_catalog

BASE_ROWS = dict(region=5, nation=25, supplier=10000, customer=150000,
                 part=200000, partsupp=800000, orders=1500000)
TABLES = [*BASE_ROWS, 'lineitem']

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--output', type=Path, default=Path(__file__).resolve().parent / 'data')
    ap.add_argument('--scale-factor', type=int, default=1)
    ap.add_argument('--batch-rows', type=int, default=262144,
                    help='Maximum Arrow rows materialized for one append')
    ap.add_argument('--memory-limit', default='4GB', help='DuckDB memory limit; temporary spill uses the dataset directory')
    ap.add_argument('--threads', type=int, default=4)
    args = ap.parse_args()
    if args.scale_factor < 1 or args.batch_rows < 1 or args.threads < 1:
        ap.error('Scale factor, batch rows and threads must be positive')
    out = args.output.resolve()
    if (out / 'manifest.json').exists():
        existing = json.loads((out / 'manifest.json').read_text())
        if existing['scale_factor'] != args.scale_factor:
            ap.error('Existing immutable dataset has a different scale factor; use a new output directory')
        print('Dataset already complete:', out, flush=True)
        return
    if out.exists() and any(out.iterdir()):
        ap.error('Incomplete output directory cannot be reused; use a new directory')
    out.mkdir(parents=True, exist_ok=True)
    db = duckdb.connect(str(out / 'tpch.duckdb'))
    db.execute('SET memory_limit = ?', [args.memory_limit])
    db.execute('SET threads = ?', [args.threads])
    db.execute('SET temp_directory = ?', [str(out / 'duckdb-tmp')])
    extension = out / "tpch.duckdb_extension"
    if not extension.exists():
        platform = db.execute("PRAGMA platform").fetchone()[0]
        url = f"https://extensions.duckdb.org/v{duckdb.__version__}/{platform}/tpch.duckdb_extension.gz"
        compressed = subprocess.check_output(["curl", "-fsSL", url])
        extension.write_bytes(gzip.decompress(compressed))
    db.execute(f"LOAD '{extension}'")
    if not db.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='lineitem'").fetchone()[0]:
        print('Generating DuckDB TPC-H SF', args.scale_factor, flush=True)
        db.execute(f'CALL dbgen(sf={args.scale_factor})')
    # Keep canonical source queries inside this dataset. Never overwrite the
    # qualified/frozen benchmark SQL shared by running SF1 comparisons.
    queries = out / 'queries'
    queries.mkdir(exist_ok=True)
    for qid, sql in db.execute('SELECT query_nr, query FROM tpch_queries()').fetchall():
        # Preserve the canonical query. Relation names are resolved with USE tpch.
        (queries / f'q{qid:02d}.sql').write_text(sql)
    warehouse = out / 'warehouse'
    cat = load_catalog('local', type='sql', uri=f'sqlite:///{out / "catalog.db"}', warehouse=warehouse.as_uri())
    cat.create_namespace_if_not_exists('tpch')
    manifest = dict(scale_factor=args.scale_factor, generator='DuckDB dbgen', generator_version=duckdb.__version__,
                    generation_batch_rows=args.batch_rows, generation_memory_limit=args.memory_limit,
                    iceberg_format_version=1, tables={})
    for name in TABLES:
        identifier = ('tpch', name)
        expected = db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
        if name in BASE_ROWS:
            reference = BASE_ROWS[name] * (1 if name in ['region', 'nation'] else args.scale_factor)
            assert expected == reference, (name, expected, reference)
        elif args.scale_factor == 1:
            assert expected == 6001215, expected
        else:
            assert 1500000 * args.scale_factor <= expected <= 10500000 * args.scale_factor, expected
        batches = db.execute(f'SELECT * FROM {name}').fetch_record_batch(args.batch_rows)
        location = warehouse / name
        if not cat.table_exists(identifier):
            table = cat.create_table(identifier, schema=batches.schema, location=location.as_uri(),
                                     properties={'format-version':'1', 'write.target-file-size-bytes':'33554432'})
        else:
            table = cat.load_table(identifier)
        written = 0
        for batch in batches:
            # Limit each append's executor input as well as the Arrow reader;
            # passing the whole reader to executor.map may queue all data.
            table.append(pa.Table.from_batches([batch]))
            written += batch.num_rows
        assert written == expected, (name, written, expected)
        active = list(table.scan().plan_files())
        assert sum(task.file.record_count for task in active) == expected, name
        # Publish a frozen HadoopTables-compatible pointer after the commit. No
        # subsequent catalog writes are allowed by this benchmark generator.
        metadata = Path(table.metadata_location.removeprefix('file://'))
        dst = location / 'metadata' / 'v1.metadata.json'
        shutil.copyfile(metadata, dst)
        (dst.parent / 'version-hint.text').write_text('1\n')
        files = list(location.glob('data/*.parquet'))
        assert len(files) == len(active), (name, len(files), len(active))
        manifest['tables'][name] = dict(rows=expected, location=str(location),
                snapshot_id=table.current_snapshot().snapshot_id, data_files=len(files),
                metadata_sha256=hashlib.sha256(dst.read_bytes()).hexdigest())
        print(name, expected, 'rows', len(files), 'files', flush=True)
        del batches
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    db.close()
    print('Complete:', out / 'manifest.json', flush=True)
if __name__ == '__main__':
    main()
