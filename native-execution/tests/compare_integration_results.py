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
"""Compare complete row multisets; historical Java rows require identical inputs."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path

from test_paths import REPO_ROOT, MODULE_ROOT, BENCHMARK, TOOLS, DEFAULT_DATA, maven
ROOT = REPO_ROOT

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def rows(values):
    return Counter(tuple(row) for row in values)

def historical_rows(path):
    return rows([[None if value is None or value == 'NULL' else value.split(':', 1)[1]
                  for value in json.loads(row)] for row in json.loads(path.read_text())])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--native-run', type=Path, required=True)
    parser.add_argument('--java-run', type=Path)
    parser.add_argument('--independent-reference', type=Path)
    parser.add_argument('--historical-java-run', type=Path)
    parser.add_argument('--dataset', type=Path, default=DEFAULT_DATA)
    parser.add_argument('--queries', default=','.join(map(str, range(1, 23))))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not args.java_run and not args.historical_java_run:
        parser.error('a Java reference run is required')
    numbers = [int(number) for number in args.queries.split(',')]
    if not numbers or len(set(numbers)) != len(numbers) or any(n < 1 or n > 22 for n in numbers):
        parser.error('--queries must contain distinct TPC-H numbers from 1 to 22')
    inputs = []
    for directory in (args.native_run, args.java_run):
        if directory:
            metadata = json.loads((directory / 'run.json').read_text())
            if Path(metadata['dataset']).resolve() != args.dataset.resolve():
                raise ValueError(f'{directory}: different source data directory')
            digest = metadata.get('dataset_manifest_sha256')
            if digest and digest != sha(args.dataset / 'manifest.json'):
                raise ValueError(f'{directory}: different source data manifest')
            inputs.append(metadata)
    native_queries = inputs[0].get('queries_sha256', {})
    if len(inputs) == 2 and native_queries and inputs[1].get('queries_sha256'):
        for number in numbers:
            name = f'q{number:02d}.sql'
            if native_queries.get(name) != inputs[1]['queries_sha256'].get(name):
                raise ValueError(f'{name}: Java and native runs use different SQL')
    query_dir = Path(inputs[0].get('queries_dir', args.dataset / 'queries'))
    if not query_dir.is_dir(): query_dir = BENCHMARK / 'queries'
    environment = None
    if args.historical_java_run:
        environment = json.loads((args.historical_java_run / 'snapshot/environment.json').read_text())
        if environment['dataset_manifest_sha256'] != sha(args.dataset / 'manifest.json'):
            raise ValueError('Historical Java reference uses different source data')
    results = []
    for number in numbers:
        name = f'q{number:02d}'
        native = args.native_run / (name + '.json')
        current = args.java_run / (name + '.json') if args.java_run else None
        reference = current if current and current.exists() else None
        historical = False
        if reference is None and args.historical_java_run:
            query_digest = native_queries.get(name + '.sql', sha(query_dir / (name + '.sql')))
            if environment['queries_sha256'][name + '.sql'] != query_digest:
                raise ValueError(f'{name}: historical reference uses different SQL')
            candidates = sorted(args.historical_java_run.glob(name + '.java.*.rows.json'))
            if candidates: reference, historical = candidates[0], True
        java_reference = reference
        independent = args.independent_reference / (name + '.json') if args.independent_reference else None
        if independent and independent.exists():
            data = json.loads(independent.read_text())
            query_digest = native_queries.get(name + '.sql', sha(query_dir / (name + '.sql')))
            if data['dataset_manifest_sha256'] != sha(args.dataset / 'manifest.json') or data['sql_sha256'] != query_digest:
                raise ValueError(f'{name}: independent reference has different inputs')
            reference = independent
        entry = {'query': number, 'native_result': str(native.resolve()),
                 'reference': str(reference.resolve()) if reference else None,
                 'reference_kind': 'independent_duckdb' if independent and reference == independent else 'historical_java' if historical else 'new_java'}
        if native.exists() and reference:
            actual = rows(json.loads(native.read_text())['rows'])
            expected = historical_rows(reference) if historical and reference != independent else rows(json.loads(reference.read_text())['rows'])
            if independent and reference == independent and java_reference:
                java_rows = historical_rows(java_reference) if historical else rows(json.loads(java_reference.read_text())['rows'])
                entry['java_reference_equal'] = actual == java_rows
                entry['java_reference'] = str(java_reference.resolve())
            entry.update(equal=actual == expected, rows=sum(actual.values()))
            if actual != expected:
                entry['unexpected_rows'] = list((actual - expected).elements())[:3]
                entry['missing_rows'] = list((expected - actual).elements())[:3]
        else:
            entry.update(equal=False, error='query result or reference is missing')
        results.append(entry)
        print(name, 'equal' if entry['equal'] else 'DIFF', entry['reference_kind'])
    report = {'all_equal': all(x['equal'] for x in results), 'queries': results,
              'scope': 'correctness only; historical timings are not a current performance baseline'}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    if not report['all_equal']: raise SystemExit(1)

if __name__ == '__main__':
    main()
