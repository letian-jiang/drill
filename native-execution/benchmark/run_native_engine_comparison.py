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
"""Serial Java/JNI-scan/native-scan comparisons on homogeneous Drillbits."""
import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import statistics

from validate_worker_topology import validate

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tests'))
from test_paths import REPO_ROOT, MODULE_ROOT, TOOLS, DEFAULT_DATA, BENCHMARK
ROOT = REPO_ROOT
HARNESS = MODULE_ROOT / 'tests/run_iceberg_integration.py'
COMPARE = MODULE_ROOT / 'tests/compare_integration_results.py'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--queries', default='6,16,21')
    parser.add_argument('--dataset', type=Path, default=DEFAULT_DATA)
    parser.add_argument('--queries-dir', type=Path, default=BENCHMARK / 'queries')
    parser.add_argument('--query-timeout', type=int, default=120)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--iterations', type=int, default=1)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--deployment', choices=['single', 'distributed', 'all'], default='single')
    parser.add_argument('--independent-reference', type=Path,
                        help='Pinned independent results for known Java differences; mismatched queries have no speedup ratio')
    args = parser.parse_args()
    if args.warmups < 0 or args.iterations < 1:
        parser.error('warmups must be nonnegative and iterations positive')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=args.resume)
    modes = []
    numbers = list(map(int, args.queries.split(',')))
    phases = ([f'warmup{i:02}' for i in range(1, args.warmups + 1)]
              + [f'iteration{i:02}' for i in range(1, args.iterations + 1)])
    measured_phases = [f'iteration{i:02}' for i in range(1, args.iterations + 1)]

    def result_path(directory, number, phase):
        # The Java fixture uses the primary filename for a single un-warmed run.
        suffix = '' if args.warmups == 0 and args.iterations == 1 else '.' + phase
        return directory / f'q{number:02}{suffix}.json'

    deployments = ['single', 'distributed'] if args.deployment == 'all' else [args.deployment]
    for deployment in deployments:
        for backend in ['java', 'native-jni', 'native-sdk']:
            mode = deployment + '-' + backend
            modes.append(mode)
            directory = output / mode
            if directory.exists():
                if not args.resume: raise FileExistsError(directory)
                metadata = json.loads((directory / 'run.json').read_text())
                complete = (metadata['warmups'] == args.warmups and metadata['iterations'] == args.iterations
                            and metadata['queries'] == args.queries
                            and all(result_path(directory, number, phase).exists()
                                    for number in numbers for phase in phases))
                same_artifact = True
                if backend != 'java':
                    same_artifact = metadata.get('native_engine_sha256') == hashlib.sha256(
                        Path(metadata['native_engine']).read_bytes()).hexdigest()
                    if backend == 'native-sdk':
                        sdk = TOOLS / 'iceberg-build/libdrill_iceberg_reader.so'
                        same_artifact = same_artifact and metadata.get('iceberg_reader_sha256') == hashlib.sha256(sdk.read_bytes()).hexdigest()
                if complete and same_artifact:
                    try:
                        validate(directory)
                        print('Reusing validated', mode, flush=True)
                        continue
                    except AssertionError:
                        pass
                index = 1
                while (output / (mode + f'.previous{index}')).exists(): index += 1
                directory.rename(output / (mode + f'.previous{index}'))
            command = [sys.executable, str(HARNESS), '--deployment', deployment,
                       '--worker-mode', 'java' if backend == 'java' else 'native',
                       '--dataset', str(args.dataset.resolve()), '--queries', args.queries,
                       '--queries-dir', str(args.queries_dir.resolve()),
                       '--warmups', str(args.warmups), '--iterations', str(args.iterations),
                       '--query-timeout', str(args.query_timeout),
                       '--timeout', str(max(600, len(numbers) * ((args.warmups + args.iterations) * args.query_timeout + 10))),
                       '--output', str(output / mode)]
            if backend == 'native-jni': command.append('--force-jni-scan')
            print('Running', mode, flush=True)
            with (output / (mode + '.harness.log')).open('w') as log:
                subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            validate(output / mode)
    # Java and native primary files refer to their single measured iteration.
    # Validate every warmup as well; these timings are not included in ratios.
    phase_checks = []
    for deployment in deployments:
        baseline = output / (deployment + '-java')
        for backend in ['native-jni', 'native-sdk']:
            directory = output / (deployment + '-' + backend)
            compare_command = [sys.executable, str(COMPARE), '--native-run', str(directory),
                            '--java-run', str(baseline), '--dataset', str(args.dataset.resolve()),
                            '--queries', args.queries, '--output', str(directory / 'correctness.json')]
            if args.independent_reference:
                compare_command += ['--independent-reference', str(args.independent_reference.resolve())]
            subprocess.run(compare_command, check=True)
            for number in numbers:
                for phase in phases:
                    name = result_path(directory, number, phase).name
                    actual = json.loads(result_path(directory, number, phase).read_text())['rows']
                    expected = json.loads(result_path(baseline, number, phase).read_text())['rows']
                    equal = Counter(map(tuple, actual)) == Counter(map(tuple, expected))
                    independent = (args.independent_reference / f'q{number:02}.json'
                                   if args.independent_reference else None)
                    reference = json.loads(independent.read_text()) if independent and independent.exists() else None
                    authoritative_equal = (Counter(map(tuple, actual)) == Counter(map(tuple, reference['rows']))
                                           if reference else equal)
                    phase_checks.append({'mode': directory.name, 'query': number,
                                         'phase': phase, 'equal': equal,
                                         'authoritative_equal': authoritative_equal})
                    if not authoritative_equal: raise ValueError(f'{directory.name}/{name}: row mismatch')
    rows = []
    samples = []
    for number in numbers:
        entry = {'query': number}
        for mode in modes:
            values = [json.loads(result_path(output / mode, number, phase).read_text())['wall_ms']
                      for phase in measured_phases]
            entry[mode + '_ms'] = statistics.median(values)
            samples.append({'query': number, 'mode': mode, 'wall_ms': values,
                            'median_ms': statistics.median(values), 'min_ms': min(values), 'max_ms': max(values)})
        for deployment in deployments:
            java = entry[deployment + '-java_ms']
            for backend in ['native-jni', 'native-sdk']:
                comparable = all(check['equal'] for check in phase_checks
                                 if check['query'] == number and check['mode'] == deployment + '-' + backend)
                entry[deployment + '-' + backend + '_speedup'] = (java / entry[deployment + '-' + backend + '_ms']
                                                                 if comparable else None)
        rows.append(entry)
    report = {'dataset': str(args.dataset.resolve()), 'queries': numbers,
              'warmups': args.warmups, 'measured_iterations': args.iterations, 'execution_order': modes,
              'statistic': 'median wall_ms of measured iterations',
              'scope': 'Serial single-node comparisons by default; exploratory timings; repeated samples do not cover all environmental variation',
              'timing_samples': samples,
              'all_rows_equal': all(check['equal'] for check in phase_checks),
              'all_authoritative_rows_equal': all(check['authoritative_equal'] for check in phase_checks),
              'independent_reference': str(args.independent_reference.resolve()) if args.independent_reference else None,
              'phase_correctness': phase_checks, 'results': rows}
    (output / 'comparison.json').write_text(json.dumps(report, indent=2) + '\n')
    with (output / 'comparison.csv').open('w') as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    print('Validated comparison:', output, flush=True)


if __name__ == '__main__':
    main()
