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
"""Run homogeneous Java-hosted Drillbits with Java or native execution backends."""
import argparse
import json
import hashlib
import platform
import os
from pathlib import Path
import signal
import socket
import subprocess
import time

from test_paths import REPO_ROOT, MODULE_ROOT, BENCHMARK, TOOLS, DEFAULT_DATA, maven
ROOT = REPO_ROOT

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

def descendants(pid):
    """Surefire can put its forked JVM in a different process group."""
    result = []
    pending = [pid]
    while pending:
        parent = pending.pop()
        try:
            children = Path(f'/proc/{parent}/task/{parent}/children').read_text().split()
        except FileNotFoundError:
            continue
        for child in children:
            child = int(child)
            try:
                identity = Path(f'/proc/{child}/stat').read_text().rsplit(')', 1)[1].split()[19]
            except FileNotFoundError:
                continue
            result.append((child, identity))
            pending.append(child)
    return result

def signal_owned(processes, signum):
    for pid, identity in reversed(processes):
        try:
            current = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
            if current == identity:
                os.kill(pid, signum)
        except (FileNotFoundError, ProcessLookupError):
            pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--native-threads', type=int)
    parser.add_argument('--dataset', type=Path, default=DEFAULT_DATA)
    parser.add_argument('--worker-mode', choices=['native', 'java'], default='native')
    parser.add_argument('--deployment', choices=['single', 'distributed'], default='single')
    parser.add_argument('--native-engine', type=Path, default=TOOLS / 'native-runtime/build/libdrill_native_engine.so')
    parser.add_argument('--worker-count', type=int, default=1)
    parser.add_argument('--foreman-participates', action='store_true')
    parser.add_argument('--max-parallelization', type=int, default=1)
    parser.add_argument('--force-jni-scan', action='store_true')
    parser.add_argument('--format', choices=['iceberg', 'paimon', 'json'], default='iceberg')
    parser.add_argument('--queries', default='6')
    parser.add_argument('--queries-dir', type=Path)
    parser.add_argument('--warmups', type=int, default=0)
    parser.add_argument('--iterations', type=int, default=1)
    parser.add_argument('--timeout', type=int, default=900)
    parser.add_argument('--query-timeout', type=int, default=120)
    parser.add_argument('--logback-config', type=Path, default=BENCHMARK / 'logback.xml')
    args = parser.parse_args()
    if args.worker_count < 1:
        parser.error('--worker-count must be positive')
    if args.native_threads is not None and args.native_threads < 1:
        parser.error('--native-threads must be positive')
    if args.warmups < 0 or args.iterations < 1:
        parser.error('--warmups must be nonnegative and --iterations positive')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    logs, workers = [], []
    worker_count = 0 if args.deployment == 'single' else args.worker_count
    query_directory = args.queries_dir or args.dataset / 'queries'
    if not query_directory.is_dir():
        query_directory = BENCHMARK / 'queries'
    query_directory = query_directory.resolve()
    metadata = {'format': args.format, 'worker_mode': args.worker_mode,
                'deployment': args.deployment,
                'foreman_count': 1, 'worker_count': worker_count,
                'dataset': str(args.dataset.resolve()), 'queries': args.queries,
                'dataset_manifest_sha256': hashlib.sha256((args.dataset / 'manifest.json').read_bytes()).hexdigest()
                    if (args.dataset / 'manifest.json').exists() else None,
                'query_timeout_seconds': args.query_timeout,
                'cpu_affinity': sorted(os.sched_getaffinity(0)),
                'platform': platform.platform(),
                'iterations': args.iterations, 'warmups': args.warmups, 'force_jni_scan': args.force_jni_scan}
    metadata['homogeneous_drillbits'] = True
    metadata['foreman_participates'] = args.foreman_participates
    metadata['max_parallelization'] = args.max_parallelization
    metadata['architecture'] = 'homogeneous-native-rpc' if args.worker_mode == 'native' else 'java'
    metadata['native_threads'] = args.native_threads
    if args.format == 'iceberg' and args.worker_mode == 'native' and not args.force_jni_scan:
        sdk = TOOLS / 'iceberg-build/libdrill_iceberg_reader.so'
        metadata['iceberg_reader'] = str(sdk.resolve())
        metadata['iceberg_reader_sha256'] = hashlib.sha256(sdk.read_bytes()).hexdigest()
    if args.worker_mode == 'native':
        metadata['native_engine'] = str(args.native_engine.resolve())
        metadata['native_engine_sha256'] = hashlib.sha256(args.native_engine.read_bytes()).hexdigest()
    metadata['queries_dir'] = str(query_directory)
    schema_file = args.dataset / 'schema.sql'
    if not schema_file.exists(): schema_file = BENCHMARK / 'schema.sql'
    metadata['schema_file'] = str(schema_file.resolve())
    metadata['schema_file_sha256'] = hashlib.sha256(schema_file.read_bytes()).hexdigest()
    metadata['queries_sha256'] = {
        f'q{int(number):02d}.sql': hashlib.sha256(
            (query_directory / f'q{int(number):02d}.sql').read_bytes()).hexdigest()
        for number in args.queries.split(',')}
    (output / 'run.json').write_text(json.dumps(metadata, indent=2))
    maven_command = maven()
    opens = ' '.join('--add-opens ' + module + '=ALL-UNNAMED' for module in (
        'java.base/java.lang', 'java.base/java.net', 'java.base/java.nio',
        'java.base/java.util', 'java.base/sun.nio.ch', 'java.security.jgss/sun.security.krb5'))
    foreman_ports = []
    while len(foreman_ports) < 3:
        value = port()
        if value not in foreman_ports: foreman_ports.append(value)
    module = 'exec/java-exec' if args.format == 'json' else 'contrib/format-' + args.format
    command = [maven_command, '-B', '-pl', module, '-am', 'package',
               '-Dtest=PureNative' + args.format.title() + 'Benchmark', '-Dsurefire.failIfNoSpecifiedTests=false',
               '-DenableAssertions=false', '-Dcheckstyle.skip', '-Drat.skip=true',
               '-Djunit.args=' + opens + ' -Dzookeeper.sasl.client=false -da',
               '-Ddrill.native.dataset=' + str(args.dataset.resolve()),
               '-Ddrill.native.results_dir=' + str(output),
               '-Ddrill.native.queries=' + args.queries,
               '-Ddrill.native.queries_dir=' + str(query_directory),
               '-Ddrill.native.warmups=' + str(args.warmups),
               '-Ddrill.native.iterations=' + str(args.iterations),
               '-Ddrill.native.force_jni_scan=' + str(args.force_jni_scan).lower(),
               '-Ddrill.native.query_timeout_seconds=' + str(args.query_timeout),
               '-Ddrill.native.worker_mode=' + args.worker_mode,
               '-Ddrill.native.deployment=' + args.deployment,
               '-Ddrill.native.worker_count=' + str(worker_count),
               '-Ddrill.native.foreman_participates=' + str(args.foreman_participates).lower(),
               '-Ddrill.native.max_parallelization=' + str(args.max_parallelization),
               '-Ddrill.native.foreman.user_port=' + str(foreman_ports[0]),
               '-Ddrill.native.foreman.control_port=' + str(foreman_ports[1]),
               '-Ddrill.native.foreman.data_port=' + str(foreman_ports[2]),
               '-Dlogback.configurationFile=' + str(args.logback_config.resolve())]
    if args.worker_mode == 'native':
        command += ['-Ddrill.native.engine.library=' + str(args.native_engine.resolve())]
        if args.native_threads is not None:
            command += ['-Ddrill.native.engine.threads=' + str(args.native_threads)]
    env = dict(os.environ,
               DRILL_NATIVE_PLAN_DIR=str(output / 'plans'),
               DRILL_NATIVE_STATS_DIR=str(output / 'stats'),
               JAVA_HOME=os.environ.get('JAVA_HOME', '/usr/lib/jvm/java-21-openjdk-amd64'),
               DRILL_NATIVE_ICEBERG_LIBRARY=str(TOOLS / 'iceberg-build/libdrill_iceberg_reader.so'))
    env['LD_LIBRARY_PATH'] = str(TOOLS / 'iceberg-toolchain/env/lib') + ':' + env.get('LD_LIBRARY_PATH', '')
    log = (output / 'foreman.log').open('w'); logs.append(log)
    started_at = time.time()
    foreman = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    (output / 'foreman-process.json').write_text(json.dumps({'pid': foreman.pid, 'process_group': foreman.pid}))
    deadline = time.monotonic() + args.timeout
    try:
        ready = output / 'foreman-ready.json'
        while not ready.exists():
            if foreman.poll() is not None:
                raise RuntimeError('Foreman startup failed; see foreman.log')
            if time.monotonic() >= deadline:
                raise TimeoutError('Foreman startup timed out')
            time.sleep(.2)
        discovery = json.loads(ready.read_text())
        env = dict(env,
                   DRILL_NATIVE_SCAN_CLASSPATH=discovery['classpath'],
                   DRILL_NATIVE_ZK_CONNECT=discovery['zk'],
                   DRILL_NATIVE_ZK_ROOT=discovery['zk_root'],
                   DRILL_NATIVE_CLUSTER_ID=discovery['cluster_id'],
                   DRILL_NATIVE_ICEBERG_LIBRARY=str(TOOLS / 'iceberg-build/libdrill_iceberg_reader.so'))
        env['LD_LIBRARY_PATH'] = str(TOOLS / 'iceberg-toolchain/env/lib') + ':' + env.get('LD_LIBRARY_PATH', '')
        endpoints = []
        allocated = set(foreman_ports)
        for n in range(worker_count):
            ports = []
            while len(ports) < 3:
                p = port()
                if p not in allocated:
                    allocated.add(p); ports.append(p)
            log = (output / f'worker-{n}.log').open('w'); logs.append(log)
            worker_command = [str(Path(env['JAVA_HOME']) / 'bin/java'), '-Xmx4g', '-XX:MaxDirectMemorySize=4g']
            worker_command += opens.split() + ['-Dzookeeper.sasl.client=false', '-da',
                '-Dlogback.configurationFile=' + str(args.logback_config.resolve())]
            if args.worker_mode == 'native':
                worker_command += ['-Ddrill.native.engine.library=' + str(args.native_engine.resolve())]
                if args.native_threads is not None:
                    worker_command += ['-Ddrill.native.engine.threads=' + str(args.native_threads)]
            worker_command += ['-cp', discovery['classpath'],
                'org.apache.drill.exec.physical.impl.velox.JavaBenchmarkWorker',
                discovery['zk'], discovery['zk_root'], discovery['cluster_id']] + list(map(str, ports))
            worker = subprocess.Popen(worker_command, cwd=ROOT, env=env, stdout=log,
                                      stderr=subprocess.STDOUT, start_new_session=True)
            workers.append(worker)
            endpoints.append({'pid': worker.pid, 'control_port': ports[-2], 'data_port': ports[-1], 'mode': args.worker_mode, 'host': 'java-drillbit', 'user_port': ports[0]})
        (output / 'workers.json').write_text(json.dumps(endpoints, indent=2))
        workers_stopped = False
        while foreman.poll() is None:
            if not workers_stopped and (output / 'queries-complete.json').exists():
                for worker in workers:
                    if worker.poll() is None: os.killpg(worker.pid, signal.SIGTERM)
                for worker in workers:
                    worker.wait(timeout=20)
                    if worker.returncode not in (0, 143):
                        raise RuntimeError('Worker shutdown failed; see worker logs')
                (output / 'workers-stopped.json').write_text(json.dumps({'pids': [w.pid for w in workers]}))
                workers_stopped = True
            if time.monotonic() >= deadline:
                raise TimeoutError('Cluster integration timed out')
            for worker in workers:
                if not workers_stopped and worker.poll() is not None:
                    raise RuntimeError('Worker exited; see worker logs')
            time.sleep(.2)
        if foreman.returncode:
            raise RuntimeError('Query validation failed; see foreman.log')
        print(args.worker_mode + ' integration passed:', output)
    finally:
        forced = set()
        owned_children = [child for process in [foreman] + workers for child in descendants(process.pid)]
        signal_owned(owned_children, signal.SIGTERM)
        for process in [foreman] + workers:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
        for process in [foreman] + workers:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                forced.add(process.pid)
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        signal_owned(owned_children, signal.SIGKILL)
        # Surefire captures native stdout as dumpstream entries, separately
        # from Java logging. Preserve only files produced by this reactor run.
        reports = ROOT / module / 'target/surefire-reports'
        native_output = [file.read_text(errors='replace') for file in sorted(reports.glob('*.dumpstream'))
                         if file.stat().st_mtime >= started_at]
        if native_output:
            (output / 'native-stdout.log').write_text('\n'.join(native_output))
        for log in logs:
            log.close()
        (output / 'shutdown.json').write_text(json.dumps({
            'foreman': {'pid': foreman.pid, 'returncode': foreman.returncode, 'forced': foreman.pid in forced},
            'workers': [{'pid': worker.pid, 'returncode': worker.returncode, 'forced': worker.pid in forced}
                        for worker in workers]}, indent=2) + '\n')

if __name__ == '__main__':
    main()
