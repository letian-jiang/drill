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
"""Validate saved real-worker placements and native execution/scan evidence."""
import argparse
import json
from pathlib import Path
import re


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def validate(path):
    meta = json.loads((path / 'run.json').read_text())
    workers = json.loads((path / 'workers.json').read_text())
    ports = {worker['control_port'] for worker in workers}
    assert meta['foreman_count'] == 1
    assert len(workers) == len(ports) == meta['worker_count']
    assert all(worker['mode'] == meta['worker_mode'] for worker in workers)
    native = meta['worker_mode'] == 'native'
    single = meta.get('deployment') == 'single'
    homogeneous = meta.get('architecture') == 'homogeneous-native-rpc'
    if homogeneous: assert all(w.get('host') == 'java-drillbit' for w in workers)
    shutdown = None
    if (path / 'shutdown.json').exists():
        shutdown = json.loads((path / 'shutdown.json').read_text())
        assert shutdown['foreman']['returncode'] == 0 and not shutdown['foreman']['forced']
        assert len(shutdown['workers']) == len(workers)
        if native and not single and not homogeneous:
            for index, worker in enumerate(shutdown['workers']):
                assert worker['pid'] == workers[index]['pid']
                assert worker['returncode'] == 0 and not worker['forced'], worker
                log = (path / f'worker-{index}.log').read_text()
                assert 'C++ worker NativeEngine drained.' in log and 'C++ worker closed.' in log
                assert log.index('C++ worker NativeEngine drained.') < log.index('C++ worker closed.')
                assert 'pools_.size() != 0' not in log and 'Memory leak' not in log
                if meta['force_jni_scan']:
                    assert 'JNI standalone scan hosts closed; active scans=0' in log
                    assert log.index('C++ worker NativeEngine drained.') < log.index('JNI standalone scan hosts closed; active scans=0') < log.index('C++ worker closed.')
    if meta.get("homogeneous_drillbits") and shutdown:
        for worker in shutdown['workers']:
            assert worker['returncode'] in (0, 143) and not worker['forced'], worker
        for index in range(meta['worker_count']):
            log = (path / f'worker-{index}.log').read_text()
            assert 'Java worker ready:' in log
            assert 'C++ worker ready:' not in log
            if native:
                assert 'Native RPC service drained.' in log and 'Native RPC ScanHost released.' in log
                assert log.index('Native RPC service drained.') < log.index('Native RPC ScanHost released.')
            assert 'pools_.size() != 0' not in log and 'Memory leak' not in log
    if single:
        assert meta['worker_count'] == 0
    expected_scan = 'jniScan' if meta['force_jni_scan'] else 'nativeScan'
    wrong_operator = 'NativeScan' if meta['force_jni_scan'] else 'JniPluginScan'
    roots = nonroots = scans = 0
    queries = []
    foreman_ports = set()
    # Primary files alias the last iteration. Inspect actual cycles only once.
    pattern = r'q\d{2}(?:\.(?:warmup|iteration)\d+)?\.json'
    files = sorted(file for file in path.glob('q*.json')
                   if re.fullmatch(pattern, file.name)
                   and not (re.fullmatch(r'q\d{2}\.json', file.name)
                            and list(path.glob(file.stem + '.iteration*.json'))))
    expected_files = set()
    for number in map(int, meta['queries'].split(',')):
        base = f'q{number:02}'
        if meta['warmups'] == 0 and meta['iterations'] == 1:
            expected_files.add(base + '.json')
        else:
            for phase, count in [('warmup', meta['warmups']), ('iteration', meta['iterations'])]:
                expected_files.update(f'{base}.{phase}{index:02}.json' for index in range(1, count + 1))
    failed = sorted(file.name for file in path.glob('q*.error.json'))
    assert ({file.name for file in files}
            | {name.replace('.error.json', '.json') for name in failed}) == expected_files
    for file in files:
        value = json.loads(file.read_text())
        placements = value['placements']
        root = [p for p in placements if p['major'] == 0]
        others = [p for p in placements if p['major'] != 0]
        assert len(root) == 1 and root[0]['control_port'] not in ports, file
        allowed = ports | ({root[0]['control_port']} if meta.get('foreman_participates') else set())
        assert all(p['control_port'] == root[0]['control_port'] if single
                   else p['control_port'] in allowed for p in others), file
        assert all(p['state'] == 'FINISHED' for p in placements), file
        foreman_ports.add(root[0]['control_port'])
        roots += 1
        nonroots += len(others)
        entry = {'file': file.name, 'query': value['query'],
                 'wall_ms': value['wall_ms'], 'nonroot_minors': len(others)}
        if native:
            scan_count = 0
            runtime_sources = 0
            work_counts = []
            task_states = []
            query_id = value['query_id'].replace('-', '')
            for minor in others:
                name = f"{query_id}.{minor['major']}.{minor['minor']}.json"
                plan = json.loads((path / 'plans' / name).read_text())
                for node in walk(plan):
                    assert ('nativeScan' if meta['force_jni_scan'] else 'jniScan') not in node, name
                    scan_count += int(expected_scan in node)
                    if expected_scan in node:
                        scan = node[expected_scan]
                        # Generic plugins retain one original SubScan reader;
                        # optional workList providers expose independent work.
                        work_counts.append(len(scan['scan'].get('workList', [scan['scan']]))
                                           if meta['force_jni_scan'] else len(scan['splits']))
                stats = json.loads((path / 'stats' / name).read_text())
                if 'native_task_created' not in stats:
                    # Historical files lack task state and early-finish evidence.
                    assert stats['completed_drivers'] == stats['total_drivers'] > 0, name
                else:
                    assert stats['status_state'] == 3 and not stats['cancelled'], name
                    if stats['native_task_created']:
                        assert stats['total_drivers'] > 0, name
                        assert stats['task_state'] == 'Finished' or (
                            stats['task_state'] == 'Aborted' and stats['early_complete']), name
                        assert 0 <= stats['completed_drivers'] <= stats['total_drivers'], name
                    else:
                        assert stats['early_complete'] and stats['task_state'] == 'NotStarted', name
                    task_states.append({'major': minor['major'], 'minor': minor['minor'],
                                        'state': stats['task_state'], 'early_complete': stats['early_complete'],
                                        'total_drivers': stats['total_drivers'],
                                        'completed_driver_slots': stats['completed_drivers'],
                                        'terminated_driver_slots': stats.get('terminated_drivers', 0)})
                if homogeneous and stats.get('native_task_created'):
                    assert stats['execution_transport'] == 'native-rpc', name
                    host = stats['host']
                    assert host['transport'] == 'rpc', name
                    assert host['jni_engine_calls'] == host['jni_status_callbacks'] == host['jni_root_callbacks'] == 0, name
                    if not meta['force_jni_scan']: assert not stats['jni_scans'], name
                for pipeline in stats['pipelines']:
                    for op in pipeline['operators']:
                        assert op['operator'] != wrong_operator, name
                        runtime_sources += int(op['operator'] in ('NativeScan', 'JniPluginScan'))
            # Small LIMIT queries can be planned entirely in the Java root.
            # They have no non-root task to place or native scan to audit.
            if others:
                assert scan_count > 0 and runtime_sources > 0, file
            scans += scan_count
            entry.update(planned_scan_nodes=scan_count, runtime_scan_operators=runtime_sources,
                         work_counts_per_scan=sorted(work_counts), native_task_states=task_states)
        queries.append(entry)
    assert files and len(foreman_ports) == 1
    cleanup = []
    if native and meta['force_jni_scan']:
        hosts = ['foreman'] if single else list(range(meta['worker_count'])) + (['foreman'] if meta.get('foreman_participates') else [])
        for index in hosts:
            log = (path / ('native-stdout.log' if index == 'foreman' else f'worker-{index}.log')).read_text(errors='replace')
            closed = re.findall(r'JNI plugin scan closed; active scans=(\d+)', log)
            assert closed and closed[-1] == '0'
            groups = [tuple(map(int, values)) for values in re.findall(
                r'JNI plugin scan work finished; works=(\d+); readers=(\d+); peak reads=(\d+)', log)]
            scheduling = [tuple(map(int, values)) for values in re.findall(
                r'; batches=(\d+); data wakes=(\d+); terminal wakes=(\d+); queue peak=(\d+); attachments=(\d+); detachments=(\d+)', log)]
            assert groups and len(groups) == len(scheduling)
            assert all(readers <= works and peak <= readers for works, readers, peak in groups)
            assert len(closed) == sum(max(readers, 1) for works, readers, peak in groups)
            assert all(queue <= 2 and wakes <= batches and detach <= attach
                       for batches, wakes, terminal, queue, attach, detach in scheduling)
            cleanup.append({'worker': index, 'last_active_scans': 0,
                            'peak_simultaneous_reads': max(peak for works, readers, peak in groups),
                            'reader_handles_closed': len(closed),
                            'queue_peak': max(values[3] for values in scheduling),
                            'attachments_observed': max(values[4] for values in scheduling),
                            'detachments_observed': max(values[5] for values in scheduling)})
    report = {'finished_query_execution_valid': True, 'all_requested_queries_finished': not failed,
              'foreman_count': 1, 'worker_count': len(workers),
              'worker_mode': meta['worker_mode'], 'deployment': meta.get('deployment', 'distributed'), 'root_minors': roots,
              'nonroot_minors': nonroots, 'planned_scan_nodes': scans,
              'scan_kind': expected_scan if native else 'java',
              'failures': failed, 'queries': queries, 'cleanup': cleanup,
              'process_shutdown': shutdown,
              'scope': 'placements and native plan/stats evidence; row correctness is validated separately'}
    (path / 'topology-validation.json').write_text(json.dumps(report, indent=2) + '\n')
    print(path.name, len(queries), 'finished executions;', roots, 'root,', nonroots,
          'nonroot minors;', len(failed), 'failed executions')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', type=Path, nargs='+')
    for run in parser.parse_args().runs:
        validate(run)
