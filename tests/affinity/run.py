#!/usr/bin/env python3
# Copyright 2026 The Apache Software Foundation.
# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
# in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

"""Run isolated, randomized blocks; retain exact commands and raw JSON results."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import tempfile
import time

from matrix import case_label, cases_for_phase, validate_matrix

def process_cpu_seconds(pid):
    fields = Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cpus', default='8,10,12,14')
    parser.add_argument('--server-cpus', default='17,18,20,23')
    parser.add_argument('--duration', default='10s')
    parser.add_argument('--warmup', default='2s')
    parser.add_argument('--repeats', type=int, default=6)
    parser.add_argument('--phase', choices=['all', 'smoke', 'cpu', 'tcp', 'skew'], default='all')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parent
    root = source.parent.parent
    cpus, server_cpus = [int(n) for n in args.cpus.split(',')], [int(n) for n in args.server_cpus.split(',')]
    allowed = os.sched_getaffinity(0)
    assert set(cpus + server_cpus) <= allowed
    assert not set(cpus) & set(server_cpus)
    cores = [(Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/physical_package_id').read_text().strip(),
              Path(f'/sys/devices/system/cpu/cpu{cpu}/topology/core_id').read_text().strip())
             for cpu in cpus + server_cpus]
    assert len(set(cores)) == len(cores), 'client/server CPUs must use distinct physical cores'
    topology = {}
    for cpu in cpus + server_cpus:
        topology[cpu] = [{name: (p/name).read_text().strip()
                          for name in ('level', 'type', 'size', 'shared_cpu_list')}
                         for p in sorted(Path(f'/sys/devices/system/cpu/cpu{cpu}/cache').glob('index*'))]
    client_llc = {tuple((c['size'], c['shared_cpu_list']) for c in topology[cpu] if c['level'] == '3')
                  for cpu in cpus}
    assert len(client_llc) == 1, 'client CPUs must share one last-level cache domain'
    assert args.repeats > 0
    args.output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GO111MODULE='off', GOWORK='off', GOMAXPROCS=str(len(cpus)))
    metadata = {
        'schema_version': 2,
        'started_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'arguments': vars(args) | {'output': str(args.output)},
        'platform': platform.platform(), 'go': subprocess.check_output(['go', 'version'], text=True).strip(),
        'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(),
        'source_sha256': {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in sorted(source.glob('*')) if p.is_file()},
        'lscpu': subprocess.check_output(['lscpu'], text=True),
        'cpuinfo': Path('/proc/cpuinfo').read_text(),
        'cgroup': Path('/proc/self/cgroup').read_text(),
        'affinity': sorted(allowed), 'load_start': os.getloadavg(),
        'cache_topology': topology,
        'perf_event_paranoid': Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip(),
        'rps_sock_flow_entries': Path('/proc/sys/net/core/rps_sock_flow_entries').read_text().strip(),
        'rusage_units': 'Linux: seconds for time; context switches are counts',
        'cpu_busy_scope': 'overall host CPU utilization, including unrelated processes',
        'runtime_environment': {name: env.get(name) for name in ('GOMAXPROCS', 'GOGC', 'GOMEMLIMIT', 'GODEBUG', 'GOFLAGS')},
        'latency_scope': 'one random batch per 256-batch window; whole-run reservoir; queueing included only for skew',
        'latency_sampling': 'stratified-random-reservoir-v1',
        'socket_treatment': 'one-time SO_INCOMING_CPU option; sustained affinity is not enforced or claimed',
        'cgroup_cpu_limits': {},
    }
    cgroup_path = Path('/sys/fs/cgroup') / metadata['cgroup'].strip().split(':')[-1].lstrip('/')
    for ancestor in [cgroup_path, *cgroup_path.parents]:
        if str(ancestor).startswith('/sys/fs/cgroup') and (ancestor / 'cpu.max').exists():
            metadata['cgroup_cpu_limits'][str(ancestor)] = (ancestor / 'cpu.max').read_text().strip()
    (args.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
    cases = cases_for_phase(args.phase)
    metadata['expected_cases'] = [case_label(case) for case in cases]
    with tempfile.TemporaryDirectory(prefix='gocql-affinity-run-') as temporary:
        binary = str(Path(temporary) / 'affinity')
        subprocess.run(['go', 'build', '-o', binary, './' + str(source.relative_to(root))], env=env, cwd=root, check=True)
        metadata['binary_sha256'] = hashlib.sha256(Path(binary).read_bytes()).hexdigest()
        server_log = open(args.output / 'server.stderr', 'w')
        server_command = ['taskset', '-c', args.server_cpus, binary, '-mode=server', '-batch=32']
        server = subprocess.Popen(server_command, env=env | {'GOMAXPROCS': str(len(server_cpus))},
                                  stdout=subprocess.PIPE, stderr=server_log, text=True)
        try:
            info = json.loads(server.stdout.readline())
            metadata['server'] = info | {'command': server_command}
            (args.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')
            total = len(cases)*args.repeats
            finished = 0
            validated_rows = []
            with open(args.output / 'runs.jsonl', 'w', buffering=1) as records:
                for repetition in range(args.repeats):
                    order = list(cases)
                    random.Random(20261029+repetition).shuffle(order)
                    for mode, mib, updates, skew, variant in order:
                        name, cache, pin, socket_cpu = variant
                        label = case_label((mode, mib, updates, skew, variant))
                        command = ['taskset', '-c', args.cpus, binary, f'-mode={mode}', f'-cache={cache}',
                                   f'-pin={str(pin).lower()}', f'-socket-cpu={str(socket_cpu).lower()}',
                                   f'-skew={str(skew).lower()}', f'-cpus={args.cpus}', f'-mib={mib}',
                                   f'-updates={updates}', '-batch=32', f'-duration={args.duration}',
                                   f'-sample-seed={20261029+repetition}',
                                   f'-warmup={args.warmup}', f'-address={info["address"]}']
                        started = time.time()
                        server_cpu_before = process_cpu_seconds(server.pid)
                        completed = subprocess.run(command, env=env, text=True, capture_output=True, timeout=120)
                        record = {'label': label, 'repetition': repetition+1, 'command': command,
                                  'started_unix': started, 'wall_seconds': time.time()-started,
                                  'returncode': completed.returncode, 'stderr': completed.stderr,
                                  'server_cpu_seconds_including_client_setup_and_warmup': process_cpu_seconds(server.pid) - server_cpu_before,
                                  'host_load': os.getloadavg()}
                        try:
                            record['result'] = json.loads(completed.stdout)
                        except json.JSONDecodeError:
                            record['stdout'] = completed.stdout
                        records.write(json.dumps(record) + '\n')
                        if completed.returncode or 'result' not in record:
                            raise RuntimeError(f'failed {label}: {completed.stderr}')
                        result = record['result']
                        validated_rows.append(record)
                        assert not result.get('Error'), record
                        assert result['Operations'] > 0, record
                        for worker in result['Workers']:
                            assert not worker.get('Error')
                            assert worker['Operations'] > 0
                            assert sum(worker['CPUObservations'].values()) > 0
                            if pin:
                                assert set(map(int, worker['CPUObservations'])) == {worker['TargetCPU']}
                        finished += 1
                        print(f'{finished}/{total} repeat={repetition+1} {label}: '
                              f'{result["OperationsPerSecond"]/1e6:.3f} Mops/s '
                              f'CV={result["WorkerOperationsCV"]:.3f}', flush=True)
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait()
            server_log.close()
    metadata['finished_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    metadata['load_finish'] = os.getloadavg()
    metadata['runs'] = total
    _, missing = validate_matrix(validated_rows, metadata)
    if missing:
        raise ValueError(f'incomplete case/repetition matrix: {missing}')
    (args.output / 'environment.json').write_text(json.dumps(metadata, indent=2) + '\n')


if __name__ == '__main__':
    main()
