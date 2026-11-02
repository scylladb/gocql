#!/usr/bin/env python3
"""Qualify unchanged original-v2 writes after benchmark-only storage repair.

Requires a frozen original source manifest and binary. This gate does not grant
driver performance acceptance. Keep all development/testing off CPU 8 and the
server CPUs throughout the run. Failed observations remain in the new directory.
"""
import argparse
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('redesign_runner', HERE / 'roadmap_run.py')
roadmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(roadmap)
r = roadmap.r
ORIGINAL = '7e1ad6f718929342200c5b9bc126e5a5e7fc7d1c'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    build = args.build.resolve()
    manifest = roadmap.read_manifest(build / 'manifest.json', build / 'v2')
    if manifest['git_head'] != ORIGINAL:
        parser.error('original production revision required')
    # Verify that the frozen original source, rather than an edited checkout,
    # produced the selected binary. Preserve its manifest with each qualification.
    for name, expected in manifest['driver_source_hashes'].items():
        if r.sha(Path(manifest['frozen_source']) / name) != expected:
            parser.error('frozen source changed: ' + name)
    os.sched_setaffinity(0, {2, 18})
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    r.write(out / 'source.json', manifest)
    shutil.copy2(Path(__file__), out / Path(__file__).name)
    binary = r.CACHE / 'redesign-original-qualified-client'
    shutil.copy2(build / 'v2', binary)
    nodes = json.loads((r.CACHE / 'cluster.json').read_text())
    hosts = ','.join(n['address'] for n in nodes)
    records = []
    status = {'qualified': False, 'performance_accepted': False, 'observations': records}
    with (r.CACHE / 'run.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        monitor = subprocess.Popen(['python3', str(HERE / 'redesign_health.py'),
                                    '--nodes-json', str(r.CACHE / 'cluster.json'),
                                    '--output', str(out / 'health.jsonl'), '--seconds', '1200'])
        try:
            for round in range(1, 4):
                for rate in (0, 52503.01):
                    d = out / f'write-{round}-{rate:g}'
                    d.mkdir()
                    if round == 1 and rate == 0:
                        for mode in ('check', 'prepare'):
                            if r.client(d / f'{mode}.json', hosts, binary.name, mode, 'write', False):
                                raise RuntimeError(mode + ' failed')
                        for node in nodes:
                            r.command('sudo', '-n', 'podman', 'exec', node['name'], 'nodetool',
                                      'flush', 'bench_compare', 'point', timeout=120)
                        r.write(d / 'settled.json', r.settled(nodes))
                    if r.sha(binary) != manifest['binary_sha256']:
                        raise RuntimeError('original binary changed')
                    if r.client(d / 'warm.json', hosts, binary.name, 'warm', 'write', False):
                        raise RuntimeError('complete dataset warm failed')
                    print(r.now(), 'RUN', round, rate, flush=True)
                    rc = r.client(d / 'measurement.json', hosts, binary.name, 'run', 'write', False,
                                  concurrency=64, duration='30s', warmup='0s', rate=rate,
                                  output=d / 'measurement.json')
                    result = json.loads((d / 'measurement.json').read_text())
                    issues = roadmap.failures(result, 'write', rate)
                    if rc:
                        issues.append('client exited unsuccessfully')
                    record = {'directory': str(d), 'round': round, 'rate': rate,
                              'issues': issues, 'throughput': result['measurement']['throughput'],
                              'errors': result['measurement']['errors'],
                              'rejected': result['measurement']['rejected']}
                    records.append(record)
                    print(json.dumps(record), flush=True)
                    r.write(out / 'qualification.json', status)
                    if issues:
                        raise RuntimeError('original baseline failed: ' + str(issues))
            status['qualified'] = True
        except Exception as error:
            status.update(error=str(error), dedicated_storage_required=True)
            raise
        finally:
            monitor.terminate()
            monitor.wait(timeout=10)
            status['finished'] = r.now()
            r.write(out / 'qualification.json', status)


if __name__ == '__main__':
    main()
