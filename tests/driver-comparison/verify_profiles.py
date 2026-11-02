#!/usr/bin/env python3
"""Audit completed v2-only 30-second profiling observations, not convergence."""
import argparse
import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    return json.loads(path.read_text())


def audit(directory):
    result = read(directory / 'measurement.json')
    measured = result['measurement']
    process = read(directory / 'measurement.json.process.json')
    environment = read(directory / 'environment.json')
    invalid = read(directory / 'INVALID.json') if (directory / 'INVALID.json').exists() else None
    binary = Path(environment['binary'])
    require(result['driver'] == 'v2', 'driver must be v2')
    require(result['gomaxprocs'] == 1, 'GOMAXPROCS must be 1')
    require(measured['duration_seconds'] == 30, 'measurement must be 30 seconds')
    require('warmup' not in result, 'profiling observation must have zero warmup')
    start, end = (datetime.datetime.fromisoformat(measured[k].replace('Z', '+00:00'))
                  for k in ('start', 'end'))
    require((end - start).total_seconds() == 30, 'measurement timestamps disagree')
    if not invalid:
        require(measured['errors'] == measured['timeouts'] == 0,
                'measurement has errors or timeouts without explicit INVALID.json')
    require(0 <= measured['timeouts'] <= measured['errors'], 'invalid error counters')
    require(measured['offered'] == measured['admitted'] + measured['rejected'],
            'arrival accounting mismatch')
    if measured['target_rate'] == 0:
        require(measured['rejected'] == 0, 'closed-loop observation rejected work')
    require(measured['completed'] == measured['admitted'] > 0, 'admitted work not joined')
    require(measured['completed'] == measured['in_window'] + measured['drained'] + measured['errors'],
            'completion/drain accounting mismatch')
    require(not measured['affinity_error'] and measured['affinity_checks'] > 0,
            'internal affinity check missing or failed')
    expected_returncode = 1 if measured['errors'] else 0
    require(process['returncode'] == expected_returncode and process['checks'] > 0
            and process['masks'] == ['8'] and not process['affinity_error'],
            'external process/affinity check failed')
    args = process['args']
    require(args[:5] == ['taskset', '-c', '8', 'env', 'GOMAXPROCS=1'],
            'client affinity/runtime invocation mismatch')
    require(Path(args[5]) == binary, 'measured binary differs from environment record')
    for flag, expected in (('-duration', '30s'), ('-warmup', '0s'), ('-mode', 'run'),
                           ('-scenario', result['scenario']),
                           ('-concurrency', str(result['concurrency']))):
        require(args[args.index(flag) + 1] == expected, f'invocation mismatch: {flag}')
    require(f"-stream={str(result['stream']).lower()}" in args, 'adapter mismatch')
    if measured['target_rate'] > 0 or '-rate' in args:
        require(float(args[args.index('-rate') + 1]) == measured['target_rate'],
                'target rate differs from invocation')
    require(hashlib.sha256(binary.read_bytes()).hexdigest() == environment['binary_sha256'],
            'retained binary hash mismatch')
    require(bool(environment['driver_source_hashes']), 'missing driver source identities')
    require((directory / 'driver.patch').exists(), 'missing driver patch')
    if environment.get('source_identity_from'):
        original = Path(environment['source_identity_from'])
        provenance = read(original / 'environment.json')
        require(provenance['binary_sha256'] == environment['binary_sha256']
                and provenance['git_head'] == environment['git_head']
                and provenance['driver_source_hashes'] == environment['driver_source_hashes'],
                'replayed binary source identity mismatch')
        require((original / 'driver.patch').read_bytes() ==
                (directory / 'driver.patch').read_bytes(), 'replay driver patch mismatch')
    untracked = directory / 'untracked-driver'
    if untracked.exists():
        for path in untracked.rglob('*'):
            if path.is_file():
                name = path.relative_to(untracked).as_posix()
                require(hashlib.sha256(path.read_bytes()).hexdigest() ==
                        environment['driver_source_hashes'].get(name),
                        f'untracked source hash mismatch: {name}')
    require((directory / 'harness' / 'profile.go').is_file(), 'missing profiling source snapshot')
    require((directory / 'measurement.metrics.jsonl.gz').is_file(), 'missing server metrics')
    with gzip.open(directory / 'measurement.metrics.jsonl.gz', 'rt') as stream:
        require(sum(1 for _ in stream) >= 2, 'insufficient metric samples')
    with gzip.open(directory / 'measurement.histograms.json.gz', 'rt') as stream:
        histograms = json.load(stream)
    for name in ('latency', 'dispatch_latency'):
        require(measured[name]['overflow'] == 0, f'{name} overflow')
        require(sum(histograms['measurement-' + name]['Counts']) == measured['completed'],
                f'{name} histogram count mismatch')
    for suffix, sample in (('cpu', 'cpu'), ('heap.before', 'alloc_space'),
                           ('heap.after', 'inuse_space')):
        path = directory / f'measurement.{suffix}.pprof'
        require(path.stat().st_size > 0, f'missing/empty profile: {suffix}')
        subprocess.run(['go', 'tool', 'pprof', '-top', '-nodecount=1',
                        '-sample_index=' + sample, str(binary), str(path)],
                       check=True, capture_output=True, text=True)
    return {'directory': str(directory), 'scenario': result['scenario'],
            'invalid': invalid, 'execution_errors': measured['errors'],
            'unhealthy': (read(directory / 'UNHEALTHY.json')
                          if (directory / 'UNHEALTHY.json').exists() else None),
            'excluded': (read(directory / 'EXCLUDED.json')
                         if (directory / 'EXCLUDED.json').exists() else None),
            'stream': result['stream'], 'concurrency': result['concurrency'],
            'throughput': measured['throughput'],
            'bytes_per_op': measured['alloc_bytes'] / measured['completed'],
            'allocs_per_op': measured['mallocs'] / measured['completed'],
            'target_rate': measured['target_rate'], 'rejected': measured['rejected'],
            'target_delivered': (not invalid and measured['errors'] == 0 and
                                 (measured['target_rate'] == 0 or
                                  (measured['rejected'] == 0 and measured['throughput'] >=
                                   0.99 * measured['target_rate']))),
            'binary_sha256': environment['binary_sha256']}


def main():
    # Keep profile decoding off the client/server CPUs during ongoing iterations.
    os.sched_setaffinity(0, {2, 18})
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directories', type=Path, nargs='+')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    rows, failures = [], []
    for directory in args.directories:
        try:
            rows.append(audit(directory))
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            failures.append({'directory': str(directory), 'error': str(error)})
    report = {'observations': rows, 'failures': failures,
              'scope': 'Artifact integrity only; does not establish optimization convergence.'}
    text = json.dumps(report, indent=2) + '\n'
    if args.output:
        args.output.write_text(text)
    print(text, end='')
    if failures:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
