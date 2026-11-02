#!/usr/bin/env python3
"""Run a single, fully validated 30-second v2 profiling observation."""
import argparse
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('comparison_runner', HERE / 'run.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--scenario', choices=('read', 'write', 'paged'), default='read')
    parser.add_argument('--concurrency', type=int, default=64)
    parser.add_argument('--stream', action='store_true')
    parser.add_argument('--rate', type=float, default=0,
                        help='scheduled arrivals/s; zero measures maximum throughput')
    parser.add_argument('--setup-only', action='store_true')
    parser.add_argument('--reuse-prepared', action='store_true')
    parser.add_argument('--identity-from', type=Path,
                        help='reuse validated source provenance for a retained binary replay')
    args = parser.parse_args()
    if not math.isfinite(args.rate) or args.rate < 0:
        parser.error('--rate must be finite and nonnegative')
    os.sched_setaffinity(0, {2, 18})
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    binary = args.binary.resolve()
    if binary.parent != r.CACHE or not binary.name.startswith('profile-'):
        raise ValueError('binary must be a separately named profile-* binary in the comparison cache')
    binary_hash = r.sha(binary)
    identity = None
    if args.identity_from:
        args.identity_from = args.identity_from.resolve()
        identity = json.loads((args.identity_from / 'environment.json').read_text())
        if identity['binary_sha256'] != binary_hash:
            raise ValueError('--identity-from binary hash differs from selected binary')
    import fcntl
    lock = (r.CACHE / 'run.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    nodes = json.loads((r.CACHE / 'cluster.json').read_text())
    hosts = ','.join(n['address'] for n in nodes)
    source_paths = sorted(set(r.command('git', 'ls-files', '--cached', '--others',
                                        '--exclude-standard', '--', '*.go', 'go.mod', 'go.sum').stdout.splitlines()))
    source_paths = [p for p in source_paths if not p.startswith('tests/driver-comparison/')]
    if identity:
        if (args.identity_from / 'untracked-driver').exists():
            shutil.copytree(args.identity_from / 'untracked-driver', out / 'untracked-driver')
    else:
        untracked = r.command('git', 'ls-files', '--others', '--exclude-standard', '--',
                              '*.go', 'go.mod', 'go.sum').stdout.splitlines()
        for path in untracked:
            if path not in source_paths:
                continue
            target = out / 'untracked-driver' / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(r.ROOT / path, target)
    r.write(out / 'environment.json', {
        'time': r.now(), 'git_head': (identity['git_head'] if identity else
                                    r.command('git', 'rev-parse', 'HEAD').stdout.strip()),
        'binary': str(binary), 'binary_sha256': binary_hash,
        'build': r.command('go', 'version', '-m', binary).stdout,
        'driver_source_hashes': (identity['driver_source_hashes'] if identity else
                                 {p: r.sha(r.ROOT / p) for p in source_paths}),
        'source_identity_from': str(args.identity_from) if identity else None,
        'nodes': nodes, 'lscpu': r.command('lscpu', '-J').stdout,
        'disk': r.command('df', '-B1', r.CACHE).stdout,
        'qdisc': r.command('sudo', '-n', 'tc', '-s', 'qdisc', 'show').stdout,
        'measurement_seconds': 30, 'warmup_seconds': 0,
        'target_rate': args.rate,
        'reuse_prepared': args.reuse_prepared,
    })
    if identity:
        shutil.copy2(args.identity_from / 'driver.patch', out / 'driver.patch')
    else:
        (out / 'driver.patch').write_text(r.command('git', 'diff', 'HEAD', '--', '*.go', 'go.mod', 'go.sum').stdout)
    snapshot = out / 'harness'
    snapshot.mkdir()
    for path in HERE.iterdir():
        if path.suffix in ('.go', '.py', '.sh') or path.name in ('go.mod', 'go.sum'):
            shutil.copy2(path, snapshot / path.name)

    def invoke(name, mode, **options):
        print(r.now(), name, flush=True)
        if r.client(out / (name + '.json'), hosts, binary.name, mode,
                    args.scenario, args.stream, **options):
            raise RuntimeError(f'{name} failed; see log')

    if not args.reuse_prepared:
        invoke('correctness', 'check')
        for node in nodes:
            r.command('sudo', '-n', 'podman', 'exec', node['name'], 'nodetool',
                      'clearsnapshot', '--keyspaces', 'bench_compare', timeout=120)
        invoke('prepare', 'prepare')
        table = 'paged' if args.scenario == 'paged' else 'point'
        for node in nodes:
            r.command('sudo', '-n', 'podman', 'exec', node['name'], 'nodetool',
                      'flush', 'bench_compare', table, timeout=120)
        deadline = time.monotonic() + 60
        while True:
            description = r.command('sudo', '-n', 'podman', 'exec', nodes[0]['name'],
                                    'nodetool', 'describecluster').stdout
            versions = re.findall(r'([0-9a-f]{8}-[0-9a-f-]{27}): \[(.*?)\]', description)
            if len(versions) == 1 and {ip.strip() for ip in versions[0][1].split(',')} == {n['address'] for n in nodes}:
                break
            if time.monotonic() > deadline:
                raise RuntimeError('schema agreement timeout')
            time.sleep(1)
        (out / 'schema-agreement.txt').write_text(description)
        r.write(out / 'settled.json', r.settled(nodes))
    invoke('warm-dataset', 'warm')
    if args.setup_only:
        return
    result = out / 'measurement.json'
    with r.Monitor(out / 'measurement.metrics.jsonl.gz', nodes):
        invoke('measurement', 'run', concurrency=args.concurrency, warmup='0s',
               duration='30s', rate=args.rate, output=result,
               **{'profile-prefix': out / 'measurement'})
    data = r.compact(result)
    if not r.valid(data):
        raise RuntimeError('invalid measurement')
    if data['driver'] != 'v2' or data['measurement']['duration_seconds'] != 30:
        raise RuntimeError('wrong driver or duration')
    data['server_activity'] = r.activity(out / 'measurement.metrics.jsonl.gz', data)
    r.write(result, data)
    print(json.dumps({k: data['measurement'][k] for k in ('throughput', 'errors', 'alloc_bytes', 'mallocs', 'gc', 'completed')}, indent=2), flush=True)


if __name__ == '__main__':
    main()
