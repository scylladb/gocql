#!/usr/bin/env python3
"""Paired screening of frozen v2 clients; this runner never grants acceptance.

Build manifests identify immutable binaries and the sources used to build them.
Keep the original benchmark configuration in main.go/v2.go unchanged. Real
Scylla preparation, health qualification and final acceptance belong to the
experiment coordinator; --backend=scylla does not certify those prerequisites.
"""
import argparse
import fcntl
import gzip
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import subprocess
import urllib.request

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location('roadmap_comparison', HERE / 'run.py')
r = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(r)

# Retained targets, not recomputed from new maximum-throughput observations.
CASES = {
    'point-buffered': ('read', False, 64, 56794.75666666667),
    'point-stream': ('read', True, 4, 32233.27333333333),
    'paged-buffered': ('paged', False, 4, 1878.0066666666667),
    'paged-stream': ('paged', True, 4, 1265.11),
    'write': ('write', False, 64, 52503.01),
}
HEX64 = re.compile(r'^[0-9a-f]{64}$')
HEX40 = re.compile(r'^[0-9a-f]{40}$')


def order(stage):
    """Return pairs in execution order, including the opposite ABBA pair."""
    if stage == 'baseline':
        return [('A',)]
    if stage in ('screen', 'controls'):
        return [('A', 'B'), ('B', 'A')]
    if stage == 'confirm':
        return [('A', 'B'), ('B', 'A')] * 3
    raise ValueError('unknown stage')


def bootstrap(values, samples=20000, seed=74183):
    if not values or samples < 10000 or not all(math.isfinite(v) for v in values):
        raise ValueError('finite paired values and at least 10000 resamples required')
    rng = random.Random(seed)
    n = len(values)
    means = sorted(statistics.mean(rng.choices(values, k=n)) for _ in range(samples))
    return {'mean_percent': statistics.mean(values),
            'ci95_percent': [means[int(samples * .025)], means[int(samples * .975)]],
            'paired_percent': values, 'pairs': n, 'resamples': samples, 'seed': seed}


def read_manifest(path, binary):
    data = json.loads(path.read_text())
    if not HEX40.fullmatch(data.get('git_head', '')):
        raise ValueError(f'{path}: expected full git_head')
    hashes = data.get('driver_source_hashes', {})
    if not isinstance(hashes, dict) or not hashes:
        raise ValueError(f'{path}: nonempty driver_source_hashes required')
    for name, value in hashes.items():
        if Path(name).is_absolute() or '..' in Path(name).parts or not HEX64.fullmatch(value):
            raise ValueError(f'{path}: invalid source hash entry {name}')
    if not data.get('build'):
        raise ValueError(f'{path}: build settings required')
    expected = data.get('binary_sha256', '')
    if not HEX64.fullmatch(expected) or r.sha(binary) != expected:
        raise ValueError(f'{path}: binary hash mismatch')
    build = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    if ('gocql-driver-comparison' not in build or
            'github.com/scylladb/gocql/v2' not in build or
            re.search(r'-tags=(?:[^\n]*,)?(?:old|scyllago)(?:,|\s|$)', build)):
        raise ValueError(f'{binary}: expected a v2 comparison binary')
    return dict(data, verified_build=build, manifest_sha256=r.sha(path))


def failures(result, case, rate, driver='v2', streaming=None, seconds=30, workers=None, gomaxprocs=1,
             allow_capacity_shortfall=False):
    scenario, stream, canonical_workers, _ = CASES[case]
    workers = canonical_workers if workers is None else workers
    m = result['measurement']
    issues = []
    if result.get('measurement_error'):
        issues.append('measurement reported an error: ' + result['measurement_error'])
    expected = {'driver': driver, 'scenario': scenario, 'stream': stream if streaming is None else streaming,
                'concurrency': workers, 'gomaxprocs': gomaxprocs}
    for key, value in expected.items():
        if result.get(key) != value:
            issues.append(f'{key} differs from canonical configuration')
    if m.get('duration_seconds') != seconds or m.get('target_rate') != rate:
        issues.append('duration or rate differs from canonical configuration')
    if not r.valid(result) or m.get('timeouts') != 0 or (not allow_capacity_shortfall and m.get('rejected') != 0):
        issues.append('error, timeout, rejection, affinity, histogram or drain failure')
    if m.get('queue_capacity') != max(1024, workers * 4):
        issues.append('arrival queue capacity differs from canonical worker-scaled capacity')
    if not m.get('affinity_checks', 0):
        issues.append('missing client affinity checks')
    for name in ('dispatch_latency', 'queue_latency', 'schedule_lag'):
        if m.get(name, {}).get('overflow') != 0:
            issues.append(f'{name} histogram overflow or missing histogram')
    if rate and not allow_capacity_shortfall:
        q = m.get('backlog_samples', [])[:math.ceil(seconds)]
        growing = (len(q) > 8 and statistics.mean(q[-len(q)//4:]) >
                   statistics.mean(q[:len(q)//4]) + max(1, workers / 4))
        if m['throughput'] < .99 * rate or growing:
            issues.append('fixed-rate delivery or backlog failure')
    if m['completed'] <= 0:
        issues.append('no completed operations')
    for phase in (m, result.get('warmup', {})):
        if 'offered' in phase and phase['offered'] != phase['admitted'] + phase['rejected']:
            issues.append('arrival accounting does not conserve offered operations')
        if 'queue_rejected' in phase and phase['rejected'] != phase['queue_rejected'] + phase['arrival_deadline_rejected']:
            issues.append('rejected-arrival reasons do not match total rejections')
    warm = result.get('warmup')
    if warm and (warm.get('errors') or warm.get('timeouts') or (not allow_capacity_shortfall and warm.get('rejected')) or
                 warm.get('affinity_error') or warm.get('admitted') != warm.get('completed') or
                 any(warm.get(name, {}).get('overflow') != 0 for name in
                     ('latency', 'dispatch_latency', 'queue_latency', 'schedule_lag'))):
        issues.append('warmup error, rejection, affinity, histogram or drain failure')
    configuration = {'protocol': 4, 'consistency': 'LOCAL_QUORUM',
                     'compression': 'none', 'tls': False, 'coalesce_wait_ns': 0,
                     'page_size': 32, 'prefetch': 0, 'retries': 0,
                     'metadata_refresh_ns': 0}
    for key, value in configuration.items():
        if result.get('evidence', {}).get(key) != value:
            issues.append(f'{key} differs from canonical driver configuration')
    return issues


def only_queue_capacity_failures(result):
    """Producer deadline failures and legacy ambiguous rejects are not overload."""
    for phase in (result.get('warmup', {}), result['measurement']):
        if phase.get('arrival_deadline_rejected', 0):
            return False
        if phase.get('rejected', 0) != phase.get('queue_rejected', 0):
            return False
    if any(phase.get('queue_rejected', 0) for phase in (result.get('warmup', {}), result['measurement'])):
        return True
    m = result['measurement']
    q = m.get('backlog_samples', [])[:math.ceil(m['duration_seconds'])]
    return (len(q) > 8 and statistics.mean(q[-len(q)//4:]) >
            statistics.mean(q[:len(q)//4]) + max(1, result['concurrency'] / 4))


def metrics(result):
    m = result['measurement']
    completed = m['completed']
    return {'throughput': m['throughput'],
            'cpu_per_op': m['cpu_seconds_including_drain'] / completed if completed else None,
            'bytes_per_op': m['alloc_bytes'] / completed if completed else None,
            'allocs_per_op': m['mallocs'] / completed if completed else None,
            'p99_us': m['latency']['p99_us']}


def read_observation(directory, case, rate, pair, variant, returncode, driver='v2', streaming=None,
                     seconds=30, workers=None, profiling=True, cpus='8', gomaxprocs=1):
    """Retain measurements even when the client exits unsuccessfully."""
    path = directory / 'measurement.json'
    if not path.exists():
        raise RuntimeError(f'{directory.name}: client returned {returncode}; no measurement result')
    result = r.compact(path)
    issues = failures(result, case, rate, driver=driver, streaming=streaming,
                      seconds=seconds, workers=workers, gomaxprocs=gomaxprocs)
    if returncode:
        issues.append(f'client returned {returncode}')
    for suffix in (('cpu.pprof', 'heap.before.pprof', 'heap.after.pprof') if profiling else ()):
        profile = directory / ('measurement.' + suffix)
        if not profile.exists() or not profile.stat().st_size:
            issues.append(f'missing {suffix}')
    if not profiling and result.get('profiles'):
        issues.append('profiling enabled during performance comparison')
    instrumentation = result.get('instrumentation')
    if not profiling and instrumentation and (instrumentation.get('cpu_profiling') or
            instrumentation.get('forced_gc') or instrumentation.get('heap_sample_rate') != 512 * 1024):
        issues.append('profiling, forced collection or changed allocation sampling during performance comparison')
    process_path = directory / 'measurement.json.process.json'
    process = json.loads(process_path.read_text()) if process_path.exists() else {}
    if process.get('masks') != [cpus] or not process.get('checks') or process.get('affinity_error'):
        issues.append('external process affinity audit missing or failed')
    if not profiling:
        for option in (process.get('runtime_environment', {}).get('GODEBUG') or '').split(','):
            if option.startswith('memprofilerate=') and option != 'memprofilerate=524288':
                issues.append('changed allocation sampling in runtime environment')
    m = result['measurement']
    return {'case': case, 'rate': rate, 'pair': pair, 'variant': variant,
            'directory': directory.name, 'returncode': returncode,
            'counters': {key: m.get(key) for key in
                         ('errors', 'timeouts', 'rejected', 'offered', 'admitted',
                          'completed', 'in_window', 'drained', 'peak_queue', 'end_queue')},
            'error_samples': m.get('error_samples'),
            'metrics': metrics(result), 'failures': issues}


def median_present(values):
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def summarize(records, stage):
    groups = {}
    for record in records:
        groups.setdefault((record['case'], record['rate']), []).append(record)
    summaries = {}
    for (case, rate), rows in groups.items():
        paired = {}
        for row in rows:
            paired.setdefault(row['pair'], {})[row['variant']] = row
        pairs = [p for _, p in sorted(paired.items()) if set(p) == {'A', 'B'}]
        summary = {'completed_pairs': len(pairs),
                   'expected_pairs': 0 if stage == 'baseline' else len(order(stage)),
                   'valid': all(not x['failures'] for x in rows), 'rate': rate}
        summary['variant_medians'] = {
            variant: {key: median_present(x['metrics'][key] for x in rows
                                           if x['variant'] == variant)
                      for key in ('throughput', 'cpu_per_op', 'bytes_per_op',
                                  'allocs_per_op', 'p99_us')}
            for variant in ('A', 'B') if any(x['variant'] == variant for x in rows)}
        for key in ('throughput', 'cpu_per_op', 'bytes_per_op', 'allocs_per_op'):
            deltas = []
            for p in pairs:
                a, b = p['A']['metrics'][key], p['B']['metrics'][key]
                if a is not None and b is not None and a > 0:
                    deltas.append(100 * ((b / a - 1) if key == 'throughput' else (1 - b / a)))
            if deltas:
                summary[key] = bootstrap(deltas)
        if pairs:
            a = statistics.median(p['A']['metrics']['p99_us'] for p in pairs)
            b = statistics.median(p['B']['metrics']['p99_us'] for p in pairs)
            if a > 0:
                summary['median_p99_regression_percent'] = 100 * (b / a - 1)
        complete = stage != 'baseline' and len(pairs) == len(order(stage)) and summary['valid']
        perf = any(summary.get(k, {}).get('mean_percent', 0) >= 3 and
                   summary[k]['ci95_percent'][0] > 0
                   for k in ('throughput', 'cpu_per_op'))
        memory = (summary.get('bytes_per_op', {}).get('mean_percent', 0) >= 10 and
                  summary['bytes_per_op']['ci95_percent'][0] > 0 and
                  summary.get('cpu_per_op', {}).get('mean_percent', -math.inf) >= -2)
        summary['promising'] = complete and (perf or memory)
        summary['confirmed_primary_threshold'] = stage == 'confirm' and summary['promising']
        summary['p99_pass'] = complete and summary.get('median_p99_regression_percent', math.inf) <= 5
        summaries[f'{case}:{rate:g}'] = summary
    return {'accepted': False,
            'acceptance_note': 'Coordinator must verify healthy real Scylla confirmation, all canonical controls, correctness and memory bounds.',
            'cases': summaries}


def snapshot(backend, emulator_pid=None):
    data = {'time': r.now(), 'cpu': Path('/proc/stat').read_text(),
            'loadavg': Path('/proc/loadavg').read_text(),
            'disk_free_bytes': shutil.disk_usage(r.ROOT).free}
    if backend == 'emulator':
        with urllib.request.urlopen('http://127.0.0.1:19080/stats', timeout=5) as response:
            data['emulator'] = json.load(response)
        if emulator_pid is not None:
            process = Path('/proc') / str(emulator_pid)
            threads = {}
            for task in (process / 'task').iterdir():
                try:
                    fields = (task / 'stat').read_text().rsplit(')', 1)[1].split()
                    threads[task.name] = {
                        'cpu_seconds': (int(fields[11]) + int(fields[12])) / os.sysconf('SC_CLK_TCK'),
                        'affinity': sorted(os.sched_getaffinity(int(task.name)))}
                except FileNotFoundError:
                    continue
            data['emulator_process'] = {'pid': emulator_pid,
                                        'stat': (process / 'stat').read_text(),
                                        'threads': threads}
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for variant in ('baseline', 'candidate'):
        parser.add_argument('--' + variant, type=Path, required=True)
        parser.add_argument('--' + variant + '-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stage', choices=('baseline', 'screen', 'confirm', 'controls'), required=True)
    parser.add_argument('--case', choices=CASES)
    parser.add_argument('--backend', choices=('emulator', 'scylla'), default='emulator')
    parser.add_argument('--hosts', default='127.0.0.2,127.0.0.3,127.0.0.4')
    parser.add_argument('--nodes-json', type=Path)
    parser.add_argument('--emulator-pid', type=int,
                        help='retain emulator per-thread CPU and affinity at each boundary')
    args = parser.parse_args()
    if args.stage != 'controls' and not args.case:
        parser.error('--case is required for baseline/screen/confirm')
    if args.backend == 'scylla' and (not args.nodes_json or args.hosts.startswith('127.')):
        parser.error('Scylla requires explicit --hosts and --nodes-json for metrics')
    binaries = {'A': args.baseline.resolve(), 'B': args.candidate.resolve()}
    identities = {'A': read_manifest(args.baseline_manifest, binaries['A']),
                  'B': read_manifest(args.candidate_manifest, binaries['B'])}
    # run.client performs basename-based process affinity auditing. Passing an
    # absolute driver path would disable that audit; use a relative cache path.
    for binary in binaries.values():
        if binary.parent != r.CACHE:
            parser.error('frozen binaries must be directly inside .codex-cache/driver-comparison')
    os.sched_setaffinity(0, {2, 18})
    with (r.CACHE / 'run.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        out = args.output.resolve()
        out.mkdir(parents=True, exist_ok=False)
        shutil.copy2(Path(__file__), out / 'roadmap_run.py')
        shutil.copy2(HERE / 'run.py', out / 'run.py')
        nodes = json.loads(args.nodes_json.read_text()) if args.nodes_json else []
        context = {'time': r.now(), 'stage': args.stage, 'backend': args.backend,
                   'hosts': args.hosts, 'nodes': nodes, 'identities': identities,
                   'client_cpu': 8, 'gomaxprocs': 1, 'monitor_cpus': [2, 18],
                   'seconds': 30, 'queue_capacity': 1024,
                   'warning': 'Synthetic emulator results are local screening only.'}
        r.write(out / 'context.json', context)
        records = []
        selected_cases = [args.case] if args.case else list(CASES)
        expected = len(selected_cases) * sum(len(x) for x in order(args.stage))
        if args.stage in ('baseline', 'controls'):
            expected *= 2
        state = {'status': 'running', 'observations': 0, 'expected_observations': expected}
        try:
            cases = selected_cases
            for case in cases:
                scenario, stream, workers, target = CASES[case]
                rates = [0, target] if args.stage in ('baseline', 'controls') else [0]
                for rate in rates:
                    for pair_index, variants in enumerate(order(args.stage), 1):
                        for variant in variants:
                            binary = binaries[variant]
                            if r.sha(binary) != identities[variant]['binary_sha256']:
                                raise RuntimeError('frozen binary changed during run')
                            name = f'{case}-{rate:g}-pair{pair_index}-{variant}'
                            d = out / name
                            d.mkdir()
                            print(r.now(), 'WARM', name, flush=True)
                            if r.client(d / 'warm.json', args.hosts, binary.name, 'warm', scenario, stream):
                                raise RuntimeError(f'{name}: complete dataset warm failed')
                            before = snapshot(args.backend, args.emulator_pid)
                            r.write(d / 'before.json', before)
                            print(r.now(), 'RUN', name, flush=True)
                            with r.Monitor(d / 'health.metrics.jsonl.gz', nodes):
                                rc = r.client(d / 'measurement.json', args.hosts, binary.name,
                                              'run', scenario, stream, concurrency=workers,
                                              duration='30s', warmup='0s', rate=rate,
                                              output=d / 'measurement.json',
                                              **{'profile-prefix': d / 'measurement'})
                            after = snapshot(args.backend, args.emulator_pid)
                            r.write(d / 'after.json', after)
                            record = read_observation(d, case, rate, pair_index, variant, rc)
                            issues = record['failures']
                            if nodes:
                                with gzip.open(d / 'health.metrics.jsonl.gz', 'rt') as health:
                                    samples = [json.loads(line) for line in health]
                                if len(samples) < 2 or any(
                                        not isinstance(sample['nodes'].get(node['name']), list)
                                        for sample in samples for node in nodes):
                                    issues.append('server metrics unavailable')
                            if args.backend == 'emulator' and before['emulator']['failures'] != after['emulator']['failures']:
                                issues.append('emulator protocol failures increased')
                            records.append(record)
                            r.write(out / 'observations.json', records)
                            r.write(out / 'summary.json', summarize(records, args.stage))
                            state['observations'] = len(records)
                            r.write(out / 'state.json', state)
                            print(json.dumps(record), flush=True)
                            if issues:
                                raise RuntimeError(f'{name}: {issues}')
            state.update(status='complete', finished=r.now())
        except BaseException as error:
            state.update(status='failed', error=str(error), finished=r.now())
            r.write(out / 'state.json', state)
            raise
        r.write(out / 'state.json', state)


if __name__ == '__main__':
    main()
