#!/usr/bin/env python3
"""Balanced, frozen-reference comparisons; emulator evidence never qualifies Scylla.

Performance observations are unprofiled. Diagnostic profiles are a separate stage.
A screen selects experiments; only a complete confirmation matrix can retain one.
"""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import shutil
import signal
import socket
import statistics
import subprocess
import time
from contextlib import nullcontext

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('redesign_roadmap', HERE / 'roadmap_run.py')
roadmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(roadmap)
r = roadmap.r
REFERENCES = {
    'master': '1ceaac8172bda26e8b8d701ac356982d33729b27',
    'original-v2': '7e1ad6f718929342200c5b9bc126e5a5e7fc7d1c',
    'parked': 'f363e371832e7d8260e2858fba867fe591f655a4',
}
MASTER = REFERENCES['master']
WORKERS = (1, 4, 16, 64, 256, 1024)
LOADS = ('maximum', '70', '85', 'historical')


def cpu_set(value):
    cpus = set()
    for part in value.split(','):
        ends = part.split('-')
        if len(ends) > 2:
            raise ValueError('invalid CPU range')
        first, last = int(ends[0]), int(ends[-1])
        if first < 0 or last < first:
            raise ValueError('invalid CPU range')
        cpus.update(range(first, last + 1))
    return cpus


def cpu_list(value):
    groups = []
    for cpu in sorted(cpu_set(value)):
        if groups and cpu == groups[-1][1] + 1:
            groups[-1][1] = cpu
        else:
            groups.append([cpu, cpu])
    return ','.join(str(a) if a == b else f'{a}-{b}' for a, b in groups)


def production_source(name):
    return not name.startswith('tests/driver-comparison/') and (
        name in ('go.mod', 'go.sum') or name.endswith('.go') and not name.endswith('_test.go'))


def pinned_hashes(revision):
    """Read immutable Git objects, so a dirty tree cannot impersonate a reference."""
    names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', revision],
                                    cwd=r.ROOT, text=True).splitlines()
    names = [name for name in names if production_source(name)]
    raw = subprocess.run(['git', 'cat-file', '--batch'], cwd=r.ROOT,
                         input=''.join(f'{revision}:{name}\n' for name in names).encode(),
                         capture_output=True, check=True).stdout
    hashes, position = {}, 0
    for name in names:
        header_end = raw.index(b'\n', position)
        _, kind, size = raw[position:header_end].split()
        if kind != b'blob':
            raise ValueError('expected pinned source blob: ' + name)
        start = header_end + 1
        position = start + int(size)
        hashes[name] = hashlib.sha256(raw[start:position]).hexdigest()
        position += 1
    return hashes


def paired_ratio(values, samples=20000, seed=74183):
    """Geometric mean of paired B/A ratios, bootstrapped in log space."""
    if samples < 20000 or not values or not all(math.isfinite(x) and x >= 0 for x in values):
        raise ValueError('nonnegative finite paired ratios required')
    rng = random.Random(seed)
    logs = [math.log(x) if x else -math.inf for x in values]
    draws = sorted(math.exp(sum(rng.choices(logs, k=len(logs))) / len(logs))
                   for _ in range(samples))
    return {'ratio': math.exp(statistics.mean(logs)),
            'ci95': [draws[int(samples * .025)], draws[int(samples * .975)]],
            'paired_ratios': values, 'pairs': len(values), 'resamples': samples, 'seed': seed}


def capacity_only_failure(result, record, **configuration):
    if not record['rate'] or not record['failures'] or record['returncode']:
        return False
    if not roadmap.only_queue_capacity_failures(result):
        return False
    normal = roadmap.failures(result, record['case'], record['rate'], **configuration)
    remaining = roadmap.failures(result, record['case'], record['rate'], allow_capacity_shortfall=True, **configuration)
    # Extra process/profiling/provenance errors can never be called overload.
    return not remaining and set(record['failures']).issubset(normal)


def summarize(records, stage, required, confirmation_sets=1, allow_reference_overload=False):
    """Never discard failed rows or allow missing/duplicate observations to pass."""
    groups = {}
    for row in records:
        groups.setdefault((row['case'], row['workers'], row['load']), []).append(row)
    summaries = {}
    benefit = regression = False
    complete = stage == 'confirm' and set(groups) == set(required)
    valid = bool(records) and all(not x['failures'] for x in records)
    decision_valid = bool(records)
    blocked_references, candidate_delivery_failures = [], []
    expected = 6 * confirmation_sets if stage == 'confirm' else 2
    for key, rows in groups.items():
        by_pair = {}
        duplicate = False
        for row in rows:
            pair = by_pair.setdefault(row['pair'], {})
            duplicate |= row['variant'] in pair
            pair[row['variant']] = row
        pairs = [p for p in by_pair.values() if set(p) == {'A', 'B'}]
        group_valid = not duplicate and all(not x['failures'] for x in rows)
        group_complete = (len(pairs) == expected and len(rows) == expected * 2 and
                          set(by_pair) == set(range(1, expected + 1)))
        overloaded = []
        unexplained = duplicate
        for pair_id, pair in by_pair.items():
            a, b = pair.get('A'), pair.get('B')
            if a and b and not a['failures'] and b.get('capacity_only_failure'):
                candidate_delivery_failures.append({'case': key, 'pair': pair_id})
            reference_overloaded = bool(allow_reference_overload and key[2] != 'maximum' and a and a.get('capacity_only_failure'))
            if reference_overloaded:
                overloaded.append(pair_id)
            for variant, row in pair.items():
                if row['failures'] and not (reference_overloaded and row.get('capacity_only_failure')):
                    unexplained = True
        permitted_block = bool(overloaded) and not unexplained
        decision_valid &= group_valid or permitted_block
        complete &= (group_valid or permitted_block) and group_complete
        valid &= group_valid
        summary = {'valid': group_valid, 'complete': group_complete,
                   'pairs': len(pairs), 'rate': rows[0]['rate'], 'metrics': {},
                   'blocked_reference_pairs': overloaded,
                   'status': 'blocked-reference-overload' if permitted_block else 'valid' if group_valid else 'invalid'}
        if permitted_block:
            blocked_references.append('%s:w%d:%s' % key)
        for metric in ('throughput', 'p99_us', 'cpu_per_op', 'bytes_per_op', 'allocs_per_op'):
            ratios = []
            for p in pairs:
                a, b = p['A']['metrics'].get(metric), p['B']['metrics'].get(metric)
                if a is not None and b is not None and a > 0 and b >= 0:
                    ratios.append(b / a)
            if len(ratios) != len(pairs) or not ratios:
                complete = False
                continue
            measured = paired_ratio(ratios)
            low, high = measured['ci95']
            # Maximum-rate p99 and fixed-rate throughput are descriptive only.
            primary = (metric == 'throughput' and key[2] == 'maximum' or
                       metric == 'p99_us' and key[2] != 'maximum' or
                       metric in ('cpu_per_op', 'bytes_per_op', 'allocs_per_op'))
            favorable = low > 1 if metric == 'throughput' else high < 1
            unfavorable = high < 1 if metric == 'throughput' else low > 1
            measured.update(primary=primary, inferential=group_valid,
                            benefit=primary and favorable and group_valid,
                            regression=primary and unfavorable and group_valid)
            benefit |= group_valid and group_complete and measured['benefit']
            regression |= group_valid and group_complete and measured['regression']
            summary['metrics'][metric] = measured
        summaries['%s:w%d:%s' % key] = summary
    if candidate_delivery_failures:
        outcome = 'rejected'
    elif not decision_valid:
        outcome = 'blocked'
    elif regression:
        outcome = 'rejected'
    elif complete and benefit:
        outcome = 'retained'
    else:
        outcome = 'inconclusive'
    return {'outcome': outcome, 'complete': bool(complete), 'valid': valid,
            'decision_valid': decision_valid, 'blocked_reference_cases': blocked_references,
            'candidate_delivery_failures': candidate_delivery_failures,
            'allow_reference_overload': allow_reference_overload,
            'demonstrated_benefit': benefit, 'confirmed_regression': regression,
            'performance_accepted': False, 'confirmation_sets': confirmation_sets,
            'repeat_required': stage == 'confirm' and complete and outcome == 'inconclusive' and confirmation_sets == 1,
            'scope': 'Backend-local evidence; dedicated-host and final qualification are separate.',
            'cases': summaries}


def identity(path, binary, reference=None):
    data = json.loads(path.read_text())
    if reference and data.get('git_head') != REFERENCES.get(reference, data.get('git_head')):
        raise ValueError('reference commit mismatch')
    if not roadmap.HEX40.fullmatch(data.get('git_head', '')) or not data.get('build'):
        raise ValueError('full source revision and toolchain build settings required')
    if r.sha(binary) != data['binary_sha256']:
        raise ValueError('binary hash mismatch')
    if not data.get('driver_source_hashes'):
        raise ValueError('nonempty driver source hashes required')
    for name, expected in data['driver_source_hashes'].items():
        if Path(name).is_absolute() or '..' in Path(name).parts or not roadmap.HEX64.fullmatch(expected):
            raise ValueError('invalid source path')
        if r.sha(Path(data['frozen_source']) / name) != expected:
            raise ValueError('frozen source changed: ' + name)
    if reference in REFERENCES:
        actual = {name: value for name, value in data['driver_source_hashes'].items() if production_source(name)}
        if actual != pinned_hashes(REFERENCES[reference]):
            raise ValueError('pinned reference production source differs from Git revision')
    # Earlier master manifests call this directory master-harness.
    harness = path.parent / ('master-harness' if reference == 'master' and
                             (path.parent / 'master-harness').exists() else 'harness')
    hashes = data.get('harness_source_hashes', {})
    if not hashes:
        raise ValueError('frozen harness hashes required')
    for name, expected in hashes.items():
        if Path(name).name != name or r.sha(harness / name) != expected:
            raise ValueError('frozen harness changed: ' + name)
    build = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    if 'gocql-driver-comparison' not in build:
        raise ValueError('comparison client binary required')
    return dict(data, manifest_sha256=r.sha(path), verified_build=build, verified_harness=str(harness))


def capacity_reference(manifest, binary, current, rates):
    """Preserve healthy frozen maxima while correcting arrival bookkeeping."""
    previous = identity(manifest, binary, 'master')
    if previous['binary_sha256'] != rates['master_binary_sha256']:
        raise ValueError('capacity reference is not the frozen master binary')
    if previous['verified_build'].splitlines()[0].split(': ', 1)[-1] != current['verified_build'].splitlines()[0].split(': ', 1)[-1]:
        raise ValueError('capacity reference toolchain differs')
    before, after = Path(previous['verified_harness']), Path(current['verified_harness'])
    if r.sha(before / 'old.go') != r.sha(after / 'old.go'):
        raise ValueError('master adapter differs from frozen capacity reference')
    helper = HERE / 'tools/harness-identity/main.go'
    environment = dict(os.environ, GOTMPDIR=os.environ.get('GOTMPDIR', str(r.CACHE / 'tmp')))
    hashes = [json.loads(subprocess.check_output(['go', 'run', str(helper), '--', str(path / 'main.go')],
                                                env=environment, text=True)) for path in (before, after)]
    if hashes[0] != hashes[1]:
        raise ValueError('canonical workload differs from frozen capacity reference')
    return dict(identity=previous, workload_hashes=hashes[0], helper_sha256=r.sha(helper),
                reason='Corrected fixed-arrival delivery bookkeeping; original healthy maximum capacities and every target rate remain unchanged')


def qualification(path, hosts, local_cluster=False, allow_reference_overload=False):
    """Bind evidence to endpoints; local testing requires an explicit opt-in."""
    data = json.loads(path.read_text())
    local = local_cluster and data.get('local_cluster') is True and data.get('dedicated_host') is False
    if (data.get('qualified') is not True or not (local or data.get('dedicated_host') is True) or
            data.get('hostname') != socket.gethostname() or data.get('hosts') != hosts or
            data.get('references') != REFERENCES):
        raise ValueError('dedicated, qualified local host and matching references/endpoints required')
    names = ['compiler', 'server_image', 'rf3_topology', 'cpu_topology', 'storage',
             'original_write_maxima', 'write_controls_52503_01', 'wire_dataset_equivalence']
    names += ['network_delay'] if local else ['master_stability']
    for name in names:
        evidence = data.get('evidence', {}).get(name, {})
        if not evidence or r.sha(path.parent / evidence['path']) != evidence['sha256']:
            raise ValueError('missing or changed qualification evidence: ' + name)
    if local:
        if data.get('boot_id') != Path('/proc/sys/kernel/random/boot_id').read_text().strip():
            raise ValueError('local cluster receipt belongs to another boot')
        if len(data.get('cpu_roles', {}).get('client', [])) != 1 or data.get('gomaxprocs') != 1:
            raise ValueError('local canonical client requires exactly one CPU and GOMAXPROCS=1')
        if data.get('blocked_controls') and not allow_reference_overload:
            raise ValueError('blocked reference controls require the explicit overload policy')
    cores = {}
    for role in ('client', 'server', 'monitoring', 'development'):
        cpus = data.get('cpu_roles', {}).get(role, [])
        if not cpus:
            raise ValueError('missing isolated CPU role: ' + role)
        cores[role] = set()
        for cpu in cpus:
            siblings = Path(f'/sys/devices/system/cpu/cpu{int(cpu)}/topology/thread_siblings_list').read_text().strip()
            cores[role].update(cpu_set(siblings))
    for role, assigned in cores.items():
        if any(assigned & other for name, other in cores.items() if name != role):
            raise ValueError('CPU roles share a physical core')
    return dict(data, receipt_sha256=r.sha(path))


def resume_records(previous, context):
    """Continue an interrupted matrix without removing any completed window."""
    before = json.loads((previous / 'context.json').read_text())
    state = json.loads((previous / 'state.json').read_text())
    if before['stage'] != 'confirm' or context['stage'] != 'confirm' or state['status'] != 'interrupted' or before.get('repeat_of'):
        raise ValueError('resume requires an interrupted first confirmation set')
    keys = ('backend', 'backend_identity', 'hosts', 'reference', 'seconds', 'warmup_seconds', 'settle_seconds',
            'client_cpus', 'gomaxprocs', 'monitor_cpus', 'emulator_cpus', 'profiling', 'rates_sha256',
            'hostname', 'boot_id', 'allow_reference_overload')
    for key in keys:
        if before.get(key) != json.loads(json.dumps(context.get(key))):
            raise ValueError('resume configuration differs: ' + key)
    for name in ('roadmap_run.py', 'run.py'):
        if before['runner_hashes'][name] != context['runner_hashes'][name]:
            raise ValueError('resume measurement harness differs: ' + name)
    for variant in ('A', 'B'):
        if before['identities'][variant]['binary_sha256'] != context['identities'][variant]['binary_sha256']:
            raise ValueError('resume binary differs: ' + variant)
    previous_matrix = before.get('matrix', [])
    current_matrix = json.loads(json.dumps(context.get('matrix', [])))
    reordered = previous_matrix != current_matrix
    if {tuple(x) for x in previous_matrix} != {tuple(x) for x in current_matrix}:
        raise ValueError('resume configuration differs: matrix')
    completed, interrupted = [], []
    for row in json.loads((previous / 'observations.json').read_text()):
        directory = Path(row['directory'])
        if not directory.is_absolute():
            directory = previous / directory
        row = dict(row, directory=str(directory))
        if row['failures'] == ['observation incomplete'] and not row.get('metrics'):
            if (directory / 'measurement.json').exists():
                raise ValueError('unprocessed measurement requires recovery before resume: ' + str(directory))
            interrupted.append(row)
        else:
            completed.append(row)
    coordinates = [(x['case'], x['workers'], x['load'], x['pair'], x['variant']) for x in completed]
    if len(set(coordinates)) != len(coordinates):
        raise ValueError('duplicate completed observations in resume source')
    if reordered:
        pairs = {}
        for case, workers, load, pair, variant in coordinates:
            pairs.setdefault((case, workers, load, pair), set()).add(variant)
        if any(variants != {'A', 'B'} for variants in pairs.values()):
            raise ValueError('resume cannot reorder an incomplete pair')
    provenance = dict(directory=str(previous),
                      files={name: r.sha(previous / name) for name in ('context.json', 'state.json', 'observations.json')},
                      execution_order_changed=reordered,
                      orchestration_changed=before['runner_hashes']['redesign_compare.py'] != context['runner_hashes']['redesign_compare.py'])
    return completed, interrupted, provenance


def qualification_supplement(path, cpus):
    """Attach fresh original controls without changing physical backend identity."""
    data = json.loads(path.read_text())
    rows = data.get('observations', [])
    expected = {(n, rate) for n in (1, 2, 3) for rate in (0, 52503.01)}
    if (not data.get('qualified_for_valid_comparisons') or len(rows) != 6 or
            {(r['round'], r['rate']) for r in rows} != expected):
        raise ValueError('complete original write controls required')
    files = {str(path): r.sha(path)}
    for row in rows:
        directory = Path(row['directory'])
        measured = directory / 'measurement.json'
        result = json.loads(measured.read_text())
        if 'queue_rejected' not in result['measurement']:
            raise ValueError('supplement requires corrected arrival accounting')
        issues = roadmap.failures(result, 'write', row['rate'])
        if issues and not (row['rate'] and roadmap.only_queue_capacity_failures(result) and
                           not roadmap.failures(result, 'write', row['rate'], allow_capacity_shortfall=True)):
            raise ValueError('invalid original write control: ' + str(issues))
        process = measured.with_suffix('.json.process.json')
        audit = json.loads(process.read_text())
        if audit.get('masks') != [cpus] or not audit.get('checks') or audit.get('returncode') != 0 or audit.get('affinity_error'):
            raise ValueError('original control process audit failed')
        files.update({str(measured): r.sha(measured), str(process): r.sha(process)})
    source = path.parent / 'source.json'
    frozen = json.loads(source.read_text())
    binary = Path(frozen['binary'])
    original = identity(binary.parent / 'manifest.json', binary, 'original-v2')
    if frozen['manifest_sha256'] != original['manifest_sha256']:
        raise ValueError('original source manifest changed')
    files[str(source)] = r.sha(source)
    return dict(receipt=data, receipt_sha256=r.sha(path), files=files, identity=original)


def freeze_rates(records, context):
    rates = {}
    for row in records:
        if row['failures'] or row['variant'] != 'A' or row['rate'] != 0:
            raise ValueError('healthy master maxima required')
        rates.setdefault('%s:w%d' % (row['case'], row['workers']), []).append(row)
    frozen = {}
    for key, rows in rates.items():
        if len(rows) != 3 or len({x['pair'] for x in rows}) != 3:
            raise ValueError('three distinct healthy master observations required')
        values = [x['metrics']['throughput'] for x in rows]
        if r.cv(values) > .05:
            raise ValueError('unstable master capacity: ' + key)
        capacity = statistics.median(values)
        frozen[key] = {'capacity': capacity, '70': .7 * capacity, '85': .85 * capacity,
                       'directories': [x['directory'] for x in rows]}
    return {'backend': context['backend'], 'hosts': context['hosts'],
            'client_cpus': context.get('client_cpus', '8'), 'gomaxprocs': context['gomaxprocs'],
            'backend_identity': context.get('backend_identity'),
            'master_binary_sha256': context['identities']['A']['binary_sha256'],
            'reference': MASTER, 'profiling': False, 'rates': frozen}


def load_rates(path, backend, hosts, cpus='8', gomaxprocs=1, backend_identity=None):
    data = json.loads(path.read_text())
    if (data['backend'] != backend or data['hosts'] != hosts or
            data['reference'] != MASTER or data['profiling'] is not False or not data['rates'] or
            data.get('client_cpus', '8') != cpus or data.get('gomaxprocs', 1) != gomaxprocs):
        raise ValueError('frozen unprofiled master rates for this backend/endpoints required')
    if backend_identity is not None and data.get('backend_identity') != backend_identity:
        raise ValueError('backend configuration differs from frozen master baseline')
    for entry in data['rates'].values():
        capacity = entry['capacity']
        if not math.isfinite(capacity) or capacity <= 0 or any(
                entry[load] != fraction * capacity for load, fraction in (('70', .7), ('85', .85))):
            raise ValueError('invalid frozen master rates')
    provenance = data.get('provenance')
    if not provenance:
        raise ValueError('frozen baseline evidence hashes required')
    baseline = Path(provenance['directory'])
    for name, expected in provenance['files'].items():
        if Path(name).is_absolute() or '..' in Path(name).parts or r.sha(baseline / name) != expected:
            raise ValueError('frozen baseline evidence changed: ' + name)
    context = json.loads((baseline / 'context.json').read_text())
    records = json.loads((baseline / 'observations.json').read_text())
    if (context['reference'] != 'master' or context['profiling'] or
            context['identities']['A']['git_head'] != MASTER):
        raise ValueError('healthy unprofiled pinned master baseline required')
    regenerated = freeze_rates(records, context)
    if any(data[key] != regenerated[key] for key in regenerated):
        raise ValueError('frozen rates differ from retained baseline')
    return data


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--master', '--baseline', dest='baseline', type=Path, required=True)
    parser.add_argument('--master-manifest', '--baseline-manifest', dest='baseline_manifest', type=Path, required=True)
    parser.add_argument('--reference', choices=(*REFERENCES, 'latest'), default='master')
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stage', choices=('baseline', 'screen', 'confirm', 'profile'), default='screen')
    parser.add_argument('--synchronization-profiles', action='store_true',
                        help='Collect block/mutex profiles in the diagnostic profile stage only')
    parser.add_argument('--syscall-trace', action='store_true',
                        help='Attach strace during diagnostic profile runs; record network syscalls and write batch bytes')
    parser.add_argument('--backend', choices=('emulator', 'scylla'), default='emulator')
    parser.add_argument('--hosts', help='Explicit endpoints; required for dedicated Scylla')
    parser.add_argument('--qualification', type=Path)
    parser.add_argument('--qualification-supplement', type=Path,
                        help='Fresh corrected-harness original controls; preserve the existing physical backend/rate identity')
    parser.add_argument('--local-cluster', action='store_true',
                        help='Use explicitly authorized local cluster evidence; results do not claim dedicated-host qualification')
    parser.add_argument('--nodes-json', type=Path)
    parser.add_argument('--emulator-pid', type=int)
    parser.add_argument('--client-cpus', type=cpu_list, default='8')
    parser.add_argument('--monitor-cpus', type=cpu_list, default='2,18')
    parser.add_argument('--emulator-cpus', type=cpu_list, default='0-1,4-5')
    parser.add_argument('--gomaxprocs', type=int, default=1, help='Values above one are separate scalability diagnostics')
    parser.add_argument('--rates', type=Path, help='Immutable master baseline rates')
    parser.add_argument('--capacity-master-binary', type=Path, help='Original capacity binary when master is rebuilt to correct harness bookkeeping')
    parser.add_argument('--capacity-master-manifest', type=Path)
    parser.add_argument('--case', choices=roadmap.CASES, action='append')
    parser.add_argument('--workers', type=int, choices=WORKERS, nargs='+', help='Separate worker sweep; default canonical 64/4')
    parser.add_argument('--load', choices=LOADS, action='append')
    parser.add_argument('--seconds', type=float, help='Default 30 screen, 120 confirmation')
    parser.add_argument('--warmup', type=float, default=5)
    parser.add_argument('--settle', type=float, default=2)
    parser.add_argument('--repeat-of', type=Path, help='Repeat one complete inconclusive confirmation; preserve and combine both sets')
    parser.add_argument('--resume-from', type=Path, help='Continue an interrupted confirmation in a new directory, preserving every completed window')
    parser.add_argument('--allow-reference-overload', action='store_true',
                        help='Explicitly allow adoption from valid confirmed comparisons while overloaded-reference latency cases remain blocked')
    parser.add_argument('--stop-on-confirmed-regression', action='store_true',
                        help='Preserve the completed observations and end confirmation when a full six-pair group confirms a regression')
    args = parser.parse_args()
    if bool(args.capacity_master_binary) != bool(args.capacity_master_manifest):
        parser.error('both capacity-master binary and manifest are required')
    if args.resume_from and args.repeat_of:
        parser.error('--resume-from and --repeat-of are mutually exclusive')
    if args.local_cluster and args.backend != 'scylla':
        parser.error('--local-cluster requires --backend scylla')
    if args.qualification_supplement and args.backend != 'scylla':
        parser.error('--qualification-supplement requires --backend scylla')
    if (args.synchronization_profiles or args.syscall_trace) and args.stage != 'profile':
        parser.error('synchronization sampling and syscall tracing require --stage profile')
    seconds = args.seconds if args.seconds is not None else (120 if args.stage == 'confirm' else 30)
    if (args.gomaxprocs < 1 or not math.isfinite(seconds) or seconds <= 1 or any(
            not math.isfinite(x) or x < 0 for x in (args.warmup, args.settle))):
        parser.error('finite positive duration >1s and nonnegative warmup/settling required')
    args.hosts = args.hosts or ('127.0.0.2,127.0.0.3,127.0.0.4' if args.backend == 'emulator' else None)
    receipt = None
    if args.backend == 'scylla':
        if not args.hosts or not args.qualification or not args.nodes_json:
            parser.error('Scylla requires explicit endpoints, nodes and a dedicated-host qualification receipt')
        receipt = qualification(args.qualification, args.hosts, args.local_cluster, args.allow_reference_overload)
        if (set(receipt['cpu_roles']['client']) != cpu_set(args.client_cpus) or
                set(receipt['cpu_roles']['monitoring']) != cpu_set(args.monitor_cpus)):
            parser.error('client/monitor CPUs differ from cluster qualification')
        if args.local_cluster and (len(cpu_set(args.client_cpus)) != 1 or args.gomaxprocs != 1):
            parser.error('local canonical client requires exactly one CPU and GOMAXPROCS=1')
    elif not args.emulator_pid:
        parser.error('emulator PID required for affinity audit')
    supplement = qualification_supplement(args.qualification_supplement.resolve(), args.client_cpus) if args.qualification_supplement else None
    if supplement and supplement['receipt'].get('blocked_controls') and not args.allow_reference_overload:
        parser.error('blocked supplementary controls require the explicit overload policy')
    if args.backend == 'emulator':
        emulator = Path('/proc') / str(args.emulator_pid)
        environ = dict(entry.split('=', 1) for entry in (emulator / 'environ').read_text().split('\0') if '=' in entry)
        backend_identity = {'binary_sha256': r.sha(emulator / 'exe'), 'cpus': args.emulator_cpus,
                            'gomaxprocs': environ.get('GOMAXPROCS')}
    else:
        backend_identity = {'qualification_sha256': receipt['receipt_sha256']}
    if args.stage == 'baseline' and args.reference != 'master':
        parser.error('rate freezing requires pinned master')
    loads = args.load or (['maximum'] if args.stage in ('baseline', 'profile') else list(LOADS))
    if args.stage in ('baseline', 'profile') and loads != ['maximum']:
        parser.error('baseline/profile requires maximum load only')
    rates = load_rates(args.rates, args.backend, args.hosts, args.client_cpus, args.gomaxprocs, backend_identity) if args.rates else None
    if any(x in loads for x in ('70', '85')) and not rates:
        parser.error('matched rates must be frozen from healthy master baselines first')
    binaries = {'A': args.baseline.resolve(), 'B': args.candidate.resolve()}
    identities = {'A': identity(args.baseline_manifest, binaries['A'], args.reference),
                  'B': identity(args.candidate_manifest, binaries['B'])}
    capacity = None
    if args.reference == 'master' and rates and rates['master_binary_sha256'] != identities['A']['binary_sha256']:
        if not args.capacity_master_binary or not args.capacity_master_manifest:
            parser.error('master binary differs from the frozen rate baseline; an explicit capacity reference is required')
        capacity = capacity_reference(args.capacity_master_manifest.resolve(), args.capacity_master_binary.resolve(), identities['A'], rates)
    for binary in binaries.values():
        if binary.parent != r.CACHE:
            parser.error('binaries must be directly in comparison cache for affinity audit')
    if cpu_set(args.client_cpus) & cpu_set(args.monitor_cpus) or (args.backend == 'emulator' and
            (cpu_set(args.client_cpus) | cpu_set(args.monitor_cpus)) & cpu_set(args.emulator_cpus)):
        parser.error('client, monitoring and emulator CPU sets must be separate')
    os.sched_setaffinity(0, cpu_set(args.monitor_cpus))
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    for name in ('redesign_compare.py', 'roadmap_run.py', 'run.py'):
        shutil.copy2(HERE / name, out / name)
    if capacity is not None:
        shutil.copy2(HERE / 'tools/harness-identity/main.go', out / 'harness-identity.go.txt')
    if args.syscall_trace:
        shutil.copy2(HERE / 'syscall_diagnostics.py', out / 'syscall_diagnostics.py')
        trace_spec = importlib.util.spec_from_file_location('comparison_syscalls', HERE / 'syscall_diagnostics.py')
        tracing = importlib.util.module_from_spec(trace_spec)
        trace_spec.loader.exec_module(tracing)
    cases = args.case or list(roadmap.CASES)
    matrix = list(dict.fromkeys((case, workers, load) for load in loads for case in cases
              for workers in (args.workers or [roadmap.CASES[case][2]])))
    required = list(dict.fromkeys(matrix + [(case, values[2], load)
                    for case, values in roadmap.CASES.items() for load in LOADS]))
    # Resolve all rates before starting any load, so missing sweep baselines fail early.
    try:
        targets = {key: (0 if key[2] == 'maximum' else roadmap.CASES[key[0]][3]
                        if key[2] == 'historical' else rates['rates']['%s:w%d' % key[:2]][key[2]])
                   for key in matrix}
    except KeyError as error:
        parser.error('missing frozen master baseline for case/worker count: ' + str(error))
    nodes = json.loads(args.nodes_json.read_text()) if args.nodes_json else []
    if args.backend == 'scylla' and (len(nodes) != 3 or
            {node.get('address') for node in nodes} != set(args.hosts.split(','))):
        parser.error('RF3 monitoring nodes must match all three explicit Scylla endpoints')
    context = {'started': r.now(), 'stage': args.stage, 'backend': args.backend,
               'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
               'kernel': os.uname().release,
               'hosts': args.hosts, 'backend_identity': backend_identity,
               'reference': args.reference, 'references': REFERENCES,
               'identities': identities, 'client_cpus': args.client_cpus, 'gomaxprocs': args.gomaxprocs,
               'master_capacity_reference': capacity,
               'monitor_cpus': args.monitor_cpus, 'emulator_cpus': args.emulator_cpus,
               'emulator_pid': args.emulator_pid, 'qualification': receipt,
               'qualification_supplement': supplement,
               'seconds': seconds, 'warmup_seconds': args.warmup, 'settle_seconds': args.settle,
               'profiling': args.stage == 'profile', 'rates': rates,
               'synchronization_profiles': args.synchronization_profiles,
               'syscall_tracing': args.syscall_trace,
               'syscall_collector_sha256': r.sha(out / 'syscall_diagnostics.py') if args.syscall_trace else None,
               'allow_reference_overload': args.allow_reference_overload,
               'stop_on_confirmed_regression': args.stop_on_confirmed_regression,
               'rates_sha256': r.sha(args.rates) if args.rates else None,
               'runner_hashes': {name: r.sha(out / name) for name in ('redesign_compare.py', 'roadmap_run.py', 'run.py')},
               'matrix': matrix, 'hostname': socket.gethostname()}
    prior = []
    if args.repeat_of:
        previous = args.repeat_of.resolve()
        previous_context = json.loads((previous / 'context.json').read_text())
        previous_summary = json.loads((previous / 'summary.json').read_text())
        if (args.stage != 'confirm' or previous_context.get('repeat_of') or
                not previous_summary.get('repeat_required') or
                json.loads((previous / 'state.json').read_text())['status'] != 'complete'):
            parser.error('--repeat-of requires one complete, valid, inconclusive confirmation')
        for key in ('backend', 'backend_identity', 'hosts', 'reference', 'seconds', 'warmup_seconds', 'settle_seconds',
                    'client_cpus', 'gomaxprocs', 'monitor_cpus', 'emulator_cpus',
                    'profiling', 'rates_sha256', 'matrix', 'runner_hashes'):
            if previous_context[key] != json.loads(json.dumps(context[key])):
                parser.error('repeat configuration differs: ' + key)
        if previous_context.get('allow_reference_overload', False) != args.allow_reference_overload:
            parser.error('repeat overload policy differs')
        for variant in ('A', 'B'):
            if previous_context['identities'][variant]['binary_sha256'] != identities[variant]['binary_sha256']:
                parser.error('repeat binary differs')
        prior = json.loads((previous / 'observations.json').read_text())
        context['repeat_of'] = {'directory': str(previous), 'observations_sha256': r.sha(previous / 'observations.json'),
                                'summary_sha256': r.sha(previous / 'summary.json')}
    records = []
    if args.resume_from:
        try:
            records, interrupted, context['resume_from'] = resume_records(args.resume_from.resolve(), context)
        except ValueError as error:
            parser.error(str(error))
        r.write(out / 'interrupted-observations.json', interrupted)
    r.write(out / 'context.json', context)
    completed_coordinates = {(x['case'], x['workers'], x['load'], x['pair'], x['variant']) for x in records}
    status = {'status': 'running', 'performance_accepted': False}
    def report():
        combined = prior + [dict(row, pair=row['pair'] + 6) for row in records] if prior else records
        summary = summarize(combined, args.stage, required, confirmation_sets=2 if prior else 1,
                            allow_reference_overload=args.allow_reference_overload)
        if (seconds != (120 if args.stage == 'confirm' else 30) or args.warmup == 0 or
                set(loads) != set(LOADS) or args.gomaxprocs != 1):
            if summary['outcome'] == 'retained':
                summary.update(outcome='inconclusive', complete=False, repeat_required=False)
        return summary
    variants_order = ([('A',)] * 3 if args.stage == 'baseline' else [('A', 'B')]
                      if args.stage == 'profile' else roadmap.order(args.stage))
    r.write(out / 'execution-order.json', [{'case': case, 'workers': workers, 'load': load,
            'pair': pair, 'variant': variant} for case, workers, load in matrix
            for pair, variants in enumerate(variants_order, 1) for variant in variants])
    # Turn SIGTERM into an exception so interruption metadata is written in finally.
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(InterruptedError('SIGTERM')))
    with (r.CACHE / 'run.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            for case, workers, load in matrix:
                scenario, stream, _, _ = roadmap.CASES[case]
                rate = targets[case, workers, load]
                for pair, variants in enumerate(variants_order, 1):
                    for variant in variants:
                        if (case, workers, load, pair, variant) in completed_coordinates:
                            continue
                        binary = binaries[variant]
                        if r.sha(binary) != identities[variant]['binary_sha256']:
                            raise RuntimeError('binary changed')
                        actual_stream = stream and not (variant == 'A' and args.reference == 'master')
                        d = out / f'{case}-w{workers}-{load}-pair{pair}-{variant}'
                        d.mkdir()
                        observation = dict(case=case, workers=workers, load=load, rate=rate,
                                           pair=pair, variant=variant, directory=d.name,
                                           identity=identities[variant], runner_hashes=context['runner_hashes'],
                                           backend=args.backend, profiling=context['profiling'])
                        observation['boot_id'] = context['boot_id']
                        # Register before warmup; interrupted/missing-result runs survive too.
                        records.append(dict(observation, metrics={}, failures=['observation incomplete']))
                        r.write(out / 'observations.json', records)
                        r.write(d / 'manifest.json', observation)
                        if r.client(d / 'warm.json', args.hosts, binary.name, 'warm', scenario, actual_stream,
                                    cpus=args.client_cpus, gomaxprocs=args.gomaxprocs):
                            raise RuntimeError('dataset warm failed')
                        if nodes:
                            r.write(d / 'settled.json', r.settled(nodes))
                        time.sleep(args.settle)
                        before = roadmap.snapshot(args.backend, args.emulator_pid)
                        r.write(d / 'before.json', before)
                        print(r.now(), 'RUN', d.name, flush=True)
                        options = {'profile-prefix': d / 'measurement'} if context['profiling'] else {}
                        if args.synchronization_profiles:
                            options['synchronization-profiles'] = True
                        monitor = r.Monitor(d / 'server.metrics.jsonl.gz', nodes) if nodes else None
                        if monitor:
                            monitor.__enter__()
                        tracer = tracing.Trace(binary, d / 'measurement.strace', args.monitor_cpus) if args.syscall_trace else None
                        try:
                            with tracer if tracer else nullcontext():
                                rc = r.client(d / 'measurement.json', args.hosts, binary.name, 'run', scenario,
                                              actual_stream, concurrency=workers, duration=f'{seconds:g}s',
                                              warmup=f'{args.warmup:g}s', rate=rate,
                                              cpus=args.client_cpus, gomaxprocs=args.gomaxprocs,
                                              output=d / 'measurement.json', **options)
                        finally:
                            if monitor:
                                monitor.__exit__()
                        after = roadmap.snapshot(args.backend, args.emulator_pid)
                        r.write(d / 'after.json', after)
                        measured = roadmap.read_observation(d, case, rate, pair, variant, rc,
                            driver='old' if variant == 'A' and args.reference == 'master' else 'v2',
                            streaming=actual_stream, seconds=seconds, workers=workers,
                            profiling=context['profiling'], cpus=args.client_cpus, gomaxprocs=args.gomaxprocs)
                        if args.synchronization_profiles:
                            for name in ('block', 'mutex'):
                                path = d / ('measurement.' + name + '.pprof')
                                if not path.exists() or not path.stat().st_size:
                                    measured['failures'].append('missing ' + name + ' profile')
                        if args.backend == 'emulator':
                            if after['emulator']['failures'] != before['emulator']['failures']:
                                measured['failures'].append('emulator failure counter increased')
                            for snap in (before, after):
                                if not snap['emulator_process']['threads'] or any(
                                        set(t['affinity']) != cpu_set(args.emulator_cpus) for t in snap['emulator_process']['threads'].values()):
                                    measured['failures'].append('emulator affinity violation')
                        result = json.loads((d / 'measurement.json').read_text())
                        if tracer is not None:
                            try:
                                if tracer.error:
                                    raise ValueError(tracer.error)
                                r.write(d / 'syscall-summary.json', tracing.network_syscalls(d / 'measurement.strace', result['measurement']))
                            except (ValueError, KeyError, OSError) as error:
                                measured['failures'].append('syscall diagnostics failed: ' + str(error))
                        measured['capacity_only_failure'] = capacity_only_failure(result, measured,
                            driver='old' if variant == 'A' and args.reference == 'master' else 'v2',
                            streaming=actual_stream, seconds=seconds, workers=workers, gomaxprocs=args.gomaxprocs)
                        records[-1] = dict(observation, **{k: v for k, v in measured.items() if k not in observation})
                        r.write(out / 'observations.json', records)
                        summary = report()
                        r.write(out / 'summary.json', summary)
                        print(json.dumps({'directory': d.name, 'metrics': measured['metrics'],
                                          'failures': measured['failures']}), flush=True)
                        if measured['failures'] and not (args.allow_reference_overload and load != 'maximum' and measured['capacity_only_failure']):
                            raise RuntimeError(str(measured['failures']))
                        if args.stage == 'confirm' and args.stop_on_confirmed_regression and summary['confirmed_regression']:
                            status.update(status='rejected', reason='Completed six-pair group confirms a regression; remaining observations unstarted')
                            return
                        if args.stage == 'baseline' and pair == 3:
                            # Stop at the first unstable baseline rather than warming
                            # other workloads after qualification has already failed.
                            freeze_rates([row for row in records if row['case'] == case and row['workers'] == workers], context)
            if args.stage == 'baseline':
                frozen = freeze_rates(records, context)
                files = ['context.json', 'observations.json']
                files.extend(str(p.relative_to(out)) for p in out.glob('*/measurement*') if p.is_file())
                frozen['provenance'] = {'directory': str(out), 'files': {name: r.sha(out / name) for name in files}}
                r.write(out / 'frozen-rates.json', frozen)
            status['status'] = 'complete'
        except BaseException as error:
            status.update(status='interrupted' if isinstance(error, (KeyboardInterrupt, InterruptedError)) else 'failed',
                          outcome='blocked', error=str(error))
            raise
        finally:
            status.update(observations=len(records), finished=r.now())
            r.write(out / 'state.json', status)
            r.write(out / 'observations.json', records)
            if status['status'] in ('failed', 'interrupted'):
                summary = report()
                summary.update(outcome='blocked', complete=False, error=status.get('error'))
                r.write(out / 'summary.json', summary)


if __name__ == '__main__':
    main()
