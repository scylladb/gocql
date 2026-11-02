#!/usr/bin/env python3
"""Read-only artifact audit for the v2 roadmap; never grants acceptance."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import tempfile

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location('roadmap_runner_audit', HERE / 'roadmap_run.py')
runner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(runner)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(path):
    return json.loads(path.read_text())


def check_hash(path, expected, issues):
    if not path.is_file() or sha(path) != expected:
        issues.append(f'hash mismatch or missing file: {path}')


def harness_provenance(harness, expected, source, old_source):
    actual = {name: sha(harness / name) for name in expected if (harness / name).is_file()}
    issues = []
    relocation = None
    for name, value in expected.items():
        if actual.get(name) == value:
            continue
        if name == 'go.mod' and (harness / name).is_file():
            text = (harness / name).read_text()
            replacements = {
                f'replace github.com/gocql/gocql => {old_source}':
                    'replace github.com/gocql/gocql => ../../.codex-cache/driver-comparison/old-source',
                f'replace github.com/scylladb/gocql/v2 => {source}':
                    'replace github.com/scylladb/gocql/v2 => ../..',
            }
            original = text
            for relocated, input_text in replacements.items():
                original = original.replace(relocated, input_text)
            if hashlib.sha256(original.encode()).hexdigest() == value:
                relocation = {'manifest_hash_scope': 'input harness go.mod before path relocation',
                              'manifest_input_sha256': value,
                              'actual_frozen_sha256': actual[name],
                              'reconstructed_input_sha256': hashlib.sha256(original.encode()).hexdigest(),
                              'relocations': replacements,
                              'actual_text': text}
                continue
        issues.append(f'frozen harness hash mismatch or missing file: {harness / name}')
    return {'actual_frozen_hashes': actual, 'go_mod_input_relocation': relocation,
            'issues': issues}


def audit_manifest(path):
    data = read(path)
    issues = []
    source = Path(data['frozen_source'])
    for name, expected in data['driver_source_hashes'].items():
        if Path(name).is_absolute() or '..' in Path(name).parts:
            issues.append(f'unsafe source path: {name}')
        else:
            check_hash(source / name, expected, issues)
    binary = Path(data['binary'])
    check_hash(binary, data['binary_sha256'], issues)
    harness = harness_provenance(source.parent / 'harness', data['harness_source_hashes'],
                                 source, runner.r.CACHE / 'old-source')
    issues.extend(harness['issues'])
    return {'manifest': str(path), 'manifest_sha256': sha(path),
            'binary_sha256': data['binary_sha256'],
            'source_files_checked': len(data['driver_source_hashes']),
            'harness': harness, 'issues': issues}


def gzip_integrity(path):
    size = 0
    with gzip.open(path, 'rb') as stream:
        while chunk := stream.read(1024 * 1024):
            size += len(chunk)
    if not size:
        raise ValueError('empty gzip payload')
    return {'sha256': sha(path), 'uncompressed_bytes': size}


def audit_histograms(directory, result):
    raw_path = directory / 'measurement.histograms.json.gz'
    raw = read_gzip(raw_path) if raw_path.is_file() else {}
    counts = {}
    for name in ('latency', 'dispatch_latency', 'queue_latency', 'schedule_lag'):
        histogram = result['measurement'][name]
        retained = raw.get('measurement-' + name, histogram.get('snapshot'))
        if retained is None:
            raise ValueError(f'missing retained {name} histogram')
        total = sum(retained['Counts'])
        if total != histogram['count']:
            raise ValueError(f'{name} raw counts differ: {total} != {histogram["count"]}')
        if (retained['LowestTrackableValue'], retained['HighestTrackableValue'],
                retained['SignificantFigures']) != (1, 60000000, 3):
            raise ValueError(f'{name} histogram configuration changed')
        counts[name] = total
    return {'format': 'gzip' if raw else 'inline snapshots', 'counts': counts,
            'sha256': sha(raw_path) if raw_path.is_file() else sha(directory / 'measurement.json')}


def read_gzip(path):
    with gzip.open(path, 'rt') as stream:
        return json.load(stream)


def audit_observation(directory, context, archived):
    issues, gaps = [], []
    result = read(directory / 'measurement.json')
    m = result['measurement']
    case = next((name for name, config in runner.CASES.items()
                 if config[:2] == (result['scenario'], result['stream'])), None)
    rate = m['target_rate']
    workload_failures = runner.failures(result, case, rate) if case else ['unknown canonical case']
    if case and rate not in (0, runner.CASES[case][3]):
        issues.append('fixed-rate target differs from retained original target')
    process = read(directory / 'measurement.json.process.json')
    if process.get('masks') != ['8'] or not process.get('checks'):
        issues.append('missing or invalid external CPU8 affinity checks')
    if process.get('returncode'):
        workload_failures.append(f'client returned {process["returncode"]}')
    variant = directory.name.rsplit('-', 1)[-1]
    identity = context.get('identities', {}).get(variant)
    if identity:
        binary = next((Path(arg) for arg in process['args'] if Path(arg).name.startswith('profile-roadmap-')), None)
        if binary is None:
            issues.append('cannot identify measured frozen binary')
        else:
            check_hash(binary, identity['binary_sha256'], issues)
    else:
        issues.append('missing variant build identity')
    files = {}
    for suffix in ('cpu.pprof', 'heap.before.pprof', 'heap.after.pprof'):
        path = directory / ('measurement.' + suffix)
        try:
            files[path.name] = gzip_integrity(path)
        except (OSError, ValueError, EOFError) as error:
            issues.append(f'{path.name}: {error}')
    try:
        histograms = audit_histograms(directory, result)
    except (OSError, ValueError, KeyError, EOFError) as error:
        issues.append(f'raw histograms: {error}')
        histograms = None
    warm = directory / 'warm.json.process.json'
    if not warm.is_file():
        issues.append('missing complete untimed dataset warming process')
    else:
        w = read(warm)
        if w['returncode'] or w.get('masks') != ['8'] or not w.get('checks'):
            issues.append('untimed dataset warming failed or affinity unverified')
    boundaries = {}
    for name in ('before', 'after'):
        path = directory / (name + '.json')
        if path.is_file():
            boundaries[name] = read(path)
            files[path.name] = {'sha256': sha(path)}
        else:
            gaps.append(f'missing {name} health snapshot')
    server = {}
    if context['backend'] == 'emulator':
        for name, boundary in boundaries.items():
            if 'emulator' not in boundary:
                gaps.append(f'{name}: missing emulator request/failure counters')
            process_info = boundary.get('emulator_process')
            if not process_info:
                gaps.append(f'{name}: missing emulator per-thread CPU/affinity metadata')
            else:
                threads = process_info['threads']
                masks = {tuple(thread['affinity']) for thread in threads.values()}
                if not threads or masks != {(0, 1, 4, 5)}:
                    issues.append(f'{name}: unexpected emulator thread affinity')
                server[name] = {'pid': process_info['pid'], 'thread_count': len(threads),
                                'cpu_seconds': sum(thread['cpu_seconds'] for thread in threads.values()),
                                'affinity_masks': [list(mask) for mask in sorted(masks)]}
        if set(boundaries) == {'before', 'after'}:
            before = boundaries['before'].get('emulator', {})
            after = boundaries['after'].get('emulator', {})
            if before.get('failures') != after.get('failures'):
                workload_failures.append('emulator failure counter changed')
        gaps.append('synthetic backend cannot qualify real Scylla acceptance')
    metrics_path = directory / 'health.metrics.jsonl.gz'
    if metrics_path.is_file():
        try:
            files[metrics_path.name] = gzip_integrity(metrics_path)
            if context['backend'] == 'scylla':
                with gzip.open(metrics_path, 'rt') as stream:
                    samples = [json.loads(line) for line in stream]
                if len(samples) < 2 or any(not isinstance(sample['nodes'].get(node['name']), list)
                                           for sample in samples for node in context['nodes']):
                    gaps.append('real backend metrics incomplete')
        except (OSError, ValueError, EOFError) as error:
            issues.append(f'health metrics: {error}')
    else:
        gaps.append('missing system/server metrics')
    if archived is None:
        gaps.append('measurement retained but absent from original observation ledger')
    elif 'counters' not in archived:
        gaps.append('older observation ledger lacks explicit counters; recovered from raw result')
    elif any(value != m.get(key) for key, value in archived['counters'].items()):
        issues.append('ledger counters differ from raw result')
    return {'directory': str(directory), 'backend': context['backend'], 'case': case,
            'rate': rate, 'variant': variant,
            'counters': {key: m.get(key) for key in ('errors', 'timeouts', 'rejected', 'admitted',
                                                   'completed', 'in_window', 'drained')},
            'returncode': process['returncode'], 'metrics': runner.metrics(result),
            'workload_valid': not workload_failures, 'workload_failures': workload_failures,
            'artifact_issues': issues, 'qualification_gaps': gaps,
            'histograms': histograms, 'server_boundaries': server,
            'files': dict(files, **{'measurement.json': {'sha256': sha(directory / 'measurement.json')},
                                    'measurement.json.process.json': {'sha256': sha(directory / 'measurement.json.process.json')}}),
            'accepted': False}


def audit(results):
    manifests, sessions, issues = [], [], []
    roots = sorted(path for path in results.glob('roadmap*') if path.is_dir())
    for root in roots:
        path = root / 'frozen-build.json'
        if path.is_file():
            manifests.append(audit_manifest(path))
    by_hash = {item['manifest_sha256']: read(Path(item['manifest'])) for item in manifests}
    for root in roots:
        for path in root.rglob('context.json'):
            context = read(path)
            if 'stage' not in context or 'identities' not in context:
                continue
            directory = path.parent
            session_issues, session_gaps = [], []
            for variant, identity in context['identities'].items():
                original = by_hash.get(identity.get('manifest_sha256'))
                if original is None:
                    session_issues.append(f'{variant}: no retained authoritative frozen build manifest')
                elif any(identity.get(key) != original.get(key) for key in
                         ('git_head', 'binary_sha256', 'driver_source_hashes', 'harness_source_hashes', 'build')):
                    session_issues.append(f'{variant}: context identity differs from authoritative build manifest')
            state = read(directory / 'state.json') if (directory / 'state.json').is_file() else {'status': 'running'}
            observations = read(directory / 'observations.json') if (directory / 'observations.json').is_file() else []
            archived = {record['directory']: record for record in observations}
            measured = []
            pending = []
            for child in sorted(directory.iterdir()):
                if not child.is_dir():
                    continue
                if (child / 'measurement.json').is_file():
                    try:
                        measured.append(audit_observation(child, context, archived.get(child.name)))
                    except (FileNotFoundError, json.JSONDecodeError) as error:
                        if state['status'] != 'running':
                            raise
                        pending.append(child.name)
                        session_gaps.append(f'{child.name}: observation still being written: {error}')
                elif (child / 'warm.json.log').is_file():
                    pending.append(child.name)
            if state.get('observations') != len(measured):
                session_gaps.append('state observation count differs from retained raw measurement count')
            if 'expected_observations' not in state:
                session_gaps.append('older state lacks explicit expected observation count')
            runner_hashes = {name: sha(directory / name) for name in ('roadmap_run.py', 'run.py')
                             if (directory / name).is_file()}
            sessions.append({'directory': str(directory), 'stage': context['stage'],
                             'state': state, 'partial': state['status'] == 'running' or bool(pending),
                             'pending_observations': pending, 'observations': measured,
                             'artifact_issues': session_issues, 'qualification_gaps': session_gaps,
                             'archived_runner_hashes': runner_hashes,
                             'context_sha256': sha(path), 'accepted': False})
    observations = [observation for session in sessions for observation in session['observations']]
    for manifest in manifests:
        issues.extend(manifest['issues'])
    for session in sessions:
        issues.extend(f'{session["directory"]}: {issue}' for issue in session['artifact_issues'])
    for observation in observations:
        issues.extend(f'{observation["directory"]}: {issue}' for issue in observation['artifact_issues'])
    return {'time': runner.r.now(), 'accepted': False,
            'scope': 'artifact integrity and qualification gaps; never optimization acceptance',
            'partial': any(session['partial'] for session in sessions),
            'artifact_integrity_pass': not issues, 'artifact_issues': issues,
            'counts': {'frozen_builds': len(manifests), 'sessions': len(sessions),
                       'measured_observations': len(observations),
                       'successful_observations': sum(x['workload_valid'] for x in observations),
                       'failed_observations': sum(not x['workload_valid'] for x in observations)},
            'auditor_source_hashes': {name: sha(HERE / name) for name in
                                     ('roadmap_audit.py', 'roadmap_run.py', 'run.py')},
            'builds': manifests, 'sessions': sessions}


def atomic_new_json(path, data):
    """Publish a complete new audit without overwriting existing evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, default=HERE / 'results')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    os.sched_setaffinity(0, {2, 18})
    result = audit(args.results.resolve())
    atomic_new_json(args.output.resolve(), result)
    print(json.dumps({'counts': result['counts'], 'partial': result['partial'],
                      'artifact_integrity_pass': result['artifact_integrity_pass']}))


if __name__ == '__main__':
    main()
