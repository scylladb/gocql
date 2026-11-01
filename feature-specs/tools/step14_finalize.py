# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Close step14 from retained successful evidence; never rerun the matrix."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile

import step14_acceptance
import step14_matrix as matrix


def gone(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    raise AssertionError(f'recorded owned process remains alive: {pid}')


def main():
    report, streaming = step14_acceptance.build()
    source = report['source_sha256']
    assert report['gate'] == streaming['gate'] == 'passed'
    assert report['current_effective_native_passes'] == 174
    assert len(matrix.read(matrix.E/'step14-source.json')) == 623
    native = report['native']
    supplemental = matrix.read(matrix.TASK/'supplemental-checks.json')
    for command in supplemental:
        assert command['exit_code'] == 0 and command['source_sha256'] == source
        assert matrix.sha(command['log']) == command['sha256']
        for path, digest in command.get('inputs', {}).items():
            assert matrix.sha(Path(command['directory'])/path) == digest
    assert {c['name'] for c in supplemental} == {
        'external-contracts', 'external-examples', 'public-audit', 'module-drift'}
    dependency = matrix.read(matrix.TASK/'dependency-audit.json')
    assert not dependency['findings']
    public = matrix.read(matrix.TASK/'api-audit/public.json')
    assert not public['issues'] and not public['unnameable_required_interface_types']
    assert len(public['public_exports']) == 507
    assert len(public['thin_facade_functions']) == 130
    assert len(public['canonical_session_methods']) == 9

    # A zero subprocess exit alone and hollow parent passes are insufficient.
    assert not matrix.effective({'T': 'pass', 'T/a': 'pass', 'T/a/x': 'skip'}, 'T')
    assert matrix.effective({'T': 'pass', 'T/a': 'pass', 'T/a/x': 'pass'}, 'T')
    with tempfile.NamedTemporaryFile('w') as log:
        log.write(json.dumps(dict(Package='p', Test='T', Action='pass'))+'\n')
        log.write(json.dumps(dict(Package='p', Action='fail'))+'\n')
        log.flush()
        assert not matrix.package_outcomes(dict(exit_code=0, log=log.name))
        assert not matrix.package_outcomes(dict(exit_code=1, log=log.name))

    lanes = []
    processes = []
    for name, receipt in native.items():
        processes.append(dict(lane=name, kind='native-runner', pid=receipt['pid'], gone=gone(receipt['pid'])))
        for command in receipt['commands']:
            processes.append(dict(lane=name, kind='command', pid=command['pid'], gone=gone(command['pid'])))
        row = receipt['lanes'][0]
        for command in row.get('node_control', []):
            if command.get('pid_before'):
                processes.append(dict(lane=name, kind='controlled-node', pid=command['pid_before'], gone=gone(command['pid_before'])))
            if command.get('pid_after'):
                processes.append(dict(lane=name, kind='controlled-node', pid=command['pid_after'], gone=gone(command['pid_after'])))
        assert all(row['joined'].values()) and row['endpoints_refused']
        for mode in receipt['configuration']['lanes'][0]['modes']:
            command = next(c for c in receipt['commands'] if c['name'] == row['name']+'-'+mode['name'])
            cleanup = next(c for c in receipt['commands'] if c['name'] == command['name']+'-cleanup')
            assert command['exit_code'] == cleanup['exit_code'] == 0
            events = matrix.events(command['log'])
            skips = []
            for event in events:
                if event.get('Test') and event.get('Action') == 'skip':
                    text = ''.join(e.get('Output', '') for e in events if e.get('Test') == event['Test'])
                    skips.append(dict(test=event['Test'], package=event['Package'], output=text))
            final = matrix.package_outcomes(command)
            passed = [test for (package, test), action in final.items() if action == 'pass']
            lanes.append(dict(receipt=name, mode=mode['name'], arguments=mode['args'], selected_tests=mode['tests'], command=command, cleanup=cleanup, skips=skips, passed_test_events=len(passed), joined=row['joined'], fixture_root=receipt['fixture_root'], server=receipt['configuration']['lanes'][0], source_sha256=receipt['source_sha256']))
    assert len(lanes) == 17

    cancel = matrix.read(matrix.TASK/'cancel-probe/verification.json')
    assert cancel['source_sha256'] == source and cancel['expected_cancelled_receipt']
    assert cancel['all_nodes_joined'] and cancel['endpoints_refused']
    assert matrix.sha(cancel['receipt']) == cancel['receipt_sha256']
    assert matrix.sha(cancel['log']) == cancel['log_sha256']
    canceled_receipt = matrix.read(cancel['receipt'])
    assert canceled_receipt['status'] == 'failed' and canceled_receipt['cancel_requested']
    assert {row['phase'] for row in cancel['phases']['observed']} == {'testing','cleanup','stop-join'}
    assert len(cancel['signals_sent']) == 5
    gone(cancel['runner_pid'])
    for command in canceled_receipt['commands']:
        gone(command['pid'])
        assert matrix.sha(command['log']) == command['sha256']
    cleanup = dict(runners_and_children=processes, cancellation_probe=cancel, endpoints=[])
    for subnet in (20, 21, 22):
        for node in range(1, 4):
            address = f'127.0.{subnet}.{node}'
            for port in (9042, 9142, 19042, 19142, 7000, 7001, 10000):
                try:
                    connection = socket.create_connection((address, port), .1)
                except OSError:
                    cleanup['endpoints'].append(dict(address=address, port=port, refused=True))
                    continue
                connection.close()
                raise AssertionError(f'owned endpoint still active {address}:{port}')

    fuzz = matrix.read(matrix.TASK/'fuzz-verification.json')
    assert len(fuzz) == 12
    for row in fuzz:
        assert row['source_sha256'] == source and row['exit_code'] == 0
        assert matrix.sha(row['log']) == row['sha256']
        seeds = row['seed_provenance']
        assert seeds['initial_cache_empty'] and seeds['initial_builtin_seed_count'] > 0
        corpus = next(arg.split('=',1)[1] for arg in row['command'] if arg.startswith('-test.fuzzcachedir='))
        for path, digest in seeds['retained_mutation_corpus'].items():
            assert matrix.sha(Path(corpus)/path) == digest
    # Preserve final metadata/seed reports in the repository; raw logs stay in
    # their immutable task-owned paths and are individually hash bound.
    copies = {
        'step14-checks.json': dict(source_sha256=source, checks=report['checks'], supplemental=supplemental),
        'step14-cleanup.json': cleanup,
        'step14-fuzz.json': dict(source_sha256=source, campaigns=fuzz, persistence_scope='Saved interesting mutations and built-in seed functions are bound. The persisted corpus is not every in-memory execution.'),
        'step14-dependency-audit.json': dict(source_sha256=source, audit=dependency),
        'step14-public-api.json': dict(source_sha256=source, audit=public),
        'step14-lanes.json': dict(source_sha256=source, modes=lanes),
    }
    for name, value in copies.items():
        matrix.write(matrix.E/name, value)
    artifacts = {str(path.relative_to(matrix.ROOT)):matrix.sha(path) for path in sorted(matrix.E.glob('step14-*.json')) if path.name != 'step14-verification.json'}
    tools = {str(path.relative_to(matrix.ROOT)):matrix.sha(path) for path in sorted((matrix.ROOT/'feature-specs/tools').glob('step14_*.py'))}
    diagnostics = []
    for folder in (matrix.TASK.parent, matrix.TASK.parent/'final', matrix.TASK):
        for path in sorted(folder.glob('*.json')):
            if path.parent == matrix.TASK and path.name != 'cassandra.json':
                continue
            try:
                data = matrix.read(path)
            except (ValueError, OSError):
                continue
            if not isinstance(data, dict) or data.get('status') != 'failed':
                continue
            diagnostics.append(dict(path=str(path), sha256=matrix.sha(path), source_sha256=data.get('source_sha256'), error=data.get('error'), credited=False))
    gates = [
        dict(gate=1, name='API/build', evidence=['step14-public-api.json','step14-dependency-audit.json','step14-checks.json'], assertions='507 exports, 130 thin facade functions, nine Session methods; external interfaces/examples, all-tag/arm64 builds, module verification/drift, zero transitive architecture violations.'),
        dict(gate=2, name='Codec/protocol', evidence=['step14-streaming-acceptance.json','step14-fuzz.json','step14-checks.json'], assertions='Golden/type/capacity/null/empty/unset/mixed iteration, bounded framing/text/metadata fuzz and terminal-state assertions pass in successful whole packages.'),
        dict(gate=3, name='Ownership/race', evidence=['step14-checks.json','step14-streaming-acceptance.json','step14-cleanup.json'], assertions='Full unit race plus live race packages; leases, admission, final writes, paging, retries and Close joins; explicit cancellation lifetime campaigns and repeated-signal cleanup.'),
        dict(gate=4, name='Transport', evidence=['step14-lanes.json','step14-streaming-acceptance.json'], assertions='Raw v3/v4/v5 and TLS, v5 integrity units, compression, >500KB, direct final ranges and separated unit copy/byte-capacity measurements.'),
        dict(gate=5, name='Runtime features', evidence=['step14-lanes.json','step14-streaming-acceptance.json','step14-checks.json'], assertions='Actual tablet-v2/global leader, stable tablets/shards/LWT, metadata/events, profiles/retries/observers/config tests; idle/exhausted reservations still permit live schema event dispatch.'),
        dict(gate=6, name='Integration', evidence=['step14-test-mapping.json','step14-lanes.json','step14-cleanup.json'], assertions='All174 exact original identities and translated expected nested subcases have real passes in successful same-source packages; all17 native modes plus actual CCM pass; explicit version/fixture skip text retained.'),
        dict(gate=7, name='Streaming receive', evidence=['step14-streaming-acceptance.json','step14-fuzz.json','step14-checks.json'], assertions='All16 criterion groups, eight deterministic modes,40 measured byte-storage shapes,24 final-write lifecycle combinations and all eight live supported mode combinations pass; focused equivalent-result/copy checks included, complete independent performance remains15.'),
    ]
    final = dict(branch=report['branch'], accepted_predecessor=matrix.BASE, gate='passed', source_sha256=source, source_inputs=623, source_snapshot='feature-specs/evidence/step14-source.json', source_unchanged=True, baseline_identities=174, effective_native_passes=174, missing_expected_subcase_passes=[], mandatory_native_modes=17, successful_native_receipts=len(native), acceptance_gates=gates, streaming_criterion_groups=16, measured_storage_shapes=40, fuzz_campaigns=12, artifacts=artifacts, verifier_tools=tools, command_scope='Existing complete current-source receipts only; finalization performs no new database or mandatory-matrix runs.', documentation=dict(path='tests/fixtures/README.md', sha256=matrix.sha(matrix.ROOT/'tests/fixtures/README.md'), outside_native_source_manifest=True, verification=dict(command=['python3','feature-specs/verify.py'],exit_code=0,log=str(matrix.TASK/'final-docs-verifier.log'),sha256=matrix.sha(matrix.TASK/'final-docs-verifier.log')), disposition='Documentation-only fixture lifecycle/isolation instructions added after freeze; matches observed runner behavior and does not alter native inputs.'), handoff_sha256=matrix.sha(matrix.ROOT/'feature-specs/execution/14-full-matrix.md'), negative_verifier_controls=['failed whole-package event despite zero subprocess exit','nonzero subprocess exit','hollow parent whose terminal descendants all skip'], diagnostics=diagnostics, limitations=['Storage values are actual retained driver byte capacities plus conservative descriptor charges, not total heap. TLS record buffers, fixed schema plans, generated source and caller output are separate.','Saved fuzz mutations are the persisted interesting corpus, not every in-memory execution.','Focused equivalent-result allocation/copy checks are included; comprehensive independent performance, review and final release audit remain steps15–17.'], ownership='Only the exact namespaced locked fixtures were provisioned/stopped; root STATUS.md and RESUME.md are excluded; no push or publication.')
    matrix.write(matrix.E/'step14-verification.json', final)
    print(json.dumps(dict(gate=final['gate'], source_sha256=source, effective_native_passes=174, modes=17, artifacts=len(artifacts), endpoints_refused=len(cleanup['endpoints']))))


if __name__ == '__main__':
    main()
