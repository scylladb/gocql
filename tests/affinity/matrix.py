# Copyright 2026 The Apache Software Foundation.
# Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
# in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

"""Canonical experiment cases shared by execution and evidence validation."""


def cases_for_phase(phase, legacy=False):
    socket_name = 'E-sharded-pinned-socket' if legacy else 'E-sharded-pinned-socket-once'
    variants = [('A-shared', 'shared', False, False), ('B-shared-pinned', 'shared', True, False),
                ('C-sharded', 'sharded', False, False), ('D-sharded-pinned', 'sharded', True, False),
                (socket_name, 'sharded', True, True)]
    cases = []
    if phase in ('all', 'cpu'):
        for mib in (1, 64):
            for updates in (0, 1):
                for variant in variants[:4]:
                    cases.append(('cpu', mib, updates, False, variant))
    if phase in ('all', 'tcp'):
        for variant in variants:
            cases.append(('tcp', 64, 1, False, variant))
    if phase in ('all', 'skew'):
        for variant in variants[2:]:
            cases.append(('tcp', 64, 1, True, variant))
    if phase == 'smoke':
        cases = [('cpu', 1, 0, False, variants[0]), ('cpu', 1, 1, False, variants[3]),
                 ('tcp', 1, 0, False, variants[2]), ('tcp', 1, 1, False, variants[4]),
                 ('tcp', 1, 1, True, variants[3])]
    if not cases:
        raise ValueError(f'unknown phase: {phase!r}')
    return cases


def case_label(case):
    mode, mib, updates, skew, variant = case
    return f'{mode}-{mib}MiB-update{updates}-' + ('skew-' if skew else '') + variant[0]


def validate_matrix(rows, environment):
    """Reject contradictory evidence; return missing case/repetition identities."""
    args = environment['arguments']
    repeats = args['repeats']
    if type(repeats) is not int or repeats <= 0:
        raise ValueError('repeats must be a positive integer')
    cases = cases_for_phase(args['phase'], legacy=environment.get('schema_version', 1) < 2)
    expected = {case_label(case): case for case in cases}
    if 'expected_cases' in environment and environment['expected_cases'] != list(expected):
        raise ValueError('declared case manifest differs from the selected phase')
    groups, seen = {}, set()
    for row in rows:
        label, repetition = row['label'], row['repetition']
        if label not in expected:
            raise ValueError(f'unexpected case: {label}')
        if type(repetition) is not int or not 1 <= repetition <= repeats:
            raise ValueError(f'invalid repetition: {label}/{repetition}')
        identity = label, repetition
        if identity in seen:
            raise ValueError(f'duplicate case/repetition: {label}/{repetition}')
        if row['returncode'] != 0 or row['result'].get('Error'):
            raise ValueError(f'failed case/repetition: {label}/{repetition}')
        mode, mib, updates, skew, variant = expected[label]
        config = row['result']['Config']
        settings = {'Mode': mode, 'MiB': mib, 'Updates': updates, 'Skew': skew,
                    'Cache': variant[1], 'Pin': variant[2], 'SocketCPU': variant[3],
                    'Batch': 32, 'CPUs': args['cpus']}
        if any(config.get(name) != value for name, value in settings.items()):
            raise ValueError(f'configuration does not match case: {label}/{repetition}')
        if environment.get('schema_version', 1) >= 2 and config.get('SampleSeed') != 20261029+repetition-1:
            raise ValueError(f'latency seed does not match repetition: {label}/{repetition}')
        seen.add(identity)
        groups.setdefault(label, []).append(row)
    missing = sorted((label, repeat) for label in expected for repeat in range(1, repeats+1)
                     if (label, repeat) not in seen)
    if 'runs' in environment and environment['runs'] != len(expected)*repeats:
        raise ValueError('declared total differs from expected case/repetition matrix')
    return groups, missing
