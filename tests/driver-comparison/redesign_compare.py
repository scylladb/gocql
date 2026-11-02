#!/usr/bin/env python3
"""Balanced canonical comparisons of frozen clients against pinned master.

This runner records screening evidence only. It never grants acceptance without
healthy real Scylla, fixed-rate latency controls and six balanced paired runs.
The master stream comparison uses its buffered adapter at four workers.
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
spec = importlib.util.spec_from_file_location('redesign_roadmap', HERE / 'roadmap_run.py')
roadmap = importlib.util.module_from_spec(spec)
spec.loader.exec_module(roadmap)
r = roadmap.r
MASTER = '1ceaac8172bda26e8b8d701ac356982d33729b27'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--master', type=Path, required=True)
    parser.add_argument('--master-manifest', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--candidate-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--stage', choices=('screen', 'confirm', 'controls'), default='screen')
    parser.add_argument('--emulator-pid', type=int, required=True)
    args = parser.parse_args()
    binaries = {'A': args.master.resolve(), 'B': args.candidate.resolve()}
    identities = {'A': json.loads(args.master_manifest.read_text()),
                  'B': roadmap.read_manifest(args.candidate_manifest, binaries['B'])}
    if identities['A']['git_head'] != MASTER or r.sha(binaries['A']) != identities['A']['binary_sha256']:
        parser.error('pinned master source and binary identity required')
    for variant, identity in identities.items():
        if binaries[variant].parent != r.CACHE:
            parser.error('binaries must be directly in comparison cache for affinity audit')
        for name, expected in identity['driver_source_hashes'].items():
            if r.sha(Path(identity['frozen_source']) / name) != expected:
                parser.error('frozen source changed: ' + name)
    os.sched_setaffinity(0, {2, 18})
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    shutil.copy2(Path(__file__), out / Path(__file__).name)
    shutil.copy2(HERE / 'roadmap_run.py', out / 'roadmap_run.py')
    r.write(out / 'context.json', {'stage': args.stage, 'backend': 'emulator',
                                 'identities': identities, 'client_cpu': 8,
                                 'gomaxprocs': 1, 'emulator_pid': args.emulator_pid,
                                 'seconds': 30, 'performance_accepted': False})
    records = []
    status = {'status': 'running', 'performance_accepted': False}
    hosts = '127.0.0.2,127.0.0.3,127.0.0.4'
    with (r.CACHE / 'run.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            for case, (scenario, stream, workers, target) in roadmap.CASES.items():
                rate = target if args.stage == 'controls' else 0
                for pair, variants in enumerate(roadmap.order(args.stage), 1):
                    for variant in variants:
                        binary = binaries[variant]
                        if r.sha(binary) != identities[variant]['binary_sha256']:
                            raise RuntimeError('binary changed')
                        actual_stream = stream and variant == 'B'
                        d = out / f'{case}-pair{pair}-{variant}'
                        d.mkdir()
                        if r.client(d / 'warm.json', hosts, binary.name, 'warm', scenario, actual_stream):
                            raise RuntimeError('dataset warm failed')
                        r.write(d / 'before.json', roadmap.snapshot('emulator', args.emulator_pid))
                        print(r.now(), 'RUN', d.name, flush=True)
                        rc = r.client(d / 'measurement.json', hosts, binary.name, 'run', scenario,
                                      actual_stream, concurrency=workers, duration='30s', warmup='0s',
                                      rate=rate, output=d / 'measurement.json',
                                      **{'profile-prefix': d / 'measurement'})
                        r.write(d / 'after.json', roadmap.snapshot('emulator', args.emulator_pid))
                        record = roadmap.read_observation(d, case, rate, pair, variant, rc,
                                                          driver='old' if variant == 'A' else 'v2',
                                                          streaming=actual_stream)
                        records.append(record)
                        r.write(out / 'observations.json', records)
                        summary = roadmap.summarize(records, args.stage)
                        summary['master_target_accepted'] = False
                        summary['note'] = 'Screening is not six-pair confirmation; maximum-rate p99 is descriptive, not a matched-rate latency gate.'
                        r.write(out / 'summary.json', summary)
                        print(json.dumps(record), flush=True)
                        if record['failures']:
                            raise RuntimeError(str(record['failures']))
            status['status'] = 'complete'
        except BaseException as error:
            status.update(status='failed', error=str(error))
            raise
        finally:
            status['observations'] = len(records)
            status['finished'] = r.now()
            r.write(out / 'state.json', status)


if __name__ == '__main__':
    main()
