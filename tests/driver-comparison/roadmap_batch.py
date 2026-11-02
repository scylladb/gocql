#!/usr/bin/env python3
"""Run remaining independent local screens sequentially; confirm promising ones."""
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
CACHE = ROOT / '.codex-cache/driver-comparison'


def main():
    os.sched_setaffinity(0, {2, 18})
    baseline = CACHE / 'profile-roadmap-baseline'
    manifest = CACHE / 'roadmap/build-baseline/manifest.json'
    emulator = CACHE / 'roadmap/emulator'
    pids = []
    for process in Path('/proc').iterdir():
        if process.name.isdigit():
            try:
                if (process / 'exe').resolve() == emulator:
                    pids.append(process.name)
            except (FileNotFoundError, PermissionError):
                pass
    if len(pids) != 1:
        raise RuntimeError('expected exactly one roadmap emulator')
    for number, case, build in [(2, 'paged-stream', '02-final'),
                                (3, 'paged-buffered', '03-final'),
                                (4, 'point-buffered', '04'),
                                (5, 'point-buffered', '05')]:
        out = HERE / f'results/roadmap-{number:02}'
        for stage in ('screen', 'confirm'):
            if stage == 'confirm':
                summary = json.loads((out / 'local-screen/summary.json').read_text())
                if not all(c['promising'] for c in summary['cases'].values()):
                    break
            print(f'Candidate {number}: local {stage}', flush=True)
            args = [sys.executable, str(HERE / 'roadmap_run.py'),
                    '--baseline', str(baseline), '--baseline-manifest', str(manifest),
                    '--candidate', str(CACHE / f'profile-roadmap-{number:02}'),
                    '--candidate-manifest', str(CACHE / f'roadmap/build-{build}/manifest.json'),
                    '--output', str(out / f'local-{stage}'), '--stage', stage, '--case', case,
                    '--emulator-pid', pids[0]]
            with (out / f'local-{stage}.log').open('x') as log:
                subprocess.run(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
            print((out / f'local-{stage}/summary.json').read_text(), flush=True)


if __name__ == '__main__':
    main()
