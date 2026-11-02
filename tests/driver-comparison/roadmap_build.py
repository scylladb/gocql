#!/usr/bin/env python3
"""Freeze one v2 source tree and its unchanged comparison client before timing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    os.sched_setaffinity(0, {10, 11, 12, 13, 14, 15})
    git = lambda *a: subprocess.check_output(['git', '-C', str(source), *a], text=True)
    paths = git('ls-files', '--cached', '--others', '--exclude-standard', '--', '*.go', 'go.mod', 'go.sum').splitlines()
    paths = sorted(p for p in paths if not p.startswith('tests/driver-comparison/'))
    frozen = out / 'source'
    hashes = {}
    for path in paths:
        data = (source / path).read_bytes()
        target = frozen / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        hashes[path] = hashlib.sha256(data).hexdigest()
    harness = out / 'harness'
    harness.mkdir()
    harness_hashes = {}
    for path in HERE.iterdir():
        if path.suffix == '.go' or path.name in ('go.mod', 'go.sum'):
            shutil.copy2(path, harness / path.name)
            harness_hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    mod = harness / 'go.mod'
    old = HERE.parents[1] / '.codex-cache/driver-comparison/old-source'
    text = mod.read_text().replace('=> ../../.codex-cache/driver-comparison/old-source', '=> ' + str(old))
    mod.write_text(text.replace('=> ../..', '=> ' + str(frozen)))
    binary = out / 'v2'
    env = dict(os.environ, GOWORK='off')
    with (out / 'build.log').open('w') as log:
        subprocess.run(['go', 'build', '-trimpath', '-buildvcs=false', '-o', str(binary), '.'],
                       cwd=harness, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    build = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    frozen_harness_hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                             for p in harness.iterdir() if p.is_file()}
    manifest = {'git_head': git('rev-parse', 'HEAD').strip(), 'source': str(source),
                'frozen_source': str(frozen), 'driver_source_hashes': hashes,
                'harness_input_hashes': harness_hashes,
                'harness_source_hashes': frozen_harness_hashes, 'binary': str(binary),
                'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'build': build}
    (out / 'driver.patch').write_text(git('diff', 'HEAD', '--', '*.go', 'go.mod', 'go.sum'))
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'binary': str(binary), 'manifest': str(out / 'manifest.json')}))


if __name__ == '__main__':
    main()
