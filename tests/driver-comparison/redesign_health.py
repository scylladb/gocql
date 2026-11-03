#!/usr/bin/env python3
"""Read-only one-second host/device/cgroup and benchmark-server health capture.

Pass explicit benchmark node addresses. No provisioning, service changes or
acceptance decisions occur here. Raw cumulative counters preserve attribution
and let analysis derive deltas using monotonic_ns rather than nominal periods.
"""
import argparse
import concurrent.futures
import datetime
import gzip
import json
from pathlib import Path
import signal
import threading
import time
import urllib.request


def read(path):
    try:
        return Path(path).read_text()
    except (OSError, PermissionError) as error:
        return {'error': str(error)}


def metrics(node):
    try:
        with urllib.request.urlopen('http://' + node['address'] + ':9180/metrics', timeout=.8) as response:
            text = response.read().decode()
        # Preserve full metric lines, including shard labels, for these families.
        selected = ('commitlog', 'memtable', 'flush', 'compaction', 'io_queue',
                    'scheduler', 'transport_requests', 'storage_proxy', 'reactor_utilization')
        return [line for line in text.splitlines() if not line.startswith('#') and
                any(name in line.partition('{')[0].partition(' ')[0] for name in selected)]
    except Exception as error:
        return {'error': str(error)}


def cgroups(root=Path('/sys/fs/cgroup')):
    # Include all tenants, not just benchmark nodes, to identify competing I/O.
    # Read-only traversal never changes another tenant's configuration.
    result = {}
    for path in root.rglob('io.stat'):
        io = read(path)
        if isinstance(io, str) and io.strip():
            result[str(path.parent.relative_to(root))] = {
                'io.stat': io, 'io.pressure': read(path.parent / 'io.pressure'),
                'cpu.stat': read(path.parent / 'cpu.stat'),
                'memory.events': read(path.parent / 'memory.events')}
    return result


def sample(nodes, executor):
    futures = {node['name']: executor.submit(metrics, node) for node in nodes}
    return {'time': datetime.datetime.now(datetime.timezone.utc).isoformat(),
            'monotonic_ns': time.monotonic_ns(), 'diskstats': read('/proc/diskstats'),
            'cpu': read('/proc/stat'), 'io_pressure': read('/proc/pressure/io'),
            'cgroups': cgroups(), 'nodes': {name: f.result() for name, f in futures.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--nodes-json', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=600)
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error('--seconds must be positive')
    nodes = json.loads(args.nodes_json.read_text())
    if not nodes or any(not n['name'].startswith('codex-gocql-compare-') for n in nodes):
        parser.error('explicit benchmark-owned nodes required')
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    output = gzip.open(args.output, 'xt') if args.output.suffix == '.gz' else args.output.open('x')
    with output, concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        deadline = time.monotonic()
        for _ in range(args.seconds):
            started = time.monotonic()
            record = sample(nodes, executor)
            record['collection_seconds'] = time.monotonic() - started
            output.write(json.dumps(record) + '\n')
            output.flush()
            deadline += 1
            if stop.wait(max(0, deadline - time.monotonic())):
                break


if __name__ == '__main__':
    main()
