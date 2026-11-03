#!/usr/bin/env python3
"""Capture local-cluster evidence without recreating or preparing its dataset.

This is the user-authorized local alternative to dedicated-host qualification.
Master rate freezing remains a separate healthy-baseline step. Historical
overload is preserved and requires the explicit comparison overload policy.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('comparison', HERE / 'redesign_compare.py')
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
r = c.r
IMAGE = 'docker.io/scylladb/scylla@sha256:2a3cf99b55d793a41e19b4d8afa4084744bc05517f4b4a211483c8d2abe4ef3a'


def original_controls(data):
    records = data['observations']
    if len(records) != 6 or not data.get('qualified_for_valid_comparisons'):
        raise ValueError('complete original write qualification required')
    for round_id in range(1, 4):
        for rate in (0, 52503.01):
            rows = [x for x in records if x['round'] == round_id and x['rate'] == rate]
            if len(rows) != 1:
                raise ValueError('missing or duplicate original write control')
            row = rows[0]
            if row['issues'] and not (rate and row.get('capacity_only_failure')):
                raise ValueError('unhealthy original write observation')
    return data.get('blocked_controls', [])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--provision', type=Path, required=True)
    p.add_argument('--original-qualification', type=Path, required=True)
    p.add_argument('--master', type=Path, required=True)
    p.add_argument('--original', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    os.sched_setaffinity(0, {2, 18})
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    evidence = {}

    def save(name, value):
        path = out / (name + '.json')
        r.write(path, value)
        evidence[name] = dict(path=path.name, sha256=r.sha(path))

    provision = args.provision.resolve()
    nodes = json.loads((provision / 'cluster.json').read_text())
    if [x['name'] for x in nodes] != [f'codex-gocql-compare-{i}' for i in (1, 2, 3)]:
        raise ValueError('explicit benchmark-owned three-node cluster required')
    hosts = ','.join(x['address'] for x in nodes)
    original = json.loads(args.original_qualification.read_text())
    blocked = original_controls(original)
    save('original_write_maxima', original)
    save('write_controls_52503_01', original)
    compiler = subprocess.check_output(['go', 'version'], text=True)
    if not compiler.startswith('go version go1.25.0 '):
        raise ValueError('pinned Go 1.25.0 compiler required')
    save('compiler', compiler)
    save('cpu_topology', subprocess.check_output(['lscpu', '-J', '-e=CPU,CORE,SOCKET,ONLINE'], text=True))
    inspected = json.loads(r.command('sudo', '-n', 'podman', 'inspect', *[x['name'] for x in nodes]).stdout)
    storage = []
    for node, info, cpus in zip(nodes, inspected, ('0-1', '4-5', '6-7')):
        cfg = info['HostConfig']
        cmd = info['Args']
        if (not info['State']['Running'] or info['ImageName'] != IMAGE or
                info['Config']['Labels'].get('io.codex.gocql-compare') != 'true' or
                cfg['CpusetCpus'] != cpus or cfg['Memory'] != 5 * 1024**3 or
                cfg['MemorySwap'] != cfg['Memory'] or cmd[cmd.index('--smp') + 1] != '2' or
                cmd[cmd.index('--memory') + 1] != '3G'):
            raise ValueError('node configuration differs from pinned local cluster: ' + node['name'])
        io = r.command('sudo', '-n', 'podman', 'exec', node['name'], 'cat', '/var/lib/scylla/physical-io.yaml').stdout
        storage.append(dict(node=node['name'], io_properties=io, mounts=info['Mounts']))
    save('server_image', inspected)
    save('storage', storage)
    schema = r.command('sudo', '-n', 'podman', 'exec', nodes[0]['name'], 'cqlsh', '-e',
                       "SELECT JSON replication FROM system_schema.keyspaces WHERE keyspace_name='bench_compare';").stdout
    schema_rows = [json.loads(line.strip()) for line in schema.splitlines() if line.strip().startswith('{')]
    if (len(schema_rows) != 1 or schema_rows[0]['replication'].get('datacenter1') != '3' or
            not schema_rows[0]['replication'].get('class', '').endswith('NetworkTopologyStrategy')):
        raise ValueError('preserved benchmark keyspace must use datacenter1 RF3')
    status = r.command('sudo', '-n', 'podman', 'exec', nodes[0]['name'], 'nodetool', 'status').stdout
    if sum(line.startswith('UN ') for line in status.splitlines()) != 3:
        raise ValueError('three healthy nodes required')
    save('rf3_topology', dict(nodes=nodes, schema=schema, status=status))
    delay_dir = provision / 'network-delay'
    increments = json.loads((delay_dir / 'rtt-increments.json').read_text())
    if len(increments) != 9 or any(value < .3 for value in increments.values()):
        raise ValueError('verified network delay required on all nine paths')
    save('network_delay', {path.name: json.loads(path.read_text()) for path in delay_dir.glob('*.json')})
    validations = []
    for name, binary in (('master', args.master.resolve()), ('original', args.original.resolve())):
        for case, (scenario, streaming, _, _) in c.roadmap.CASES.items():
            actual_streaming = streaming and name != 'master'
            path = out / f'{name}-{case}-warm.json'
            rc = r.client(path, hosts, binary.name, 'warm', scenario, actual_streaming, cpus='8', gomaxprocs=1)
            if rc:
                raise ValueError('complete payload validation failed: ' + name + '/' + case)
            log = path.with_suffix('.json.log')
            validations.append(dict(driver=name, case=case, binary_sha256=r.sha(binary),
                                    output=log.name, output_sha256=r.sha(log),
                                    audit=json.loads(path.with_suffix('.json.process.json').read_text())))
    save('wire_dataset_equivalence', validations)
    receipt = dict(qualified=True, dedicated_host=False, local_cluster=True,
                   scope='User-authorized local testing; no dedicated-host performance claim',
                   hostname=socket.gethostname(), hosts=hosts, references=c.REFERENCES,
                   boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                   gomaxprocs=1, blocked_controls=blocked, evidence=evidence,
                   cpu_roles=dict(client=[8], server=[0, 1, 4, 5, 6, 7],
                                  monitoring=[2, 18], development=[10, 11, 12, 13, 14, 15]))
    r.write(out / 'qualification.json', receipt)
    c.qualification(out / 'qualification.json', hosts, local_cluster=True, allow_reference_overload=True)
    shutil.copy2(Path(__file__), out / Path(__file__).name)
    print(out / 'qualification.json', flush=True)


if __name__ == '__main__':
    main()
