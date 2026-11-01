# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Independently verify exact owned fixture/command joins and endpoint refusal."""
import json,os,socket
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
def gone(pid):
    try:os.kill(pid,0)
    except ProcessLookupError:return True
    raise AssertionError('recorded owned process remains alive: '+str(pid))
def build():
    processes=[];lanes=[];roots=[];run_ids=[]
    for path in OUT.glob('*.json'):
        receipt=matrix.read(path)
        if not isinstance(receipt,dict)or receipt.get('status')!='passed':continue
        roots.append(receipt['fixture_root']);run_ids.append(receipt['run_id'])
        processes.append(dict(kind='native-runner',lane=path.stem,pid=receipt['pid'],gone=gone(receipt['pid'])))
        for command in receipt['commands']:
            processes.append(dict(kind='command',lane=path.stem,pid=command['pid'],gone=gone(command['pid'])))
            assert not command['exit_code']and matrix.sha(command['log'])==command['sha256']
        for lane in receipt['lanes']:
            assert lane['joined']and all(lane['joined'].values())and lane['endpoints_refused']
            for event in lane.get('node_control',[]):
                for key in ['pid_before','pid_after']:
                    if event.get(key):processes.append(dict(kind='controlled-node',lane=path.stem,pid=event[key],gone=gone(event[key])))
            lanes.append(dict(receipt=str(path),joined=lane['joined'],endpoints_refused=lane['endpoints_refused']))
    assert len(lanes)==11,len(lanes)
    diagnostics=[]
    for path in [OUT.parent/'stable.json',OUT.parent/'tablet-diagnostic/native.json',OUT.parent/'tablet-diagnostic-expanded/native.json',OUT.parent/'fix-check/native.json',OUT.parent/'final/stable.json',OUT/'cassandra-v5-no-compression-tls.json',OUT.parent/'cassandra-agreement-diagnostic/run/native.json',OUT.parent/'cassandra-agreement-diagnostic/run-full/native.json']:
        prior=matrix.read(path);roots.append(prior['fixture_root']);run_ids.append(prior['run_id'])
        processes.append(dict(kind='diagnostic-native-runner',pid=prior['pid'],gone=gone(prior['pid'])))
        for command in prior['commands']:
            assert matrix.sha(command['log'])==command['sha256']
            processes.append(dict(kind='diagnostic-command',pid=command['pid'],gone=gone(command['pid'])))
        assert all(all(lane['joined'].values())and lane['endpoints_refused']for lane in prior['lanes'])
        diagnostics.append(dict(path=str(path),sha256=matrix.sha(path),source_sha256=prior['source_sha256'],status=prior['status'],credited=False,joined=True))
    cancellation=matrix.read(OUT/'cancel-probe/verification.json');receipt=matrix.read(cancellation['receipt'])
    assert cancellation['source_sha256']==matrix.sha(OUT/'checks/source.json')and cancellation['expected_cancelled_receipt']
    assert receipt['status']=='failed'and receipt['cancel_requested']
    assert matrix.sha(cancellation['receipt'])==cancellation['receipt_sha256']and matrix.sha(cancellation['log'])==cancellation['log_sha256']
    assert len(cancellation['signals_sent'])==5 and {r['phase']for r in cancellation['signals_sent']}=={'testing','cleanup','stop-join'}
    assert cancellation['all_nodes_joined']and cancellation['endpoints_refused']
    processes.append(dict(kind='cancel-native-runner',pid=receipt['pid'],gone=gone(receipt['pid'])))
    for command in receipt['commands']:
        processes.append(dict(kind='canceled-test-or-cleanup',pid=command['pid'],gone=gone(command['pid'])))
        assert matrix.sha(command['log'])==command['sha256']
    roots.append(receipt['fixture_root']);run_ids.append(receipt['run_id'])
    # Check the exact joined supervisors as well. The final verification process
    # is independent; it never terminates/restarts a live observed process.
    supervisor_paths=[OUT/'pipeline.json',OUT/'release-run.json']
    if(OUT/'resume-run.json').exists():supervisor_paths.append(OUT/'resume-run.json')
    for path in supervisor_paths:
        data=matrix.read(path)
        processes.append(dict(kind='supervisor',pid=data['pid'],gone=gone(data['pid'])))
        for row in data['steps']:
            assert row['phase']=='joined'
            if path.name=='resume-run.json'or not(OUT/'resume-run.json').exists():assert row['exit_code']==0
            else:assert row['exit_code']in [0,1]
            processes.append(dict(kind='joined-supervisor-child',pid=row['pid'],gone=gone(row['pid'])))
    endpoints=[]
    for subnet in [20,21,22]:
        for node in [1,2,3]:
            address=f'127.0.{subnet}.{node}'
            for port in [9042,9142,19042,19142,7000,7001,10000]:
                try:connection=socket.create_connection((address,port),.1)
                except OSError:endpoints.append(dict(address=address,port=port,refused=True));continue
                connection.close();raise AssertionError('owned endpoint remains active: '+address+':'+str(port))
    survivors=[]
    for path in Path('/proc').glob('[0-9]*'):
        try:
            command=(path/'cmdline').read_bytes();environment=(path/'environ').read_bytes()
        except (OSError,PermissionError):continue
        owned=any(str(root).encode()in command for root in roots)or any(('GOCQL_TEST_RUN='+run).encode()+b'\0'in environment for run in run_ids)
        if owned:survivors.append(int(path.name))
    assert not survivors,survivors
    result=dict(runtime_source_sha256=matrix.sha(OUT/'checks/source.json'),lanes=lanes,diagnostic_receipts=diagnostics,processes=processes,cancellation_probe=cancellation,endpoints=endpoints,exact_owned_fixture_roots=roots,exact_owned_run_ids=run_ids,matching_process_survivors=survivors,scope='Read-only final probes of exact task-owned addresses, roots and run environments. No unrelated process/container reset, kill or publication.',gate='passed')
    matrix.write(E/'step17-cleanup.json',result)
    print(json.dumps(dict(lanes=len(lanes),processes=len(processes),endpoints_refused=len(endpoints),owned_survivors=survivors)))
    return result
if __name__=='__main__':build()
