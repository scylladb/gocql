# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Reconcile all 23 cost rows with fresh final-source measurements and assertions."""
import ast,collections,json,re,subprocess
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E

def build():
    source=matrix.sha(OUT/'checks/source.json');commands={};receipts={};results=[];memory=[];modes=[]
    for folder in ['checks','performance']:
        for c in matrix.read(OUT/folder/'checks.json'):
            assert c['exit_code']==0 and c['source_sha256']==source and matrix.sha(c['log'])==c['sha256']
            commands[folder+'/'+c['name']]=c
    for path in OUT.glob('*.json'):
        r=matrix.read(path)
        if not isinstance(r,dict) or r.get('status')!='passed':continue
        assert r['source_sha256']==source
        receipts[path.stem]=r
        for c in r['commands']:
            assert not c['exit_code'] and matrix.sha(c['log'])==c['sha256'];commands[path.stem+'/'+c['name']]=c
        for mode in r['configuration']['lanes'][0]['modes']:
            if path.stem=='development':continue
            name=r['configuration']['lanes'][0]['name']+'-'+mode['name'];folder=OUT/'native-performance'/name
            measured=matrix.read(folder/'results.json');old=matrix.read(folder/'baseline/results.json');parked=matrix.read(folder/'concurrent-memory.json')
            compressed4=mode['name'].startswith('v4') and any(x in mode['name']for x in ['snappy','lz4'])
            apis={'row','into'} if compressed4 else {'row','into','stream'}
            shapes={('small',1,32,1,1,False),('page1',128,4096,1,1,False),('page32',128,4096,32,1,False),('rows1',1,4096,128,1,False),('rows128',128,4096,128,1,False),('cell256K',1,256<<10,1,1,False),('cell8M',1,8<<20,1,1,False),('concurrent',128,4096,32,4,False),('slow',128,4096,32,1,True)}
            shape=lambda x:tuple(x['Shape'][key]for key in ['Name','Rows','Cell','Page','Concurrency','Slow'])
            assert len(measured)==9*len(apis) and len(old)==9
            assert {x['API']for x in measured}==apis
            assert {shape(x)for x in old}==shapes
            for api in apis:assert {shape(x)for x in measured if x['API']==api}==shapes
            assert {(x['API'],x['Workers'],x['Rows'],x['Cell'],x['Page'])for x in parked}=={(a,w,128,4096,128)for a in apis for w in [1,4,8]}
            normalize=lambda x:sorted((n['host_id'],n['host_connections'],n['missing_connections'],n['shards'],n['shard'],n['protocol'],n['compression'],n['metadata_id'],n.get('tls_version'),n.get('tls_cipher'),n['authentication_fixture'])for n in x['Negotiated'])
            negotiated=normalize(old[0]);assert negotiated
            for x in measured+old:
                assert x['Operations']==(8 if x['Shape']['Slow'] else 32)*x['Shape']['Concurrency']==x['PercentileSamples']
                assert x['Protocol']==int(mode['name'][1]) and x['TLS']==('-tls' in mode['name'])
                assert normalize(x)==negotiated,('actual negotiated configuration mismatch',name,x['Shape'],x['API'])
                assert all(n['missing_connections']==0 for n in x['Negotiated'])
                if x['TLS']:assert all(n.get('tls_handshake_complete')and n.get('tls_version')and n.get('tls_cipher')for n in x['Negotiated'])
                x=dict(x,mode=name,source_sha256=source)
                x['derived']=dict(cpu_ns_per_operation=x['CPUNS']/x['Operations'],allocated_bytes_per_operation=x['AllocatedBytes']/x['Operations'],allocations_per_operation=x['Allocations']/x['Operations'],ns_per_operation=x['WallNS']/x['Operations'])
                results.append(x)
            memory.extend(dict(x,mode=name,source_sha256=source)for x in parked)
            for suffix in ['-performance','-performance-baseline','-performance-concurrent-memory','-performance-affected-stream','-cleanup']:
                assert any(c['name']==name+suffix for c in r['commands']),(name,suffix)
            modes.append(dict(name=name,measured=len(measured),baseline=len(old),parked=len(parked),actual_negotiated=negotiated,output=str(folder)))
        for pin in r.get('comparison_sources',{}).values():
            assert pin['revision']=='82b52574146b42f34ed3fe967d2d5e17cc53996b'
            assert matrix.sha(pin['snapshot'])==pin['sha256']
            for name,digest in matrix.read(pin['snapshot']).items():assert matrix.sha(Path(pin['root'])/name)==digest
    assert len(modes)==16,len(modes)
    baseline=Path('/extra/scylladb/gocql-api-test-cluster/step15/baseline-matched')
    assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=baseline,text=True).strip()=='82b52574146b42f34ed3fe967d2d5e17cc53996b'
    assert subprocess.check_output(['git','diff','--stat'],cwd=baseline,text=True)==''
    supplemental={}
    for name in ['text-identity','nested-vectors','batch-envelope','segment-throughput','live-heap-overlay']:
        row=matrix.read(OUT/'performance'/(name+'.json'));assert row['exit_code']==0 and matrix.sha(row['log'])==row['sha256']
        supplemental[name]=row;commands['supplemental/'+name]=row
    for row in matrix.read(OUT/'performance/probe-checks.json'):
        assert row['exit_code']==0 and matrix.sha(row['log'])==row['sha256'];commands['supplemental/'+row['name']]=row
    latency=matrix.read(OUT/'latency-costs/verification.json')
    assert latency['current_production_sha256']==matrix.sha(ROOT/'internal/routing/v2_policy.go')
    for row in latency['checks']:
        assert row['exit_code']==0 and matrix.sha(row['log'])==row['sha256']
    supplemental['latency-deadlines']=latency
    assertions={};benchmarks={}
    for key,c in commands.items():
        parsed=matrix.package_outcomes(c)
        for (package,test),action in parsed.items():
            if action=='pass':assertions.setdefault(test.split('/')[0],[]).append(dict(command=key,package=package,test=test,log=c['log'],sha256=c['sha256']))
        raw=Path(c['log']).read_text()
        if not parsed and c['exit_code']==0 and re.search(r'(?m)^ok\s+github.com/scylladb/gocql/v2(?:/\S+)?\s',raw):
            for test in re.findall(r'--- PASS: ([^ (]+)',raw):assertions.setdefault(test.split('/')[0],[]).append(dict(command=key,test=test,log=c['log'],sha256=c['sha256'],scope='Successful whole plain-output Go package, additive test-only oracle.'))
        for line in raw.splitlines():
            match=re.match(r'^(Benchmark[^/\s-]+)',line)
            if match:benchmarks.setdefault(match[1],[]).append(dict(command=key,sample=line,log=c['log'],sha256=c['sha256']))
    # Retain the explicit per-row test/benchmark selections; current observations
    # below, rather than the step15 report, discharge the final cost requirements.
    tree=ast.parse((ROOT/'feature-specs/tools/step15_evidence.py').read_text())
    binding=next(ast.literal_eval(node.value)for node in ast.walk(tree)if isinstance(node,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='bindings'for t in node.targets))
    text=(ROOT/'feature-specs/gocql-v2-implementation.md').read_text();table=text[text.index('| Benchmark (steady state'):text.index('Measure ns/op, bytes/op',text.index('| Benchmark (steady state'))]
    rows=[line.split('|')[1:3]for line in table.splitlines()if line.startswith('| ')and not line.startswith(('| Benchmark ','| Scaling/','| ---'))]
    assert len(rows)==len(binding)==23
    requirements=[]
    for (requirement,cost),(identity,tests,measurements,method)in zip(rows,binding):
        assert all(n in assertions for n in tests),(identity,'missing current assertions',[n for n in tests if n not in assertions])
        assert all(n in benchmarks or n in assertions for n in measurements),(identity,'missing current measurement',[n for n in measurements if n not in benchmarks and n not in assertions])
        requirements.append(dict(id=identity,requirement=requirement.strip(),required_evidence=cost.strip(),method=method,tests={n:assertions[n]for n in tests},measurements={n:benchmarks.get(n,assertions.get(n))for n in measurements},status='current-evidence-passed'))
    comparisons=[]
    for mode in modes:
        old={x['Shape']['Name']:x for x in results if x['mode']==mode['name']and x['API']=='legacy-scan'}
        for x in results:
            if x['mode']!=mode['name']or x['API']=='legacy-scan':continue
            before=old[x['Shape']['Name']]
            comparisons.append(dict(mode=x['mode'],shape=x['Shape']['Name'],api=x['API'],cpu_ratio=x['derived']['cpu_ns_per_operation']/before['derived']['cpu_ns_per_operation'],allocation_bytes_ratio=x['derived']['allocated_bytes_per_operation']/before['derived']['allocated_bytes_per_operation'],end_p50_ratio=x['EndNS'][0]/before['EndNS'][0]))
    profiles={str(p):matrix.sha(p)for folder in [OUT/'performance',OUT/'latency-costs',OUT/'native-performance']for p in folder.rglob('*.pprof')}
    result=dict(runtime_source_sha256=source,requirements=requirements,modes=modes,results=results,memory=memory,comparisons=comparisons,commands=commands,supplemental=supplemental,profiles=profiles,baseline=dict(revision='82b52574146b42f34ed3fe967d2d5e17cc53996b',root=str(baseline),tracked_production_diff_empty=True,configuration=matrix.read(Path('/extra/scylladb/gocql-api-test-cluster/step15/matched-configuration.json'))),limits=['Normal timing intervals have no forced GC, heap sampler or profiler. Diagnostics/profiles are separate commands/intervals.','32 ordinary,128 concurrent and8 slow observations support empirical quantiles only; shared-host observations do not establish stable tails or universal speedups.','HeapAlloc may include garbage; post-GC synchronized live heap includes driver+TLS+harness after baseline and separately reported final-output deductions. It is distinct from actual driver byte-capacity ceilings.','TCP_INFO counts actual client socket bytes; process syscall snapshots include background/control/harness I/O; strace runs separately on compiled binaries. TLS and validated-unit copies are explicit transforms.','Compressed native v3/v4 streaming rejects before submission as specified; all required raw/TLS/v5 modes execute.','Small-row and128-row CPU/latency overheads, cache idle retention and mixed-buffer churn remain visible. No timing can waive forbidden payload-copy/per-cell/per-element allocation or algorithm bounds.','The existing per-host sample mutex now protects the full probe deadline. Default disabled/nonexpired decision paths remain unchanged. Probe checks allocate0; warm publication allocates0, while due/competing publication candidates allocate24bytes each and integer allocs/op can round fractional rates to0. Boundary/loser profiles and forced-expiry contention timings are explicit.'],gate='passed')
    matrix.write(E/'step17-performance.json',result)
    print(json.dumps(dict(rows=len(requirements),modes=len(modes),results=len(results),parked=len(memory),profiles=len(result['profiles']))))
    return result
if __name__=='__main__':build()
