#!/usr/bin/env python3
"""Pinned, resumable one-vCPU comparison. All application clients run on CPU 8."""
import argparse, concurrent.futures, csv, datetime, gzip, hashlib, importlib.util, json, math, os
from pathlib import Path
import re, shutil, signal, socket, statistics, subprocess, threading, time, urllib.request

ROOT=Path(__file__).resolve().parents[2]
HERE=Path(__file__).resolve().parent
CACHE=ROOT/'.codex-cache/driver-comparison'
OLD='1ceaac8172bda26e8b8d701ac356982d33729b27'
V2='5a07badd5f3a567f3990e0c70aba6c8d28e7fbc0'
CONCURRENCIES=[1,4,16,64,256,1024]

def command(*args,check=True,timeout=180):
    p=subprocess.run([str(a) for a in args],capture_output=True,text=True,timeout=timeout)
    if check and p.returncode: raise RuntimeError(f'{args}: {p.stderr[-2000:]}')
    return p

def write(path,value):
    tmp=path.with_suffix(path.suffix+'.tmp');tmp.write_text(json.dumps(value,indent=2)+'\n');tmp.replace(path)
def compact(path):
    result=json.loads(path.read_text());raw={}
    for phase in ('warmup','measurement'):
        for name in ('latency','dispatch_latency','queue_latency','schedule_lag'):
            h=result.get(phase,{}).get(name,{})
            if 'snapshot' in h:
                raw[phase+'-'+name]=h.pop('snapshot')
                h['raw_histogram_file']=path.stem+'.histograms.json.gz'
    if raw:
        with gzip.open(path.with_suffix('.histograms.json.gz'),'wt') as f:json.dump(raw,f)
        write(path,result)
    return result

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def cv(values):return statistics.stdev(values)/statistics.mean(values) if statistics.mean(values) else math.inf

def client(out,hosts,driver,mode,scenario='read',stream=False,cpus='8',gomaxprocs=1,**kw):
    args=['taskset','-c',cpus,'env',f'GOMAXPROCS={gomaxprocs}',f'GOCQL_BENCH_CPUS={cpus}',f'GOCQL_BENCH_GOMAXPROCS={gomaxprocs}',str(CACHE/driver),'-hosts',hosts,'-mode',mode,'-scenario',scenario,f'-stream={str(stream).lower()}']
    for k,v in kw.items():
        if isinstance(v,bool): args.append('-'+k+'='+str(v).lower())
        else: args += ['-'+k,str(v)]
    log=out.with_suffix(out.suffix+'.log')
    with log.open('w') as f:
        p=subprocess.Popen(args,stdout=f,stderr=subprocess.STDOUT)
        start=time.monotonic();checks=0;tids=set();masks=set();error=None;resources=[]
        try:
            while p.poll() is None:
                try: is_client=Path(f'/proc/{p.pid}/exe').resolve().name==driver
                except FileNotFoundError: is_client=False
                if not is_client:
                    time.sleep(.01);continue
                if checks % 5 == 0:
                    try:
                        fields = {}
                        for line in Path(f'/proc/{p.pid}/status').read_text().splitlines():
                            name, _, value = line.partition(':')
                            if name in ('VmRSS', 'VmHWM', 'VmSize', 'Threads'):
                                fields[name] = value.strip()
                        resources.append({'time': now(), **fields,
                                          'file_descriptors': len(list(Path(f'/proc/{p.pid}/fd').iterdir()))})
                    except FileNotFoundError:
                        pass
                for status in Path(f'/proc/{p.pid}/task').glob('*/status'):
                    try:
                        for line in status.read_text().splitlines():
                            if line.startswith('Cpus_allowed_list:'):
                                mask=line.split(':',1)[1].strip();masks.add(mask);tids.add(status.parent.name)
                                if mask!=cpus:error=f'{status}: {mask}'
                    except FileNotFoundError:pass
                checks+=1
                if error: p.terminate();raise RuntimeError(error)
                if time.monotonic()-start>900: p.terminate();raise RuntimeError('client timeout')
                time.sleep(.2)
        finally:
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=15)
                except subprocess.TimeoutExpired:p.kill();p.wait()
            write(out.with_suffix(out.suffix+'.process.json'),{'args':args,'pid':p.pid,'returncode':p.returncode,'elapsed':time.monotonic()-start,'checks':checks,'tids':sorted(tids),'masks':sorted(masks),'affinity_error':error,'resources':resources,'runtime_environment':{name:os.environ.get(name) for name in ('GODEBUG','GOGC','GOMEMLIMIT','GOEXPERIMENT')}})
    return p.returncode

def getmetrics(node):
    with urllib.request.urlopen(f'http://{node["address"]}:9180/metrics',timeout=4) as r:
        return r.read().decode()

def settled(nodes):
    # Require no active compaction or queued flush work for three consecutive probes.
    end=time.monotonic()+180;quiet=0;observed=[]
    while time.monotonic()<end:
        pending=[]
        for n in nodes:
            for l in getmetrics(n).splitlines():
                if l.startswith(('scylla_compaction_manager_compactions{','scylla_database_active_disk_reads{','scylla_database_queued_flushes{','scylla_memtables_pending_flushes{','scylla_compaction_manager_pending_compactions{')):
                    pending.append((l.split()[0],float(l.split()[-1])))
        observed.append({'time':now(),'pending':pending})
        if all(v==0 for _,v in pending):quiet+=1
        else:quiet=0
        if quiet==3:return observed
        time.sleep(2)
    raise RuntimeError(f'preparation work did not settle: {observed[-3:]}')

class Monitor:
    def __init__(self,out,nodes):self.out=out;self.nodes=nodes;self.stop=threading.Event();self.thread=threading.Thread(target=self.run)
    def __enter__(self):self.thread.start();return self
    def __exit__(self,*args):self.stop.set();self.thread.join()
    def run(self):
        with gzip.open(self.out,'wt') as f:
            while not self.stop.is_set():
                record={'time':now(),'disk_free_bytes':shutil.disk_usage(CACHE).free,'diskstats':Path('/proc/diskstats').read_text(),'cpu':Path('/proc/stat').read_text(),'loadavg':Path('/proc/loadavg').read_text(),'nodes':{}}
                for n in self.nodes:
                    try:
                        metrics=getmetrics(n)
                        record['nodes'][n['name']]=[l for l in metrics.splitlines() if l.startswith(('scylla_reactor_','scylla_io_queue_','scylla_compaction_manager_','scylla_database_queued_flushes','scylla_database_active_disk_reads','scylla_storage_proxy_coordinator_','scylla_transport_','scylla_memtables_','scylla_commitlog_'))]
                    except Exception as e:record['nodes'][n['name']]={'error':str(e)}
                f.write(json.dumps(record)+'\n');f.flush();self.stop.wait(5)

def evidence(out,nodes):
    records={'time':now(),'go':command('go','version').stdout,'goenv':command('go','env','-json').stdout,'uname':command('uname','-a').stdout,'lscpu':command('lscpu','-J','-e=CPU,CORE,SOCKET,NODE,CACHE').stdout,'storage':command('lsblk','-o','NAME,TYPE,SIZE,MOUNTPOINTS,MODEL').stdout,'findmnt':command('findmnt','-T',CACHE).stdout,'disk_free':command('df','-B1',CACHE).stdout,'qdisc':command('sudo','-n','tc','-s','qdisc','show').stdout,'git':{'old':command('git','-C',CACHE/'old-source','rev-parse','HEAD').stdout.strip(),'v2':command('git','rev-parse','HEAD').stdout.strip()},'builds':{d:{'sha256':sha(CACHE/d),'settings':command('go','version','-m',CACHE/d).stdout} for d in ('old','v2')},'source_files':{p.name:sha(p) for p in HERE.iterdir() if p.is_file()},'nodes':[],'device':{}}
    assert records['git']=={'old':OLD,'v2':V2}
    for p in Path('/sys/block/nvme0n1/queue').iterdir():
        try:records['device'][p.name]=p.read_text()
        except OSError:pass
    for n in nodes:
        ins=json.loads(command('sudo','-n','podman','inspect',n['name']).stdout)[0]
        pid=ins['State']['Pid'];cg=Path('/proc')/str(pid)/'cgroup';relative=cg.read_text().strip().split('::')[-1]
        chain=[];cur=Path('/sys/fs/cgroup')/relative.lstrip('/')
        while True:
            row={'path':str(cur)}
            for name in ('cpu.max','cpu.stat','cpuset.cpus.effective','memory.max','memory.swap.max','io.max','io.stat'):
                if (cur/name).exists():row[name]=(cur/name).read_text()
            if 'cpu.max' in row and row['cpu.max'].split()[0]!='max':raise RuntimeError(f'CPU throttle: {row}')
            if row.get('io.max','').strip():raise RuntimeError(f'IO throttle: {row}')
            chain.append(row)
            if cur==Path('/sys/fs/cgroup'):break
            cur=cur.parent
        qdisc=command('sudo','-n','nsenter','-t',pid,'-n','tc','-s','qdisc','show').stdout
        if 'netem' in qdisc:raise RuntimeError('server netem')
        process=command('sudo','-n','podman','exec',n['name'],'pgrep','-a','-x','scylla').stdout
        if '--io-properties-file /var/lib/scylla/physical-io.yaml' not in process.replace('=', ' '):raise RuntimeError('missing effective IO properties')
        config=command('sudo','-n','podman','exec',n['name'],'cat','/var/lib/scylla/physical-io.yaml').stdout
        records['nodes'].append({'inspect':ins,'cgroups':chain,'qdisc':qdisc,'process':process,'io_properties':config})
    # Client and runner ancestor cgroups are captured separately.
    cur=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::')[-1].lstrip('/')
    records['client_cgroups']=[]
    while True:
        row={'path':str(cur)}
        for name in ('cpu.max','cpuset.cpus.effective','memory.max','io.max'):
            if (cur/name).exists():row[name]=(cur/name).read_text()
        records['client_cgroups'].append(row)
        if row.get('cpu.max','max').split()[0]!='max' or row.get('io.max','').strip():raise RuntimeError(f'client ancestor limit: {row}')
        if cur==Path('/sys/fs/cgroup'):break
        cur=cur.parent
    write(out/'environment.json',records)

def valid(r):
    m=r['measurement']
    return m['errors']==0 and m['affinity_error']=='' and m['latency']['overflow']==0 and m['admitted']==m['completed'] and m['in_window']>0

def stability(rows,phase):
    issues=[]
    if len(rows)!=3 or not all(valid(r) for r in rows):return ['missing or invalid round']
    for key,limit in [('throughput',.05),('p90_us',.10),('p95_us',.10),('p99_us',.10)]:
        values=[r['measurement'][key] if key=='throughput' else r['measurement']['latency'][key] for r in rows]
        if cv(values)>limit:issues.append(f'{key} CV {cv(values):.4f} > {limit}')
    if phase=='sustainable':
        for r in rows:
            m=r['measurement'];q=m['backlog_samples'][:int(m['duration_seconds'])];c=r['concurrency']
            growing=len(q)>8 and statistics.mean(q[-len(q)//4:])>statistics.mean(q[:len(q)//4])+max(1,c/4)
            if m['throughput']<.99*m['target_rate'] or m['rejected'] or growing:issues.append('target delivery/rejection/backlog failure')
    return issues

def activity(path,result):
    m=result['measurement'];start=m['start'];end=m['end']
    with gzip.open(path,'rt') as f:samples=[json.loads(l) for l in f]
    samples=[s for s in samples if start <= s['time'].replace('+00:00','Z') <= end]
    if len(samples)<2:return {'samples':len(samples)}
    first,last=samples[0],samples[-1]
    seconds=(datetime.datetime.fromisoformat(last['time'])-datetime.datetime.fromisoformat(first['time'])).total_seconds()
    def total(s,prefix):
        return sum(float(l.split()[-1]) for lines in s['nodes'].values() if isinstance(lines,list) for l in lines if l.startswith(prefix+'{'))
    delta=lambda prefix:total(last,prefix)-total(first,prefix)
    return {'samples':len(samples),'sample_seconds':seconds,'server_cpu_cores':delta('scylla_reactor_cpu_used_time_ms')/1000/seconds,'server_read_mib_per_second':delta('scylla_io_queue_total_read_bytes')/1048576/seconds,'server_write_mib_per_second':delta('scylla_io_queue_total_write_bytes')/1048576/seconds}

def summarize(out,state):
    allrows=[]
    for key,paths in state.get('sets',{}).items():
        for path in paths:
            if (out/path).exists():
                r=json.loads((out/path).read_text());m=r['measurement'];allrows.append({'set':key,'file':path,'driver':r['driver'],'scenario':r['scenario'],'concurrency':r['concurrency'],'stream':r['stream'],'throughput':m['throughput'],'p90_us':m['latency']['p90_us'],'p95_us':m['latency']['p95_us'],'p99_us':m['latency']['p99_us'],'dispatch_p99_us':m['dispatch_latency']['p99_us'],'queue_p99_us':m['queue_latency']['p99_us'],'target':m['target_rate'],'errors':m['errors'],'rejected':m['rejected'],'completed':m['completed'],'drained':m['drained'],'cpu':m['cpu_seconds_including_drain'],'alloc_bytes':m['alloc_bytes'],'mallocs':m['mallocs'],'gc':m['gc'],'offered':m['offered'],'timeouts':m['timeouts'],'admitted':m['admitted'],'missed_schedules':m['missed_schedules_over_1ms'],'end_queue':m['end_queue'],'dispatch_p90_us':m['dispatch_latency']['p90_us'],'dispatch_p95_us':m['dispatch_latency']['p95_us'],'server_cpu_cores':r.get('server_activity',{}).get('server_cpu_cores'),'server_read_mib_s':r.get('server_activity',{}).get('server_read_mib_per_second'),'server_write_mib_s':r.get('server_activity',{}).get('server_write_mib_per_second')})
    if allrows:
        with (out/'rounds.csv').open('w') as f:
            w=csv.DictWriter(f,fieldnames=allrows[0].keys());w.writeheader();w.writerows(allrows)
    lines=['# One client vCPU: remote master versus v2','',f'Status: {state.get("status","running")}. Sources: old `{OLD}`, v2 `{V2}`.','', '**Sustainable rates are 70% of each driver’s own measured capacity. Latencies therefore compare different absolute loads.**','', 'All latency columns are scheduled arrival to validated completion for sustainable load, dispatch to validated completion for maximum throughput. Values below are medians with min–max across three rounds.','', '|Scenario|Phase|Driver|Ops/s (min–max)|p90 ms|p95 ms|p99 ms (min–max)|Target ops/s|Stable|','|---|---|---|---:|---:|---:|---:|---:|---|']
    summary={}
    for sc in ('read','write','paged'):
        for phase in ('maximum','sustainable'):
            for d in ('old','v2'):
                key=f'{sc}-{phase}-{d}';chosen=state.get('chosen_sets',{}).get(key,key+'-initial');rows=[r for r in allrows if r['set']==chosen]
                if len(rows)!=3:continue
                med=lambda k:statistics.median(r[k] for r in rows)
                bounds=lambda k: f'{med(k):,.1f} ({min(r[k] for r in rows):,.1f}–{max(r[k] for r in rows):,.1f})'
                stable=not state.get('instability',{}).get(chosen)
                lines.append(f'|{sc}|{phase}|{d}|{bounds("throughput")}|{med("p90_us")/1000:.3f} ({min(r["p90_us"] for r in rows)/1000:.3f}–{max(r["p90_us"] for r in rows)/1000:.3f})|{med("p95_us")/1000:.3f} ({min(r["p95_us"] for r in rows)/1000:.3f}–{max(r["p95_us"] for r in rows)/1000:.3f})|{med("p99_us")/1000:.3f} ({min(r["p99_us"] for r in rows)/1000:.3f}–{max(r["p99_us"] for r in rows)/1000:.3f})|{med("target"):,.1f}|{stable}|')
                summary[key]={'throughput':med('throughput'),'p99_us':med('p99_us'),'stable':stable}
    lines+=['','## Throughput changes','']
    for sc in ('read','write','paged'):
        a=summary.get(sc+'-maximum-old');b=summary.get(sc+'-maximum-v2')
        if a and b:
            lines.append(f'- {sc}: v2 throughput {(b["throughput"]/a["throughput"]-1)*100:+.1f}%; maximum-load p99 {(b["p99_us"]/a["p99_us"]-1)*100:+.1f}%.')
            if sc=='paged':lines.append(f'  Old {a["throughput"]*128:,.0f} rows/s, {a["throughput"]*.5:,.1f} MiB/s; v2 {b["throughput"]*128:,.0f} rows/s, {b["throughput"]*.5:,.1f} MiB/s (blob bytes only).')
    lines+=['','## Resources (medians)','', '|Scenario / phase / driver|Client CPU %|Bytes allocated/op|Allocations/op|GC cycles|Server CPU cores|Server read / write MiB/s|','|---|---:|---:|---:|---:|---:|---:|']
    for key,chosen in state.get('chosen_sets',{}).items():
        rows=[r for r in allrows if r['set']==chosen]
        if len(rows)!=3:continue
        med=lambda fn:statistics.median(fn(r) for r in rows)
        lines.append(f'|{key}|{med(lambda r:r["cpu"]/120*100):.1f}|{med(lambda r:r["alloc_bytes"]/r["completed"]):,.0f}|{med(lambda r:r["mallocs"]/r["completed"]):.1f}|{med(lambda r:r["gc"]):.0f}|{med(lambda r:r["server_cpu_cores"] or 0):.2f}|{med(lambda r:r["server_read_mib_s"] or 0):.1f} / {med(lambda r:r["server_write_mib_s"] or 0):.1f}|')
    lines+=['','## Pilot choices','', '```json',json.dumps(state.get('selection',{}),indent=2),'```','','## Stability and retained evidence','', 'Throughput CV limit: 5%; p90/p95/p99 CV limit: 10%. Unstable sets are repeated once; originals remain in rounds.csv and raw JSON.','', '```json',json.dumps(state.get('instability',{}),indent=2),'```','', 'Raw histograms (1 µs–60 s, 3 significant digits), warmup results, completion/drain accounting, schedule lag, bounded queue occupancy, allocations and GC are in each run JSON. CPU time includes bounded draining. `.process.json` files verify all observed client threads stayed on CPU 8. `*.metrics.jsonl.gz` records server reactor/disk activity and host CPU/disk counters with UTC timestamps. `environment.json` records source/compiler/build hashes, topology, physical I/O properties and ancestor cgroup limits. Wire probes and connection inventories are outside measurements.']
    (out/'REPORT.md').write_text('\n'.join(lines)+'\n');write(out/'summary.json',summary)

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--smoke',action='store_true');args=parser.parse_args()
    os.sched_setaffinity(0,{2,18});out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    lock=open(CACHE/'run.lock','w');import fcntl;fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    nodes=json.loads((CACHE/'cluster.json').read_text());hosts=','.join(n['address'] for n in nodes)
    state=json.loads((out/'state.json').read_text()) if (out/'state.json').exists() else {'sets':{},'chosen_sets':{},'instability':{},'selection':{}}
    def checkpoint():write(out/'state.json',state);summarize(out,state)
    def invoke(name,d,mode,sc='read',stream=False,**kw):
        file=out/(name+'.json')
        if mode=='run' and file.exists():return compact(file)
        print(now(),name,flush=True)
        code=client(file,hosts,d,mode,sc,stream,**kw)
        if code and mode!='run':raise RuntimeError(f'{name} failed; see log')
        if mode=='run' and file.exists():return compact(file)
        if mode=='run':return None
    def reset(name,sc):
        for node in nodes:command('sudo','-n','podman','exec',node['name'],'nodetool','clearsnapshot','--keyspaces','bench_compare',timeout=120)
        invoke(name+'-prepare','old','prepare',sc)
        table='paged' if sc=='paged' else 'point'
        for node in nodes:command('sudo','-n','podman','exec',node['name'],'nodetool','flush','bench_compare',table,timeout=120)
        deadline=time.monotonic()+60
        while True:
            description=command('sudo','-n','podman','exec',nodes[0]['name'],'nodetool','describecluster').stdout
            versions=re.findall(r'([0-9a-f]{8}-[0-9a-f-]{27}): \[(.*?)\]',description)
            if len(versions)==1 and {ip.strip() for ip in versions[0][1].split(',')}=={n['address'] for n in nodes}:break
            if time.monotonic()>deadline:raise RuntimeError('schema agreement timeout: '+description)
            time.sleep(1)
        (out/(name+'-schema-agreement.txt')).write_text(description)
        quiet=settled(nodes);write(out/(name+'-settled.json'),quiet)
        invoke(name+'-warm-dataset','old','warm',sc)
    def measurement(name,d,sc,stream,c,rate=0,pilot=False):
        file=out/(name+'.json')
        if file.exists():return json.loads(file.read_text())
        reset(name,sc)
        with Monitor(out/(name+'.metrics.jsonl.gz'),nodes):
            r=invoke(name,d,'run',sc,stream,concurrency=c,warmup='5s' if pilot else '30s',duration='15s' if pilot else '120s',rate=rate,output=file)
        if r:
            r['server_activity']=activity(out/(name+'.metrics.jsonl.gz'),r);write(file,r)
        checkpoint();return r
    shutil.copy2('/extra/scylladb/alternator-client-rust/scripts/run-alternator-overload.py',out/'provision-source.py')
    evidence(out,nodes)
    (out/'sources').mkdir(exist_ok=True)
    for source in HERE.iterdir():
        if source.is_file():shutil.copy2(source,out/'sources'/source.name)
    for src in ('cluster.json','container-inventory.json','preserved-containers.json','process-inventory.json','physical-io.yaml','iotune.log','qdisc-before.json','build.log','remote-master.txt'):
        if (CACHE/src).exists():shutil.copy2(CACHE/src,out/src)
    for d in ('old','v2'):
        invoke('schema-'+d,d,'schema');invoke('check-'+d,d,'check')
        if d=='v2':invoke('check-v2-stream',d,'check',stream=True)
    if args.smoke:
        for sc in ('read','write','paged'):
            reset('smoke-'+sc,sc)
            for d in ('old','v2'):
                for stream in ([False,True] if d=='v2' and sc!='write' else [False]):
                    r=invoke(f'smoke-{sc}-{d}-{stream}',d,'run',sc,stream,concurrency=4,warmup='1s',duration='2s',output=out/f'smoke-{sc}-{d}-{stream}.json')
                    if not r or not valid(r):raise RuntimeError('invalid smoke')
        state['status']='smoke complete';checkpoint();return
    if not (out/'probes'/'v2-stream-wire.json').exists():
        reset('probe-point','read');reset('probe-paged','paged')
        command('python3',HERE/'probe.py',out/'probes',timeout=180)
    for sc in ('read','write','paged'):
        if sc not in state['selection']:
            pilots={}
            for c in CONCURRENCIES:
                order=['old','v2'] if CONCURRENCIES.index(c)%2==0 else ['v2','old']
                for d in order:
                    for stream in ([False,True] if d=='v2' and sc!='write' else [False]):
                        name=f'pilot-{sc}-{d}-{"stream" if stream else "buffered"}-{c}'
                        r=measurement(name,d,sc,stream,c,pilot=True)
                        pilots.setdefault(d+'-'+str(stream),[]).append((c,name,r))
            picks={}
            for key,items in pilots.items():
                good=[(c,n,r) for c,n,r in items if r and valid(r)]
                if not good:raise RuntimeError(f'no valid capacity pilot for {sc}/{key}')
                top=max(r['measurement']['throughput'] for _,_,r in good)
                c,name,r=min((c,n,r) for c,n,r in good if r['measurement']['throughput']>=top*.97)
                d,stream=key.split('-');stream=stream=='True';rates=[r['measurement']['throughput']]
                # Three pilot observations at each read adapter's selected concurrency.
                if d=='v2' and sc!='write':
                    for rep in (2,3):
                        extra=measurement(name+f'-rep{rep}',d,sc,stream,c,pilot=True)
                        if extra and valid(extra):rates.append(extra['measurement']['throughput'])
                picks[key]={'concurrency':c,'median_pilot_throughput':statistics.median(rates),'validated_samples':len(rates),'stream':stream,'capacity_search_limit':good[-1][0]==1024 and good[-1][2]['measurement']['throughput']>good[-2][2]['measurement']['throughput']*1.03 if len(good)>1 else True}
            old=picks['old-False'];v2=picks['v2-False']
            if 'v2-True' in picks:
                streaming=picks['v2-True']
                if streaming['validated_samples']==3 and streaming['median_pilot_throughput']>v2['median_pilot_throughput']*1.03:v2=streaming
            state['selection'][sc]={'old':old,'v2':v2,'all_candidates':picks};checkpoint()
        for phase in ('maximum','sustainable'):
            for attempt in ('initial','repeat'):
                todo=[]
                for d in ('old','v2'):
                    key=f'{sc}-{phase}-{d}';name=key+'-'+attempt
                    if attempt=='repeat' and not state['instability'].get(key+'-initial'):continue
                    todo.append(d);state['sets'].setdefault(name,[])
                if not todo:break
                for roundnum in range(1,4):
                    for d in (todo if roundnum%2 else list(reversed(todo))):
                        key=f'{sc}-{phase}-{d}';setname=key+'-'+attempt;name=setname+f'-{roundnum}';sel=state['selection'][sc][d]
                        rate=0
                        if phase=='sustainable':
                            chosen=state['chosen_sets'][f'{sc}-maximum-{d}']
                            maximum=statistics.median(json.loads((out/p).read_text())['measurement']['throughput'] for p in state['sets'][chosen]);rate=maximum*.7
                        r=measurement(name,d,sc,sel['stream'],sel['concurrency'],rate)
                        if name+'.json' not in state['sets'][setname]:state['sets'][setname].append(name+'.json')
                        checkpoint()
                for d in todo:
                    key=f'{sc}-{phase}-{d}';name=key+'-'+attempt;rows=[json.loads((out/p).read_text()) for p in state['sets'][name] if (out/p).exists()]
                    state['instability'][name]=stability(rows,phase);state['chosen_sets'][key]=name
                checkpoint()
    missing=[p for paths in state['sets'].values() for p in paths if not (out/p).exists()]
    state['status']='incomplete' if missing else 'complete';state['missing']=missing;checkpoint()

if __name__=='__main__':
    import sys
    try:main()
    finally:
        if '--smoke' not in sys.argv and '--output' in sys.argv:
            destination=Path(sys.argv[sys.argv.index('--output')+1]).resolve()
            command('python3',HERE/'shutdown.py',destination,timeout=360)
