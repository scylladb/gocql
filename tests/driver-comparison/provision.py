#!/usr/bin/env python3
"""Reuse the Alternator provisioning implementation with isolated resources."""
import argparse, hashlib, importlib.util, json, os, pathlib, shutil, subprocess, sys
ROOT = pathlib.Path(__file__).resolve().parents[2]
CACHE = ROOT / '.codex-cache/driver-comparison'
SOURCE = pathlib.Path('/extra/scylladb/alternator-client-rust/scripts/run-alternator-overload.py')
spec = importlib.util.spec_from_file_location('provision', SOURCE)
p = importlib.util.module_from_spec(spec); spec.loader.exec_module(p)
p.CACHE = CACHE; p.NODES = tuple(f'codex-gocql-compare-{i}' for i in (1,2,3))
p.CPU_SETS = ('0-1','4-5','6-7'); p.CLIENT_CPUS = '8'
p.NETWORK = 'codex-gocql-compare'; p.LABEL = 'io.codex.gocql-compare'
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('mode',choices=('start','stop','calibrated','tune','resume'))
parser.add_argument('--output',type=pathlib.Path,help='Retain new receipts without overwriting parked cache inventories')
parser.add_argument('--network-delay',action='store_true',help='Use the existing verified 200-us node/client delay helper')
args=parser.parse_args()
if args.output: args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
CACHE.mkdir(exist_ok=True); (CACHE/'tmp').mkdir(exist_ok=True)

def save(name, value): ((args.output or CACHE)/name).write_text(json.dumps(value,indent=2)+'\n')
if args.output:
    scripts=args.output/'provision-scripts';scripts.mkdir()
    sources={}
    for source in (pathlib.Path(__file__).resolve(),SOURCE,SOURCE.parent/'alternator_disk_environment.py'):
        shutil.copy2(source,scripts/source.name)
        sources[str(source)]={'path':str((scripts/source.name).relative_to(args.output)),
                             'sha256':hashlib.sha256(source.read_bytes()).hexdigest()}
    save('provision-source.json',{'mode':args.mode,'network_delay':args.network_delay,
         'server_cpus':p.CPU_SETS,'client_cpus':p.CLIENT_CPUS,'client_gomaxprocs':1,'sources':sources})
if args.mode in ('start','resume'):
    os.sched_setaffinity(0,set(range(os.cpu_count())))
if args.mode == 'start':
    p.validate_topology()
    os.sched_setaffinity(0, {2,18})
    save('process-inventory.json', p.command('ps','-eo','pid,ppid,comm,args').stdout)
    all_nodes = json.loads(p.podman('ps','--format','json').stdout)
    save('container-inventory.json', all_nodes)
    save('qdisc-before.json', p.command('sudo','-n','tc','-s','qdisc','show').stdout)
    # Inventory full container configuration before stopping; never remove old containers.
    stop = [r['Names'][0] for r in all_nodes if 'scylla' in r['Image'] or r['Names'][0]=='codex-alternator-overload-prometheus']
    save('preserved-containers.json', [p.inspect(n) for n in stop])
    runners = [r for r in pathlib.Path('/proc').iterdir() if r.name.isdigit() and int(r.name)!=os.getpid()]
    for r in runners:
        try: command_parts=(r/'cmdline').read_bytes().split(b'\0')
        except (FileNotFoundError, PermissionError): continue
        if any(a.endswith(b'/run-alternator-overload.py') for a in command_parts):
            os.kill(int(r.name),15)
    for n in stop: p.podman('stop','--time','60',n,timeout=90)
    if p.command('pgrep','-x','scylla',check=False).returncode==0: raise RuntimeError('Scylla remains running')
    if any(p.exists(n) for n in p.NODES): raise RuntimeError('fresh names already exist')
    save('cluster.json', p.create_cluster('disk',CACHE/'data'))
    save('cluster-inspect.json',[p.inspect(n) for n in p.NODES])
    save('qdisc-after.json',p.command('sudo','-n','tc','-s','qdisc','show').stdout)
    print('READY',flush=True)
elif args.mode=='stop':
    existing=[n for n in p.NODES if p.exists(n)]
    if existing:p.podman('stop','--time','60',*existing,timeout=120)
    save('shutdown.json',[p.inspect(n)['State'] for n in p.NODES if p.exists(n)])
elif args.mode=='calibrated':
    # Replace only this experiment's initial, uncalibrated containers. Preserve data.
    source=CACHE/'data'/p.NODES[0]/'physical-io.yaml'
    if not source.exists(): raise RuntimeError('iotune has not completed')
    (CACHE/'physical-io.yaml').write_bytes(source.read_bytes())
    p.podman('stop','--time','20',*p.NODES,timeout=90)
    for n in p.NODES: p.podman('rm',n)
    (CACHE/'data').rename(CACHE/'uncalibrated-data')
    for n in p.NODES:
        d=CACHE/'data'/n;d.mkdir(parents=True);(d/'physical-io.yaml').write_bytes(source.read_bytes() if source.exists() else (CACHE/'physical-io.yaml').read_bytes())
    original=p.podman
    def with_physical_io(*args,**kwargs):
        if args[0]=='run' and p.IMAGE in args:
            pos=args.index(p.IMAGE)+1;args=(*args[:pos], '--io-properties-file','/var/lib/scylla/physical-io.yaml', *args[pos:])
        return original(*args,**kwargs)
    p.podman=with_physical_io
    os.sched_setaffinity(0,{2,18})
    save('cluster.json',p.create_cluster('disk',CACHE/'data'))
    save('cluster-inspect.json',[p.inspect(n) for n in p.NODES])
    print('CALIBRATED READY',flush=True)
elif args.mode=='tune':
    os.sched_setaffinity(0,{2,18})
    with (CACHE/'iotune.log').open('w') as log:
        result=subprocess.run(['sudo','-n','podman','run','--name','codex-gocql-iotune','--network','none','--cpuset-cpus','0-1','--memory','5g','-v',str(CACHE/'data'/p.NODES[0])+':/var/lib/scylla:rw','--entrypoint','/usr/bin/iotune',p.IMAGE,'--smp','2','--evaluation-directory','/var/lib/scylla','--properties-file','/var/lib/scylla/physical-io.yaml','--duration','120'],stdout=log,stderr=subprocess.STDOUT,timeout=600)
    result.check_returncode()
    subprocess.run([sys.executable,__file__,'calibrated'],check=True)
elif args.mode=='resume':
    p.validate_topology();os.sched_setaffinity(0,{2,18})
    p.podman('start',*p.NODES)
    save('cluster.json',p.create_cluster('disk',CACHE/'data'))
    save('cluster-inspect.json',[p.inspect(n) for n in p.NODES])
    print('RESUMED READY',flush=True)

if args.network_delay and args.mode in ('start','resume','calibrated'):
    source=SOURCE.parent/'alternator_disk_environment.py'
    spec=importlib.util.spec_from_file_location('gocql_delay',source)
    delay=importlib.util.module_from_spec(spec);spec.loader.exec_module(delay)
    nodes=json.loads(((args.output or CACHE)/'cluster.json').read_text())
    delay.configure_delay(p,nodes,(args.output or CACHE)/'network-delay')
    print('NETWORK DELAY READY: 200us one-way; client CPU 8 / one vCPU',flush=True)
