#!/usr/bin/env python3
"""Capture native protocol and per-shard system.clients evidence outside timing."""
import importlib.util, json, os, signal, subprocess, time
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('runner',HERE/'run.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
spec=importlib.util.spec_from_file_location('wire',HERE/'wire.py');w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)
os.sched_setaffinity(0,{2,18})
out=Path(__import__('sys').argv[1]).resolve();out.mkdir(parents=True,exist_ok=True)
nodes=json.loads((r.CACHE/'cluster.json').read_text());hosts=','.join(n['address'] for n in nodes)
network=json.loads(r.command('sudo','-n','podman','network','inspect','codex-gocql-compare').stdout)[0]
bridge=network['network_interface']
for d,stream in [('old',False),('v2',False),('v2',True)]:
    name=d+('-stream' if stream else '-buffered');pcap=out/(name+'.pcap')
    args=['sudo','-n','tcpdump','-i',bridge,'-s','0','-U','-w',str(pcap),'host '+network['subnets'][0]['gateway']+' and tcp and (port 9042 or port 19042)']
    with (out/(name+'-tcpdump.log')).open('w') as log:
        capture=subprocess.Popen(args,stdout=log,stderr=subprocess.STDOUT)
        time.sleep(.5)
        with (out/(name+'.log')).open('w') as f:
            client=subprocess.Popen(['taskset','-c','8','env','GOMAXPROCS=1',str(r.CACHE/d),'-hosts',hosts,'-mode','probe',f'-stream={str(stream).lower()}'],stdout=f,stderr=subprocess.STDOUT)
            try:
                time.sleep(2)
                snapshot={}
                for n in nodes:
                    q='SELECT address,port,protocol_version,ssl_enabled,shard_id,driver_name,client_options FROM system.clients;'
                    snapshot[n['name']]=r.command('sudo','-n','podman','exec',n['name'],'cqlsh','-e',q).stdout
                connections={}
                for n in nodes:
                    rows=[]
                    for line in snapshot[n['name']].splitlines():
                        columns=[c.strip() for c in line.split('|')]
                        if columns[0]==network['subnets'][0]['gateway']:
                            assert columns[2]=='4' and columns[3]=='False',columns
                            rows.append({'port':int(columns[1]),'shard':int(columns[4]),'driver':columns[5]})
                    assert len(rows) in (2,3),rows
                    assert {row['shard'] for row in rows}=={0,1},rows
                    connections[n['name']]=rows
                assert sum(map(len,connections.values()))==7,connections
                snapshot['validated_connections']=connections
                snapshot['sockets']=r.command('ss','-tnp').stdout
                snapshot['client_pid']=client.pid
                r.write(out/(name+'-connections.json'),snapshot)
                code=client.wait(timeout=30)
                if code:raise RuntimeError(f'probe {name} failed')
            finally:
                if client.poll() is None:client.terminate();client.wait(timeout=15)
                r.command('sudo','-n','kill','-INT',capture.pid,check=False)
                # sudo normally execs tcpdump; terminate descendants as fallback.
                try:capture.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    r.command('sudo','-n','pkill','-INT','-P',capture.pid,check=False);capture.wait(timeout=10)
    r.write(out/(name+'-wire.json'),w.analyze(pcap))
print('wire and connection probes complete')
