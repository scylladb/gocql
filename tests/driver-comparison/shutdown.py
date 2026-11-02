#!/usr/bin/env python3
"""Stop and join only this benchmark; verify old clusters and closed endpoints."""
import importlib.util, json, socket, sys, time
from pathlib import Path
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('runner',HERE/'run.py');r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)
out=Path(sys.argv[1]);nodes=json.loads((r.CACHE/'cluster.json').read_text())
r.command('python3',HERE/'provision.py','stop',timeout=300)
result={'time':r.now(),'nodes':[],'preserved':[]}
for n in nodes:
 state=json.loads(r.command('sudo','-n','podman','inspect',n['name']).stdout)[0]['State']
 assert not state['Running'];ports={}
 for port in (9042,19042,9180,8080):
  sock=socket.socket();sock.settimeout(2)
  try:open_=sock.connect_ex((n['address'],port))==0
  finally:sock.close()
  ports[str(port)]={'open':open_}
  assert not open_,f'endpoint still open {n["name"]}:{port}'
 result['nodes'].append({'name':n['name'],'state':state,'endpoints':ports})
for old in json.loads((r.CACHE/'preserved-containers.json').read_text()):
 ins=json.loads(r.command('sudo','-n','podman','inspect',old['Id']).stdout)[0]
 assert ins['Mounts']==old['Mounts'];assert not ins['State']['Running']
 result['preserved'].append({'name':ins['Name'],'mounts':ins['Mounts'],'state':ins['State']})
assert r.command('pgrep','-x','scylla',check=False).returncode!=0
r.write(out/'shutdown.json',result)
print('all benchmark endpoints shut down; existing container data/config preserved')
