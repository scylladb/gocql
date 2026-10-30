import os,pathlib,subprocess,json,time,socket,traceback,shutil,sys
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.cluster import Cluster
from ccmlib import common
import types
root=pathlib.Path('/extra/scylladb/gocql'); evidence=root/'feature-specs/evidence'
fixture=pathlib.Path('/extra/scylladb/gocql-api-test-cluster/step06-native-'+str(time.time_ns()));fixture.mkdir()
results=[]
def run(name,cmd,env=None):
 log=evidence/('step06-handoff-'+name+'.log');start=time.time()
 with log.open('wb') as out:r=subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT,env=env)
 record=dict(name=name,command=cmd,exit_code=r.returncode,elapsed_seconds=time.time()-start,log=str(log.relative_to(root)))
 results.append(record);(evidence/'step06-handoff-native-results.json').write_text(json.dumps({'fixture_root':str(fixture),'results':results},indent=2)+'\n');print(json.dumps(record),flush=True)

def health(addresses):
 for address in addresses:
  with socket.create_connection((address,9042),5) as s:
   s.sendall(bytes.fromhex('040000000500000000'));header=s.recv(9)
   if len(header)!=9 or header[4]!=6:raise RuntimeError('OPTIONS health failed '+address+' '+header.hex())
   print('HEALTH',address,header.hex(),flush=True)
def lane(name,cluster,addresses,args):
 try:
  print('START',name,str(fixture),flush=True)
  cluster.start(wait_for_binary_proto=True,wait_other_notice=True)
  health(addresses)
  base=['go','test','-json','-race','-tags','integration gocql_debug','-count=1','-timeout=8m','-parallel=1','-run','^(TestStep06TabletRoutingLive|TestStep06LeaderRoutingLive|TestTablets)$','.','-args','-gocql.timeout=60s','-rf=3','-clusterSize=3','-autowait=2000ms','-cluster='+','.join(addresses)]
  run(name,base+args)
 finally:
  print('STOP',name,flush=True);cluster.stop(gently=True,wait=True);print('STOPPED',name,flush=True)
try:
 addresses=['127.0.20.'+str(i) for i in range(1,4)]
 experimental=['127.0.22.'+str(i) for i in range(1,4)]
 for a in addresses+experimental:
  try:sock=socket.create_connection((a,9042),.2)
  except OSError:continue
  sock.close();raise RuntimeError('lane already in use '+a)
 os.environ['SCYLLA_EXT_OPTS']=''
 for label,addrs,strong in [('stable',addresses,False),('experimental',experimental,True)]:
  scylla=ScyllaCluster(str(fixture),label,install_dir='/extra/scylladb/gocql-api-test-cluster/ccm/scylla-repository/release/2026.3.4')
  scylla.scylla_reloc=True;scylla.scylla_version='release:2026.3.4'
  configuration={'authenticator':'AllowAllAuthenticator','authorizer':'AllowAllAuthorizer'}
  if strong:configuration['experimental_features']=['strongly-consistent-tables']
  scylla.set_configuration_options(configuration)
  scylla.populate(3,ipprefix='127.0.22.' if strong else '127.0.20.')
  for node in scylla.nodelist():node.set_smp(2)
  lane(label,scylla,addrs,['-distribution=scylla','-proto=4','-compressor=no-compression','-gocql.cversion=2026.3.4'])
 if any(r['exit_code'] for r in results):raise RuntimeError('one or more complete native lanes failed')
except BaseException:
 traceback.print_exc();sys.exit(1)
