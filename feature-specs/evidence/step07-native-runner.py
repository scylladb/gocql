import os,pathlib,subprocess,json,time,socket,traceback,shutil,sys
from ccmlib.scylla_cluster import ScyllaCluster
from ccmlib.cluster import Cluster
from ccmlib import common
import types
root=pathlib.Path('/extra/scylladb/gocql'); evidence=root/'feature-specs/evidence'
fixture=pathlib.Path('/extra/scylladb/gocql-api-test-cluster/step07-native-'+str(time.time_ns()));fixture.mkdir()
import fcntl,hashlib
lock=open('/extra/scylladb/gocql-api-test-cluster/step07-reservation.lock','w')
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
lock.write(str(os.getpid())+'\n');lock.flush()
print('RESERVED pid='+str(os.getpid())+' fixture='+str(fixture),flush=True)
print('RUNNER_SHA256 '+hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),flush=True)
results=[]
def run(name,cmd,env=None):
 log=evidence/('step07-final-'+name+'.log');start=time.time()
 with log.open('wb') as out:r=subprocess.run(cmd,cwd=root,stdout=out,stderr=subprocess.STDOUT,env=env)
 record=dict(name=name,command=cmd,exit_code=r.returncode,elapsed_seconds=time.time()-start,log=str(log.relative_to(root)))
 results.append(record);(evidence/'step07-final-native-results.json').write_text(json.dumps({'fixture_root':str(fixture),'results':results},indent=2)+'\n');print(json.dumps(record),flush=True)
 if r.returncode:raise RuntimeError(name+' failed')
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
  base=['go','test','-json','-race','-tags','integration gocql_debug','-count=1','-timeout=7m','-parallel=1','-run','^(TestStep07RunStreamLive|TestV2RuntimeDatabaseSmoke)$','.','-args','-gocql.timeout=60s','-rf=3','-clusterSize=3','-autowait=2000ms','-cluster='+','.join(addresses)]
  for suffix,extra in args:
   run(name+suffix,base+extra)

 finally:
  print('STOP',name,flush=True);cluster.stop(gently=True,wait=True);print('STOPPED',name,flush=True)
  for address in addresses:
   try:probe=socket.create_connection((address,9042),.2)
   except OSError:continue
   probe.close();raise RuntimeError('endpoint remained live after stop '+address)
try:
 addresses=['127.0.20.'+str(i) for i in range(1,4)]
 for a in addresses+['127.0.21.'+str(i) for i in range(1,4)]:
  try:s=socket.create_connection((a,9042),.2)
  except OSError:continue
  s.close();raise RuntimeError('lane already in use '+a)
 os.environ['SCYLLA_EXT_OPTS']=''
 scylla=ScyllaCluster(str(fixture),'scylla',install_dir='/extra/scylladb/gocql-api-test-cluster/ccm/scylla-repository/release/2026.3.4')
 scylla.scylla_reloc=True;scylla.scylla_version='release:2026.3.4'
 scylla.set_configuration_options({'authenticator':'PasswordAuthenticator','authorizer':'CassandraAuthorizer','auth_superuser_name':'cassandra','auth_superuser_salted_password':'$6$gocqlv2$IhWMM2QWL7/C5OMPO7nPWx2ySV64yv7Rc.TA.Eb9vMLbqSHqlWy96JB93Asx8Mau0FRUjZ9BE7.70nmMJxzex0','native_transport_port_ssl':9142,'client_encryption_options':{'enabled':True,'certificate':str(root/'testdata/pki/cassandra.crt'),'keyfile':str(root/'testdata/pki/cassandra.key'),'truststore':str(root/'testdata/pki/ca.crt'),'require_client_auth':True}})
 scylla.populate(3,ipprefix='127.0.20.')
 for node in scylla.nodelist():node.set_smp(1);node.set_configuration_options({'maintenance_socket':str(pathlib.Path(node.get_path())/'cql.m')})
 scylla_common=['-distribution=scylla','-gocql.cversion=2026.3.4','-runauth']
 lane('scylla',scylla,addresses,[
 ('-v3-raw',scylla_common+['-proto=3','-compressor=no-compression']),
 ('-v4-raw',scylla_common+['-proto=4','-compressor=no-compression']),
 ('-v4-tls',scylla_common+['-proto=4','-compressor=no-compression','-runssl']),
 ('-v4-snappy-capability',scylla_common+['-proto=4','-compressor=snappy'])])
 source=pathlib.Path('/home/dkropachev/.local/share/containers/storage/overlay/635220d55ce81dd8d814691b98f1c557aa51fdc060f1dc05a749455283bd29c4/diff/opt/cassandra')
 install=fixture/'cassandra-install';install.mkdir()
 for p in source.iterdir():
  if p.name not in ('conf','logs','data'):(install/p.name).symlink_to(p,target_is_directory=p.is_dir())
 (install/'logs').mkdir();(install/'data').mkdir()
 shutil.copytree('/extra/scylladb/gocql-api-test-cluster/cassandra-base',install/'conf')
 (install/'0.version.txt').write_text('5.0.9\n')
 os.environ['JAVA_HOME']='/usr/lib/jvm/java-17-openjdk-amd64';os.environ['MAX_HEAP_SIZE']='512M';os.environ['HEAP_NEWSIZE']='100M';os.environ['JVM_EXTRA_OPTS']='-XX:ActiveProcessorCount=2'
 cassandra=Cluster(str(fixture),'cassandra',install_dir=str(install));cassandra.set_configuration_options({'num_tokens':1,'user_defined_functions_enabled':True,'materialized_views_enabled':True,'client_encryption_options':{'enabled':True,'optional':True,'keystore':str(root/'testdata/pki/cassandra.p12'),'keystore_password':'cassandra','store_type':'PKCS12','require_client_auth':False}});cassandra.populate(3,ipprefix='127.0.21.')
 def node_env(self):
  env=common.make_cassandra_env(self.get_install_dir(),self.get_path(),hardcode_java_version=None);env['CASSANDRA_LOG_DIR']=str(pathlib.Path(self.get_path())/'logs');return env
 for node in cassandra.nodelist():node.get_env=types.MethodType(node_env,node)
 cassandra_common=['-distribution=cassandra','-proto=5','-gocql.cversion=5.0.9']
 lane('cassandra',cassandra,['127.0.21.'+str(i) for i in range(1,4)],[
 ('-v5-raw',cassandra_common+['-compressor=no-compression']),
 ('-v5-lz4',cassandra_common+['-compressor=lz4']),
 ('-v5-raw-tls',cassandra_common+['-compressor=no-compression','-runssl']),
 ('-v5-lz4-tls',cassandra_common+['-compressor=lz4','-runssl'])])
except BaseException:
 traceback.print_exc();sys.exit(1)
