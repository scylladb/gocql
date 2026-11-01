# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Measure admission-policy-only overlay in the inherited owned native runner."""
import hashlib,json
from pathlib import Path
import step15_native
Original=step15_native.PerformanceRunner
OVERLAY='/extra/scylladb/gocql-api-test-cluster/step15/cache-admission-experiment/overlay.json'
class AdmissionExperiment(Original):
 def command(self,name,command,env=None,check=True):
  if command[:2]==['go','test']:
   command=command[:2]+['-overlay',OVERLAY]+command[2:]
   self.receipt['candidate_overlay']={'path':OVERLAY,'sha256':hashlib.sha256(Path(OVERLAY).read_bytes()).hexdigest(),'sources':{v:hashlib.sha256(Path(v).read_bytes()).hexdigest()for v in json.loads(Path(OVERLAY).read_text())['Replace'].values()}}
  return super().command(name,command,env,check)
step15_native.native.Runner=AdmissionExperiment
if __name__=='__main__':
 import os,subprocess,sys
 out=Path(OVERLAY).parent
 # Correctness runs before fixture startup and before any timing interval.
 cmd=['go','test','-overlay',OVERLAY,'-tags','unit','-race','-count=1','./internal/wire','./internal/wirecompress','./internal/transport','.']
 with (out/'all-affected-race.log').open('wb')as log:subprocess.run(cmd,env=dict(os.environ,GOMAXPROCS='4'),stdout=log,stderr=subprocess.STDOUT,check=True)
 config=Path(sys.argv[sys.argv.index('--config')+1]);c=json.loads(config.read_text());modes=c['lanes'][0]['modes']
 for name,args in [('v4-snappy',['-proto=4','-compressor=snappy']),('v4-snappy-tls',['-proto=4','-compressor=snappy','-runssl']),('v4-lz4',['-proto=4','-compressor=lz4']),('v4-lz4-tls',['-proto=4','-compressor=lz4','-runssl'])]:
  folder=out/name;folder.mkdir(exist_ok=True)
  modes.append({'name':name,'args':args,'tests':'^TestStep15NativePerformance$','environment':{'GOCQL_STEP15_OUTPUT':str(folder)}})
 config.write_text(json.dumps(c,indent=2)+'\n')
 step15_native.native.main()
