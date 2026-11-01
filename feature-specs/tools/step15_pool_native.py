# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Overlay-only native receive cache comparison; inherited owned lifecycle."""
import hashlib,json
from pathlib import Path
import step15_native
Original=step15_native.PerformanceRunner
OVERLAY='/extra/scylladb/gocql-api-test-cluster/step15/receive-pool-experiment/overlay.json'
class PoolExperiment(Original):
 def command(self,name,command,env=None,check=True):
  if command[:2]==['go','test']:
   command=command[:2]+['-overlay',OVERLAY]+command[2:]
   self.receipt['candidate_overlay']={'path':OVERLAY,'sha256':hashlib.sha256(Path(OVERLAY).read_bytes()).hexdigest(),'sources':{v:hashlib.sha256(Path(v).read_bytes()).hexdigest() for v in json.loads(Path(OVERLAY).read_text())['Replace'].values()}}
  return super().command(name,command,env,check)
step15_native.native.Runner=PoolExperiment
if __name__=='__main__':step15_native.native.main()
