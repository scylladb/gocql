# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Run performance in the established locked START/TEST/STOP owner.

Only this explicit entrypoint removes race instrumentation from the performance
case. Existing correctness/cleanup invocations retain their original arguments.
The full native lifecycle and signal teardown remain native.py's implementation.
"""
import os
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'tests/fixtures'))
import native

Original=native.Runner
class PerformanceRunner(Original):
    def command(self,name,command,env=None,check=True):
        if command[:2]==['go','test'] and '^TestStep15NativePerformance$' in command:
            command=[arg for arg in command if arg!='-race']
            command[command.index('-tags')+1]='integration gocql_debug'
            env=dict(env or os.environ,GOMAXPROCS='4')
        status=super().command(name,command,env,check)
        if status==0 and command[:2]==['go','test'] and '^TestStep15NativePerformance$' in command:
            # Paired original driver executes immediately after v2 in this same
            # live, locked server owner. It creates/drops only its run namespace.
            baseline=Path(os.environ.get('GOCQL_STEP15_BASELINE','/extra/scylladb/gocql-api-test-cluster/step15/baseline'))
            original_root=self.root
            original_env=dict(env)
            original_env['GOCQL_STEP15_OUTPUT']=str(Path(env['GOCQL_STEP15_OUTPUT'])/'baseline')
            Path(original_env['GOCQL_STEP15_OUTPUT']).mkdir(parents=True,exist_ok=True)
            legacy=[arg for arg in command if arg!='-runcassandrafixtures']
            baseline_sources={str(p.relative_to(baseline)):hashlib.sha256(p.read_bytes()).hexdigest() for p in baseline.rglob('*.go') if '.git' not in p.parts}
            source_file=self.output.parent/(name+'-baseline-source.json')
            source_file.write_text(json.dumps(baseline_sources,sort_keys=True,indent=2)+'\n')
            self.receipt.setdefault('comparison_sources',{})[name]={'revision':'82b52574146b42f34ed3fe967d2d5e17cc53996b','root':str(baseline),'snapshot':str(source_file),'sha256':hashlib.sha256(source_file.read_bytes()).hexdigest()}
            self.save()
            self.root=baseline
            try: super().command(name+'-baseline',legacy,original_env,True)
            finally:self.root=original_root
            diagnostic=list(command)
            diagnostic[diagnostic.index('-run')+1]='^TestStep15ConcurrentLiveMemory$'
            super().command(name+'-concurrent-memory',diagnostic,env,True)
            # Affected native correctness, slow-consumer reservation and pool
            # recovery run after all measured intervals, with race enabled.
            correctness=list(command)
            correctness.insert(2,'-race')
            correctness[correctness.index('-run')+1]='^(TestStep07RunStreamLive|TestSliceMapMapScan(Types|CounterTypes|TupleTypes|VectorTypes|CollectionTypes)|TestStep15NativeCacheOwnership)$'
            super().command(name+'-affected-stream',correctness,env,True)
        return status
native.Runner=PerformanceRunner
if __name__=='__main__':native.main()
