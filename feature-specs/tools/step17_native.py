# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Complete native correctness plus affected matched performance in one owner.

The standard runner owns locking, provisioning, startup, test children, bounded
cleanup and final node joins. Performance is a separate uninstrumented command
after each applicable complete package, followed immediately by its original
driver comparison and diagnostic memory command through the existing adapter.
No historical correctness or performance outcome supplies release credit.
"""
import os
from pathlib import Path
import step15_native

Original=step15_native.PerformanceRunner
class ReleaseRunner(Original):
    def command(self,name,command,env=None,check=True):
        status=super().command(name,command,env,check)
        selected=command[command.index('-run')+1] if '-run' in command else ''
        # Development's new capabilities are correctness-only; stable/Cassandra
        # and both raw-v3 streaming modes repeat affected receive measurements.
        applicable=(name.startswith('stable20-') or name.startswith('cassandra21-'))
        if status==0 and command[:2]==['go','test'] and applicable and selected and selected!='^TestStep08Cleanup$' and 'TestStep15' not in selected:
            perf=list(command);perf[perf.index('-run')+1]='^TestStep15NativePerformance$'
            output=Path('/extra/scylladb/gocql-api-test-cluster/step17/release/native-performance')/name
            output.mkdir(parents=True,exist_ok=True)
            perf_env=dict(env or os.environ,GOCQL_STEP15_OUTPUT=str(output),GOMAXPROCS='4')
            super().command(name+'-performance',perf,perf_env,True)
        return status

step15_native.native.Runner=ReleaseRunner
if __name__=='__main__':
    os.environ['GOCQL_STEP15_BASELINE']='/extra/scylladb/gocql-api-test-cluster/step15/baseline-matched'
    os.environ['GOMAXPROCS']='4'
    step15_native.native.main()
