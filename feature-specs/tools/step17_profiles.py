# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Retain post-measurement CPU/lock/allocation profile summaries and cost scope."""
import json,subprocess
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
def build():
    directory=OUT/'profile-summaries';directory.mkdir(exist_ok=True)
    profiles=[(p,'alloc_objects'if'alloc'in p.name else None)for folder in [OUT/'performance',OUT/'latency-costs']for p in sorted(folder.glob('*.pprof'))]
    rows=[]
    for path,sample in profiles:
        command=['go','tool','pprof','-top','-nodecount=15']+(['-sample_index='+sample]if sample else [])+[str(path)]
        log=directory/(path.parent.name+'-'+path.name+'.txt')
        with log.open('wb')as output:code=subprocess.run(command,cwd=ROOT,stdout=output,stderr=subprocess.STDOUT).returncode
        assert code==0,(path,log)
        rows.append(dict(command=command,exit_code=code,profile=str(path),profile_sha256=matrix.sha(path),log=str(log),sha256=matrix.sha(log)))
    # Exact before/current comparison uses identical test/benchmark declarations;
    # only old production is overlaid for the historical side.
    import re,statistics
    def timings(path):
        values={}
        for line in Path(path).read_text().splitlines():
            match=re.match(r'^(Benchmark\S+)-\d+\s+\d+\s+([0-9.]+) ns/op\s+.*?([0-9]+) B/op\s+([0-9]+) allocs/op',line)
            if match:values.setdefault(match[1],[]).append(dict(ns=float(match[2]),bytes=int(match[3]),allocs=int(match[4])))
        return values
    before=timings(OUT/'latency-costs/before-equivalent.log');current=timings(OUT/'latency-costs/current-equivalent.log');comparisons=[]
    for name,values in current.items():
        assert name in before
        old=statistics.median(x['ns']for x in before[name]);now=statistics.median(x['ns']for x in values)
        comparisons.append(dict(benchmark=name,before_median_ns=old,current_median_ns=now,ratio=now/old,before=before[name],current=values))
    result=dict(runtime_source_sha256=matrix.sha(OUT/'checks/source.json'),summaries=rows,latency_equivalent_comparisons=comparisons,interpretation=['Normal unprofiled timings and profiled diagnostics are separate. Profile percentages are attribution of those diagnostic workloads, not universal driver CPU shares.','Probe reservation uses an existing per-host sample mutex, not a policy-wide lock. Forced-expiry concurrent workload exposes its bounded additional contention cost; ordinary/latency-off/nonexpired decisions and 3/30/300 scaling remain separately measured.','Probe checks and warm publication claims allocate0. Each due publication candidate allocates24bytes, including CAS losers. Integer allocs/op can round a nonzero fractional rate to0; fixed-operation alloc_objects profile and total-claims metric expose candidate/loser work.','The deadline allocation profile includes benchmark setup and calibration. It is not a count of every logical request, and no per-completion zero-allocation claim is made for due-boundary workloads.'],gate='passed')
    matrix.write(E/'step17-profiles.json',result)
    print(json.dumps(dict(profile_summaries=len(rows),matched_latency_cases=len(comparisons))))
    return result
if __name__=='__main__':build()
