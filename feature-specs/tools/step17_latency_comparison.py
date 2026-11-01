# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Measure equivalent latency-policy work before/after the deadline fix.

Old production alone is overlaid for the explicitly historical comparison.
Current measurements use exact frozen production. All test additions are
identical, and timing, profiles and correctness are separate commands.
"""
import ast,hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release/latency-costs');OUT.mkdir(parents=True,exist_ok=True)
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
tree=ast.parse((ROOT/'feature-specs/tools/step17_probe_overlay.py').read_text())
addition=next(ast.literal_eval(n.value)for n in ast.walk(tree)if isinstance(n,ast.Assign)and any(isinstance(t,ast.Name)and t.id=='addition'for t in n.targets))
addition+=r'''
func BenchmarkStep17ProbeSelectionConcurrent(b *testing.B){
 for _,count:=range []int{3,30,300}{b.Run(fmt.Sprintf("hosts%d",count),func(b *testing.B){
  p,m,info,now:=step17ProbeSetup(b,count);var tick atomic.Int64;b.ReportAllocs();b.ResetTimer();b.RunParallel(func(pb *testing.PB){for pb.Next(){clock:=now.Add(time.Duration(tick.Add(1))*2*time.Minute);plan:=p.Plan(info,m).(*lazyRoutingPlan);plan.InternalNow=clock;if _,ok:=plan.Next();!ok{b.Fatal("missing target")}}})
 })}
}
func BenchmarkStep17ObserveWarm(b *testing.B){
 for _,count:=range []int{3,30,300}{b.Run(fmt.Sprintf("hosts%d",count),func(b *testing.B){
  p,_,_,_:=step17ProbeSetup(b,count);event:=AttemptEvent{Target:Target{HostID:UUID{15:2}},Started:time.Now(),Latency:3*time.Millisecond};p.Observe(event);b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{event.Started=time.Now();p.Observe(event)}
 })}
}
func BenchmarkStep17ProbeLargeClock(b *testing.B){
 for _,count:=range []int{3,30,300}{for _,period:=range []time.Duration{time.Minute,time.Duration(1<<63-1)}{b.Run(fmt.Sprintf("hosts%d/period%d",count,period),func(b *testing.B){
  p,m,info,clock:=step17ProbeSetup(b,count);p.InternalOptions.Latency.RetryPeriod=period;clock=time.Date(2262,4,11,23,47,16,800000000,time.UTC);slow:=routing.SnapshotData(m).InternalHosts[0].ID;b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{clock=clock.Add(period).Add(time.Nanosecond);plan:=p.Plan(info,m).(*lazyRoutingPlan);plan.InternalNow=clock;target,ok:=plan.Next();if !ok||target.HostID!=slow{b.Fatal("large clock probe lost",target,ok)}}
 })}}
}
'''
original=ROOT/'step16_latency_test.go';replacement=OUT/'common.overlay.go'
replacement.write_text(original.read_text().replace('"sync"','"sync"\n"encoding/binary"\n"fmt"')+addition)
common=OUT/'current-overlay.json';common.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n')
old=OUT/'before-policy.go';old.write_bytes(subprocess.check_output(['git','show','bc3d9fe132c4c1f5dc16050870cfd4991d322a73:internal/routing/v2_policy.go'],cwd=ROOT))
prior=OUT/'before-overlay.json';prior.write_text(json.dumps({'Replace':{str(original):str(replacement),str(ROOT/'internal/routing/v2_policy.go'):str(old)}},indent=2)+'\n')
rows=[]
def run(name,command,scope):
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb')as output:code=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=command,exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),scope=scope)
    rows.append(row);(OUT/'checks.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
pattern='^Benchmark(Step06FirstTarget|Step17ProbeSelection|Step17ProbeSelectionConcurrent|Step17ObserveWarm)$'
for label,overlay in [('before',prior),('current',common)]:
    run(label+'-equivalent',['go','test','-overlay',str(overlay),'-tags','unit','-run','^$','-bench',pattern,'-benchmem','-benchtime=100ms','-count=3','.'],'Identical ordinary-duration work; before changes only the old policy production file. This is not final-source correctness credit.')
run('large-clock',['go','test','-overlay',str(common),'-tags','unit','-run','^$','-bench','^BenchmarkStep17ProbeLargeClock$','-benchmem','-benchtime=100ms','-count=3','.'],'Exact current production; 3/30/300 hosts, clock beyond UnixNano, minute/MaxDuration. Includes explicit controlled-clock advance.')
run('publication-costs',['go','test','-run','^$','-bench','^BenchmarkStep17Latency','-benchmem','-benchtime=100ms','-count=3','./internal/routing'],'Current production, warm/boundary/competing publication allocations and probe claim plus explicit reset cost.')
run('contention-profile',['go','test','-overlay',str(common),'-tags','unit','-run','^$','-bench','^BenchmarkStep17ProbeSelectionConcurrent$','-benchtime=300ms','-count=1','-cpuprofile='+str(OUT/'probe-cpu.pprof'),'-mutexprofile='+str(OUT/'probe-mutex.pprof'),'-blockprofile='+str(OUT/'probe-block.pprof'),'.'],'Separate profile command, not normal timing evidence; shared slow-host mutex only.')
run('publication-allocation-profile',['go','test','-run','^$','-bench','^BenchmarkStep17LatencyPublicationClaims/competing-boundary$','-benchtime=250000x','-count=1','-memprofilerate=1','-memprofile='+str(OUT/'publication-allocs.pprof'),'./internal/routing'],'Separate fixed250000-operation diagnostic; profile identifies boundary candidates including competing losers. Setup/calibration allocations remain visible; standard integer allocs/op can round a nonzero fractional rate to0.')
report=dict(checks=rows,inputs={str(p):sha(p)for p in [original,replacement,common,old,prior,ROOT/'internal/routing/v2_policy.go',ROOT/'internal/routing/step17_deadlines_test.go']},before_revision='bc3d9fe132c4c1f5dc16050870cfd4991d322a73',current_production_sha256=sha(ROOT/'internal/routing/v2_policy.go'),measurement_scope='No new allocation for probe checks; existing per-host sample mutex also guards its time.Time deadline. Warm publication claim allocates zero; due/competing publication candidates allocate and are reported explicitly. Default disabled latency and nonexpired decision checks keep their existing branches.')
(OUT/'verification.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(commands=len(rows))),flush=True)
