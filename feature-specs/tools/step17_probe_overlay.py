# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Supplement the current concurrent probe regression with fallback/pin costs.

Only additive test/benchmark declarations and imports are overlaid. Production
bytes and the complete ordinary/race/native source snapshot stay unchanged.
"""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
OUT=Path('/extra/scylladb/gocql-api-test-cluster/step17/release/performance')
original=ROOT/'step16_latency_test.go'
addition=r'''
func step17ProbeSetup(t testing.TB, count int)(*routingPolicy,*MetadataSnapshot,RoutingInfo,time.Time){
 t.Helper();m:=v2TestMetadata();storage:=routing.SnapshotData(m);storage.InternalHosts=make([]HostSnapshot,count)
 for i:=range storage.InternalHosts{h:=v2TestHost(0,"a","r1");binary.BigEndian.PutUint64(h.ID[8:],uint64(i+1));storage.InternalHosts[i]=h}
 storage.InternalHostIndex=buildRoutingHostIndex(storage.InternalHosts);slow:=storage.InternalHosts[0].ID
 storage.InternalRanges["ks"][0].Replicas=[]Target{{HostID:slow}}
 p:=codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware:true,Latency:LatencyOptions{Enabled:true,MinimumSamples:1,RetryPeriod:time.Minute,UpdateInterval:time.Hour}})).(*routingPolicy)
 now:=time.Now();p.Observe(AttemptEvent{Target:Target{HostID:storage.InternalHosts[1].ID},Started:now.Add(-2*time.Minute),Latency:time.Millisecond});p.Observe(AttemptEvent{Target:Target{HostID:slow},Started:now.Add(-2*time.Minute),Latency:10*time.Millisecond})
 p.InternalDecisions.Store(&routingDecisions{InternalSlow:map[UUID]time.Time{slow:now.Add(-time.Minute)}})
 return p,m,RoutingInfo{Keyspace:"ks",Table:"table",Token:Option[int64]{Set:true},Kind:OrdinaryStatement},now
}
func TestStep17ProbeFallbackAndStrictPin(t *testing.T){
 p,m,info,now:=step17ProbeSetup(t,3);slow:=routing.SnapshotData(m).InternalHosts[0].ID
 plan:=p.Plan(info,m).(*lazyRoutingPlan);plan.InternalNow=now
 if target,ok:=plan.Next();!ok||target.HostID!=slow{t.Fatal("first probe",target,ok)}
 next:=p.Plan(info,m).(*lazyRoutingPlan);next.InternalNow=now
 if target,ok:=next.Next();!ok||target.HostID==slow{t.Fatal("healthy fallback while probe claimed",target,ok)}
 pinned:=info;pinned.HostID=Option[UUID]{Set:true,Value:slow};pinned.ShardID=Option[uint32]{Set:true,Value:1}
 target,ok:=p.Plan(pinned,m).Next();if !ok||target.HostID!=slow||!target.StrictShard||!target.ShardID.Set||target.ShardID.Value!=1{t.Fatal("strict pin changed by active latency claim",target,ok)}
 for i:=1;i<len(routing.SnapshotData(m).InternalHosts);i++{routing.SnapshotData(m).InternalHosts[i].Enabled=false}
 last:=p.Plan(info,m).(*lazyRoutingPlan);last.InternalNow=now
 if target,ok:=last.Next();!ok||target.HostID!=slow{t.Fatal("penalized last resort lost",target,ok)}
}
func BenchmarkStep17ProbeSelection(b *testing.B){
 for _,count:=range []int{3,30,300}{for _,kind:=range []string{"expired-probe","healthy-fallback","strict-pin","last-resort"}{b.Run(fmt.Sprintf("hosts%d/%s",count,kind),func(b *testing.B){
  p,m,info,now:=step17ProbeSetup(b,count);slow:=routing.SnapshotData(m).InternalHosts[0].ID
  if kind=="strict-pin"{info.HostID=Option[UUID]{Set:true,Value:slow};info.ShardID=Option[uint32]{Set:true,Value:1}}
  if kind=="last-resort"{for i:=1;i<count;i++{routing.SnapshotData(m).InternalHosts[i].Enabled=false}}
  first:=p.Plan(info,m).(*lazyRoutingPlan);first.InternalNow=now;first.Next()
  b.ReportAllocs();b.ResetTimer()
  for i:=0;i<b.N;i++{plan:=p.Plan(info,m).(*lazyRoutingPlan);plan.InternalNow=now;if kind=="expired-probe"{plan.InternalNow=now.Add(time.Duration(i+1)*2*time.Minute)};target,ok:=plan.Next();if !ok{b.Fatal("missing target")};if kind=="healthy-fallback"&&target.HostID==slow{b.Fatal("probe duplicated")};step06BenchmarkTarget=target}
 })}}
}
'''
body=original.read_text().replace('"sync"','"sync"\n"encoding/binary"\n"fmt"')+addition
replacement=OUT/'probe.overlay.go';replacement.write_text(body)
overlay=OUT/'probe-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
rows=[]
for name,args in [('probe-correctness',['-json','-race','-run','^Test(Step16LatencyProbeReservation|Step17ProbeFallbackAndStrictPin)$']),('probe-benchmark',['-run','^$','-bench','^BenchmarkStep17ProbeSelection$','-benchmem','-benchtime=100ms','-count=3'])]:
    command=['go','test','-overlay',str(overlay),'-tags','unit','-count=1']+args+['.']
    log=OUT/(name+'.log');start=time.monotonic()
    with log.open('wb') as output:code=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=output,stderr=subprocess.STDOUT).returncode
    row=dict(name=name,command=command,exit_code=code,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original=str(original),original_sha256=sha(original),overlay=str(overlay),overlay_sha256=sha(overlay),overlay_source=str(replacement),overlay_source_sha256=sha(replacement))
    rows.append(row);(OUT/'probe-checks.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps(row),flush=True)
    if code:raise SystemExit(code)
