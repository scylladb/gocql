# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Retain a bounded owner-cache candidate as an overlay before adoption."""
import json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path('/extra/scylladb/gocql-api-test-cluster/step15/receive-pool-experiment');OUT.mkdir(exist_ok=True)
cache=r'''
// ReceiveCache reuses only fully released raw v3/v4 frame allocations. One
// session owns it, with hard idle byte/object/individual limits independent of
// connection/shard counts. Outstanding page bytes are ordinary demand, not idle.
type ReceiveCache struct{mu sync.Mutex;entries [][]byte;bytes int;epoch uint64;closed bool}
const ReceiveCacheMaxBytes=16<<20
const ReceiveCacheMaxObjects=4
const ReceiveCacheMaxObject=9<<20
func(c *ReceiveCache)Acquire(n int)([]byte,uint64){
 if c==nil{return make([]byte,n),0};c.mu.Lock();epoch:=c.epoch
 if !c.closed{best:=-1;for i,b:=range c.entries{if cap(b)>=n&&(cap(b)<=4096||n>=cap(b)-n)&&(best<0||cap(b)<cap(c.entries[best])){best=i}};if best>=0{b:=c.entries[best];c.bytes-=cap(b);last:=len(c.entries)-1;c.entries[best]=c.entries[last];c.entries[last]=nil;c.entries=c.entries[:last];c.mu.Unlock();return b[:n],epoch}}
 c.mu.Unlock();return make([]byte,n),epoch
}
func(c *ReceiveCache)Release(b []byte,epoch uint64){if c==nil||cap(b)>ReceiveCacheMaxObject||cap(b)==0{return};c.mu.Lock();defer c.mu.Unlock();if c.closed||epoch!=c.epoch||len(c.entries)>=ReceiveCacheMaxObjects||cap(b)>ReceiveCacheMaxBytes-c.bytes{return};c.entries=append(c.entries,b[:cap(b)]);c.bytes+=cap(b)}
func(c *ReceiveCache)Clear(){if c==nil{return};c.mu.Lock();clear(c.entries);c.entries=nil;c.bytes=0;c.epoch++;c.mu.Unlock()}
func(c *ReceiveCache)Close(){if c==nil{return};c.mu.Lock();clear(c.entries);c.entries=nil;c.bytes=0;c.epoch++;c.closed=true;c.mu.Unlock()}
func(c *ReceiveCache)Storage()(bytes,objects int){if c==nil{return};c.mu.Lock();defer c.mu.Unlock();return c.bytes,len(c.entries)}
'''
replacements={}
def write(name,body):
 p=OUT/name.replace('/','__');p.write_text(body);replacements[str(ROOT/name)]=str(p)
p='internal/wire/builder.go';s=(ROOT/p).read_text().replace('import "encoding/binary"','import ("encoding/binary";"sync")');write(p,s+cache)
p='internal/transport/conn.go';s=(ROOT/p).read_text().replace('type ConnConfig struct {','type ConnConfig struct {\n InternalReceiveCache *wire.ReceiveCache');write(p,s)
p='internal/cluster/session.go';s=(ROOT/p).read_text();s=s.replace('if runtimeConfig != nil {\n\t\tconnCfg.TypedConfig','if runtimeConfig != nil {\n connCfg.InternalReceiveCache=new(wire.ReceiveCache)\n\t\tconnCfg.TypedConfig',1);s=s.replace('InternalSessionCloseV2Peripheral(s)','InternalSessionCloseV2Peripheral(s)\n if s.privateStorage.InternalConnCfg!=nil{s.privateStorage.InternalConnCfg.InternalReceiveCache.Close()}',1)
if '"github.com/scylladb/gocql/v2/internal/wire"' not in s:s=s.replace('import (','import (\n"github.com/scylladb/gocql/v2/internal/wire"',1)
write(p,s)
p='internal/transport/v2_connection_state.go';s=(ROOT/p).read_text().replace('c.InternalExclusiveFlag.Store(true)','if c.InternalCfg!=nil{c.InternalCfg.InternalReceiveCache.Clear()}\n c.InternalExclusiveFlag.Store(true)',1);write(p,s)
p='internal/transport/v2_transport_receive.go';s=(ROOT/p).read_text();s=s.replace('data := make([]byte, head.Length)','var cache *wire.ReceiveCache\n if c.InternalCfg!=nil{cache=c.InternalCfg.InternalReceiveCache}\n data,epoch:=cache.Acquire(head.Length)\n cacheInput:=data\n returned:=false\n defer func(){if !returned{cache.Release(cacheInput,epoch)}}()');s=s.replace('owner = wire.NewLease(body, nil)','if c.InternalVersion<5 && head.Flags&frm.FlagCompress==0 {returned=true;owner=wire.NewLease(body,func(){cache.Release(data,epoch)})}else{owner=wire.NewLease(body,nil)}',1);write(p,s)
(OUT/'overlay.json').write_text(json.dumps({'Replace':replacements},indent=2)+'\n');print(OUT/'overlay.json')
# Candidate-specific ownership tests enter only through the retained overlay.
unit=r'''
func TestStep15ReceiveCacheOwnership(t *testing.T){
 cache:=new(ReceiveCache);data,epoch:=cache.Acquire(4096);data[0]=71;chain,_:=NewChain(Borrow(data));lease:=NewLease(chain,func(){cache.Release(data,epoch)});if !lease.Retain(){t.Fatal("retain")};lease.Release();if n,_:=cache.Storage();n!=0{t.Fatal("returned while retained")};other,_:=cache.Acquire(4096);if &other[0]==&data[0]{t.Fatal("live page reused")};lease.Release();if n,_:=cache.Storage();n!=4096{t.Fatal(n)};reused,nextEpoch:=cache.Acquire(4096);if &reused[0]!=&data[0]||reused[0]!=71{t.Fatal("not actual backing reuse")};fresh:=NewLease(CodecTestChain(t,reused),func(){cache.Release(reused,nextEpoch)});if fresh==lease||lease.Retain(){t.Fatal("lease ABA")};fresh.Release();cache.Clear();if n,c:=cache.Storage();n!=0||c!=0{t.Fatal("clear",n,c)}
 stale,staleEpoch:=cache.Acquire(8192);cache.Clear();cache.Release(stale,staleEpoch);if n,_:=cache.Storage();n!=0{t.Fatal("stale generation returned")}
 for i:=0;i<8;i++{cache.Release(make([]byte,1<<20),cache.epoch)};if n,c:=cache.Storage();n>ReceiveCacheMaxBytes||c>ReceiveCacheMaxObjects{t.Fatal("object/byte bound",n,c)}
 cache.Clear();cache.Release(make([]byte,ReceiveCacheMaxObject+1),cache.epoch);if n,_:=cache.Storage();n!=0{t.Fatal("oversized cached")}
 for i:=0;i<3;i++{cache.Release(make([]byte,8<<20),cache.epoch)};if n,c:=cache.Storage();n!=ReceiveCacheMaxBytes||c!=2{t.Fatal("total budget",n,c)}
 active,e:=cache.Acquire(8<<20);cache.Close();cache.Release(active,e);if n,c:=cache.Storage();n!=0||c!=0{t.Fatal("shutdown retained",n,c)}
}
func CodecTestChain(t *testing.T,b []byte)Chain{t.Helper();c,e:=NewChain(Borrow(b));if e!=nil{t.Fatal(e)};return c}
'''
p='internal/wire/builder_test.go';write(p,(ROOT/p).read_text()+unit)
(OUT/'overlay.json').write_text(json.dumps({'Replace':replacements},indent=2)+'\n')

# The same complete-output workload adds a separate mixed diagnostic. The
# normal measured interval remains first and unmodified in the overlay.
p='step15_performance_integration_test.go';s=(ROOT/p).read_text()
# Make a per-call mode explicit for this candidate-only mixed workload.
s=s.replace('run := func(worker int, sample func()) (int64, int64, error) {','run := func(worker int, sample func()) (int64, int64, error) {\n mode:=api;if worker>=100{worker-=100;mode="stream"}else if worker<0{worker=-worker-1;mode="row"}')
start=s.index('run := func(worker int');end=s.index('\n\t\t\t\tfor worker := range outputs',start)
part=s[start:end].replace('if api == "stream"','if mode == "stream"').replace('if api == "row"','if mode == "row"');s=s[:start]+part+s[end:]
needle='results = append(results, r)'
extra=r'''if shape.Name=="cell8M"&&api=="row"&&config.Compression==NoCompression {
 start:=time.Now();beforeMixed:=step15Mem();for i:=0;i<16;i++{if _,_,e:=run(-1,nil);e!=nil{t.Fatal(e)};if _,_,e:=run(100,nil);e!=nil{t.Fatal(e)}};afterMixed:=step15Mem()
 t.Logf("mixed_alternating_operations=32 bytes=%d ns=%d allocated_bytes=%d allocations=%d",32*shape.Cell,time.Since(start).Nanoseconds(),afterMixed.TotalAlloc-beforeMixed.TotalAlloc,afterMixed.Mallocs-beforeMixed.Mallocs)
}
if shape.Name=="concurrent"&&api=="row"&&config.Compression==NoCompression {
 var mixed sync.WaitGroup;start:=time.Now();beforeMixed:=step15Mem()
 for worker:=range outputs{mixed.Add(1);go func(worker int){defer mixed.Done();for i:=0;i<16;i++{target:=-worker-1;if (worker+i)%2==0{target=worker+100};if _,_,e:=run(target,nil);e!=nil{t.Error(e);return}}}(worker)};mixed.Wait();afterMixed:=step15Mem()
 t.Logf("mixed_concurrent_workers=4 operations=64 ns=%d allocated_bytes=%d allocations=%d",time.Since(start).Nanoseconds(),afterMixed.TotalAlloc-beforeMixed.TotalAlloc,afterMixed.Mallocs-beforeMixed.Mallocs)
}
'''
assert needle in s;s=s.replace(needle,extra+needle)
s=s.replace('"unsafe"','"unsafe"\n implCluster "github.com/scylladb/gocql/v2/internal/cluster"')
s=s.replace('\n\tSampling ', '\n IdleReceiveCacheBytes, IdleReceiveCacheObjects int\n\tSampling ',1)
needle='r.IdleHeapBytes = step15Mem().HeapAlloc'
assert needle in s;s=s.replace(needle,needle+'\n r.IdleReceiveCacheBytes,r.IdleReceiveCacheObjects=implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache.Storage()')
write(p,s)
(OUT/'overlay.json').write_text(json.dumps({'Replace':replacements},indent=2)+'\n')

# Existing deterministic read/error/cancel tests exercise the candidate too;
# only testkit connection construction gains a cache, with explicit drain.
p='internal/testkit/transport/peer.go';body=(ROOT/p).read_text()
needle='c.InternalInitFramerCache()'
if needle in body:
 body=body.replace(needle,'if c.InternalCfg==nil{c.InternalCfg=new(runtime.ConnConfig)};c.InternalCfg.InternalReceiveCache=new(wire.ReceiveCache);t.Cleanup(c.InternalCfg.InternalReceiveCache.Close)\n'+needle,1)
 if '"github.com/scylladb/gocql/v2/internal/wire"' not in body:body=body.replace('import (','import (\n"github.com/scylladb/gocql/v2/internal/wire"',1)
 write(p,body)
(OUT/'overlay.json').write_text(json.dumps({'Replace':replacements},indent=2)+'\n')
