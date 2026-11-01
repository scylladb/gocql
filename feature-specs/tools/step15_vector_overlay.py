# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Complete the vector depth/fragment operation bound without changing source."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/final-checks'));OUT.mkdir(exist_ok=True)
original=ROOT/'internal/codec/step15_scaling_test.go'
addition=r'''
func TestStep15NestedVectorBounds(t *testing.T){
 for _,depth:=range []int{1,4,16}{for _,fragments:=range []int{1,256,65536}{t.Run(fmt.Sprintf("depth%d/S%d",depth,fragments),func(t *testing.T){
 typ:=CodecTestMust(cqltype.ScalarType(cqltype.TypeBlob));v:=BlobValue(make([]byte,1<<20));for d:=0;d<depth;d++{typ=CodecTestMust(cqltype.VectorDataType(typ,1));v=CodecTestMust(VectorValue(typ,[]Value{v}))};encoded:=CodecTestMust(EncodeValueBorrowed(typ,DynamicCodec,v));data:=encoded.Bytes().OwnedBytes();spans:=make([]wire.Span,fragments);for i:=range spans{spans[i]=wire.Borrow(data[len(data)*i/fragments:len(data)*(i+1)/fragments])};chain:=CodecTestMust(wire.NewChain(spans...));plan:=CodecTestMust(compileValueDecoder(DynamicCodec,typ));wire.ResetPerfCounters();ResetPerfCounters();value:=CodecTestMust(plan.Decode(NewValueReader(typ,cqltype.Present,chain,nil)));for d:=0;d<depth;d++{it:=CodecTestMust(value.Elements());if !it.Next(){t.Fatal(it.Err())};value=it.Value();if it.Next()||it.Err()!=nil{t.Fatal(it.Err())}}
 c:=wire.ReadPerfCounters();if c.SequentialVisits>int64(32*(depth+1))||c.IndexComparisons>int64(8*depth*(bits.Len(uint(fragments))+1))||c.CopyBytes!=0||ReadPerfTypeChecks()!=0{t.Fatal("vector nested cost",c)};t.Logf("vector depth=%d S=%d actual=%+v warm_CheckType=%d",depth,fragments,c,ReadPerfTypeChecks())
 })}}
}
'''
replacement=OUT/'vectors.overlay.go';replacement.write_text(original.read_text()+addition);overlay=OUT/'vector-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n');command=['go','test','-overlay',str(overlay),'-tags','unit gocql_perf','-v','-run','^TestStep15NestedVectorBounds$','-count=1','./internal/codec'];log=OUT/'nested-vectors.log';start=time.monotonic()
with log.open('wb')as h:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT).returncode
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();record=dict(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original_sha256=sha(original),overlay_source=str(replacement),overlay_source_sha256=sha(replacement),overlay=str(overlay),overlay_sha256=sha(overlay));(OUT/'nested-vectors.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record),flush=True);raise SystemExit(status)
