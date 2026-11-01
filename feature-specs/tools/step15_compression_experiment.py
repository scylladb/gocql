# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Compare exact raw-block semantics and test adaptive scan via a Go overlay."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path('/extra/scylladb/gocql-api-test-cluster/step15/compression-experiment');OUT.mkdir(exist_ok=True)
production=ROOT/'internal/wirecompress/compress.go';tests=ROOT/'internal/wirecompress/compress_test.go'
body=tests.read_text().replace('"testing"','"testing"\n"github.com/klauspost/compress/s2"')
body+=r'''
func BenchmarkStep15ComparableCompression(b *testing.B){
 for _,n:=range []int{1<<20}{for _,kind:=range []string{"repetitive","random","mixed"}{for _,spans:=range []int{1,16,4096}{for _,algorithm:=range []string{"lz4","snappy"}{for _,original:=range []bool{true,false}{b.Run(fmt.Sprintf("%s/n%d/%s/spans%d/original%t",algorithm,n,kind,spans,original),func(b *testing.B){
  data:=bytes.Repeat([]byte{'x'},n);if kind=="random"{rand.New(rand.NewSource(17)).Read(data)};if kind=="mixed"{rand.New(rand.NewSource(17)).Read(data[n/2:])};chain:=splitChain(b,data,max(1,n/spans));dst:=make([]byte,0,n+n/6+64);var encoded []byte
  b.ReportAllocs();b.SetBytes(int64(n));b.ResetTimer();for i:=0;i<b.N;i++{var err error;if original{if algorithm=="lz4"{encoded,err=(LZ4Compressor{}).AppendCompressed(dst[:0],data)}else{encoded=s2.EncodeSnappy(dst[:0],data)}}else if algorithm=="lz4"{encoded,err=AppendLZ4(dst[:0],chain)}else{encoded,err=AppendSnappy(dst[:0],chain)};if err!=nil{b.Fatal(err)}};b.StopTimer();b.ReportMetric(float64(len(encoded))/float64(n),"compression-ratio")
 })}}}}}
}
'''
replacement=OUT/'compression-test.overlay.go';replacement.write_text(body)
source=production.read_text();needle='for position = begin; position <= f.lastStart; position++ {';assert needle in source
adaptive=source.replace(needle,'skip,step:=32,1\n for position = begin; position <= f.lastStart; position+=min(step,f.lastStart-position+1) {\n step=skip>>5;skip++',1)
patch=OUT/'compress-adaptive.overlay.go';patch.write_text(adaptive)
records=[]
for mode in ['current','adaptive']:
 overlays={str(tests):str(replacement)}
 if mode=='adaptive':overlays[str(production)]=str(patch)
 config=OUT/(mode+'.json');config.write_text(json.dumps({'Replace':overlays},indent=2)+'\n')
 if mode=='adaptive':
  correctness=['go','test','-overlay',str(config),'-tags','unit gocql_perf','-count=1','./internal/wirecompress']
  with (OUT/'adaptive-correctness.log').open('wb')as h:subprocess.run(correctness,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT,check=True)
 command=['go','test','-overlay',str(config),'-tags','unit','-run','^$','-bench','^BenchmarkStep15ComparableCompression$','-benchtime=100ms','-count=3','-benchmem','./internal/wirecompress'];log=OUT/(mode+'.log');start=time.monotonic()
 with log.open('wb')as h:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT).returncode
 records.append(dict(mode=mode,command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=hashlib.sha256(log.read_bytes()).hexdigest()));print(json.dumps(records[-1]),flush=True)
 if status:raise SystemExit(status)
(OUT/'experiment.json').write_text(json.dumps(records,indent=2)+'\n')
