# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Measure real v5 split-frame assembly/reassembly at independent span sizes."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/release-checks'));OUT.mkdir(exist_ok=True)
original=ROOT/'step15_canonical_test.go'
addition=r'''
func BenchmarkStep15V5SplitFrames(b *testing.B){
 for _,compressed:=range []bool{false,true}{for _,fragments:=range []int{1,16,1024,65536}{b.Run(fmt.Sprintf("send/compressed%t/spans%d",compressed,fragments),func(b *testing.B){
  payload:=bytes.Repeat([]byte{0x71},1<<20);parts:=[]wire.Span{wire.Borrow(binary.BigEndian.AppendUint32(nil,uint32(len(payload))))};for i:=0;i<fragments;i++{parts=append(parts,wire.Borrow(payload[len(payload)*i/fragments:len(payload)*(i+1)/fragments]))};values:=codecTestMust(wire.NewChain(parts...));c:=&driverConn{InternalVersion:5};if compressed{c.InternalCompressor=wirecompress.LZ4Compressor{};c.InternalSegCompressor=wirecompress.LZ4Compressor{}};c.InternalInitFramerCache();request:=&attemptRequest{Statement:"INSERT INTO ks.tbl(v)VALUES(?)",Values:values,ValueCount:1,Consistency:One};b.SetBytes(int64(len(payload)));b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{frame,e:=c.InternalBuildTypedRequest(request,0);if e!=nil{b.Fatal(e)};typedTransportBenchmarkSink=frame};b.StopTimer();spans:=0;typedTransportBenchmarkSink.Each(func(wire.Span)error{spans++;return nil});b.ReportMetric(float64(spans),"output-spans/op")
 })}
 for _,width:=range []int{512,8192,130000}{b.Run(fmt.Sprintf("receive/compressed%t/unit%d",compressed,width),func(b *testing.B){
  var compressor Compressor;if compressed{compressor=wirecompress.LZ4Compressor{}};conn,peer:=typedTestConn(b,5,compressor);conn.InternalStreams=streams.NewLimited(1);go conn.InternalServe(conn.InternalCtx)
  payload:=bytes.Repeat([]byte{0x71},1<<20);frame:=typedTestResponse(b,5,0,typedTestRowsBody(payload));var packets []byte;units:=0
  for at:=0;at<len(frame);at+=width{chain:=codecTestMust(wire.NewChain(wire.Borrow(frame[at:min(at+width,len(frame))])));packet:=codecTestMust(segment.EncodeSpans(chain,false,compressed));start:=len(packets);packets=append(packets,make([]byte,packet.Len())...);packet.CopyTo(packets[start:]);units++}
  done:=make(chan error,1);go func(){for i:=0;i<b.N;i++{id:=typedTestReadRequest(b,conn,peer);if id!=0{done<-fmt.Errorf("expected recycled stream0 got%d",id);return};if _,e:=peer.Write(packets);e!=nil{done<-e;return}};done<-nil}()
  b.SetBytes(int64(len(payload)));b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{page,e:=conn.InternalExecuteTyped(context.Background(),&attemptRequest{Statement:"SELECT value",Consistency:One});if e!=nil{b.Fatal(e)};if page.Rows.Len()!=int64(len(payload)+4){b.Fatal("rows changed")};page.Owner.Release()};b.StopTimer();if e:=<-done;e!=nil{b.Fatal(e)};b.ReportMetric(float64(units),"units/op")
 })}}
}
'''
body=original.read_text().replace('"fmt"','"fmt"\n"encoding/binary"\n"github.com/scylladb/gocql/v2/internal/segment"\n"github.com/scylladb/gocql/v2/internal/streams"\n"github.com/scylladb/gocql/v2/internal/wirecompress"')+addition
replacement=OUT/'segments.overlay.go';replacement.write_text(body);overlay=OUT/'segment-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n');command=['go','test','-overlay',str(overlay),'-tags','unit','-run','^$','-bench','^BenchmarkStep15V5SplitFrames$','-benchtime=100ms','-count=3','-benchmem','-timeout=5m','.'];log=OUT/'segment-throughput.log';start=time.monotonic()
with log.open('wb')as h:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT).returncode
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();record=dict(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original_sha256=sha(original),overlay_source=str(replacement),overlay_source_sha256=sha(replacement),overlay=str(overlay),overlay_sha256=sha(overlay));(OUT/'segment-throughput.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record),flush=True);raise SystemExit(status)
