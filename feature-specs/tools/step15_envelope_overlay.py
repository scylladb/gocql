# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Measure batch/attempt envelope descriptors over the real bound canonical body."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/checks'));OUT.mkdir(exist_ok=True)
original=ROOT/'step15_canonical_test.go'
addition=r'''
func BenchmarkStep15BatchAndAttempts(b *testing.B){
 for _,version:=range []byte{3,4,5}{for _,items:=range []int{0,1,16,1024}{for _,attempts:=range []int{1,4}{b.Run(fmt.Sprintf("v%d/items%d/attempts%d",version,items,attempts),func(b *testing.B){
  payload:=bytes.Repeat([]byte{0x71},32);typ:=codecTestMust(ScalarType(TypeBlob));cols:=NewColumns(ColumnSpec{Name:"v",Type:typ});calls:=0;statement:=Statement[[]byte,Unit]{Kind:OrdinaryStatement,CQL:"INSERT INTO ks.tbl(v)VALUES(?)",Params:OneParam[[]byte](countingBlobEncoder{calls:&calls}),Rows:NoRows};bound:=codecTestMust(BindUnpreparedBorrowed(statement,cols,payload));defer bound.Close();canonical:=implExecution.BoundData(bound).InternalRequest.InternalValues
  request:=&attemptRequest{Statement:statement.CQL,Values:canonical.Framed,ValueCount:1,Consistency:One}
  if items>0{batch:=&typedBatchRequest{Kind:LoggedBatch,Items:make([]typedBatchItem,items)};for i:=range batch.Items{batch.Items[i]=typedBatchItem{Statement:statement.CQL,Values:canonical.Framed,ValueCount:1}};request.Batch=batch}
  conn:=&driverConn{InternalVersion:version};conn.InternalInitFramerCache();b.ReportAllocs();b.SetBytes(int64(max(1,items)*32*attempts));b.ResetTimer()
  for i:=0;i<b.N;i++{for attempt:=0;attempt<attempts;attempt++{if _,e:=conn.InternalBuildTypedRequest(request,attempt);e!=nil{b.Fatal(e)}}}
  b.StopTimer();if calls!=1{b.Fatal("secondary encoder",calls)};out:=codecTestMust(conn.InternalBuildTypedRequest(request,0));spans:=0;out.Each(func(wire.Span)error{spans++;return nil});b.ReportMetric(float64(spans),"frame-spans");b.ReportMetric(float64(attempts),"envelopes/op");b.ReportMetric(1,"bind-encoder-calls")
 })}}}
}
'''
replacement=OUT/'step15_canonical.overlay.go';replacement.write_text(original.read_text()+addition);overlay=OUT/'envelope-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n')
command=['go','test','-overlay',str(overlay),'-tags','unit','-run','^$','-bench','^BenchmarkStep15BatchAndAttempts$','-benchtime=100ms','-count=3','-benchmem','.'];log=OUT/'batch-attempt-envelope.log';start=time.monotonic()
with log.open('wb') as handle:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=handle,stderr=subprocess.STDOUT).returncode
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
receipt=dict(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original_sha256=sha(original),overlay_source=str(replacement),overlay_source_sha256=sha(replacement),overlay=str(overlay),overlay_sha256=sha(overlay))
(OUT/'batch-envelope.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt),flush=True);raise SystemExit(status)
