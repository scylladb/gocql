# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Assert original string backing across bind/native assembly/borrowed decode."""
import hashlib,json,os,subprocess,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];OUT=Path(os.environ.get('GOCQL_STEP15_CHECKS','/extra/scylladb/gocql-api-test-cluster/step15/release-checks'));OUT.mkdir(exist_ok=True)
original=ROOT/'step15_canonical_test.go'
addition=r'''
func TestStep15OriginalTextBacking(t *testing.T){
 for _,size:=range []int{0,32,4096,500<<10}{for _,version:=range []byte{3,4,5}{t.Run(fmt.Sprintf("bytes%d/v%d",size,version),func(t *testing.T){
  input:=string(bytes.Repeat([]byte{0x71},size));original:=uintptr(unsafe.Pointer(unsafe.StringData(input)));typ:=codecTestMust(ScalarType(TypeText));cols:=NewColumns(ColumnSpec{Name:"value",Type:typ});statement:=Statement[string,Unit]{Kind:OrdinaryStatement,CQL:"INSERT INTO ks.tbl(value)VALUES(?)",Params:OneParam(StringEncoder),Rows:NoRows};bound:=codecTestMust(BindUnpreparedBorrowed(statement,cols,input));defer bound.Close();canonical:=implExecution.BoundData(bound).InternalRequest.InternalValues
  check:=func(chain wire.Chain,want int){retained:=0;chain.Each(func(span wire.Span)error{p:=span.Bytes();if len(p)>0{start:=uintptr(unsafe.Pointer(unsafe.SliceData(p)));if start>=original&&start+uintptr(len(p))<=original+uintptr(size){retained+=len(p)}};return nil});if retained!=want{t.Fatalf("original string retained=%d want=%d",retained,want)}}
  check(canonical.Cells[0].Body,size);conn:=&driverConn{InternalVersion:version};conn.InternalInitFramerCache();frame:=codecTestMust(conn.InternalBuildTypedRequest(&attemptRequest{Statement:statement.CQL,Values:canonical.Framed,ValueCount:1,Consistency:One},0));check(frame,size)
  // Reinterpret the exact borrowed response cell range through its public codec.
  text:=codecTestMust(TextCodec.Decode(implCodec.NewValueReader(typ,Present,canonical.Cells[0].Body,nil)));if text.Bytes().Len()!=int64(size){t.Fatal("decode length")};if size>0{writer:=&step15TextAddress{expected:original,size:size};if _,e:=text.Bytes().WriteTo(writer);e!=nil{t.Fatal(e)};if writer.bytes!=size{t.Fatal("decode backing")}}
  t.Logf("bind/uncompressed-assembly/decode original_string_payload_bytes=%d copied=0",size)
 })}}
}
type step15TextAddress struct{expected uintptr;size,bytes int}
func(w *step15TextAddress)Write(p []byte)(int,error){start:=uintptr(unsafe.Pointer(unsafe.SliceData(p)));if start<w.expected||start+uintptr(len(p))>w.expected+uintptr(w.size){return 0,fmt.Errorf("text copied")};w.bytes+=len(p);return len(p),nil}
'''
replacement=OUT/'text-identity.overlay.go';replacement.write_text(original.read_text().replace('"fmt"','"fmt"\n implCodec "github.com/scylladb/gocql/v2/internal/codec"')+addition);overlay=OUT/'text-identity-overlay.json';overlay.write_text(json.dumps({'Replace':{str(original):str(replacement)}},indent=2)+'\n');command=['go','test','-overlay',str(overlay),'-tags','unit','-v','-count=1','-run','^TestStep15OriginalTextBacking$','.'];log=OUT/'text-identity.log';start=time.monotonic()
with log.open('wb')as h:status=subprocess.run(command,cwd=ROOT,env=dict(os.environ,GOMAXPROCS='4'),stdout=h,stderr=subprocess.STDOUT).returncode
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();record=dict(command=command,exit_code=status,seconds=time.monotonic()-start,log=str(log),sha256=sha(log),original_sha256=sha(original),overlay_source=str(replacement),overlay_source_sha256=sha(replacement),overlay=str(overlay),overlay_sha256=sha(overlay));(OUT/'text-identity.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record),flush=True);raise SystemExit(status)
