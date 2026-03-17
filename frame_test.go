//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"os"
	"runtime"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

func TestFuzzBugs(t *testing.T) {
	t.Parallel()

	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		t.Logf("test %d input: %q", i, test)

		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.Version))
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 3)

	framer.writeHeader(0, frm.OpStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	t.Parallel()

	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{0x03, 0x00, 0x00, 0x00, byte(frm.OpReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 3)

	head := frm.FrameHeader{
		Version: protoVersion3,
		Op:      frm.OpReady,
		Length:  r.Len() - 9,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, 9))
	if err != nil {
		t.Fatal(err)
	}
	if head.Op != frm.OpReady {
		t.Fatalf("expected to get header %v got %v", frm.OpReady, head.Op)
	}
}

func TestParseEventFrame_ClientRoutesChanged(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4)
	fr.header = &frm.FrameHeader{Version: protoVersion4}
	fr.writeString("CLIENT_ROUTES_CHANGE")
	fr.writeString("UPDATED")
	fr.writeStringList([]string{"c1", ""})
	fr.writeStringList([]string{})

	frame := fr.parseEventFrame()
	evt, ok := frame.(*frm.ClientRoutesChanged)
	if !ok {
		t.Fatalf("expected ClientRoutesChanged frame, got %T", frame)
	}
	if evt.ChangeType != "UPDATED" {
		t.Fatalf("ChangeType = %v, want UPDATED", evt.ChangeType)
	}
	if len(evt.ConnectionIDs) != 2 || evt.ConnectionIDs[1] != "" {
		t.Fatalf("ConnectionIDs = %v, want [c1 \"\"]", evt.ConnectionIDs)
	}
	if len(evt.HostIDs) != 0 {
		t.Fatalf("HostIDs = %v, want empty", evt.HostIDs)
	}
}

// --- queryValues pool tests ---

func TestQueryValuesBucket(t *testing.T) {
	tests := []struct {
		n      int
		bucket int
	}{
		{0, 0}, {1, 0}, {8, 0},
		{9, 1}, {16, 1},
		{17, 2}, {32, 2},
		{33, 3}, {64, 3},
		{65, 4}, {128, 4},
		{129, -1}, {1000, -1},
	}
	for _, tt := range tests {
		got := queryValuesBucket(tt.n)
		if got != tt.bucket {
			t.Errorf("queryValuesBucket(%d) = %d, want %d", tt.n, got, tt.bucket)
		}
	}
}

func TestGetQueryValuesLength(t *testing.T) {
	for _, n := range []int{0, 1, 5, 8, 10, 16, 30, 64, 128, 200} {
		s := getQueryValues(n)
		if len(s) != n {
			t.Errorf("getQueryValues(%d): len = %d, want %d", n, len(s), n)
		}
		// For pooled sizes, capacity should be the bucket size.
		bucket := queryValuesBucket(n)
		if bucket >= 0 {
			wantCap := 8 << bucket
			if cap(s) != wantCap {
				t.Errorf("getQueryValues(%d): cap = %d, want %d", n, cap(s), wantCap)
			}
		}
	}
}

func TestPutGetQueryValuesClearsReferences(t *testing.T) {
	s := getQueryValues(4)
	s[0].name = "col1"
	s[0].value = []byte("data")
	s[1].name = "col2"
	s[1].value = []byte("more")
	s[2].isUnset = true

	putQueryValues(s)

	// Assert directly on the original slice: putQueryValues clears the
	// backing array in place, so s still reflects the zeroed state.
	// This is deterministic and does not depend on sync.Pool returning
	// the same object on the next Get.
	for i := 0; i < 4; i++ {
		if s[i].name != "" {
			t.Errorf("element %d: name = %q, want empty after put", i, s[i].name)
		}
		if s[i].value != nil {
			t.Errorf("element %d: value = %v, want nil after put", i, s[i].value)
		}
		if s[i].isUnset {
			t.Errorf("element %d: isUnset = true, want false after put", i)
		}
	}
}

func TestPutQueryValuesNilSafe(t *testing.T) {
	// Must not panic.
	putQueryValues(nil)
}

func TestPutBatchQueryValues(t *testing.T) {
	stmts := make([]batchStatment, 3)
	stmts[0].values = getQueryValues(5)
	stmts[0].values[0].name = "a"
	// stmts[1].values is nil (simulates entry without args)
	stmts[2].values = getQueryValues(10)
	stmts[2].values[0].value = []byte("x")

	putBatchQueryValues(stmts)

	for i, s := range stmts {
		if s.values != nil {
			t.Errorf("stmts[%d].values should be nil after putBatchQueryValues", i)
		}
	}
}

func TestGetQueryValuesOversize(t *testing.T) {
	// Slices larger than 128 should not be pooled.
	s := getQueryValues(200)
	if len(s) != 200 {
		t.Errorf("getQueryValues(200): len = %d, want 200", len(s))
	}
	// Should not panic when returning oversize.
	putQueryValues(s)
}

// --- benchmarks ---

// queryValuesSink forces escape analysis to heap-allocate make() in baseline
// benchmarks. Assigned once after the loop to avoid per-iteration cache-line
// effects that would unfairly penalize the baseline.
var queryValuesSink []queryValues

func BenchmarkGetPutQueryValues_8_Seq(b *testing.B) {
	for b.Loop() {
		s := getQueryValues(8)
		s[0].name = "col"
		s[0].value = []byte("val")
		putQueryValues(s)
	}
}

func BenchmarkGetPutQueryValues_8_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := getQueryValues(8)
			s[0].name = "col"
			s[0].value = []byte("val")
			putQueryValues(s)
		}
	})
}

func BenchmarkMakeQueryValues_8_Seq(b *testing.B) {
	var s []queryValues
	for b.Loop() {
		s = make([]queryValues, 8)
		s[0].name = "col"
		s[0].value = []byte("val")
	}
	queryValuesSink = s
}

func BenchmarkMakeQueryValues_8_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		var s []queryValues
		for pb.Next() {
			s = make([]queryValues, 8)
			s[0].name = "col"
			s[0].value = []byte("val")
		}
		runtime.KeepAlive(s)
	})
}

func BenchmarkPutBatchQueryValues_10x8(b *testing.B) {
	for b.Loop() {
		stmts := make([]batchStatment, 10)
		for i := range stmts {
			stmts[i].values = getQueryValues(8)
			stmts[i].values[0].name = "col"
		}
		putBatchQueryValues(stmts)
	}
}

func BenchmarkPutBatchQueryValues_10x8_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stmts := make([]batchStatment, 10)
			for i := range stmts {
				stmts[i].values = getQueryValues(8)
				stmts[i].values[0].name = "col"
			}
			putBatchQueryValues(stmts)
		}
	})
}
