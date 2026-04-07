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

		framer := newFramer(nil, byte(head.Version), compressionOpts{})
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

	framer := newFramer(nil, 3, compressionOpts{})

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

	framer := newFramer(nil, 3, compressionOpts{})

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

func TestParseResultMetadata_PerColumnSpec(t *testing.T) {
	t.Parallel()

	// Build a synthetic ROWS result metadata frame with FlagGlobalTableSpec unset
	// (per-column keyspace/table encoding). This tests the !globalSpec optimization
	// in parseResultMetadata() which reads keyspace/table from the first column
	// position and reuses them for all columns via skipString().
	fr := newFramer(nil, protoVersion4, compressionOpts{})
	fr.header = &frm.FrameHeader{Version: protoVersion4}

	// flags: no FlagGlobalTableSpec — per-column keyspace/table
	fr.writeInt(0)
	// colCount
	fr.writeInt(3)

	// Column 0: keyspace/table + name + type
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_a")
	fr.writeShort(uint16(TypeInt))

	// Column 1: same keyspace/table (will be skipped by optimization)
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_b")
	fr.writeShort(uint16(TypeVarchar))

	// Column 2: same keyspace/table
	fr.writeString("test_ks")
	fr.writeString("test_tbl")
	fr.writeString("col_c")
	fr.writeShort(uint16(TypeBoolean))

	meta := fr.parseResultMetadata()

	if meta.colCount != 3 {
		t.Fatalf("colCount = %d, want 3", meta.colCount)
	}
	if len(meta.columns) != 3 {
		t.Fatalf("len(columns) = %d, want 3", len(meta.columns))
	}

	// Verify all columns got the correct keyspace/table from the optimization
	for i, col := range meta.columns {
		if col.Keyspace != "test_ks" {
			t.Errorf("columns[%d].Keyspace = %q, want %q", i, col.Keyspace, "test_ks")
		}
		if col.Table != "test_tbl" {
			t.Errorf("columns[%d].Table = %q, want %q", i, col.Table, "test_tbl")
		}
	}

	// Verify column names
	expectedNames := []string{"col_a", "col_b", "col_c"}
	for i, col := range meta.columns {
		if col.Name != expectedNames[i] {
			t.Errorf("columns[%d].Name = %q, want %q", i, col.Name, expectedNames[i])
		}
	}

	// Verify column types
	expectedTypes := []Type{TypeInt, TypeVarchar, TypeBoolean}
	for i, col := range meta.columns {
		nt, ok := col.TypeInfo.(NativeType)
		if !ok {
			t.Fatalf("columns[%d].TypeInfo is %T, want NativeType", i, col.TypeInfo)
		}
		if nt.typ != expectedTypes[i] {
			t.Errorf("columns[%d].Type = %v, want %v", i, nt.typ, expectedTypes[i])
		}
	}

	// Verify the entire buffer was consumed (no misalignment from skipString)
	if len(fr.buf) != 0 {
		t.Errorf("buffer has %d unconsumed bytes, want 0 (possible skipString misalignment)", len(fr.buf))
	}
}

func TestParseEventFrame_ClientRoutesChanged(t *testing.T) {
	t.Parallel()

	fr := newFramer(nil, protoVersion4, compressionOpts{})
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
