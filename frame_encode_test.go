//go:build unit
// +build unit

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package gocql

import (
	"bytes"
	"encoding/binary"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// readFrameValueSection parses the [short n][value...] section that
// writeQueryParams emits for positional (un-named) values, returning the raw
// per-value byte slices (nil for a CQL null, the sentinel unsetMarker for unset).
var unsetMarker = []byte("<unset>")

func decodeQueryParamsValues(t *testing.T, body []byte, flags uint32, named bool) [][]byte {
	t.Helper()
	// body starts after consistency(2) + flags. The caller passes the slice
	// positioned at the value count.
	pos := 0
	readShort := func() int {
		v := int(binary.BigEndian.Uint16(body[pos : pos+2]))
		pos += 2
		return v
	}
	readInt := func() int32 {
		v := int32(binary.BigEndian.Uint32(body[pos : pos+4]))
		pos += 4
		return v
	}
	n := readShort()
	out := make([][]byte, n)
	for i := 0; i < n; i++ {
		if named {
			nl := readShort()
			pos += nl // skip name
		}
		size := readInt()
		switch {
		case size == -1:
			out[i] = nil
		case size == -2:
			out[i] = unsetMarker
		default:
			out[i] = append([]byte(nil), body[pos:pos+int(size)]...)
			pos += int(size)
		}
	}
	return out
}

// buildQueryParamsFrame builds a query frame body via writeQueryParams and
// returns the framer buffer.
func buildQueryParamsFrame(proto byte, params queryParams) *framer {
	f := &framer{proto: proto}
	f.writeHeader(0, frm.OpQuery, 1)
	f.writeLongString("SELECT 1")
	f.writeQueryParams(&params)
	_ = f.finish()
	return f
}

// TestWriteQueryParamsValuesRoundTrip verifies that the pre-grown value section
// produces a correctly-framed value block: each positional value is length-
// prefixed and copied verbatim, nulls and unset are encoded as -1/-2, and the
// frame length header (set via the PutUint32 path) matches the body size.
func TestWriteQueryParamsValuesRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		values []queryValues
	}{
		{"empty", nil},
		{"single-small", []queryValues{{value: []byte{0x00, 0x00, 0x00, 0x2a}}}},
		{
			"mixed-sizes",
			[]queryValues{
				{value: []byte{1}},
				{value: bytes.Repeat([]byte{0xAB}, 300)}, // forces buffer growth
				{value: []byte{}},
				{value: bytes.Repeat([]byte{0xCD}, 5000)},
			},
		},
		{"null-value", []queryValues{{value: nil}}},
		{"unset-value", []queryValues{{isUnset: true}}},
		{
			"null-and-unset-and-data",
			[]queryValues{
				{value: nil},
				{isUnset: true},
				{value: []byte{9, 9, 9}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := queryParams{consistency: Quorum, values: tc.values}
			f := buildQueryParamsFrame(protoVersion4, params)

			// Verify the length header equals body length.
			gotLen := binary.BigEndian.Uint32(f.buf[5:9])
			if int(gotLen) != len(f.buf)-headSize {
				t.Fatalf("length header %d != body len %d", gotLen, len(f.buf)-headSize)
			}

			// Locate the value section: header(9) + longstring(4+8) +
			// consistency(2) + flags(1 for protoV4).
			off := headSize + 4 + len("SELECT 1") + 2 + 1
			if len(tc.values) == 0 {
				return
			}
			got := decodeQueryParamsValues(t, f.buf[off:], 0, false)

			if len(got) != len(tc.values) {
				t.Fatalf("got %d values, want %d", len(got), len(tc.values))
			}
			for i, qv := range tc.values {
				switch {
				case qv.isUnset:
					if !bytes.Equal(got[i], unsetMarker) {
						t.Fatalf("value %d: expected unset, got %v", i, got[i])
					}
				case qv.value == nil:
					if got[i] != nil {
						t.Fatalf("value %d: expected null, got %v", i, got[i])
					}
				default:
					if !bytes.Equal(got[i], qv.value) {
						t.Fatalf("value %d: got %d bytes, want %d bytes", i, len(got[i]), len(qv.value))
					}
				}
			}
		})
	}
}

// TestWriteQueryParamsNamedValuesRoundTrip verifies named values (proto v5)
// still frame correctly with the pre-grow path (name length prefix + name +
// value length prefix + value).
func TestWriteQueryParamsNamedValuesRoundTrip(t *testing.T) {
	t.Parallel()

	values := []queryValues{
		{name: "a", value: []byte{1, 2, 3}},
		{name: "bb", value: bytes.Repeat([]byte{0xEE}, 400)},
	}
	params := queryParams{consistency: Quorum, values: values}
	f := &framer{proto: protoVersion5}
	f.writeHeader(0, frm.OpQuery, 1)
	f.writeLongString("SELECT 1")
	f.writeQueryParams(&params)
	_ = f.finish()

	gotLen := binary.BigEndian.Uint32(f.buf[5:9])
	if int(gotLen) != len(f.buf)-headSize {
		t.Fatalf("length header %d != body len %d", gotLen, len(f.buf)-headSize)
	}

	// header(9) + longstring(4+8) + consistency(2) + flags(4 for protoV5).
	off := headSize + 4 + len("SELECT 1") + 2 + 4
	got := decodeQueryParamsValues(t, f.buf[off:], 0, true)
	if len(got) != len(values) {
		t.Fatalf("got %d values, want %d", len(got), len(values))
	}
	for i, qv := range values {
		if !bytes.Equal(got[i], qv.value) {
			t.Fatalf("value %d: got %d bytes want %d", i, len(got[i]), len(qv.value))
		}
	}
}

// BenchmarkWriteQueryParamsGrow measures allocations when assembling a query
// frame whose value section exceeds the starting buffer capacity. The
// deterministic metric is allocs/op: pre-growing the buffer once replaces the
// append-driven doubling (multiple reallocations + copies) with a single grow.
func BenchmarkWriteQueryParamsGrow(b *testing.B) {
	// 8 values totalling ~8 KB — well beyond defaultBufSize, so a non-pre-grown
	// buffer reallocates several times via append doubling.
	values := make([]queryValues, 8)
	for i := range values {
		values[i] = queryValues{value: bytes.Repeat([]byte{byte(i)}, 1024)}
	}
	params := queryParams{consistency: Quorum, values: values}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Fresh framer each iteration => cold buffer, so growth happens during
		// assembly (this isolates the realloc behaviour rather than pool reuse).
		f := &framer{proto: protoVersion4, buf: make([]byte, 0, defaultBufSize)}
		f.writeHeader(0, frm.OpQuery, 1)
		f.writeLongString("INSERT INTO t (a,b,c,d,e,f,g,h) VALUES (?,?,?,?,?,?,?,?)")
		f.writeQueryParams(&params)
		if err := f.finish(); err != nil {
			b.Fatal(err)
		}
	}
}

// TestWriteBatchFrameRoundTrip verifies the pre-grown batch frame correctly
// serializes multiple statements and their values, and that the frame length
// header matches the body.
func TestWriteBatchFrameRoundTrip(t *testing.T) {
	t.Parallel()

	w := &writeBatchFrame{
		typ:         LoggedBatch,
		consistency: Quorum,
		statements: []batchStatment{
			{
				statement: "INSERT INTO t (a,b) VALUES (?,?)",
				values: []queryValues{
					{value: []byte{0, 0, 0, 1}},
					{value: bytes.Repeat([]byte{0xAA}, 600)}, // forces growth
				},
			},
			{
				preparedID: []byte{0xDE, 0xAD, 0xBE, 0xEF},
				values: []queryValues{
					{value: bytes.Repeat([]byte{0xBB}, 4000)},
					{value: nil},
					{isUnset: true},
				},
			},
		},
	}

	f := &framer{proto: protoVersion4}
	if err := w.buildFrame(f, 1); err != nil {
		t.Fatalf("buildFrame: %v", err)
	}

	gotLen := binary.BigEndian.Uint32(f.buf[5:9])
	if int(gotLen) != len(f.buf)-headSize {
		t.Fatalf("length header %d != body len %d", gotLen, len(f.buf)-headSize)
	}

	// Walk the batch body: header(9) + type(1) + stmt count(2).
	pos := headSize + 1 + 2
	rdShort := func() int { v := int(binary.BigEndian.Uint16(f.buf[pos : pos+2])); pos += 2; return v }
	rdInt := func() int32 { v := int32(binary.BigEndian.Uint32(f.buf[pos : pos+4])); pos += 4; return v }
	rdByte := func() byte { v := f.buf[pos]; pos++; return v }

	// Statement 0: kind 0 (query), longstring, 2 values.
	if k := rdByte(); k != 0 {
		t.Fatalf("stmt0 kind=%d want 0", k)
	}
	slen := int(rdInt())
	if got := string(f.buf[pos : pos+slen]); got != "INSERT INTO t (a,b) VALUES (?,?)" {
		t.Fatalf("stmt0 text=%q", got)
	}
	pos += slen
	if vc := rdShort(); vc != 2 {
		t.Fatalf("stmt0 valuecount=%d want 2", vc)
	}
	if l := int(rdInt()); l != 4 {
		t.Fatalf("stmt0 v0 len=%d want 4", l)
	}
	pos += 4
	if l := int(rdInt()); l != 600 {
		t.Fatalf("stmt0 v1 len=%d want 600", l)
	}
	pos += 600

	// Statement 1: kind 1 (prepared), shortbytes id, 3 values.
	if k := rdByte(); k != 1 {
		t.Fatalf("stmt1 kind=%d want 1", k)
	}
	if idl := rdShort(); idl != 4 {
		t.Fatalf("stmt1 idlen=%d want 4", idl)
	}
	pos += 4
	if vc := rdShort(); vc != 3 {
		t.Fatalf("stmt1 valuecount=%d want 3", vc)
	}
	if l := int(rdInt()); l != 4000 {
		t.Fatalf("stmt1 v0 len=%d want 4000", l)
	}
	pos += 4000
	if l := rdInt(); l != -1 {
		t.Fatalf("stmt1 v1 len=%d want -1 (null)", l)
	}
	if l := rdInt(); l != -2 {
		t.Fatalf("stmt1 v2 len=%d want -2 (unset)", l)
	}
}

// BenchmarkWriteBatchFrameGrow measures allocations assembling a batch frame
// whose value section exceeds the starting buffer capacity.
func BenchmarkWriteBatchFrameGrow(b *testing.B) {
	w := &writeBatchFrame{
		typ:         LoggedBatch,
		consistency: Quorum,
	}
	for s := 0; s < 4; s++ {
		st := batchStatment{statement: "INSERT INTO t (a,b,c,d) VALUES (?,?,?,?)"}
		for v := 0; v < 4; v++ {
			st.values = append(st.values, queryValues{value: bytes.Repeat([]byte{byte(v)}, 512)})
		}
		w.statements = append(w.statements, st)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := &framer{proto: protoVersion4, buf: make([]byte, 0, defaultBufSize)}
		if err := w.buildFrame(f, 1); err != nil {
			b.Fatal(err)
		}
	}
}
