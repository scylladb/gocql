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
	"context"
	"encoding/binary"
	"reflect"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/tests/mock"
)

// buildIntRows builds a rows buffer where row r, column c holds the int32 value
// base + r*numCols + c, each encoded as [int32 len=4][int32 value].
func buildIntRows(numRows, numCols, base int) []byte {
	buf := make([]byte, 0, numRows*numCols*8)
	tmp := make([]byte, 4)
	for r := 0; r < numRows; r++ {
		for c := 0; c < numCols; c++ {
			binary.BigEndian.PutUint32(tmp, 4)
			buf = append(buf, tmp...)
			binary.BigEndian.PutUint32(tmp, uint32(int32(base+r*numCols+c)))
			buf = append(buf, tmp...)
		}
	}
	return buf
}

func newIntIter(numRows, numCols int, names []string, data []byte) *Iter {
	cols := make([]ColumnInfo, numCols)
	ti := NewNativeType(4, TypeInt)
	for i := range cols {
		cols[i] = ColumnInfo{Keyspace: "ks", Table: "t", Name: names[i], TypeInfo: ti}
	}
	f := &framer{header: &frm.FrameHeader{}, buf: data}
	return &Iter{
		framer:  f,
		numRows: numRows,
		meta: resultMetadata{
			columns:        cols,
			colCount:       numCols,
			actualColCount: numCols,
		},
	}
}

// TestMapScanReuseCorrectness: cached-values MapScan matches a fresh-map scan across many rows.
func TestMapScanReuseCorrectness(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 5, 3
	names := []string{"a", "b", "c"}
	data := buildIntRows(numRows, numCols, 100)
	iter := newIntIter(numRows, numCols, names, data)

	row := 0
	for {
		m := make(map[string]any)
		if !iter.MapScan(m) {
			break
		}
		for c := 0; c < numCols; c++ {
			want := 100 + row*numCols + c
			got, ok := m[names[c]]
			if !ok {
				t.Fatalf("row %d: missing column %q", row, names[c])
			}
			gi, ok := got.(int)
			if !ok {
				t.Fatalf("row %d col %q: expected int, got %T", row, names[c], got)
			}
			if gi != want {
				t.Fatalf("row %d col %q: got %d want %d", row, names[c], gi, want)
			}
		}
		row++
	}
	if row != numRows {
		t.Fatalf("scanned %d rows, want %d", row, numRows)
	}
	if err := iter.err; err != nil {
		t.Fatalf("iter error: %v", err)
	}
}

// TestMapScanUserPointerOverride: a pointer override in one row must not corrupt later rows.
func TestMapScanUserPointerOverride(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 4, 2
	names := []string{"x", "y"}
	data := buildIntRows(numRows, numCols, 0)
	iter := newIntIter(numRows, numCols, names, data)

	// Row 0: override column "x" with a user *int pointer.
	var userX int
	m0 := map[string]any{"x": &userX}
	if !iter.MapScan(m0) {
		t.Fatal("row 0 MapScan returned false")
	}
	if userX != 0 {
		t.Fatalf("row 0 userX = %d, want 0", userX)
	}
	// rowMap stores the dereferenced value, not the pointer.
	if v, ok := m0["x"].(int); !ok || v != 0 {
		t.Fatalf("row 0 m[x] = %v, want int 0", m0["x"])
	}
	if v, ok := m0["y"].(int); !ok || v != 1 {
		t.Fatalf("row 0 m[y] = %v, want int 1", m0["y"])
	}

	// Row 1: fresh map, NO override. Must still produce correct int values and
	// must not write into userX (the previous override's target).
	m1 := make(map[string]any)
	if !iter.MapScan(m1) {
		t.Fatal("row 1 MapScan returned false")
	}
	if v, ok := m1["x"].(int); !ok || v != 2 {
		t.Fatalf("row 1 m[x] = %v, want int 2", m1["x"])
	}
	if v, ok := m1["y"].(int); !ok || v != 3 {
		t.Fatalf("row 1 m[y] = %v, want int 3", m1["y"])
	}
	if userX != 0 {
		t.Fatalf("row 1 must not modify previous override target; userX = %d, want 0", userX)
	}

	// Row 2 and 3: drain.
	count := 2
	for {
		m := make(map[string]any)
		if !iter.MapScan(m) {
			break
		}
		wantX := count * numCols
		if v, ok := m["x"].(int); !ok || v != wantX {
			t.Fatalf("row %d m[x] = %v, want int %d", count, m["x"], wantX)
		}
		count++
	}
	if count != numRows {
		t.Fatalf("scanned %d rows, want %d", count, numRows)
	}
}

// buildBlobRows builds rows where each column holds a distinct blob value
// [byte(r), byte(c), byte(r+c)], so cross-row aliasing of the reused scan slice
// would be detectable as a value mismatch.
func buildBlobRows(numRows, numCols int) []byte {
	buf := make([]byte, 0)
	tmp := make([]byte, 4)
	for r := 0; r < numRows; r++ {
		for c := 0; c < numCols; c++ {
			payload := []byte{byte(r), byte(c), byte(r + c)}
			binary.BigEndian.PutUint32(tmp, uint32(len(payload)))
			buf = append(buf, tmp...)
			buf = append(buf, payload...)
		}
	}
	return buf
}

// TestMapScanBlobReuseNoAliasing: retained rows' []byte values survive later rows reusing the slice.
func TestMapScanBlobReuseNoAliasing(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 6, 2
	names := []string{"a", "b"}
	data := buildBlobRows(numRows, numCols)

	cols := make([]ColumnInfo, numCols)
	ti := NewNativeType(4, TypeBlob)
	for i := range cols {
		cols[i] = ColumnInfo{Keyspace: "ks", Table: "t", Name: names[i], TypeInfo: ti}
	}
	f := &framer{header: &frm.FrameHeader{}, buf: data}
	iter := &Iter{
		framer:  f,
		numRows: numRows,
		meta:    resultMetadata{columns: cols, colCount: numCols, actualColCount: numCols},
	}

	maps := make([]map[string]any, 0, numRows)
	for {
		m := make(map[string]any)
		if !iter.MapScan(m) {
			break
		}
		maps = append(maps, m)
	}
	if iter.err != nil {
		t.Fatalf("iter error: %v", iter.err)
	}
	if len(maps) != numRows {
		t.Fatalf("got %d rows, want %d", len(maps), numRows)
	}
	for r, m := range maps {
		for c := 0; c < numCols; c++ {
			want := []byte{byte(r), byte(c), byte(r + c)}
			got, ok := m[names[c]].([]byte)
			if !ok {
				t.Fatalf("row %d col %q: not []byte: %T", r, names[c], m[names[c]])
			}
			if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
				t.Fatalf("row %d col %q: got %v want %v (aliasing/corruption)", r, names[c], got, want)
			}
		}
	}
}

// buildVarBlobRows builds a one-column rows buffer with one row per entry in
// sizes; row r holds a blob of sizes[r] bytes, each byte set to r.
func buildVarBlobRows(sizes []int) []byte {
	var buf []byte
	tmp := make([]byte, 4)
	for r, size := range sizes {
		binary.BigEndian.PutUint32(tmp, uint32(size))
		buf = append(buf, tmp...)
		cell := make([]byte, size)
		for i := range cell {
			cell[i] = byte(r)
		}
		buf = append(buf, cell...)
	}
	return buf
}

func newBlobIter(numRows int, name string, data []byte) *Iter {
	cols := []ColumnInfo{{Keyspace: "ks", Table: "t", Name: name, TypeInfo: NewNativeType(4, TypeBlob)}}
	f := &framer{header: &frm.FrameHeader{}, buf: data}
	return &Iter{
		framer:  f,
		numRows: numRows,
		meta:    resultMetadata{columns: cols, colCount: 1, actualColCount: 1},
	}
}

// TestMapScanBlobCapacityNotAmplified: a large early blob must not inflate later values' capacity.
func TestMapScanBlobCapacityNotAmplified(t *testing.T) {
	t.Parallel()
	sizes := []int{4096, 3, 3}
	iter := newBlobIter(len(sizes), "b", buildVarBlobRows(sizes))

	for r, size := range sizes {
		m := make(map[string]any)
		if !iter.MapScan(m) {
			t.Fatalf("row %d failed: %v", r, iter.err)
		}
		got, ok := m["b"].([]byte)
		if !ok {
			t.Fatalf("row %d: not []byte: %T", r, m["b"])
		}
		if len(got) != size {
			t.Fatalf("row %d: len=%d want %d", r, len(got), size)
		}
		if cap(got) != size {
			t.Fatalf("row %d: cap=%d want %d (large-row capacity amplified into small row)", r, cap(got), size)
		}
	}
}

// TestMapScanCachesReleasedOnExhaustion: terminal MapScan drops both caches and any caller destinations.
func TestMapScanCachesReleasedOnExhaustion(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 2, 1
	names := []string{"a"}
	iter := newIntIter(numRows, numCols, names, buildIntRows(numRows, numCols, 0))

	var a int
	for iter.MapScan(map[string]any{"a": &a}) {
	}
	if iter.err != nil {
		t.Fatalf("iter error: %v", iter.err)
	}
	if iter.mapScanDefaults != nil {
		t.Fatal("mapScanDefaults retained after exhaustion")
	}
	if iter.mapScanWorking != nil {
		t.Fatal("mapScanWorking retained after exhaustion")
	}

	// An extra MapScan call on the finished iterator must not re-pin the
	// caches either.
	if iter.MapScan(map[string]any{"a": &a}) {
		t.Fatal("MapScan succeeded on exhausted iterator")
	}
	if iter.mapScanDefaults != nil || iter.mapScanWorking != nil {
		t.Fatal("caches re-pinned by MapScan call after exhaustion")
	}
}

// TestMapScanCachesReleasedOnEarlyClose: Close mid-iteration releases the caches.
func TestMapScanCachesReleasedOnEarlyClose(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 3, 2
	names := []string{"a", "b"}
	iter := newIntIter(numRows, numCols, names, buildIntRows(numRows, numCols, 0))

	var a int
	if !iter.MapScan(map[string]any{"a": &a}) {
		t.Fatalf("expected first row, err: %v", iter.err)
	}
	if iter.mapScanDefaults == nil || iter.mapScanWorking == nil {
		t.Fatal("precondition: both caches should be populated before Close")
	}
	if err := iter.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if iter.mapScanDefaults != nil {
		t.Fatal("mapScanDefaults retained after early Close")
	}
	if iter.mapScanWorking != nil {
		t.Fatal("mapScanWorking retained after early Close")
	}
}

// TestMapScanNoOverrideSkipsWorkingSlice: the no-override path scans into the defaults and never allocates the working slice.
func TestMapScanNoOverrideSkipsWorkingSlice(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 3, 2
	names := []string{"a", "b"}
	iter := newIntIter(numRows, numCols, names, buildIntRows(numRows, numCols, 0))

	rows := 0
	for {
		m := make(map[string]any)
		if !iter.MapScan(m) {
			break
		}
		rows++
		if iter.mapScanWorking != nil {
			t.Fatal("working slice allocated without any caller override")
		}
	}
	if iter.err != nil || rows != numRows {
		t.Fatalf("rows=%d err=%v", rows, iter.err)
	}
}

// TestMapScanAllPointersReuse: a caller pointer for every column on every row gets each row's data.
func TestMapScanAllPointersReuse(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 5, 3
	names := []string{"a", "b", "c"}
	data := buildIntRows(numRows, numCols, 1000)
	iter := newIntIter(numRows, numCols, names, data)

	var a, b, c int
	row := 0
	for {
		// Same pointers reused each iteration (the documented pattern).
		m := map[string]any{"a": &a, "b": &b, "c": &c}
		if !iter.MapScan(m) {
			break
		}
		// User variables must hold this row's values.
		if a != 1000+row*numCols+0 || b != 1000+row*numCols+1 || c != 1000+row*numCols+2 {
			t.Fatalf("row %d: a=%d b=%d c=%d", row, a, b, c)
		}
		// Map values (dereferenced) must match too.
		if v, _ := m["a"].(int); v != a {
			t.Fatalf("row %d: m[a]=%v want %d", row, m["a"], a)
		}
		row++
	}
	if iter.err != nil {
		t.Fatalf("iter error: %v", iter.err)
	}
	if row != numRows {
		t.Fatalf("scanned %d rows, want %d", row, numRows)
	}
}

// TestMapScanMixedOverrideThenDefault: a column overridden in one row falls back to the default in the next.
func TestMapScanMixedOverrideThenDefault(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 3, 1
	names := []string{"x"}
	data := buildIntRows(numRows, numCols, 0) // values 0,1,2
	iter := newIntIter(numRows, numCols, names, data)

	var userX int
	// Row 0: override.
	if !iter.MapScan(map[string]any{"x": &userX}) {
		t.Fatal("row 0 failed")
	}
	if userX != 0 {
		t.Fatalf("row 0 userX=%d want 0", userX)
	}
	// Row 1: no override; must not touch userX, must read value 1.
	m1 := map[string]any{}
	if !iter.MapScan(m1) {
		t.Fatal("row 1 failed")
	}
	if userX != 0 {
		t.Fatalf("row 1 must not modify userX; got %d", userX)
	}
	if v, _ := m1["x"].(int); v != 1 {
		t.Fatalf("row 1 m[x]=%v want 1", m1["x"])
	}
	// Row 2: override again with same var; must read value 2.
	if !iter.MapScan(map[string]any{"x": &userX}) {
		t.Fatal("row 2 failed")
	}
	if userX != 2 {
		t.Fatalf("row 2 userX=%d want 2", userX)
	}
}

// TestMapScanColumnCountChangeAcrossPageTurn: a column added on a page boundary
// (RESULT_METADATA_CHANGED) must be keyed and scanned by the new metadata.
func TestMapScanColumnCountChangeAcrossPageTurn(t *testing.T) {
	t.Parallel()
	intCol := func(name string) ColumnInfo {
		return ColumnInfo{Name: name, TypeInfo: NativeType{typ: TypeInt, proto: 4}}
	}
	marshalInt := func(v int32) []byte {
		b, err := Marshal(NativeType{typ: TypeInt, proto: 4}, v)
		if err != nil {
			t.Fatalf("unexpected error from reference Marshal: %v", err)
		}
		return b
	}

	oneColumn := resultMetadata{columns: []ColumnInfo{intCol("a")}, actualColCount: 1}
	twoColumns := resultMetadata{
		columns:        []ColumnInfo{intCol("a"), intCol("b")},
		actualColCount: 2,
	}

	iter := newPageTurnIter(oneColumn, twoColumns, [][]byte{marshalInt(1)}, [][]byte{marshalInt(2), marshalInt(3)})
	defer iter.Close()

	m1 := map[string]any{}
	if !iter.MapScan(m1) {
		t.Fatalf("expected first row, err: %v", iter.err)
	}
	if v, _ := m1["a"].(int); v != 1 {
		t.Fatalf("first row: m[a]=%v want 1", m1["a"])
	}
	if _, ok := m1["b"]; ok {
		t.Fatalf("first row: unexpected column b: %v", m1["b"])
	}

	// Page turn: metadata gains a column. MapScan must resolve columns and
	// defaults against the new page's metadata, not the cached one-column set.
	m2 := map[string]any{}
	if !iter.MapScan(m2) {
		t.Fatalf("expected second row, err: %v", iter.err)
	}
	if v, _ := m2["a"].(int); v != 2 {
		t.Fatalf("second row: m[a]=%v want 2", m2["a"])
	}
	if v, _ := m2["b"].(int); v != 3 {
		t.Fatalf("second row: m[b]=%v want 3", m2["b"])
	}
}

// TestSliceMapColumnCountChangeAcrossPageTurn: SliceMap must rebuild RowData when a
// page turn changes the metadata, not reuse the first page's columns.
func TestSliceMapColumnCountChangeAcrossPageTurn(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	marshalInt := func(v int32) []byte {
		b, err := Marshal(intType, v)
		if err != nil {
			t.Fatalf("unexpected error from reference Marshal: %v", err)
		}
		return b
	}
	col := func(name string) ColumnInfo { return ColumnInfo{Name: name, TypeInfo: intType} }

	iter := newPageTurnIter(
		resultMetadata{columns: []ColumnInfo{col("a")}, actualColCount: 1},
		resultMetadata{columns: []ColumnInfo{col("a"), col("b")}, actualColCount: 2},
		[][]byte{marshalInt(1)},
		[][]byte{marshalInt(2), marshalInt(3)},
	)

	rows, err := iter.SliceMap()
	if err != nil {
		t.Fatal(err)
	}
	want := []map[string]any{{"a": 1}, {"a": 2, "b": 3}}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("SliceMap = %v, want %v", rows, want)
	}
}

// TestMapScanUnchangedPageTurnKeepsEpoch: a page turn with the same metadata must not
// invalidate the MapScan caches.
func TestMapScanUnchangedPageTurnKeepsEpoch(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	b, err := Marshal(intType, int32(1))
	if err != nil {
		t.Fatal(err)
	}
	meta := resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: intType}}, actualColCount: 1}
	iter := newPageTurnIter(meta, meta, [][]byte{b}, [][]byte{b})
	defer iter.Close()

	for i := 0; i < 2; i++ {
		if !iter.MapScan(map[string]any{}) {
			t.Fatalf("row %d failed: %v", i, iter.err)
		}
	}
	if iter.metaEpoch != 0 {
		t.Fatalf("metaEpoch=%d after unchanged page turn, want 0", iter.metaEpoch)
	}
}

// newPageTurnIter returns a one-row Iter whose next page (also one row) arrives
// with different metadata, as with RESULT_METADATA_CHANGED.
func newPageTurnIter(first, next resultMetadata, firstData, nextData [][]byte) *Iter {
	conn := &pagingTestConn{
		executeQueryFunc: func(_ context.Context, _ *Query) *Iter {
			return &Iter{meta: next, framer: &mock.MockFramer{Data: nextData}, numRows: 1}
		},
	}
	baseQry := newWarningTestQuery()
	baseQry.conn = conn
	return &Iter{
		meta:    first,
		framer:  &mock.MockFramer{Data: firstData},
		numRows: 1,
		next:    newNextIter(baseQry, 1),
	}
}

// TestMapScanSameCountMetadataChangeAcrossPageTurn: the column count stays at
// one but name and type change, so only epoch invalidation can rebuild caches.
func TestMapScanSameCountMetadataChangeAcrossPageTurn(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	textType := NativeType{typ: TypeVarchar, proto: 4}
	marshal := func(info TypeInfo, v any) []byte {
		b, err := Marshal(info, v)
		if err != nil {
			t.Fatalf("unexpected error from reference Marshal: %v", err)
		}
		return b
	}

	iter := newPageTurnIter(
		resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: intType}}, actualColCount: 1},
		resultMetadata{columns: []ColumnInfo{{Name: "b", TypeInfo: textType}}, actualColCount: 1},
		[][]byte{marshal(intType, int32(1))},
		[][]byte{marshal(textType, "x")},
	)
	defer iter.Close()

	m1 := map[string]any{}
	if !iter.MapScan(m1) {
		t.Fatalf("expected first row, err: %v", iter.err)
	}
	if v, _ := m1["a"].(int); v != 1 {
		t.Fatalf("first row: m[a]=%v want 1", m1["a"])
	}

	m2 := map[string]any{}
	if !iter.MapScan(m2) {
		t.Fatalf("expected second row, err: %v", iter.err)
	}
	if v, _ := m2["b"].(string); v != "x" {
		t.Fatalf("second row: m[b]=%#v want \"x\"", m2["b"])
	}
	if _, ok := m2["a"]; ok {
		t.Fatalf("second row: stale column a: %v", m2["a"])
	}
}

// TestMapScanNullAfterValueResets: a null row after a non-null row must decode as
// a fresh scan would, not keep the previous row's value in the reused destinations.
func TestMapScanNullAfterValueResets(t *testing.T) {
	t.Parallel()
	nat := func(typ Type) NativeType { return NativeType{typ: typ, proto: 4} }
	bt := nat(TypeBlob)
	lt := CollectionType{NativeType: nat(TypeList), Elem: nat(TypeInt)}
	tt := TupleTypeInfo{NativeType: nat(TypeTuple), Elems: []TypeInfo{nat(TypeInt), bt}}
	cols := []ColumnInfo{{Name: "b", TypeInfo: bt}, {Name: "l", TypeInfo: lt}, {Name: "t", TypeInfo: tt}}
	newIter := func(data [][]byte, rows int) *Iter {
		return &Iter{
			framer:  &mock.MockFramer{Data: data},
			numRows: rows,
			meta:    resultMetadata{columns: cols, actualColCount: 4},
		}
	}

	lb, err := Marshal(lt, []int{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	tb, err := Marshal(tt, []any{7, []byte("q")})
	if err != nil {
		t.Fatal(err)
	}

	// Expected null row: a single-row MapScan on a fresh Iter (no reuse).
	want := map[string]any{}
	if fresh := newIter([][]byte{nil, nil, nil}, 1); !fresh.MapScan(want) {
		t.Fatal(fresh.err)
	}

	iter := newIter([][]byte{[]byte("ab"), lb, tb, nil, nil, nil}, 2)
	m1 := map[string]any{}
	if !iter.MapScan(m1) {
		t.Fatal(iter.err)
	}
	wantM1 := map[string]any{
		"b": []byte("ab"), "l": []int{1, 2}, "t[0]": 7, "t[1]": []byte("q"),
	}
	m2 := map[string]any{}
	if !iter.MapScan(m2) {
		t.Fatal(iter.err)
	}

	if !reflect.DeepEqual(m2, want) {
		t.Fatalf("row 2 = %#v, want fresh-null decode %#v", m2, want)
	}
	if !reflect.DeepEqual(m1, wantM1) {
		t.Fatalf("row 1 map changed after row 2: %#v, want %#v", m1, wantM1)
	}
}

// TestMapScanOverrideAcrossPageTurn: a caller override on a page whose type changed from
// the cached one is scanned correctly and not retained in the working slice.
func TestMapScanOverrideAcrossPageTurn(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	textType := NativeType{typ: TypeVarchar, proto: 4}
	marshal := func(info TypeInfo, v any) []byte {
		b, err := Marshal(info, v)
		if err != nil {
			t.Fatalf("unexpected error from reference Marshal: %v", err)
		}
		return b
	}

	iter := newPageTurnIter(
		resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: intType}, {Name: "b", TypeInfo: intType}}, actualColCount: 2},
		resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: textType}, {Name: "b", TypeInfo: intType}}, actualColCount: 2},
		[][]byte{marshal(intType, int32(1)), marshal(intType, int32(2))},
		[][]byte{marshal(textType, "x"), marshal(intType, int32(5))},
	)
	defer iter.Close()

	if !iter.MapScan(map[string]any{}) {
		t.Fatalf("expected first row, err: %v", iter.err)
	}

	var a string
	m2 := map[string]any{"a": &a}
	if !iter.MapScan(m2) {
		t.Fatalf("expected second row, err: %v", iter.err)
	}
	if a != "x" {
		t.Fatalf("override: a=%q want \"x\"", a)
	}
	if v, _ := m2["b"].(int); v != 5 {
		t.Fatalf("second row: m[b]=%#v want 5", m2["b"])
	}
	if _, ok := iter.mapScanDefaults[0].(*string); !ok {
		t.Fatalf("defaults not rebuilt for new page: %T", iter.mapScanDefaults[0])
	}
	for i, v := range iter.mapScanWorking {
		if v != nil {
			t.Fatalf("mapScanWorking[%d] retains %T", i, v)
		}
	}
}

// TestMapScanFailedRebuildDropsCaches: when a page-2 column type cannot build a
// destination, MapScan fails and must not keep the page-1 caches.
func TestMapScanFailedRebuildDropsCaches(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	b, err := Marshal(intType, int32(1))
	if err != nil {
		t.Fatal(err)
	}
	iter := newPageTurnIter(
		resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: intType}}, actualColCount: 1},
		resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: NativeType{typ: TypeCustom, proto: 4}}}, actualColCount: 1},
		[][]byte{b}, [][]byte{b},
	)
	defer iter.Close()

	if !iter.MapScan(map[string]any{"a": new(int)}) {
		t.Fatalf("row 1 failed: %v", iter.err)
	}
	if iter.MapScan(map[string]any{}) {
		t.Fatal("row 2 should fail: type has no destination")
	}
	if iter.err == nil {
		t.Fatal("expected iter.err to be set")
	}
	if iter.mapScanDefaults != nil || iter.mapScanWorking != nil {
		t.Fatalf("stale caches kept: defaults=%v working=%v", iter.mapScanDefaults, iter.mapScanWorking)
	}
}

// TestScanSkipTupleDestinations: a nil first destination skips the whole tuple
// column, so later columns stay aligned.
func TestScanSkipTupleDestinations(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	tt := TupleTypeInfo{NativeType: NativeType{typ: TypeTuple, proto: 4}, Elems: []TypeInfo{intType, intType}}
	tb, err := Marshal(tt, []any{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Marshal(intType, int32(9))
	if err != nil {
		t.Fatal(err)
	}
	iter := &Iter{
		framer:  &mock.MockFramer{Data: [][]byte{tb, b}},
		numRows: 1,
		meta: resultMetadata{
			columns:        []ColumnInfo{{Name: "t", TypeInfo: tt}, {Name: "b", TypeInfo: intType}},
			actualColCount: 3,
		},
	}
	var e1, got int
	if !iter.Scan(nil, &e1, &got) {
		t.Fatalf("Scan failed: %v", iter.Close())
	}
	if got != 9 {
		t.Fatalf("b=%d want 9 (e1=%d)", got, e1)
	}

	// A nil first element skips only that element; later ones still decode.
	iter.framer = &mock.MockFramer{Data: [][]byte{tb, b}}
	iter.pos, iter.numRows = 0, 1
	var e2, other int
	if !iter.Scan(nil, &e2, &other) {
		t.Fatalf("Scan(nil, &e2, &other) failed: %v", iter.Close())
	}
	if e2 != 2 || other != 9 {
		t.Fatalf("e2=%d other=%d want 2, 9", e2, other)
	}

	// An all-nil tuple is skipped without decoding, even if its bytes are malformed.
	iter.framer = &mock.MockFramer{Data: [][]byte{{0xff, 0xff}, b}}
	iter.pos, iter.numRows = 0, 1
	if !iter.Scan(nil, nil, &other) || other != 9 {
		t.Fatalf("Scan(nil, nil, &other): other=%d err=%v", other, iter.Close())
	}
}

// TestMapScanNilDestinations: a nil destination in the caller's map yields a nil
// result and never leaks the previous row's value.
func TestMapScanNilDestinations(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	tt := TupleTypeInfo{NativeType: NativeType{typ: TypeTuple, proto: 4}, Elems: []TypeInfo{intType, intType}}
	tb, err := Marshal(tt, []any{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Marshal(intType, int32(9))
	if err != nil {
		t.Fatal(err)
	}
	iter := &Iter{
		framer:  &mock.MockFramer{Data: [][]byte{tb, b, tb, b}},
		numRows: 2,
		meta: resultMetadata{
			columns:        []ColumnInfo{{Name: "t", TypeInfo: tt}, {Name: "b", TypeInfo: intType}},
			actualColCount: 3,
		},
	}
	m := map[string]any{}
	if !iter.MapScan(m) {
		t.Fatal(iter.err)
	}
	if m["t[0]"] != 1 || m["t[1]"] != 2 || m["b"] != 9 {
		t.Fatalf("first row: %v", m)
	}
	m = map[string]any{"t[0]": nil, "b": nil}
	if !iter.MapScan(m) {
		t.Fatal(iter.err)
	}
	if v, ok := m["t[0]"]; !ok || v != nil {
		t.Fatalf("t[0]=%v ok=%v want nil", v, ok)
	}
	if v, ok := m["b"]; !ok || v != nil {
		t.Fatalf("b=%v ok=%v want nil", v, ok)
	}
	if m["t[1]"] != 2 {
		t.Fatalf("t[1]=%v want 2", m["t[1]"])
	}
}

// TestRowDataColumnsMutationDoesNotAffectMapScan: RowData returns a copy, so editing it
// cannot change the keys MapScan uses.
func TestRowDataColumnsMutationDoesNotAffectMapScan(t *testing.T) {
	t.Parallel()
	intType := NativeType{typ: TypeInt, proto: 4}
	b, err := Marshal(intType, int32(1))
	if err != nil {
		t.Fatal(err)
	}
	iter := &Iter{
		framer:  &mock.MockFramer{Data: [][]byte{b}},
		numRows: 1,
		meta:    resultMetadata{columns: []ColumnInfo{{Name: "a", TypeInfo: intType}}, actualColCount: 1},
	}
	rd, err := iter.RowData()
	if err != nil {
		t.Fatal(err)
	}
	rd.Columns[0] = "mutated"

	m := map[string]any{}
	if !iter.MapScan(m) {
		t.Fatal(iter.err)
	}
	if v, _ := m["a"].(int); v != 1 || len(m) != 1 {
		t.Fatalf("MapScan keys changed: %v", m)
	}
}
