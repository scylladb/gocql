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
	"encoding/binary"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
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

// TestMapScanReuseCorrectness verifies that the cached-values MapScan returns
// the same per-row results as a naive fresh-map scan, including across many
// rows (where slice reuse happens).
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

// TestMapScanUserPointerOverride verifies that supplying pointers in the map
// works correctly and, crucially, that overriding a pointer in one row does not
// corrupt subsequent rows that do not override (exercises the repair path).
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

// TestMapScanBlobReuseNoAliasing verifies that reusing the scan-value slice
// across rows does not cause blob ([]byte) values stored in earlier rows' maps
// to be corrupted by later rows (the defensive copy in rowMap must hold). All
// row maps are retained and verified at the end.
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

// TestMapScanNonComparableOverrideNoPanic verifies that pre-seeding the map with
// a non-comparable value (e.g. a slice) for a column does not panic in the
// override-detection comparison (dest != defaults).
func TestMapScanNonComparableOverrideNoPanic(t *testing.T) {
	t.Parallel()
	const numRows, numCols = 2, 1
	names := []string{"a"}
	data := buildIntRows(numRows, numCols, 0)
	iter := newIntIter(numRows, numCols, names, data)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MapScan panicked on non-comparable map value: %v", r)
		}
	}()

	// Seed with a non-comparable []int value under the column key. MapScan must
	// not panic when comparing it against the cached default pointer.
	m := map[string]any{"a": []int{1, 2, 3}}
	_ = iter.MapScan(m)
}

// TestMapScanAllPointersReuse exercises the documented "pass pointers in the map"
// pattern where the caller supplies a pointer for every column on every row
// (reusing the same variables). It verifies correct values across many rows with
// the reused working slice, and that user variables receive each row's data.
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

// TestMapScanMixedOverrideThenDefault verifies that overriding a column with a
// user pointer in one row, then NOT overriding it in the next row, correctly
// falls back to the cached default destination (the working slice is rebuilt
// from defaults each call) and does not keep writing into the stale user var.
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
