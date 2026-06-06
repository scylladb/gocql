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
