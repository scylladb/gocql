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
	"reflect"
	"testing"

	"github.com/gocql/gocql/internal/tests/mock"
)

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
