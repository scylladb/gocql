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

// buildMapScanRows encodes each column as [int32 length][int32 value]; reads
// don't mutate it, so the buffer is reusable across iterations.
func buildMapScanRows(numRows, numCols int) []byte {
	buf := make([]byte, 0, numRows*numCols*8)
	tmp := make([]byte, 4)
	for r := 0; r < numRows; r++ {
		for c := 0; c < numCols; c++ {
			binary.BigEndian.PutUint32(tmp, 4)
			buf = append(buf, tmp...)
			binary.BigEndian.PutUint32(tmp, uint32(int32(r*numCols+c)))
			buf = append(buf, tmp...)
		}
	}
	return buf
}

func makeMapScanColumns(numCols int) []ColumnInfo {
	cols := make([]ColumnInfo, numCols)
	ti := NewNativeType(4, TypeInt)
	for i := range cols {
		cols[i] = ColumnInfo{
			Keyspace: "ks",
			Table:    "tbl",
			Name:     "col" + string(rune('a'+i%26)),
			TypeInfo: ti,
		}
	}
	return cols
}

// mapScanBench holds a reusable framer + iter pair so the benchmark measures the
// scan path itself rather than per-iteration setup allocations.
type mapScanBench struct {
	f        *framer
	iter     *Iter
	template []byte
	numRows  int
}

func newMapScanBench(numRows, numCols int) *mapScanBench {
	template := buildMapScanRows(numRows, numCols)
	f := &framer{header: &frm.FrameHeader{}, buf: template}
	iter := &Iter{
		framer:  f,
		numRows: numRows,
		meta: resultMetadata{
			columns:        makeMapScanColumns(numCols),
			colCount:       numCols,
			actualColCount: numCols,
		},
	}
	return &mapScanBench{f: f, iter: iter, template: template, numRows: numRows}
}

func (b *mapScanBench) reset() {
	b.f.buf = b.template
	b.iter.pos = 0
	b.iter.numRows = b.numRows
	b.iter.err = nil
	b.iter.closed = 0
	b.iter.framer = b.f
	// Drop the per-Iter caches: production allocates a fresh Iter per query,
	// so keeping them warm across iterations would hide the per-query cache
	// construction cost and overstate the per-row savings.
	b.iter.scanColumns = nil
	b.iter.mapScanDefaults = nil
	b.iter.mapScanWorking = nil
}

// BenchmarkMapScanRows scans numRows rows via MapScan with a fresh map per row;
// the cached scan-values slice removes the per-row []any and pointer allocations.
func BenchmarkMapScanRows(b *testing.B) {
	const numCols = 8
	bs := newMapScanBench(64, numCols)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs.reset()
		rows := 0
		for {
			m := make(map[string]any, numCols)
			if !bs.iter.MapScan(m) {
				break
			}
			rows++
		}
		// Assert full-row processing and no iterator error so a decode/iterator
		// regression cannot produce falsely-fast numbers by scanning fewer rows.
		if bs.iter.err != nil {
			b.Fatalf("MapScan failed: %v", bs.iter.err)
		}
		if rows != bs.numRows {
			b.Fatalf("expected %d rows, got %d", bs.numRows, rows)
		}
	}
}

// BenchmarkMapScanRowsAllPointers passes a pointer per column every row; watch
// allocs/op: the working slice is reused, so overrides add no allocations.
func BenchmarkMapScanRowsAllPointers(b *testing.B) {
	const numCols = 8
	bs := newMapScanBench(64, numCols)
	cols := bs.iter.meta.columns
	vals := make([]int, numCols)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs.reset()
		rows := 0
		for {
			m := make(map[string]any, numCols)
			for c := 0; c < numCols; c++ {
				m[cols[c].Name] = &vals[c]
			}
			if !bs.iter.MapScan(m) {
				break
			}
			rows++
		}
		if bs.iter.err != nil {
			b.Fatalf("MapScan failed: %v", bs.iter.err)
		}
		if rows != bs.numRows {
			b.Fatalf("expected %d rows, got %d", bs.numRows, rows)
		}
	}
}
