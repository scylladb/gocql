//go:build unit
// +build unit

package gocql

import (
	"encoding/binary"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// buildResultRowsPayload constructs the minimal binary payload that
// parseResultRows expects: resultMetadata (flags + colCount + optional paging
// state) followed by numRows int.
func buildResultRowsPayload(numRows, colCount int, pagingState []byte) []byte {
	var buf []byte

	// flags (4 bytes)
	flags := 0
	if len(pagingState) > 0 {
		flags |= frm.FlagHasMorePages
	}
	flags |= frm.FlagNoMetaData // skip column specs for simplicity
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(flags))
	buf = append(buf, b...)

	// colCount (4 bytes)
	binary.BigEndian.PutUint32(b, uint32(colCount))
	buf = append(buf, b...)

	// pagingState ([int][bytes])
	if len(pagingState) > 0 {
		binary.BigEndian.PutUint32(b, uint32(len(pagingState)))
		buf = append(buf, b...)
		buf = append(buf, pagingState...)
	}

	// numRows (4 bytes)
	binary.BigEndian.PutUint32(b, uint32(numRows))
	buf = append(buf, b...)

	return buf
}

func TestResultRowsFramePool(t *testing.T) {
	t.Parallel()

	payload := buildResultRowsPayload(100, 5, []byte("page-state-token"))
	f := newFramer(nil, protoVersion4)
	f.buf = payload

	result := f.parseResultRows()
	rows, ok := result.(*resultRowsFrame)
	if !ok {
		t.Fatalf("expected *resultRowsFrame, got %T", result)
	}
	if rows.numRows != 100 {
		t.Fatalf("expected 100 rows, got %d", rows.numRows)
	}
	if rows.meta.colCount != 5 {
		t.Fatalf("expected 5 columns, got %d", rows.meta.colCount)
	}

	// Release back to pool
	rows.release()

	// Get another from pool - should reuse
	payload2 := buildResultRowsPayload(50, 3, nil)
	f2 := newFramer(nil, protoVersion4)
	f2.buf = payload2

	result2 := f2.parseResultRows()
	rows2, ok := result2.(*resultRowsFrame)
	if !ok {
		t.Fatalf("expected *resultRowsFrame, got %T", result2)
	}
	if rows2.numRows != 50 {
		t.Fatalf("expected 50 rows, got %d", rows2.numRows)
	}
	if rows2.meta.colCount != 3 {
		t.Fatalf("expected 3 columns, got %d", rows2.meta.colCount)
	}
	rows2.release()
}

func TestResultRowsFrameReleaseClears(t *testing.T) {
	t.Parallel()

	payload := buildResultRowsPayload(42, 7, []byte("state"))
	f := newFramer(nil, protoVersion4)
	f.buf = payload

	result := f.parseResultRows().(*resultRowsFrame)
	result.release()

	// After release, if we get it back from pool it should be zeroed
	got := resultRowsFramePool.Get().(*resultRowsFrame)
	if got.numRows != 0 {
		t.Fatalf("expected zeroed numRows after release, got %d", got.numRows)
	}
	if got.meta.colCount != 0 {
		t.Fatalf("expected zeroed colCount after release, got %d", got.meta.colCount)
	}
	resultRowsFramePool.Put(got)
}

// BenchmarkParseResultRows_Pooled benchmarks the current pooled implementation.
func BenchmarkParseResultRows_Pooled(b *testing.B) {
	payload := buildResultRowsPayload(1000, 10, []byte("paging-state-0123456789abcdef"))
	f := newFramer(nil, protoVersion4)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.buf = payload
		result := f.parseResultRows()
		result.(*resultRowsFrame).release()
	}
}

// BenchmarkParseResultRows_NoPool benchmarks without pooling (allocates each time).
// Uses a sink to prevent escape analysis from stack-allocating the frame.
var resultRowsFrameSink *resultRowsFrame

func BenchmarkParseResultRows_NoPool(b *testing.B) {
	payload := buildResultRowsPayload(1000, 10, []byte("paging-state-0123456789abcdef"))
	f := newFramer(nil, protoVersion4)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.buf = payload
		result := &resultRowsFrame{}
		result.meta = f.parseResultMetadata()
		result.numRows = f.readInt()
		resultRowsFrameSink = result
	}
}
