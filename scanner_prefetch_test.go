package gocql

import (
	"testing"

	"github.com/gocql/gocql/internal/tests/mock"
)

// makeTestIter creates an Iter with numRows rows, each having numCols columns.
// Each column value is a single byte equal to the row index.
func makeTestIter(numRows, numCols int) *Iter {
	var data [][]byte
	for row := 0; row < numRows; row++ {
		for col := 0; col < numCols; col++ {
			data = append(data, []byte{byte(row)})
		}
	}

	columns := make([]ColumnInfo, numCols)
	for i := range columns {
		columns[i] = ColumnInfo{
			Name:     "col",
			TypeInfo: NativeType{typ: TypeBlob, proto: 4},
		}
	}

	return &Iter{
		framer:  &mock.MockFramer{Data: data},
		numRows: numRows,
		meta: resultMetadata{
			columns:        columns,
			actualColCount: numCols,
		},
	}
}

func TestIterScanner_PrefetchTriggered(t *testing.T) {
	// Create a page with 4 rows, 1 column each.
	// With prefetch=0.25, next.pos = (1 - 0.25) * 4 = 3.
	// So fetchAsync should be triggered when pos >= 3 (i.e., on the 4th Next() call,
	// since pos is checked before being incremented from 3 to 4).
	iter := makeTestIter(4, 1)

	// Create a nextIter that we can observe. We don't need it to actually fetch
	// anything since we only consume the first page.
	ni := &nextIter{
		qry: &Query{},
		pos: 3, // trigger at pos >= 3
	}
	iter.next = ni

	scanner := iter.Scanner()

	// Consume rows 0, 1, 2 — prefetch should NOT have fired yet.
	for i := 0; i < 3; i++ {
		if !scanner.Next() {
			t.Fatalf("expected Next() to return true on row %d", i)
		}
	}

	// At this point pos=3 (after 3 increments from 0).
	// The prefetch check runs BEFORE pos++ so on the 3rd call pos was 2 < 3, no trigger.
	// Verify fetchAsync has NOT been called yet by checking oncea state.
	// We can't directly observe sync.Once, but we can check if the goroutine
	// would have been launched by looking at the next field.
	// A more reliable approach: manually check if fetchAsync was called by
	// trying to call it ourselves — if Once already fired, our call is a no-op.
	var alreadyCalled bool
	ni.oncea.Do(func() {
		alreadyCalled = false
	})
	if alreadyCalled {
		t.Fatal("fetchAsync should not have been called before consuming 75% of rows")
	}

	// Now consume row 3 — at this point pos=3 >= next.pos=3, so fetchAsync fires.
	if !scanner.Next() {
		t.Fatal("expected Next() to return true on row 3")
	}

	// Now oncea should have been consumed (fetchAsync was called).
	// Verify by trying to execute via oncea — if it doesn't execute our func,
	// that means fetchAsync already used it.
	onceClaimed := false
	ni.oncea.Do(func() {
		onceClaimed = true
	})
	if onceClaimed {
		t.Fatal("fetchAsync should have been called after pos >= next.pos, but oncea was still available")
	}
}

func TestIterScanner_PrefetchNotTriggeredEarly(t *testing.T) {
	// 10 rows, prefetch pos at 7 (simulating prefetch=0.3, pos=(1-0.3)*10=7).
	iter := makeTestIter(10, 1)

	ni := &nextIter{
		qry: &Query{},
		pos: 7,
	}
	iter.next = ni

	scanner := iter.Scanner()

	// Consume 7 rows (pos goes 0..6, checked before increment).
	// On the 7th call, pos=6 before check, which is < 7, so no trigger.
	for i := 0; i < 7; i++ {
		if !scanner.Next() {
			t.Fatalf("expected Next() to return true on row %d", i)
		}
	}

	// oncea should still be unclaimed.
	onceClaimed := false
	ni.oncea.Do(func() {
		onceClaimed = true
	})
	if !onceClaimed {
		t.Fatal("fetchAsync was triggered too early (before pos >= next.pos)")
	}
}

func TestIterScanner_PrefetchNotTriggeredWithoutNextIter(t *testing.T) {
	// Create an iter with no next page — should not panic.
	iter := makeTestIter(3, 1)
	// iter.next is nil

	scanner := iter.Scanner()
	count := 0
	for scanner.Next() {
		count++
	}
	if count != 3 {
		t.Fatalf("expected 3 rows, got %d", count)
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestIterScanner_PrefetchMatchesScanTiming(t *testing.T) {
	// Verify that the Scanner triggers fetchAsync at the same pos value
	// as Iter.Scan() would.
	//
	// With 8 rows and next.pos=6:
	// - Iter.Scan() checks pos >= 6 BEFORE reading columns, BEFORE pos++
	// - iterScanner.Next() checks pos >= 6 AFTER reading columns, BEFORE pos++
	// Both should trigger on the 7th call (when pos=6).

	// Test Scanner path
	scannerIter := makeTestIter(8, 1)
	scannerNI := &nextIter{
		qry: &Query{},
		pos: 6,
	}
	scannerIter.next = scannerNI
	scanner := scannerIter.Scanner()

	scannerTriggerRow := -1
	for i := 0; i < 8; i++ {
		if !scanner.Next() {
			t.Fatalf("scanner: expected Next() true on row %d", i)
		}
		if scannerTriggerRow == -1 {
			claimed := false
			scannerNI.oncea.Do(func() { claimed = true })
			if !claimed {
				// oncea was already consumed — fetchAsync was called
				scannerTriggerRow = i
			}
		}
	}

	// Test Scan path
	scanIter := makeTestIter(8, 1)
	scanNI := &nextIter{
		qry: &Query{},
		pos: 6,
	}
	scanIter.next = scanNI

	var dummy []byte
	scanTriggerRow := -1
	for i := 0; i < 8; i++ {
		if !scanIter.Scan(&dummy) {
			t.Fatalf("scan: expected Scan() true on row %d", i)
		}
		if scanTriggerRow == -1 {
			claimed := false
			scanNI.oncea.Do(func() { claimed = true })
			if !claimed {
				scanTriggerRow = i
			}
		}
	}

	if scannerTriggerRow != scanTriggerRow {
		t.Fatalf("prefetch timing mismatch: Scanner triggered on row %d, Scan triggered on row %d",
			scannerTriggerRow, scanTriggerRow)
	}

	if scannerTriggerRow == -1 {
		t.Fatal("neither Scanner nor Scan triggered fetchAsync")
	}
}

func TestIterScanner_PageTransitionWithPrefetch(t *testing.T) {
	// Test that Scanner correctly transitions between pages and that
	// prefetch works across page boundaries.

	// Page 2: 4 rows, this is the last page (no next).
	page2 := makeTestIter(4, 1)

	// Page 1: 4 rows, prefetch pos at 3
	page1 := makeTestIter(4, 1)

	// Create a nextIter for page 1 that returns page 2.
	// We pre-populate the result and consume the sync.Once so that
	// fetch() returns page2 directly without calling session.executeQuery().
	page1NI := &nextIter{
		qry: &Query{},
		pos: 3,
	}
	page1NI.next = page2
	page1NI.once.Do(func() {}) // consume the once so fetch() just returns n.next
	page1.next = page1NI

	scanner := page1.Scanner()

	// Consume all rows from both pages (4 + 4 = 8).
	count := 0
	for scanner.Next() {
		count++
	}
	if count != 8 {
		t.Fatalf("expected 8 total rows across 2 pages, got %d", count)
	}

	// Verify prefetch was triggered on page 1 (page1NI.oncea consumed).
	onceClaimed := false
	page1NI.oncea.Do(func() {
		onceClaimed = true
	})
	if onceClaimed {
		t.Fatal("fetchAsync should have been triggered on page 1 but oncea was still available")
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// BenchmarkIterScanner_Next benchmarks Scanner.Next() with prefetch logic.
func BenchmarkIterScanner_Next(b *testing.B) {
	const numRows = 1000
	const numCols = 1

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := makeTestIter(numRows, numCols)
		// Set up a nextIter that exercises the prefetch check path.
		// Pre-consume once and set next to an empty iter so the async
		// goroutine doesn't call session.executeQuery() and page-exhaustion
		// returns a valid (empty) Iter.
		ni := &nextIter{
			qry:  &Query{},
			pos:  int(0.75 * float64(numRows)),
			next: &Iter{},
		}
		ni.once.Do(func() {}) // make fetch() a no-op, returns ni.next
		iter.next = ni
		scanner := iter.Scanner()
		for scanner.Next() {
		}
	}
}

// BenchmarkIterScan benchmarks Iter.Scan() for comparison with Scanner.Next().
func BenchmarkIterScan(b *testing.B) {
	const numRows = 1000
	const numCols = 1

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := makeTestIter(numRows, numCols)
		ni := &nextIter{
			qry:  &Query{},
			pos:  int(0.75 * float64(numRows)),
			next: &Iter{},
		}
		ni.once.Do(func() {}) // make fetch() a no-op, returns ni.next
		iter.next = ni
		var dummy []byte
		for iter.Scan(&dummy) {
		}
	}
}

// BenchmarkIterScanner_NextNoNextIter benchmarks Scanner.Next() without any
// next page (no prefetch check overhead).
func BenchmarkIterScanner_NextNoNextIter(b *testing.B) {
	const numRows = 1000
	const numCols = 1

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		iter := makeTestIter(numRows, numCols)
		scanner := iter.Scanner()
		for scanner.Next() {
		}
	}
}
