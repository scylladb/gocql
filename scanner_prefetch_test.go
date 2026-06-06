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

	// Create a nextIter with once pre-consumed and a dummy next so the async
	// goroutine (if launched) returns immediately without a nil session panic.
	ni := &nextIter{
		qry:  &Query{},
		pos:  3, // trigger at pos >= 3
		next: &Iter{},
	}
	ni.once.Do(func() {}) // make fetch() a no-op
	iter.next = ni

	scanner := iter.Scanner()

	// Consume rows 0, 1, 2 — prefetch should NOT have fired yet.
	for i := 0; i < 3; i++ {
		if !scanner.Next() {
			t.Fatalf("expected Next() to return true on row %d", i)
		}
	}

	// Consume row 3 — at this point pos=3 >= next.pos=3, so fetchAsync fires.
	if !scanner.Next() {
		t.Fatal("expected Next() to return true on row 3")
	}

	// fetchAsync calls oncea.Do synchronously, so after Next() returns oncea
	// is consumed if the trigger fired. Verify by trying to execute via oncea:
	// if our callback runs, oncea was still available (trigger did NOT fire).
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
		qry:  &Query{},
		pos:  7,
		next: &Iter{},
	}
	ni.once.Do(func() {}) // prevent panic if fetchAsync fires (it shouldn't in this test)
	iter.next = ni

	scanner := iter.Scanner()

	// Consume 7 rows (pos goes 0..6, checked before increment).
	// On the 7th call, pos=6 before check, which is < 7, so no trigger.
	for i := 0; i < 7; i++ {
		if !scanner.Next() {
			t.Fatalf("expected Next() to return true on row %d", i)
		}
	}

	// oncea should still be unclaimed (trigger fires at pos >= 7, and pos was
	// 0..6 during each prefetch check above). This probe consumes oncea, so
	// it must be the last oncea operation in this test.
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
	// - iterScanner.Next() checks pos >= 6 BEFORE reading columns, BEFORE pos++
	// Both should trigger on the 7th call (when pos=6).
	//
	// We must probe oncea only once per sub-iterator (after all calls),
	// because sync.Once.Do() permanently consumes the Once. Using a fresh
	// iterator per candidate row count avoids self-consumption.

	findScannerTriggerRow := func() int {
		for k := 0; k < 8; k++ {
			iter := makeTestIter(8, 1)
			ni := &nextIter{
				qry:  &Query{},
				pos:  6,
				next: &Iter{},
			}
			ni.once.Do(func() {})
			iter.next = ni
			sc := iter.Scanner()
			for i := 0; i <= k; i++ {
				sc.Next()
			}
			claimed := false
			ni.oncea.Do(func() { claimed = true })
			if !claimed {
				return k
			}
		}
		return -1
	}

	findScanTriggerRow := func() int {
		for k := 0; k < 8; k++ {
			iter := makeTestIter(8, 1)
			ni := &nextIter{
				qry:  &Query{},
				pos:  6,
				next: &Iter{},
			}
			ni.once.Do(func() {})
			iter.next = ni
			var dummy []byte
			for i := 0; i <= k; i++ {
				iter.Scan(&dummy)
			}
			claimed := false
			ni.oncea.Do(func() { claimed = true })
			if !claimed {
				return k
			}
		}
		return -1
	}

	scannerTriggerRow := findScannerTriggerRow()
	scanTriggerRow := findScanTriggerRow()

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
		b.StopTimer()
		iter := makeTestIter(numRows, numCols)
		ni := &nextIter{
			qry:  &Query{},
			pos:  int(0.75 * float64(numRows)),
			next: &Iter{},
		}
		ni.once.Do(func() {})
		iter.next = ni
		scanner := iter.Scanner()
		b.StartTimer()
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
		b.StopTimer()
		iter := makeTestIter(numRows, numCols)
		ni := &nextIter{
			qry:  &Query{},
			pos:  int(0.75 * float64(numRows)),
			next: &Iter{},
		}
		ni.once.Do(func() {})
		iter.next = ni
		b.StartTimer()
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
		b.StopTimer()
		iter := makeTestIter(numRows, numCols)
		scanner := iter.Scanner()
		b.StartTimer()
		for scanner.Next() {
		}
	}
}
