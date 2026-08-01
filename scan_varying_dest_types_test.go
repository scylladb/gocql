//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"testing"
)

// TestScanVaryingDestTypesOnSameIter verifies the fix for a JIT-decoder
// regression where Iter.rowDecoder was compiled once (from the first Scan
// call's destination types) and blindly reused for every later Scan on the
// same Iter — even if a caller varied destination types between rows.
// Before the fix, this could panic (a decoder for *int32 receiving an
// *int64) or silently misdecode; the generic (pre-JIT) Unmarshal path never
// had this restriction, since it re-derives behavior from each value's
// concrete type on every call. ensureRowDecoderFor now checks and
// recompiles when the shape changes, restoring that guarantee.
//
// Confirmed this test fails (panics in decodeIntToInt32) without the fix by
// temporarily reverting ensureRowDecoderFor to its unguarded form.
func TestScanVaryingDestTypesOnSameIter(t *testing.T) {
	session := createSession(t)
	defer session.Close()

	table := testTableName(t, "scan_varying_dest")
	if err := createTable(session, fmt.Sprintf(
		"CREATE TABLE gocql_test.%s (pk int PRIMARY KEY, val int)", table)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		if err := session.Query(fmt.Sprintf(
			"INSERT INTO gocql_test.%s (pk, val) VALUES (?, ?)", table), i, i*100).Exec(); err != nil {
			t.Fatal(err)
		}
	}

	iter := session.Query(fmt.Sprintf("SELECT pk, val FROM gocql_test.%s", table)).Iter()

	// Warm iter.rowDecoder against one destination-type shape.
	var pk1 int32
	var val1 int32
	if !iter.Scan(&pk1, &val1) {
		t.Fatalf("expected a row, iter.Close() err: %v", iter.Close())
	}

	// Now scan the next row of the SAME Iter into a different Go type for
	// the same CQL int column. Pre-fix, this either panics (type assertion
	// on the stale int32-compiled decoder) or produces wrong data; post-fix,
	// ensureRowDecoderFor recompiles for the new shape.
	var pk2 int64
	var val2 int64
	if !iter.Scan(&pk2, &val2) {
		t.Fatalf("expected a second row, iter.Close() err: %v", iter.Close())
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("iter.Close(): %v", err)
	}

	// Row order for a single-partition-per-pk table isn't guaranteed, but
	// each row's own (pk, val) pair must be internally consistent
	// (val == pk*100) regardless of which row landed in which scan.
	if val1 != int32(pk1)*100 {
		t.Errorf("row 1: pk=%d val=%d, want val=%d", pk1, val1, int32(pk1)*100)
	}
	if val2 != int64(pk2)*100 {
		t.Errorf("row 2: pk=%d val=%d, want val=%d", pk2, val2, int64(pk2)*100)
	}
}
