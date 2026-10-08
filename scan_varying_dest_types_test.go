//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"testing"
)

// TestScanVaryingDestTypesOnSameIter verifies a caller may vary destination
// types between rows of one Iter: the JIT decoder is compiled per dest shape,
// so reusing a stale one panics or misdecodes. The pre-JIT Unmarshal path had
// no such restriction, and Scan's contract still doesn't.
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

	// Same CQL int column, different Go type: must recompile, not reuse.
	var pk2 int64
	var val2 int64
	if !iter.Scan(&pk2, &val2) {
		t.Fatalf("expected a second row, iter.Close() err: %v", iter.Close())
	}

	if err := iter.Close(); err != nil {
		t.Fatalf("iter.Close(): %v", err)
	}

	// Row order isn't guaranteed, but each row's own (pk, val) must be consistent.
	if val1 != int32(pk1)*100 {
		t.Errorf("row 1: pk=%d val=%d, want val=%d", pk1, val1, int32(pk1)*100)
	}
	if val2 != int64(pk2)*100 {
		t.Errorf("row 2: pk=%d val=%d, want val=%d", pk2, val2, int64(pk2)*100)
	}
}
