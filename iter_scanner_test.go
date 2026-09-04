//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

import (
	"context"
	"testing"

	"github.com/gocql/gocql/internal/tests/mock"
)

// TestIterScannerColumnCountChangeAcrossPageTurn covers a schema change landing
// on a page boundary mid-iteration (RESULT_METADATA_CHANGED): iterScanner.cols
// is sized once, when Scanner() is called, but the column count a row must be
// read with comes from iter.meta, which copyPageData replaces when the next
// page is fetched. Left unresized, Next() reads the wrong number of columns off
// the framer — desyncing every later row — and Scan() then indexes cols by the
// new, longer column list.
func TestIterScannerColumnCountChangeAcrossPageTurn(t *testing.T) {
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

	// The first page carries one row of the old schema. The next page is served
	// by a real fetch through iter.next, whose response installs the changed
	// metadata (extra column) and a further row via copyPageData.
	firstFramer := &mock.MockFramer{Data: [][]byte{marshalInt(1)}}
	nextFramer := &mock.MockFramer{Data: [][]byte{
		marshalInt(2), marshalInt(3),
		marshalInt(4), marshalInt(5),
	}}

	conn := &pagingTestConn{
		executeQueryFunc: func(_ context.Context, _ *Query) *Iter {
			return &Iter{
				meta:    twoColumns,
				framer:  nextFramer,
				numRows: 2,
			}
		},
	}

	baseQry := newWarningTestQuery()
	baseQry.conn = conn

	iter := &Iter{
		meta:    oneColumn,
		framer:  firstFramer,
		numRows: 1,
		next:    newNextIter(baseQry, 1),
	}
	defer iter.Close()

	scanner := iter.Scanner()

	if !scanner.Next() {
		t.Fatalf("expected the first row, err: %v", scanner.Err())
	}
	var a int32
	if err := scanner.Scan(&a); err != nil {
		t.Fatalf("first row: %v", err)
	}
	if a != 1 {
		t.Fatalf("first row: got a=%d, want 1", a)
	}

	// The page turn: the first page is exhausted, so the second Next() fetches
	// the next page. Its changed metadata with an extra column is installed by
	// copyPageData, and Next() must resize cols to match before reading.
	if !scanner.Next() {
		t.Fatalf("expected the second row, err: %v", scanner.Err())
	}
	var b, c int32
	if err := scanner.Scan(&b, &c); err != nil {
		t.Fatalf("second row: %v", err)
	}
	if b != 2 || c != 3 {
		t.Fatalf("second row: got b=%d c=%d, want 2 and 3", b, c)
	}

	// A further row on the same (resized) page must read correctly too, proving
	// the resize held and the scanner did not desync from the new schema.
	if !scanner.Next() {
		t.Fatalf("expected the third row, err: %v", scanner.Err())
	}
	var d, e int32
	if err := scanner.Scan(&d, &e); err != nil {
		t.Fatalf("third row: %v", err)
	}
	if d != 4 || e != 5 {
		t.Fatalf("third row: got d=%d e=%d, want 4 and 5", d, e)
	}
}
