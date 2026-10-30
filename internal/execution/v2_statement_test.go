// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import . "github.com/scylladb/gocql/v2"

import implExecution "github.com/scylladb/gocql/v2/internal/execution"

import (
	"bytes"
	"context"
	"testing"
)

type countingBlobEncoder struct{ calls *int }

func (c countingBlobEncoder) CheckType(t DataType) error { return BytesEncoder.CheckType(t) }
func (c countingBlobEncoder) Encode(w *ValueWriter, b []byte) error {
	*c.calls++
	return BytesEncoder.Encode(w, b)
}
func TestV2RuntimeBindCanonicalOwnership(t *testing.T) {
	typ, _ := ScalarType(TypeBlob)
	columns := NewColumns(ColumnSpec{Name: "v", Type: typ})
	calls := 0
	statement := Statement[[]byte, Unit]{CQL: "INSERT INTO ks.tbl(v) VALUES (?)", Params: OneParam[[]byte](countingBlobEncoder{calls: &calls}), Rows: NoRows}
	input := []byte("original")
	borrowed, err := BindUnpreparedBorrowed(statement, columns, input)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := BindUnpreparedOwned(statement, columns, input)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("encoder calls %d", calls)
	}
	input[0] = 'X'
	borrowedCell := implExecution.BoundData(borrowed).InternalRequest.InternalValues.Cells[0].Body
	ownedCell := implExecution.BoundData(owned).InternalRequest.InternalValues.Cells[0].Body
	a := make([]byte, borrowedCell.Len())
	b := make([]byte, ownedCell.Len())
	borrowedCell.CopyTo(a)
	ownedCell.CopyTo(b)
	if !bytes.Equal(a, input) || string(b) != "original" {
		t.Fatalf("borrowed=%q owned=%q", a, b)
	}
	for _, bound := range []*Bound[Unit]{borrowed, owned} {
		bound.Close()
		if err := bound.Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
