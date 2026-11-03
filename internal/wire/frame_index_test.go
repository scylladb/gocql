// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package wire

import "testing"

func TestFrameIndexClearsAllBorrowedReferences(t *testing.T) {
	var builder Builder
	var index FrameIndex
	builder.BeginReusableScalar(nil, 256)
	for range 32 {
		if err := builder.Uint32(1); err != nil {
			t.Fatal(err)
		}
		if err := builder.Borrow(Borrow([]byte("payload"))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := builder.FreezeWithIndex(&index); err != nil {
		t.Fatal(err)
	}
	index.Clear()
	builder.RetireFrameReferences()
	for _, entry := range index.tail[:cap(index.tail)] {
		if entry.span.Len() != 0 {
			t.Fatal("idle index retained payload")
		}
	}
	for _, entry := range builder.parts[:cap(builder.parts)] {
		if entry.span.Len() != 0 {
			t.Fatal("idle builder retained payload")
		}
	}
}
