// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStreamPageValidationOwnership(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: integer})
	data := StreamCell([]byte{0, 0, 0, 7})
	prior, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
	plan := CodecTestMust(StreamOneColumn(StreamInt32).Compile(columns))
	var old int32
	if err := plan.DecodeInto(prior, &old); err != nil || old != 7 {
		t.Fatal(old, err)
	}
	prior.shared.fail(errors.New("previous page failure"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	scope := CodecTestMust(wire.NewStream(ctx, bytes.NewReader(data), int64(len(data)), wire.StreamLimits{DiscardBytes: 17}))
	next := CodecTestMust(NewStreamRowReaderReusingValidation(columns, scope, prior))
	if next == prior || next.shared == prior.shared || next.scope != scope || next.Context() != ctx {
		t.Fatal("new page inherited mutable reader state")
	}
	if next.validations[0] != prior.validations[0] || &next.validations[0] != &prior.validations[0] {
		t.Fatal("identical immutable metadata did not share validation storage")
	}
	if next.shared.discardBytes != 17 {
		t.Fatal("previous page limits inherited", next.shared.discardBytes)
	}
	// Releasing the old reader must not invalidate the new page's plans.
	*prior = StreamRowReader{}
	var value int32
	if err := plan.DecodeInto(next, &value); err != nil || value != 7 {
		t.Fatal(value, err)
	}
	if err := next.RequireEnd(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := next.check(); !errors.Is(err, context.Canceled) {
		t.Fatal("new page context not enforced", err)
	}
	if _, err := NewStreamRowReaderReusingValidation(columns, nil, next); err == nil {
		t.Fatal("nil page scope accepted")
	}
}

func TestStreamPageValidationMetadataReplacement(t *testing.T) {
	boolean := FoundationScalar(t, cqltype.TypeBoolean)
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: boolean})
	prior, _ := streamReaderFixture(t, columns, bytes.NewReader(nil), 0)
	for _, candidate := range []struct {
		name    string
		columns cqltype.Columns
	}{
		{"equal-new-backing", cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: boolean})},
		{"equal-new-descriptor", cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: FoundationScalar(t, cqltype.TypeBoolean)})},
		{"changed-type", cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: FoundationScalar(t, cqltype.TypeInt)})},
		{"changed-name", cqltype.NewColumns(cqltype.ColumnSpec{Name: "new", Type: boolean})},
	} {
		t.Run(candidate.name, func(t *testing.T) {
			scope := CodecTestMust(wire.NewStream(context.Background(), bytes.NewReader(nil), 0, wire.StreamLimits{}))
			next := CodecTestMust(NewStreamRowReaderReusingValidation(candidate.columns, scope, prior))
			if next.validations[0] == prior.validations[0] {
				t.Fatal("replacement metadata kept old validation descriptors")
			}
			if cqltype.DescriptorOf(next.validations[0].typ) != cqltype.DescriptorOf(cqltype.ColumnSlice(candidate.columns)[0].Type) {
				t.Fatal("validation did not use exact new metadata")
			}
		})
	}
}

func TestStreamPageValidationNestedMalformedAndIndependentReaders(t *testing.T) {
	boolean := FoundationScalar(t, cqltype.TypeBoolean)
	list := CodecTestMust(cqltype.ListType(boolean))
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: list})
	valid := streamEncodedFixture(t, list, ListEncoder(Bool), []bool{true, false})
	invalid := append([]byte(nil), valid...)
	invalid[len(invalid)-1] = 2
	prior, _ := streamReaderFixture(t, columns, bytes.NewReader(valid), len(valid))
	// Each reader has its own mutable skip scratch, cursor and sticky error.
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, data := range [][]byte{valid, invalid, valid} {
				scope, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{DiscardBytes: 3})
				if err != nil {
					t.Error(err)
					return
				}
				next, err := NewStreamRowReaderReusingValidation(columns, scope, prior)
				if err != nil {
					t.Error(err)
					return
				}
				err = next.SkipColumn()
				if (err != nil) != (data[len(data)-1] == 2) {
					t.Error("nested boolean validation changed", err)
					return
				}
				if err == nil {
					if err := next.RequireEnd(); err != nil {
						t.Error(err)
					}
					if err := scope.RequireEnd(); err != nil {
						t.Error(err)
					}
				}
			}
		}()
	}
	wg.Wait()
	if prior.shared.err != nil || prior.index != 0 {
		t.Fatal("independent readers mutated prior page")
	}
}

func TestStreamPageValidationUsesNewEnvelopeLimits(t *testing.T) {
	integer := FoundationScalar(t, cqltype.TypeInt)
	list := CodecTestMust(cqltype.ListType(integer))
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "v", Type: list})
	data := streamEncodedFixture(t, list, ListEncoder(Int32), []int32{1, 2})
	prior, _ := streamReaderFixture(t, columns, bytes.NewReader(data), len(data))
	if err := prior.SkipColumn(); err != nil {
		t.Fatal(err)
	}
	for _, limits := range []wire.StreamLimits{
		{MaxDepth: 1},
		{MaxCount: 1},
	} {
		scope := CodecTestMust(wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), limits))
		next := CodecTestMust(NewStreamRowReaderReusingValidation(columns, scope, prior))
		if err := next.SkipColumn(); !errors.Is(err, wire.ErrLimit) {
			t.Fatal("shared validation bypassed stricter envelope limit", limits, err)
		}
	}
	if _, err := wire.NewStream(context.Background(), bytes.NewReader(data), int64(len(data)), wire.StreamLimits{MaxBytes: int64(len(data) - 1)}); !errors.Is(err, wire.ErrLimit) {
		t.Fatal("new envelope byte limit bypassed", err)
	}
}
