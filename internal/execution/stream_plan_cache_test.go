// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type opaqueCachedRowPlan struct{ StreamRowPlan[int32] }

func TestStreamPlanCacheKeepsOnlyCurrentOpaquePlan(t *testing.T) {
	typ, err := cqltype.ScalarType(cqltype.TypeInt)
	if err != nil {
		t.Fatal(err)
	}
	columns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	scope, err := wire.NewStream(context.Background(), wire.NewCursor(wire.Chain{}), 0, wire.StreamLimits{})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := codec.NewStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	validation := codec.CaptureStreamValidation(reader)
	plan, err := codec.StreamOneColumn(codec.StreamInt32).Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	var cache streamPlanCache[int32]
	first := &opaqueCachedRowPlan{plan}
	cache.put(validation, first)
	got, opaque := cache.find(columns, scope.Limits())
	if got != validation || opaque != first {
		t.Fatal("current custom plan was recompiled or immutable validation was lost")
	}
	otherColumns := cqltype.NewColumns(cqltype.ColumnSpec{Name: "value", Type: typ})
	reader, err = codec.NewStreamRowReader(otherColumns, scope)
	if err != nil {
		t.Fatal(err)
	}
	otherValidation := codec.CaptureStreamValidation(reader)
	second := &opaqueCachedRowPlan{plan}
	cache.put(otherValidation, second)
	got, opaque = cache.find(columns, scope.Limits())
	if got != validation || opaque != nil {
		t.Fatal("an older custom plan survived metadata replacement")
	}
	got, opaque = cache.find(otherColumns, scope.Limits())
	if got != otherValidation || opaque != second {
		t.Fatal("current custom plan was not reusable")
	}
	cache.put(otherValidation, plan)
	got, cached := cache.find(otherColumns, scope.Limits())
	if got != otherValidation || cached == nil || cache.opaquePlan != nil || cache.retainedBytes > codec.StreamValidationCacheMaxBytes {
		t.Fatal("bounded built-in plan was not retained")
	}
	cache.put(validation, first)
	cache.put(nil, plan)
	if cache.opaquePlan != nil || cache.opaqueValidation != nil {
		t.Fatal("uncacheable metadata retained an obsolete custom plan")
	}
}
