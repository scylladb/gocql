// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
	"unsafe"
)

// StreamValidationCache contains only immutable metadata/validation. It cannot
// retain execution contexts, reader scratch, callbacks, or output destinations.
type StreamValidationCache struct {
	columns cqltype.Columns
	plans   []*streamValidationPlan
	limits  wire.StreamLimits
	bytes   int
}

const StreamValidationCacheMaxBytes = 64 << 10

func (c *StreamValidationCache) RetainedBytes() int {
	if c == nil {
		return 0
	}
	return c.bytes
}
func (c *StreamValidationCache) Matches(columns cqltype.Columns, limits wire.StreamLimits) bool {
	if c == nil || c.limits != limits || c.columns.Len() != columns.Len() {
		return false
	}
	current, prior := cqltype.ColumnSlice(columns), cqltype.ColumnSlice(c.columns)
	return len(current) == 0 || &current[0] == &prior[0]
}
func CaptureStreamValidation(r *StreamRowReader) *StreamValidationCache {
	if r == nil || r.scope == nil {
		return nil
	}
	// Include column owners, immutable validation nodes and conservative
	// per-column compiled-plan closure storage; stop before retaining oversize shapes.
	cost := 512 + int(unsafe.Sizeof(StreamValidationCache{})) + r.columns.Len()*(int(unsafe.Sizeof(cqltype.ColumnSpec{}))+int(unsafe.Sizeof(uintptr(0)))+256)
	if cost > StreamValidationCacheMaxBytes {
		return nil
	}
	seen := make(map[*cqltype.DataTypeDescriptor]bool)
	var visit func(cqltype.DataType)
	visit = func(typ cqltype.DataType) {
		if cost > StreamValidationCacheMaxBytes {
			return
		}
		d := cqltype.DescriptorOf(typ)
		if d == nil || seen[d] {
			return
		}
		seen[d] = true
		cost += int(unsafe.Sizeof(*d)) + len(d.Keyspace) + len(d.Name) + len(cqltype.ColumnSlice(d.Fields))*int(unsafe.Sizeof(cqltype.ColumnSpec{}))
		visit(d.Element)
		visit(d.Key)
		for _, field := range cqltype.ColumnSlice(d.Fields) {
			cost += len(field.Keyspace) + len(field.Table) + len(field.Name)
			visit(field.Type)
		}
	}
	for _, column := range cqltype.ColumnSlice(r.columns) {
		cost += len(column.Keyspace) + len(column.Table) + len(column.Name)
		visit(column.Type)
	}
	plans := make(map[*streamValidationPlan]bool)
	var visitPlan func(*streamValidationPlan)
	visitPlan = func(plan *streamValidationPlan) {
		if plan == nil || plans[plan] || cost > StreamValidationCacheMaxBytes {
			return
		}
		plans[plan] = true
		cost += int(unsafe.Sizeof(*plan)) + len(plan.fields)*int(unsafe.Sizeof(uintptr(0)))
		if plan.children != nil {
			cost += 512 + len(plan.children)*64
		}
		visitPlan(plan.element)
		visitPlan(plan.key)
		for _, field := range plan.fields {
			visitPlan(field)
		}
		for _, child := range plan.children {
			visitPlan(child)
		}
	}
	for _, plan := range r.validations {
		visitPlan(plan)
	}
	if cost > StreamValidationCacheMaxBytes {
		return nil
	}
	return &StreamValidationCache{columns: r.columns, plans: r.validations, limits: r.scope.Limits(), bytes: cost}
}
func NewStreamRowReaderUsingValidation(columns cqltype.Columns, scope *wire.Stream, cache *StreamValidationCache) (*StreamRowReader, error) {
	if scope == nil || !cache.Matches(columns, scope.Limits()) {
		return NewStreamRowReader(columns, scope)
	}
	return &StreamRowReader{columns: columns, scope: scope, shared: &streamDecodeState{discardBytes: scope.Limits().DiscardBytes}, validations: cache.plans, active: true}, nil
}
