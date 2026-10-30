/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func BenchmarkV2PagingHistoryLookup(b *testing.B) {
	for _, count := range []int{1, 1024, 65536} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			h := pagingHistory{InternalMaxCount: count, InternalMaxBytes: 16 << 20}
			var state PageState
			for i := 0; i < count; i++ {
				state = RestorePageState(binary.LittleEndian.AppendUint64(nil, uint64(i)))
				if err := h.InternalAdd(state); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if h.InternalAdd(state) != ErrPagingNoProgress {
					b.Fatal("history lost state")
				}
			}
		})
	}
}
func BenchmarkV2WarmPreparedSelection(b *testing.B) {
	s := func() *Session {
		opaqueStorageValue := new(Session)
		implCluster.SessionData(opaqueStorageValue).InternalLogger = nopLogger{}
		return opaqueStorageValue
	}()
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	tracker := runtimeFor(s).InternalSchemaTracker()
	deps := tracker.InternalDependencies("ks", NewColumns(), columns)
	hp := &hostPrepared{InternalEpoch: deps.InternalSnapshot()}
	id := UUID{15: 1}
	state := &preparedState{InternalHosts: map[UUID]*hostPrepared{id: hp}, InternalDependencies: deps}
	conn := &driverConn{InternalHost: func() *HostInfo {
		opaqueStorageValuex := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = id
		return opaqueStorageValuex
	}()}
	rt := runtimeFor(s)
	rt.InternalMu.Lock()
	defer rt.InternalMu.Unlock()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := runtimeFor(s).InternalPrepareOnConn(context.Background(), conn, state, encodedValues{}, false)
		if err != nil || got != hp {
			b.Fatal(err)
		}
	}
}
func BenchmarkV2PageCallerDestination(b *testing.B) {
	type row struct {
		Value   Bytes
		Padding [128]int64
	}
	typ := codecTestMust(ScalarType(TypeBlob))
	columns := NewColumns(ColumnSpec{Name: "value", Type: typ})
	decoder := Row(Column("value", BlobCodec, func(r *row) *Bytes { return &r.Value }))
	for _, fragments := range []int{1, 1024} {
		for _, into := range []bool{false, true} {
			b.Run(fmt.Sprintf("fragments=%d/into=%t", fragments, into), func(b *testing.B) {
				data := binary.BigEndian.AppendUint32(nil, 512000)
				data = append(data, make([]byte, 512000)...)
				spans := make([]wire.Span, 0, fragments)
				for i := 0; i < fragments; i++ {
					lo, hi := len(data)*i/fragments, len(data)*(i+1)/fragments
					spans = append(spans, wire.Borrow(data[lo:hi]))
				}
				chain := codecTestMust(wire.NewChain(spans...))
				owner := wire.NewLease(chain, nil)
				defer owner.Release()
				p := func() *Page[row] {
					opaqueStorageValuexx := new(Page[row])
					implExecution.PageData(opaqueStorageValuexx).InternalResponse = &responsePage{InternalResponseData: responseData{Columns: columns, Rows: chain, RowCount: 1, Owner: owner}}
					implExecution.PageData(opaqueStorageValuexx).InternalPlan = codecTestMust(decoder.Compile(columns))
					implExecution.PageData(opaqueStorageValuexx).InternalCursor = wire.NewCursor(chain)
					return opaqueStorageValuexx
				}()
				cursor := *implExecution.PageData(p).InternalCursor
				var dst row
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					*implExecution.PageData(p).InternalCursor = cursor
					implExecution.PageData(p).InternalIndex = 0
					implExecution.PageData(p).InternalExhausted = false
					if into {
						if !p.NextInto(&dst) {
							b.Fatal(p.Err())
						}
					} else if !p.Next() {
						b.Fatal(p.Err())
					}
				}
			})
		}
	}
}
func BenchmarkV2ResultPlanExactEntry(b *testing.B) {
	columns := NewColumns(ColumnSpec{Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	entry := &typedResultMetadata{InternalColumns: columns}
	response := &responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: columns}}
	cache := &resultPlanCache[Bytes]{}
	decoder := OneColumn(BlobCodec)
	if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cache.InternalSelectPlan(response, decoder); err != nil {
			b.Fatal(err)
		}
	}
}
