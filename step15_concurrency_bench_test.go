// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
)

// The discovery LRU lock stays held throughout warm concurrent bindings and
// host-state selection. Unrelated schema/placement publication runs concurrently;
// neither may cause reprepare or serialize warm calls through the discovery LRU.
func BenchmarkStep15WarmConcurrentBindings(b *testing.B) {
	for _, updates := range []bool{false, true} {
		for _, owned := range []bool{false, true} {
			b.Run(fmt.Sprintf("updates%t/owned%t", updates, owned), func(b *testing.B) {
				s := step06Session(b)
				rt := runtimeFor(s)
				columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
				tracker := rt.InternalSchemaTracker()
				deps := tracker.InternalDependencies("ks", columns, columns)
				hp := &hostPrepared{InternalEpoch: deps.InternalSnapshot()}
				id := UUID{15: 1}
				state := &preparedState{InternalSession: s, InternalProtocol: 4, InternalStatement: "SELECT value FROM ks.tbl WHERE value=?", InternalParameters: columns, InternalColumns: columns, InternalHosts: map[UUID]*hostPrepared{id: hp}, InternalDependencies: deps, InternalInitialEpoch: deps.InternalSnapshot(), InternalInfo: PreparedInfo{Keyspace: "ks", Table: "tbl", PartitionKeyIndexes: []int{0}}}
				prepared := new(Prepared[[]byte, Bytes])
				implExecution.PreparedData(prepared).InternalState = state
				implExecution.PreparedData(prepared).InternalKind = OrdinaryStatement
				implExecution.PreparedData(prepared).InternalParams = codecTestMust(OneParam(BytesEncoder).Compile(columns))
				implExecution.PreparedData(prepared).InternalRows = OneColumn(BlobCodec)
				host := new(HostInfo)
				implEndpoint.HostInfoData(host).InternalHostId = id
				conn := &driverConn{InternalHost: host}
				input := make([]byte, 4096)
				stop := make(chan struct{})
				var updater sync.WaitGroup
				if updates {
					updater.Add(1)
					go func() {
						defer updater.Done()
						for i := uint64(1); ; i++ {
							select {
							case <-stop:
								return
							default:
							}
							implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "other", Object: "unrelated", Target: "TABLE"})
							p := implCluster.InternalSessionPeripheral(s)
							current := s.Metadata()
							info := RoutingInfo{Keyspace: "ks", Table: "table"}
							stamp := implCluster.InternalSessionCaptureTabletFeedback(s, info, current)
							hint := step06Hint(b, 0, 1, Option[uint64]{Set: true, Value: i}, hTarget(1, 0))
							implCluster.InternalSessionPublishTabletFeedback(s, p, map[v2TabletFeedbackKey]v2PendingTabletFeedback{{InternalKeyspace: "ks", InternalTable: "table", InternalEnd: 1}: {InternalHint: hint, InternalStamp: stamp}}, false)
							runtime.Gosched()
						}
					}()
				}
				rt.InternalMu.Lock()
				b.ReportAllocs()
				b.SetBytes(4096)
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						var bound *Bound[Bytes]
						var e error
						if owned {
							bound, e = BindOwned(prepared, input)
						} else {
							bound, e = BindBorrowed(prepared, input)
						}
						if e != nil {
							b.Error(e)
							return
						}
						got, e := rt.InternalPrepareOnConn(context.Background(), conn, state, encodedValues{}, false)
						if e != nil || got != hp {
							b.Error("unrelated reprepare", e)
							return
						}
						bound.Close()
						if e = bound.Wait(context.Background()); e != nil {
							b.Error(e)
							return
						}
					}
				})
				b.StopTimer()
				rt.InternalMu.Unlock()
				close(stop)
				updater.Wait()
			})
		}
	}
}
