//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/maphash"
	"sync/atomic"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implCodec "github.com/scylladb/gocql/v2/internal/codec"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/routing"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestStep15HostWorkCounts(t *testing.T) {
	for _, hosts := range []int{3, 30, 300} {
		for _, kind := range []string{"ordinary", "pinned", "conditional", "leader", "latency", "fallback"} {
			t.Run(fmt.Sprintf("hosts%d/%s", hosts, kind), func(t *testing.T) {
				m := v2TestMetadata()
				storage := routing.SnapshotData(m)
				storage.InternalHosts = make([]HostSnapshot, hosts)
				for i := range storage.InternalHosts {
					h := v2TestHost(0, "a", "r1")
					binary.BigEndian.PutUint64(h.ID[8:], uint64(i+1))
					storage.InternalHosts[i] = h
				}
				storage.InternalHostIndex = buildRoutingHostIndex(storage.InternalHosts)
				hint := step06Hint(t, 0, 100, Option[uint64]{Set: true, Value: 1}, hTarget(1, 0), hTarget(2, 0), hTarget(3, 0))
				root, _ := updateTabletRange(nil, hint)
				storage.InternalPlacements = placementPut(storage.InternalPlacements, metadataTable{InternalKeyspace: "ks", InternalTable: "table"}, tabletPlacement{InternalRanges: root}, comparePlacementTable)
				info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true, Value: 1}, Consistency: Quorum, Kind: OrdinaryStatement}
				opts := RoutingOptions{TokenAware: true, Datacenter: "a", Rack: "r1", LeaderAware: kind == "leader"}
				if kind == "pinned" {
					info.HostID = Option[UUID]{Set: true, Value: storage.InternalHosts[0].ID}
				}
				if kind == "conditional" {
					info.Conditional = true
				}
				if kind == "leader" {
					storage.InternalKeyspaces["ks"].ConsistencyMode = "global"
				}
				if kind == "fallback" {
					for i := 0; i < 3; i++ {
						storage.InternalHosts[i].Connected = false
					}
				}
				if kind == "latency" {
					opts.Latency.Enabled = true
				}
				policy := codecTestMust(NewRoutingPolicy(opts))
				if kind == "latency" {
					policy.(*routingPolicy).InternalDecisions.Store(&routingDecisions{InternalSlow: map[UUID]time.Time{storage.InternalHosts[2].ID: time.Now().Add(time.Hour)}})
				}
				routing.ResetPerfCounters()
				plan := policy.Plan(info, m)
				target, ok := plan.Next()
				if !ok {
					t.Fatal("no target")
				}
				c := routing.ReadPerfCounters()
				if c.HostIndexVisits != 0 || kind != "fallback" && c.FallbackVisits != 0 || kind != "fallback" && c.HostLookups > 3 {
					t.Fatal(c)
				}
				t.Logf("first=%v counters=%+v", target, c)
			})
		}
	}
}
func TestStep15HistoryCounts(t *testing.T) {
	for _, count := range []int{1, 1000, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			h := pagingHistory{InternalMaxCount: count + 4, InternalMaxBytes: 1 << 20}
			tokens := make([]PageState, count)
			for i := range tokens {
				tokens[i] = model.PageStateOwned(binary.BigEndian.AppendUint64(nil, uint64(i)), 1)
				if e := h.InternalAdd(tokens[i]); e != nil {
					t.Fatal(e)
				}
			}
			for _, index := range []int{0, count - 1, count / 2} {
				paging.ResetPerfCounters()
				if h.InternalAdd(tokens[index]) != ErrPagingNoProgress {
					t.Fatal("cycle")
				}
				c := paging.ReadPerfCounters()
				if c.HashBytes != 8 || c.EqualityChecks != 1 || c.Insertions != 0 {
					t.Fatal(c)
				}
				t.Logf("history=%d token=%d counts=%+v", count, index, c)
			}
			empty := model.PageStateOwned([]byte{}, 1)
			if e := h.InternalAdd(empty); e != nil {
				t.Fatal(e)
			}
			if h.InternalAdd(empty) != ErrPagingNoProgress {
				t.Fatal("empty")
			}
			collision := model.PageStateOwned([]byte("collision"), 1)
			hash := maphash.Bytes(h.InternalSeed, model.PageToken(collision))
			h.InternalEntries[hash] = pagingHistoryEntry{InternalState: tokens[0]}
			paging.ResetPerfCounters()
			if e := h.InternalAdd(collision); e != nil {
				t.Fatal(e)
			}
			if h.InternalAdd(collision) != ErrPagingNoProgress {
				t.Fatal("collision equality")
			}
			c := paging.ReadPerfCounters()
			if c.HashBytes != 18 || c.EqualityChecks != 2 || c.Insertions != 1 {
				t.Fatal(c)
			}
			entry := h.InternalEntries[hash]
			if &model.PageToken(entry.InternalState)[0] != &model.PageToken(collision)[0] {
				t.Fatal("copied history token")
			}
			t.Logf("collision=%+v token_payload_bytes=%d", c, h.InternalBytes)
		})
	}
}
func TestStep15WarmAndChangedPlans(t *testing.T) {
	cols := NewColumns(ColumnSpec{Name: "v", Type: codecTestMust(ScalarType(TypeBlob))})
	entry := &typedResultMetadata{InternalColumns: cols}
	response := &responsePage{InternalMetadata: entry, InternalResponseData: responseData{Columns: cols}}
	cache := &resultPlanCache[Bytes]{}
	var calls atomic.Int32
	decoder := step03CountingDecoder{calls: &calls}
	if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
		t.Fatal(e)
	}
	implCodec.ResetPerfCounters()
	for i := 0; i < 10000; i++ {
		if _, e := cache.InternalSelectPlan(response, decoder); e != nil {
			t.Fatal(e)
		}
	}
	comp, maps := implCodec.ReadPerfCounters()
	if comp != 0 || maps != 0 || calls.Load() != 1 || implCodec.ReadPerfTypeChecks() != 0 {
		t.Fatal(comp, maps, calls.Load())
	}
	changed := NewColumns(ColumnSpec{Name: "changed", Type: codecTestMust(ScalarType(TypeBlob))})
	second := &responsePage{InternalMetadata: &typedResultMetadata{InternalColumns: changed}, InternalResponseData: responseData{Columns: changed}}
	if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 10000; i++ {
		if _, e := cache.InternalSelectPlan(second, decoder); e != nil {
			t.Fatal(e)
		}
	}
	if calls.Load() != 2 || implCodec.ReadPerfTypeChecks() != 1 {
		t.Fatal("changed entry compiled repeatedly", calls.Load())
	}
	t.Logf("warm pages=10000 compiles=0 name_maps=0 changed_candidate_compiles=%d", calls.Load()-1)
}

type step15Observer struct{ requests, attempts atomic.Int64 }

func (o *step15Observer) ObserveRequest(RequestEvent) { o.requests.Add(1) }
func (o *step15Observer) ObserveAttempt(AttemptEvent) { o.attempts.Add(1) }

type step15Speculation struct{}

func (step15Speculation) Delay(int, MetricsSnapshot) (time.Duration, bool) { return time.Hour, true }
func TestStep15OptionalWorkCounts(t *testing.T) {
	for _, mode := range []string{"disabled", "enabled", "default"} {
		enabled := mode == "enabled"
		t.Run(mode, func(t *testing.T) {
			s, conns, peers := runtimeTestSession(t, 1)
			observer := new(step15Observer)
			profile := defaultProfile()
			profile.Prefetch = 0
			if enabled {
				profile.Speculation = step15Speculation{}
			}
			handle := codecTestMust(NewProfile(profile))
			options := Options{Profile: handle, Idempotent: true}
			if mode == "default" {
				options = Options{}
			}
			if enabled {
				options.Observer = observer
			}
			bound := runtimeQuery(t)
			defer bound.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				for page := 0; page < 2; page++ {
					id := typedTestReadRequest(t, conns[0], peers[0])
					var state []byte
					if page == 0 {
						state = []byte("next")
					}
					peers[0].Write(typedTestResponse(t, 4, id, runtimeRowsBody(state, "value")))
				}
			}()
			implExecution.ResetPerfCounters()
			wire.ResetPerfCounters()
			rows := codecTestMust(Run(context.Background(), s, bound, options))
			for rows.Next() {
			}
			if e := rows.Err(); e != nil {
				t.Fatal(e)
			}
			rows.Close()
			<-done
			c := implExecution.ReadPerfCounters()
			if !enabled && (c.RequestEvents != 0 || c.AttemptCallbacks != 0 || c.SpeculationSnapshots != 0 || c.SpeculationTimers != 0 || mode != "default" && c.PrefetchWorkers != 0) {
				t.Fatal(c)
			}
			if enabled && (c.RequestEvents != 1 || c.AttemptCallbacks != 2 || c.SpeculationSnapshots != 2 || c.SpeculationTimers != 2 || observer.requests.Load() != 1 || observer.attempts.Load() != 2) {
				t.Fatal(c)
			}
			if mode == "default" && c.PrefetchWorkers != 1 {
				t.Fatal("default prefetch demand", c)
			}
			t.Logf("mode=%s scheduler=%+v owner_transfers=%+v", mode, c, wire.ReadPerfCounters())
		})
	}
	// Warm prepared lookup remains usable while the discovery LRU lock is held;
	// unrelated schema events leave the captured table generation current.
	s := new(Session)
	implCluster.SessionData(s).InternalLogger = nopLogger{}
	cols := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "v", Type: codecTestMust(ScalarType(TypeBlob))})
	_, hp := step03PreparedState(t, s, cols)
	for i := 0; i < 1000; i++ {
		implCluster.InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: "unrelated", Object: fmt.Sprint(i), Target: "TABLE"})
	}
	if !hp.InternalEpoch.InternalCurrent() {
		t.Fatal("unrelated invalidation")
	}
}

// All workers retain exactly one old page and consume two fresh pages. Ordinary
// row views add no owner transfers. Prefetch is one demand-bounded extra page.
func TestStep15RetainedPrefetchCharges(t *testing.T) {
	for _, workers := range []int{1, 4} {
		for _, prefetch := range []int{0, 1} {
			for _, cells := range []int{1, 1024} {
				t.Run(fmt.Sprintf("workers%d/prefetch%d/cells%d", workers, prefetch, cells), func(t *testing.T) {
					wire.ResetPerfCounters()
					wire.ResetPerfMemoryCounters()
					implExecution.ResetPerfCounters()
					errors := make(chan error, workers)
					done := make(chan struct{})
					var finished atomic.Int32
					for worker := 0; worker < workers; worker++ {
						go func() {
							s, cs, ps := runtimeTestSession(t, 1)
							bound := runtimeQuery(t)
							defer bound.Close()
							profile := defaultProfile()
							profile.Prefetch = prefetch
							options := Options{Profile: codecTestMust(NewProfile(profile))}
							values := make([]string, cells)
							for i := range values {
								values[i] = "q"
							}
							peerDone := make(chan struct{})
							go func() {
								defer close(peerDone)
								for page := 0; page < 3; page++ {
									id := typedTestReadRequest(t, cs[0], ps[0])
									var token []byte
									if page < 2 {
										token = []byte{byte(page)}
									}
									ps[0].Write(typedTestResponse(t, 4, id, runtimeRowsBody(token, values...)))
								}
							}()
							rows, e := Run(context.Background(), s, bound, options)
							if e != nil {
								errors <- e
								return
							}
							if !rows.Next() {
								errors <- rows.Err()
								return
							}
							view := rows.Row()
							lease, e := rows.Retain()
							if e != nil {
								errors <- e
								return
							}
							count := 1
							for rows.Next() {
								count++
							}
							if rows.Err() != nil || count != 3*cells {
								errors <- fmt.Errorf("count %d err %v", count, rows.Err())
								return
							}
							rows.Close()
							if value, e := view.At(0); e != nil || value != 'q' {
								errors <- fmt.Errorf("retained page changed %d %v", value, e)
								return
							}
							lease.Close()
							<-peerDone
							errors <- nil
							if finished.Add(1) == int32(workers) {
								close(done)
							}
						}()
					}
					for i := 0; i < workers; i++ {
						if e := <-errors; e != nil {
							t.Fatal(e)
						}
					}
					<-done
					live, peak, slab := wire.ReadPerfMemoryCounters()
					c := wire.ReadPerfCounters()
					scheduler := implExecution.ReadPerfCounters()
					if live != 0 || c.Retains != int64(4*workers) {
						t.Fatal("per-cell owner churn or leak", live, c)
					}
					// Three received pages per worker plus one retained handle; prefetch never
					// exceeds the same total received pages and cannot pin arbitrary demand.
					maxBytes := int64(workers * 3 * (cells*5 + 128))
					if peak > maxBytes {
						t.Fatal("unbounded leased charge", peak, maxBytes)
					}
					if scheduler.PrefetchWorkers != int64(prefetch*workers) {
						t.Fatal(scheduler)
					}
					t.Logf("live_lease_charge=%d peak_lease_charge=%d scalar_slab_allocated=%d transfers=%+v optional=%+v", live, peak, slab, c, scheduler)
				})
			}
		}
	}
}
