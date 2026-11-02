//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type step15ConcurrentMemory struct {
	IdleCacheBytes, IdleCacheObjects, ParkedCacheBytes, ParkedCacheObjects, PostCloseCacheBytes, PostCloseCacheObjects                                  int
	API                                                                                                                                                 string
	Workers, Rows, Cell, Page                                                                                                                           int
	RuntimeHeapBeforeConnect, FixedSessionHeap, IdleHeapWithApplication, AllParkedPostGCHeap, AllParkedStackBytes, PeakSampledHeap, PostClosePostGCHeap uint64
	ApplicationStorageBytes, InclusiveDriverAndHarnessBytes, IncrementalOverWarmIdleBytes                                                               int64
	WorkerAllocatedBytes, WorkerAllocations                                                                                                             uint64
}

// This diagnostic is deliberately separate from the unchanged normal latency,
// CPU and baseline harness. Every worker is parked after its first complete
// validated final output while its cursor still owns its current response. A
// fail-safe context releases and joins every worker before owned schema cleanup.
func TestStep15ConcurrentLiveMemory(t *testing.T) {
	out := os.Getenv("GOCQL_STEP15_OUTPUT")
	if out == "" {
		t.Skip("explicit performance diagnostic runner only")
	}
	if e := os.MkdirAll(out, 0755); e != nil {
		t.Fatal(e)
	}
	runtime.GC()
	initial := step15Mem().HeapAlloc
	config := createV2Cluster(func(c *Config) {
		c.Pool.ConnectionsPerHost = 8
		c.KeepaliveInterval = 0
		c.Metadata.PeriodicRefresh = 0
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s, e := Connect(ctx, config)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	ks := testKeyspaceName(t, fmt.Sprintf("%x", time.Now().UnixNano()))
	exec := func(cql string) {
		t.Helper()
		b := codecTestMust(BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}))
		defer b.Close()
		if _, e := Exec(ctx, s, b, Options{}); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE KEYSPACE " + ks + " WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1}")
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		b, e := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: "DROP KEYSPACE " + ks, Kind: OrdinaryStatement, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
		if e == nil {
			defer b.Close()
			_, e = Exec(cleanup, s, b, Options{})
		}
		if e != nil {
			t.Error(e)
		}
	}()
	exec("CREATE TABLE " + ks + ".records(cell int,id int,payload blob,pieces list<blob>,PRIMARY KEY(cell,id))")
	insert := codecTestMust(Prepare(ctx, s, Statement[step15Input, Unit]{CQL: "INSERT INTO " + ks + ".records(cell,id,payload,pieces)VALUES(?,?,?,?)", Kind: OrdinaryStatement, Params: Params(Param("cell", Int32, func(v step15Input) int32 { return v.Cell }), Param("id", Int32, func(v step15Input) int32 { return v.ID }), Param("payload", BytesEncoder, func(v step15Input) []byte { return v.Payload }), Param("pieces", ListEncoder(BytesEncoder), func(v step15Input) [][]byte { return v.Pieces })), Rows: NoRows}, PrepareOptions{}))
	// Only one 4KiB source exists locally. All source bindings drain before the
	// fixed-session baseline, and source payload/nested buffers are then released.
	func() {
		payload := bytes.Repeat([]byte{0x71}, 4096)
		for id := int32(0); id < 128; id++ {
			b := codecTestMust(BindBorrowed(insert, step15Input{4096, id, payload, [][]byte{[]byte("first"), []byte("second")}}))
			_, e := Exec(ctx, s, b, Options{})
			b.Close()
			if e != nil {
				t.Fatal(e)
			}
			if e = b.Wait(ctx); e != nil {
				t.Fatal(e)
			}
		}
	}()
	decoder := Row(Column("id", Int32, func(v *step15Borrowed) *int32 { return &v.ID }), Column("payload", BlobCodec, func(v *step15Borrowed) *Bytes { return &v.Payload }), Column("pieces", ListDecoder(BlobCodec), func(v *step15Borrowed) *Sequence[Bytes] { return &v.Pieces }))
	prepared := codecTestMust(Prepare(ctx, s, Statement[Unit, step15Borrowed]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=4096 AND id<128", Kind: OrdinaryStatement, Params: NoParams, Rows: decoder}, PrepareOptions{}))
	bound := codecTestMust(BindOwned(prepared, Unit{}))
	defer bound.Close()
	streamDecoder := StreamRow(func(*step15Output) {}, StreamColumn("id", StreamInt32, func(v *step15Output) *int32 { return &v.ID }), StreamColumn("payload", StreamBlob, func(v *step15Output) *[]byte { return &v.Payload }), StreamColumn("pieces", StreamList(StreamBlob), func(v *step15Output) *[][]byte { return &v.Pieces }))
	runtime.GC()
	fixed := step15Mem().HeapAlloc
	records := []step15ConcurrentMemory{}
	for _, workers := range []int{1, 4, 8} {
		for _, api := range []string{"row", "into", "stream"} {
			if api == "stream" && config.Protocol < 5 && config.Compression != NoCompression {
				continue
			}
			t.Run(fmt.Sprintf("%s/workers%d", api, workers), func(t *testing.T) {
				caseCtx, caseCancel := context.WithTimeout(ctx, 30*time.Second)
				defer caseCancel()
				release := make(chan struct{})
				var releaseOnce sync.Once
				unpark := func() { releaseOnce.Do(func() { close(release) }) }
				defer unpark()
				ready := make(chan error, workers)
				done := make(chan error, workers)
				outputs := make([]step15Output, workers)
				for i := range outputs {
					outputs[i] = step15Output{Payload: make([]byte, 0, 4096), Pieces: [][]byte{make([]byte, 0, 5), make([]byte, 0, 6)}}
				}
				runtime.GC()
				before := step15Mem()
				r := step15ConcurrentMemory{API: api, Workers: workers, Rows: 128, Cell: 4096, Page: 128, RuntimeHeapBeforeConnect: initial, FixedSessionHeap: fixed, IdleHeapWithApplication: before.HeapAlloc, ApplicationStorageBytes: int64(workers * (4096 + 11))}
				r.IdleCacheBytes, r.IdleCacheObjects = implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache.Storage()
				var peak atomic.Uint64
				monitorStop := make(chan struct{})
				monitorDone := make(chan struct{})
				go func() {
					defer close(monitorDone)
					ticker := time.NewTicker(100 * time.Microsecond)
					defer ticker.Stop()
					for {
						select {
						case <-monitorStop:
							return
						case <-ticker.C:
							m := step15Mem()
							for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
								if peak.CompareAndSwap(old, m.HeapAlloc) {
									break
								}
							}
						}
					}
				}()
				for worker := range outputs {
					go func(worker int) {
						parked := false
						count := 0
						dst := &outputs[worker]
						complete := func() error {
							if dst.ID != int32(count) || len(dst.Payload) != 4096 || bytes.Count(dst.Payload, []byte{0x71}) != 4096 || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "first" || string(dst.Pieces[1]) != "second" {
								return fmt.Errorf("unequal final output worker%d row%d", worker, count)
							}
							count++
							if !parked {
								parked = true
								ready <- nil
								select {
								case <-release:
								case <-caseCtx.Done():
									return caseCtx.Err()
								}
							}
							return nil
						}
						run := func() error {
							options := Options{PageSize: Option[int32]{Set: true, Value: 128}, Prefetch: Option[int]{Set: true, Value: 0}}
							if api == "stream" {
								cursor, e := RunStream(caseCtx, s, bound, streamDecoder, StreamOptions{Incremental: true, Options: options})
								if e != nil {
									return e
								}
								defer cursor.Close()
								for cursor.NextInto(dst) {
									if e = complete(); e != nil {
										return e
									}
								}
								if e = cursor.Err(); e != nil {
									return e
								}
								return cursor.Close()
							}
							rows, e := Run(caseCtx, s, bound, options)
							if e != nil {
								return e
							}
							defer rows.Close()
							var view step15Borrowed
							for {
								ok := false
								if api == "row" {
									ok = rows.Next()
									if ok {
										view = rows.Row()
									}
								} else {
									ok = rows.NextInto(&view)
								}
								if !ok {
									break
								}
								dst.ID = view.ID
								dst.Payload = dst.Payload[:4096]
								if _, e = view.Payload.CopyTo(dst.Payload); e != nil {
									return e
								}
								it := view.Pieces.Iterator()
								index := 0
								for it.Next() {
									if index >= 2 {
										return fmt.Errorf("extra nested output")
									}
									v := it.Value()
									dst.Pieces[index] = dst.Pieces[index][:int(v.Len())]
									if _, e = v.CopyTo(dst.Pieces[index]); e != nil {
										return e
									}
									index++
								}
								if e = it.Err(); e != nil {
									return e
								}
								if index != 2 {
									return fmt.Errorf("short nested output")
								}
								if e = complete(); e != nil {
									return e
								}
							}
							if e = rows.Err(); e != nil {
								return e
							}
							return rows.Close()
						}
						e := run()
						if e == nil && count != 128 {
							e = fmt.Errorf("worker%d rows=%d", worker, count)
						}
						if !parked {
							ready <- e
						}
						done <- e
					}(worker)
				}
				var firstErr error
				for i := 0; i < workers; i++ {
					select {
					case e := <-ready:
						if e != nil && firstErr == nil {
							firstErr = e
						}
					case <-caseCtx.Done():
						firstErr = caseCtx.Err()
					}
					if firstErr != nil {
						caseCancel()
						break
					}
				}
				if firstErr == nil {
					runtime.GC()
					parked := step15Mem()
					r.AllParkedPostGCHeap = parked.HeapAlloc
					r.ParkedCacheBytes, r.ParkedCacheObjects = implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache.Storage()
					r.AllParkedStackBytes = parked.StackInuse
					r.WorkerAllocatedBytes = parked.TotalAlloc - before.TotalAlloc
					r.WorkerAllocations = parked.Mallocs - before.Mallocs
				}
				unpark()
				// Completion is joined even after a failed park; context cancellation closes
				// in-progress socket reads and prevents a goroutine from pinning fixture data.
				for i := 0; i < workers; i++ {
					select {
					case e := <-done:
						if e != nil && firstErr == nil {
							firstErr = e
						}
					case <-time.After(35 * time.Second):
						t.Fatal("diagnostic worker did not join")
					}
				}
				close(monitorStop)
				<-monitorDone
				r.PeakSampledHeap = peak.Load()
				runtime.GC()
				r.PostClosePostGCHeap = step15Mem().HeapAlloc
				r.PostCloseCacheBytes, r.PostCloseCacheObjects = implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache.Storage()
				r.InclusiveDriverAndHarnessBytes = int64(r.AllParkedPostGCHeap) - int64(initial) - r.ApplicationStorageBytes
				r.IncrementalOverWarmIdleBytes = int64(r.AllParkedPostGCHeap) - int64(r.IdleHeapWithApplication)
				if firstErr != nil {
					t.Fatal(firstErr)
				}
				for i := range outputs {
					if outputs[i].ID != 127 || bytes.Count(outputs[i].Payload, []byte{0x71}) != 4096 {
						t.Fatal("output changed after join")
					}
				}
				records = append(records, r)
				data, _ := json.Marshal(r)
				t.Log(string(data))
				runtime.KeepAlive(outputs)
			})
		}
	}
	data, e := json.MarshalIndent(records, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(out, "concurrent-memory.json"), append(data, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
}
