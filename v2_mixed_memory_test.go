//go:build gocql_perf

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type mixedMemoryObserver struct{ active atomic.Int64 }

func (o *mixedMemoryObserver) ObserveAttempt(AttemptEvent) {
	o.active.Add(1)
	runtime.Gosched()
	o.active.Add(-1)
}
func (o *mixedMemoryObserver) ObserveRequest(RequestEvent) {
	o.active.Add(1)
	runtime.Gosched()
	o.active.Add(-1)
}

type mixedMemorySample struct {
	Seconds                                 float64
	Heap, HeapInuse, RSS, NormalizedHeap    uint64
	Goroutines, Streams                     int
	Cache                                   wire.ReceiveCacheStats
	LeaseBytes, PeakLeaseBytes              int64
	InFlight, Callbacks                     int64
	RetainedPages, OwnedDestinationCapacity int
}

// This is an opt-in native transport/decoder memory diagnostic, not a performance
// comparison. GC is forced at quiescent sample boundaries to compare live heaps.
// A short run exercises the test but does not qualify the required 30-minute plateau.
func TestMixedResponseMemoryPlateau(t *testing.T) {
	durationText := os.Getenv("GOCQL_MIXED_MEMORY_DURATION")
	if durationText == "" {
		t.Skip("set GOCQL_MIXED_MEMORY_DURATION=30m and GOCQL_MIXED_MEMORY_OUTPUT to run memory qualification")
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil || duration < time.Second {
		t.Fatal("invalid mixed memory duration", durationText)
	}
	path := os.Getenv("GOCQL_MIXED_MEMORY_OUTPUT")
	if path == "" {
		t.Fatal("GOCQL_MIXED_MEMORY_OUTPUT is required to retain samples")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	log := json.NewEncoder(file)
	if err := log.Encode(map[string]any{"kind": "configuration", "duration": duration.String(), "runtime": runtime.Version(), "pid": os.Getpid(), "gomaxprocs": runtime.GOMAXPROCS(0), "forced_gc": true, "qualification_window": duration >= 30*time.Minute, "transport": "native v4 private multiplexed connection", "page_rows": 32, "partition_rows": 128, "large_bytes": 12 << 20}); err != nil {
		t.Fatal(err)
	}

	session, conns, peers := runtimeTestSession(t, 1)
	defer session.Close()
	conn, peer := conns[0], peers[0]
	conn.InternalCfg.TypedLimits = ProtocolLimits{MaxFrameBytes: 16 << 20, MaxDecompressedBytes: 16 << 20, MaxMetadataDepth: 64, MaxPagingStates: 64, MaxPagingStateBytes: 64 << 10}
	cache := conn.InternalCfg.InternalReceiveCache
	implCluster.SessionData(session).InternalConnCfg = conn.InternalCfg
	wire.ResetPerfMemoryCounters()
	statement := Statement[Unit, Bytes]{Kind: OrdinaryStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}
	options := Options{PageSize: Option[int32]{Set: true, Value: 32}, Prefetch: Option[int]{Set: true, Value: 0}}
	query := codecTestMust(NewUnpreparedQuery(session, statement, NewColumns(), options))
	defer query.Close()
	observer := &mixedMemoryObserver{}
	observedOptions := options
	observedOptions.Observer = observer
	observed := codecTestMust(NewUnpreparedQuery(session, statement, NewColumns(), observedOptions))
	defer observed.Close()
	streaming := codecTestMust(NewUnpreparedStreamQuery(session, statement, NewColumns(), StreamOneColumn(StreamBlob), StreamOptions{Options: options}))
	defer streaming.Close()

	type response struct {
		bodies [][]byte
		hash   [32]byte
		rows   int
		size   int
	}
	responses := make([]response, 4)
	for i, size := range []int{64, 4096, 2 << 20, 12 << 20} {
		value := strings.Repeat(string(byte(61+i)), size)
		r := response{hash: sha256.Sum256([]byte(value)), rows: 1, size: size}
		if i == 1 {
			values := make([]string, 32)
			for j := range values {
				values[j] = value
			}
			for page := 1; page <= 4; page++ {
				r.bodies = append(r.bodies, runtimeRowsBody([]byte{byte(page)}, values...))
			}
			r.bodies = append(r.bodies, runtimeRowsBody(nil))
			r.rows = 128
		} else {
			r.bodies = [][]byte{runtimeRowsBody(nil, value)}
		}
		responses[i] = r
	}
	type reply struct {
		r      response
		cancel context.CancelFunc
		done   chan struct{}
	}
	plans := make(chan reply)
	serverErrors := make(chan error, 1)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for plan := range plans {
			for _, body := range plan.r.bodies {
				var header [9]byte
				if _, err := io.ReadFull(peer, header[:]); err != nil {
					serverErrors <- err
					return
				}
				if _, err := io.CopyN(io.Discard, peer, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
					serverErrors <- err
					return
				}
				if plan.cancel != nil {
					plan.cancel()
				}
				header[0], header[1], header[4] = 0x84, 0, 8
				binary.BigEndian.PutUint32(header[5:], uint32(len(body)))
				if _, err := peer.Write(header[:]); err != nil {
					serverErrors <- err
					return
				}
				if _, err := peer.Write(body); err != nil {
					serverErrors <- err
					return
				}
			}
			close(plan.done)
		}
	}()
	defer func() { close(plans); peer.Close(); <-serverDone }()
	type retained struct {
		value Bytes
		lease *PageLease
		hash  [32]byte
		bytes int64
	}
	var held []retained
	defer func() {
		for _, page := range held {
			page.lease.Close()
		}
	}()
	destination := make([]byte, 0, 12<<20)
	destinationHash := sha256.Sum256(destination)
	var samples []mixedMemorySample
	start, nextSample := time.Now(), time.Now()
	deadline := start.Add(duration)
	parent, stop := context.WithDeadline(context.Background(), deadline.Add(30*time.Second))
	defer stop()
	expectedHeldBytes := func() int64 {
		var n int64
		for _, page := range held {
			n += page.bytes
		}
		return n
	}
	var operations, cancellations, oversize int

	for time.Now().Before(deadline) {
		for burst := 0; burst < 32 && time.Now().Before(deadline); burst++ {
			sequence := operations
			operations++
			ctx, cancel := context.WithTimeout(parent, 10*time.Second)
			if sequence%37 == 13 {
				cancel()
				if _, err := query.Run(ctx, Unit{}); !errors.Is(err, context.Canceled) {
					t.Fatal("pre-admission cancellation", err)
				}
				cancellations++
				continue
			}
			kind := 0
			if sequence%64 == 0 {
				kind = 3
			} else if sequence%8 == 0 {
				kind = 2
			} else if sequence%4 == 0 {
				kind = 1
			}
			r := responses[kind]
			plan := reply{r: r, done: make(chan struct{})}
			cancelled := sequence%41 == 17
			if cancelled {
				plan.r = responses[0]
				plan.cancel = cancel
			}
			select {
			case plans <- plan:
			case err := <-serverErrors:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			before := cache.Stats()
			if !cancelled && sequence%3 == 1 {
				cursor, err := streaming.Run(ctx, Unit{})
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for cursor.NextInto(&destination) {
					if len(destination) != r.size || sha256.Sum256(destination) != r.hash {
						t.Fatal("stream payload changed")
					}
					count++
				}
				if err := cursor.Err(); err != nil {
					t.Fatal(err)
				}
				cursor.Close()
				if count != r.rows {
					t.Fatal("stream row count", count, r.rows)
				}
				destinationHash = sha256.Sum256(destination)
				if err := streaming.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				selected := query
				if sequence%5 == 0 && !cancelled {
					selected = observed
				}
				cursor, runErr := selected.Run(ctx, Unit{})
				if cancelled {
					if runErr == nil {
						if cursor.Next() {
							t.Fatal("cancelled row delivered")
						}
						runErr = cursor.Err()
						cursor.Close()
					}
					if !errors.Is(runErr, context.Canceled) {
						t.Fatal("accepted cancellation", runErr)
					}
					cancellations++
				} else {
					if runErr != nil {
						t.Fatal(runErr)
					}
					count := 0
					for cursor.Next() {
						value := cursor.Row()
						data := make([]byte, value.Len())
						if _, err := value.CopyTo(data); err != nil {
							t.Fatal(err)
						}
						if len(data) != r.size || sha256.Sum256(data) != r.hash {
							t.Fatal("buffered payload changed")
						}
						if count == 0 && sequence%11 == 0 && kind != 3 {
							lease := codecTestMust(cursor.Retain())
							if len(held) == 4 {
								held[0].lease.Close()
								copy(held, held[1:])
								held[len(held)-1] = retained{}
								held = held[:len(held)-1]
							}
							held = append(held, retained{value: value, lease: lease, hash: r.hash, bytes: cqltype.BytesLease(value).Chain().Len()})
						}
						count++
					}
					if err := cursor.Err(); err != nil {
						t.Fatal(err)
					}
					cursor.Close()
					if count != r.rows {
						t.Fatal("buffered row count", count, r.rows)
					}
					if destination != nil && sha256.Sum256(destination) != destinationHash {
						t.Fatal("owned destination changed during another query")
					}
				}
				wait, endWait := context.WithTimeout(parent, time.Second)
				if err := selected.Wait(wait); err != nil {
					t.Fatal(err)
				}
				endWait()
			}
			select {
			case <-plan.done:
			case err := <-serverErrors:
				t.Fatal(err)
			case <-parent.Done():
				t.Fatal(parent.Err())
			}
			cancel()
			if kind == 3 && !cancelled {
				oversize++
				after := cache.Stats()
				if after.Misses <= before.Misses || after.RetainedBytes != before.RetainedBytes {
					t.Fatal("oversize response was cached", before, after)
				}
			}
		}
		for _, page := range held {
			data := make([]byte, page.value.Len())
			if _, err := page.value.CopyTo(data); err != nil || sha256.Sum256(data) != page.hash {
				t.Fatal("retained page changed", err)
			}
		}
		if !time.Now().Before(nextSample) {
			// Allow the final receive handoff to unwind, then force a collection
			// outside all operations. This diagnostic intentionally includes GC.
			until := time.Now().Add(time.Second)
			for {
				live, _, _ := wire.ReadPerfMemoryCounters()
				if live == expectedHeldBytes() {
					break
				}
				if time.Now().After(until) {
					t.Fatal("outstanding response leases", live, expectedHeldBytes())
				}
				time.Sleep(time.Millisecond)
			}
			runtime.GC()
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			live, peak, _ := wire.ReadPerfMemoryCounters()
			conn.InternalMu.Lock()
			calls := len(conn.InternalCalls)
			conn.InternalMu.Unlock()
			sample := mixedMemorySample{Seconds: time.Since(start).Seconds(), Heap: memory.HeapAlloc, HeapInuse: memory.HeapInuse, RSS: mixedMemoryRSS(), Goroutines: runtime.NumGoroutine(), Streams: calls, Cache: cache.Stats(), LeaseBytes: live, PeakLeaseBytes: peak, InFlight: session.Metrics().InFlight, Callbacks: observer.active.Load(), RetainedPages: len(held), OwnedDestinationCapacity: cap(destination)}
			excluded := uint64(sample.Cache.RetainedBytes+cap(destination)) + uint64(live)
			if sample.Heap > excluded {
				sample.NormalizedHeap = sample.Heap - excluded
			}
			if sample.Streams != 0 || sample.InFlight != 0 || sample.Callbacks != 0 || sample.Cache.RetainedBytes > wire.ReceiveCacheMaxBytes || sample.Cache.RetainedObjects > wire.ReceiveCacheMaxObjects {
				t.Fatal("quiescent accounting failure", sample)
			}
			if err := log.Encode(sample); err != nil {
				t.Fatal(err)
			}
			if err := file.Sync(); err != nil {
				t.Fatal(err)
			}
			samples = append(samples, sample)
			nextSample = time.Now().Add(5 * time.Second)
		}
		time.Sleep(100 * time.Millisecond)
	}
	query.Close()
	observed.Close()
	streaming.Close()
	wait, finish := context.WithTimeout(context.Background(), 5*time.Second)
	defer finish()
	for _, join := range []func(context.Context) error{query.Wait, observed.Wait, streaming.Wait} {
		if err := join(wait); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	for _, page := range held {
		data := make([]byte, page.value.Len())
		if _, err := page.value.CopyTo(data); err != nil || sha256.Sum256(data) != page.hash {
			t.Fatal("session close invalidated a retained page", err)
		}
		page.lease.Close()
	}
	held = nil
	if bytes, objects := cache.Storage(); bytes != 0 || objects != 0 {
		t.Fatal("closed session cache repopulated after retained-page release", bytes, objects)
	}
	if live, _, _ := wire.ReadPerfMemoryCounters(); live != 0 {
		t.Fatal("final response leases", live)
	}
	if duration >= 30*time.Minute {
		checkMixedMemoryPlateau(t, samples, path)
	}
	if err := log.Encode(map[string]any{"kind": "complete", "operations": operations, "cancellations": cancellations, "oversize_evictions": oversize, "samples": len(samples), "qualified_window": duration >= 30*time.Minute}); err != nil {
		t.Fatal(err)
	}
}

func mixedMemoryRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			n, _ := strconv.ParseUint(fields[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}

func checkMixedMemoryPlateau(t *testing.T, all []mixedMemorySample, output string) {
	t.Helper()
	var samples []mixedMemorySample
	for _, sample := range all {
		if sample.Seconds >= 60 {
			samples = append(samples, sample)
		}
	}
	if len(samples) < 24 {
		t.Fatal("insufficient post-warmup samples")
	}
	n := len(samples) / 4
	mean := func(rows []mixedMemorySample, field func(mixedMemorySample) uint64) float64 {
		var total float64
		for _, row := range rows {
			total += float64(field(row))
		}
		return total / float64(len(rows))
	}
	first, last := samples[:n], samples[len(samples)-n:]
	heapGrowth := mean(last, func(s mixedMemorySample) uint64 { return s.NormalizedHeap }) - mean(first, func(s mixedMemorySample) uint64 { return s.NormalizedHeap })
	rssGrowth := mean(last, func(s mixedMemorySample) uint64 { return s.RSS }) - mean(first, func(s mixedMemorySample) uint64 { return s.RSS })
	rssProgressive := rssGrowth > 8<<20
	for quarter := 1; quarter < 4; quarter++ {
		previous := samples[(quarter-1)*n : quarter*n]
		current := samples[quarter*n : (quarter+1)*n]
		rssProgressive = rssProgressive && mean(current, func(s mixedMemorySample) uint64 { return s.RSS }) > mean(previous, func(s mixedMemorySample) uint64 { return s.RSS })+1<<20
	}
	goroutineGrowth := float64(last[len(last)-1].Goroutines - first[0].Goroutines)
	if heapGrowth > 8<<20 || rssGrowth > 32<<20 || rssProgressive || goroutineGrowth > 4 {
		if file, err := os.Create(output + ".heap.pprof"); err == nil {
			_ = pprof.WriteHeapProfile(file)
			_ = file.Close()
		}
		if file, err := os.Create(output + ".goroutines.txt"); err == nil {
			_ = pprof.Lookup("goroutine").WriteTo(file, 2)
			_ = file.Close()
		}
		t.Fatal(fmt.Sprintf("post-warmup process memory/goroutine growth requires diagnosis: heap %.1f MiB, RSS %.1f MiB, goroutines %.0f", heapGrowth/(1<<20), rssGrowth/(1<<20), goroutineGrowth))
	}
}
