//go:build integration

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// Keep native sockets unwrapped so net.Buffers retains the OS writev path.
// TCP_INFO counters are kernel-accounted bytes for exactly these client sockets.
// /proc/self/io syscr/syscw are whole-process syscall counts (including background
// control work and the counter read); they are labeled separately from Read calls.
type step15IO struct {
	mu    sync.Mutex
	conns []*net.TCPConn
}

func (s *step15IO) add(c net.Conn) {
	if tcp, ok := c.(*net.TCPConn); ok {
		s.mu.Lock()
		s.conns = append(s.conns, tcp)
		s.mu.Unlock()
	}
}
func (s *step15IO) snapshot() (reads, writes, readBytes, writeBytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, conn := range s.conns {
		raw, e := conn.SyscallConn()
		if e != nil {
			continue
		}
		raw.Control(func(fd uintptr) {
			var info [256]byte
			length := uint32(len(info))
			_, _, err := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, syscall.IPPROTO_TCP, syscall.TCP_INFO, uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&length)), 0)
			if err == 0 && length >= 208 {
				readBytes += int64(binary.NativeEndian.Uint64(info[128:136]))
				writeBytes += int64(binary.NativeEndian.Uint64(info[200:208]))
			}
		})
	}
	data, e := os.ReadFile("/proc/self/io")
	if e == nil {
		for _, line := range strings.Split(string(data), "\n") {
			parts := strings.Fields(line)
			if len(parts) == 2 {
				n, _ := strconv.ParseInt(parts[1], 10, 64)
				if parts[0] == "syscr:" {
					reads = n
				}
				if parts[0] == "syscw:" {
					writes = n
				}
			}
		}
	}
	return
}

type step15Input struct {
	Cell, ID int32
	Payload  []byte
	Pieces   [][]byte
}
type step15Query struct{ Cell, Rows int32 }
type step15Borrowed struct {
	ID      int32
	Payload Bytes
	Pieces  Sequence[Bytes]
}
type step15Output struct {
	ID      int32
	Payload []byte
	Pieces  [][]byte
}
type step15Shape struct {
	Name                          string
	Rows, Cell, Page, Concurrency int
	Slow                          bool
}
type step15Result struct {
	Shape                                                                             step15Shape
	API                                                                               string
	Operations                                                                        int
	Protocol                                                                          int
	TLS                                                                               bool
	Compression                                                                       string
	FirstNS, EndNS                                                                    [3]int64
	WallNS, CPUNS, AllocatedBytes, Allocations                                        int64
	ProcessReadSyscalls, ProcessWriteSyscalls, TCPReceivedBytes, TCPSentBytes         int64
	ApplicationCopyBytes, ApplicationStorageBytes                                     int64
	IdleHeapBytes, PeakSampledHeapAllocBytes, PeakPostGCHeapBytes, PostCloseHeapBytes uint64
	RuntimeHeapBeforeConnect, SessionHeapAfterConnect, HeapAfterSetup                 uint64
	WarmIdleDriverAndHarnessBytes, InclusivePeakDriverAndHarnessBytes                 int64
	PercentileSamples                                                                 int

	IdleReceiveCacheBytes, IdleReceiveCacheObjects int
	Negotiated                                     []map[string]interface{}

	DriverOwnedOutputCopyBytes int64 // zero: borrowed views or direct socket/transform final writes

	Sampling string
}

func step15Quantiles(v []int64) [3]int64 {
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	var r [3]int64
	for i, q := range []float64{.5, .95, .99} {
		r[i] = v[min(len(v)-1, int(float64(len(v)-1)*q))]
	}
	return r
}
func step15CPU() int64 {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return r.Utime.Nano() + r.Stime.Nano()
}
func step15Mem() runtime.MemStats { var m runtime.MemStats; runtime.ReadMemStats(&m); return m }

// This gate performs identical application work: every API fills the same
// reusable output shape, validates all payload bytes/nested elements and scalar
// ordinals. Borrowed APIs' explicit application copies are counted separately.
// Latency/CPU runs never force GC. A separate diagnostic forces GC while the
// cursor remains live, and samples HeapAlloc at 100us; it reports both metrics
// without labeling uncollected garbage as retained/live driver memory.
func TestStep15NativePerformance(t *testing.T) {
	outputDir := os.Getenv("GOCQL_STEP15_OUTPUT")
	if outputDir == "" {
		t.Skip("explicit performance runner only")
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		t.Fatal(err)
	}
	stats := new(step15IO)
	// Quiescent endpoint calibration: the first snapshot precedes connection
	// creation, so these process syscall deltas contain only counter/harness I/O.
	cr, cw, _, _ := stats.snapshot()
	calCPU := step15CPU()
	calMem := step15Mem()
	calStart := time.Now()
	for i := 0; i < 64; i++ {
		stats.snapshot()
	}
	calEnd := step15Mem()
	calibration := map[string]int64{"iterations": 64, "wall_ns": time.Since(calStart).Nanoseconds(), "cpu_ns": step15CPU() - calCPU, "allocated_bytes": int64(calEnd.TotalAlloc - calMem.TotalAlloc)}
	ar, aw, _, _ := stats.snapshot()
	calibration["process_read_syscalls"] = ar - cr
	calibration["process_write_syscalls"] = aw - cw
	calibrationBytes, _ := json.MarshalIndent(calibration, "", "  ")
	if e := os.WriteFile(filepath.Join(outputDir, "counter-calibration.json"), append(calibrationBytes, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
	config := createV2Cluster(func(c *Config) {
		c.Pool.ConnectionsPerHost = 0
		c.Pool.ConnectionsPerShard = 1
		c.Pool.SourcePortStart = 32768
		c.Pool.SourcePortEnd = 65535
		c.CoalesceWait = 0
		c.KeepaliveInterval = 30 * time.Second
		c.Metadata.PeriodicRefresh = 0
		c.Dialer = DialerFunc(func(ctx context.Context, target DialTarget) (net.Conn, error) {
			conn, err := (DefaultDialer{}).DialContext(ctx, target)
			if err != nil {
				return nil, err
			}
			stats.add(conn)
			return conn, nil
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	runtime.GC()
	baseRuntime := step15Mem().HeapAlloc
	s, err := Connect(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	time.Sleep(1100 * time.Millisecond)
	runtime.GC()
	sessionHeap := step15Mem().HeapAlloc
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
			t.Error("owned cleanup", e)
		}
	}()
	exec("CREATE TABLE " + ks + ".records(cell int,id int,payload blob,pieces list<blob>,PRIMARY KEY(cell,id))")
	insert := codecTestMust(Prepare(ctx, s, Statement[step15Input, Unit]{CQL: "INSERT INTO " + ks + ".records(cell,id,payload,pieces)VALUES(?,?,?,?)", Kind: OrdinaryStatement, Params: Params(Param("cell", Int32, func(v step15Input) int32 { return v.Cell }), Param("id", Int32, func(v step15Input) int32 { return v.ID }), Param("payload", BytesEncoder, func(v step15Input) []byte { return v.Payload }), Param("pieces", ListEncoder(BytesEncoder), func(v step15Input) [][]byte { return v.Pieces })), Rows: NoRows}, PrepareOptions{}))
	for _, size := range []int{32, 4096, 256 << 10, 8 << 20} {
		count := 128
		if size >= 256<<10 {
			count = 1
		}
		payload := bytes.Repeat([]byte{0x71}, size)
		for i := 0; i < count; i++ {
			b := codecTestMust(BindBorrowed(insert, step15Input{int32(size), int32(i), payload, [][]byte{[]byte("first"), []byte("second")}}))
			_, e := Exec(ctx, s, b, Options{})
			b.Close()
			if e != nil {
				t.Fatal(e)
			}
			if e = b.Wait(ctx); e != nil {
				t.Fatal(e)
			}
		}
	}
	borrowedDecoder := Row(Column("id", Int32, func(v *step15Borrowed) *int32 { return &v.ID }), Column("payload", BlobCodec, func(v *step15Borrowed) *Bytes { return &v.Payload }), Column("pieces", ListDecoder(BlobCodec), func(v *step15Borrowed) *Sequence[Bytes] { return &v.Pieces }))
	prepared := codecTestMust(Prepare(ctx, s, Statement[step15Query, step15Borrowed]{CQL: "SELECT id,payload,pieces FROM " + ks + ".records WHERE cell=? AND id<?", Kind: OrdinaryStatement, Params: Params(Param("cell", Int32, func(v step15Query) int32 { return v.Cell }), Param("id", Int32, func(v step15Query) int32 { return v.Rows })), Rows: borrowedDecoder}, PrepareOptions{}))
	streamDecoder := StreamRow(func(*step15Output) {}, StreamColumn("id", StreamInt32, func(v *step15Output) *int32 { return &v.ID }), StreamColumn("payload", StreamBlob, func(v *step15Output) *[]byte { return &v.Payload }), StreamColumn("pieces", StreamList(StreamBlob), func(v *step15Output) *[][]byte { return &v.Pieces }))
	shapes := []step15Shape{{"small", 1, 32, 1, 1, false}, {"page1", 128, 4096, 1, 1, false}, {"page32", 128, 4096, 32, 1, false}, {"rows1", 1, 4096, 128, 1, false}, {"rows128", 128, 4096, 128, 1, false}, {"cell256K", 1, 256 << 10, 1, 1, false}, {"cell8M", 1, 8 << 20, 1, 1, false}, {"concurrent", 128, 4096, 32, 4, false}, {"slow", 128, 4096, 32, 1, true}}
	runtime.GC()
	setupHeap := step15Mem().HeapAlloc
	results := []step15Result{}
	var measureMu sync.Mutex
	for _, shape := range shapes {
		for _, api := range []string{"row", "into", "stream"} {
			if api == "stream" && config.Protocol < 5 && config.Compression != NoCompression {
				continue
			}
			t.Run(shape.Name+"/"+api, func(t *testing.T) {
				bound := codecTestMust(BindOwned(prepared, step15Query{int32(shape.Cell), int32(shape.Rows)}))
				defer bound.Close()
				outputs := make([]step15Output, shape.Concurrency)
				for i := range outputs {
					outputs[i] = step15Output{Payload: make([]byte, 0, shape.Cell), Pieces: [][]byte{make([]byte, 0, 5), make([]byte, 0, 6)}}
				}
				options := Options{PageSize: Option[int32]{Set: true, Value: int32(shape.Page)}, Prefetch: Option[int]{Set: true, Value: 0}}
				run := func(worker int, sample func()) (int64, int64, error) {
					dst := &outputs[worker]
					backing := unsafe.SliceData(dst.Payload[:cap(dst.Payload)])
					start := time.Now()
					first := int64(0)
					n := 0
					validate := func() error {
						if dst.ID != int32(n) || len(dst.Payload) != shape.Cell || bytes.Count(dst.Payload, []byte{0x71}) != shape.Cell || len(dst.Pieces) != 2 || string(dst.Pieces[0]) != "first" || string(dst.Pieces[1]) != "second" || unsafe.SliceData(dst.Payload) != backing {
							return fmt.Errorf("unequal output id=%d row=%d payload=%d", dst.ID, n, len(dst.Payload))
						}
						n++
						if n == 1 {
							first = time.Since(start).Nanoseconds()
						}
						if sample != nil {
							sample()
						}
						if shape.Slow {
							time.Sleep(time.Millisecond)
						}
						return nil
					}
					if api == "stream" {
						cursor, e := RunStream(ctx, s, bound, streamDecoder, StreamOptions{Options: options})
						if e != nil {
							return 0, 0, e
						}
						defer cursor.Close()
						for cursor.NextInto(dst) {
							if e = validate(); e != nil {
								return 0, 0, e
							}
						}
						if cursor.Err() != nil {
							return 0, 0, cursor.Err()
						}
						if e = cursor.Close(); e != nil {
							return 0, 0, e
						}
					} else {
						rows, e := Run(ctx, s, bound, options)
						if e != nil {
							return 0, 0, e
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
							dst.Payload = dst.Payload[:shape.Cell]
							if _, e = view.Payload.CopyTo(dst.Payload); e != nil {
								return 0, 0, e
							}
							it := view.Pieces.Iterator()
							index := 0
							for it.Next() {
								if index >= 2 {
									return 0, 0, fmt.Errorf("extra nested value")
								}
								v := it.Value()
								dst.Pieces[index] = dst.Pieces[index][:int(v.Len())]
								if _, e = v.CopyTo(dst.Pieces[index]); e != nil {
									return 0, 0, e
								}
								index++
							}
							if e = it.Err(); e != nil {
								return 0, 0, e
							}
							if index != 2 {
								return 0, 0, fmt.Errorf("short nested list: %d", index)
							}
							if e = validate(); e != nil {
								return 0, 0, e
							}
						}
						if rows.Err() != nil {
							return 0, 0, rows.Err()
						}
						if e = rows.Close(); e != nil {
							return 0, 0, e
						}
					}
					if n != shape.Rows {
						return 0, 0, fmt.Errorf("rows=%d wanted=%d", n, shape.Rows)
					}
					return first, time.Since(start).Nanoseconds(), nil
				}
				for worker := range outputs {
					for warm := 0; warm < 2; warm++ {
						if _, _, e := run(worker, nil); e != nil {
							t.Fatal(e)
						}
					}
				}
				perWorker := 32
				if shape.Slow {
					perWorker = 8
				}
				r := step15Result{Shape: shape, API: api, Operations: perWorker * shape.Concurrency, Protocol: config.Protocol, TLS: config.TLS != nil, Compression: *flagCompressTest, ApplicationStorageBytes: int64(shape.Concurrency * (shape.Cell + 11)), Sampling: "100us HeapAlloc during separate diagnostic; post-GC at every row and post-Close; inclusive driver+harness delta uses pre-Connect runtime baseline and separately seeded output; warm idle retained pools included; no process syscall overhead subtraction"}
				if api != "stream" {
					r.ApplicationCopyBytes = int64(r.Operations * shape.Rows * (shape.Cell + 11))
				}
				runtime.GC()
				reads, writes, rb, wb := stats.snapshot()
				before := step15Mem()
				startCPU := step15CPU()
				start := time.Now()
				firsts, ends := make([]int64, 0, r.Operations), make([]int64, 0, r.Operations)
				var wg sync.WaitGroup
				for worker := range outputs {
					wg.Add(1)
					go func(worker int) {
						defer wg.Done()
						for j := 0; j < perWorker; j++ {
							f, e, err := run(worker, nil)
							if err != nil {
								t.Error(err)
								return
							}
							measureMu.Lock()
							firsts = append(firsts, f)
							ends = append(ends, e)
							measureMu.Unlock()
						}
					}(worker)
				}
				wg.Wait()
				r.WallNS = time.Since(start).Nanoseconds()
				r.CPUNS = step15CPU() - startCPU
				after := step15Mem()
				r.AllocatedBytes = int64(after.TotalAlloc - before.TotalAlloc)
				r.Allocations = int64(after.Mallocs - before.Mallocs)
				ar, aw, arb, awb := stats.snapshot()
				r.ProcessReadSyscalls = ar - reads
				r.ProcessWriteSyscalls = aw - writes
				r.TCPReceivedBytes = arb - rb
				r.TCPSentBytes = awb - wb
				if len(firsts) != r.Operations {
					t.Fatal("incomplete measurements")
				}
				r.FirstNS = step15Quantiles(firsts)
				r.EndNS = step15Quantiles(ends)
				if shape.Name == "rows128" || shape.Name == "concurrent" {
					f, e := os.Create(filepath.Join(outputDir, shape.Name+"-"+api+"-cpu.pprof"))
					if e != nil {
						t.Fatal(e)
					}
					runtime.SetMutexProfileFraction(1)
					runtime.SetBlockProfileRate(1)
					if e = pprof.StartCPUProfile(f); e != nil {
						t.Fatal(e)
					}
					deadline := time.Now().Add(time.Second)
					var profileWorkers sync.WaitGroup
					for worker := range outputs {
						profileWorkers.Add(1)
						go func(worker int) {
							defer profileWorkers.Done()
							for time.Now().Before(deadline) {
								if _, _, e := run(worker, nil); e != nil {
									t.Error(e)
									return
								}
							}
						}(worker)
					}
					profileWorkers.Wait()
					pprof.StopCPUProfile()
					runtime.SetMutexProfileFraction(0)
					runtime.SetBlockProfileRate(0)
					f.Close()
				}
				runtime.GC()
				r.IdleHeapBytes = step15Mem().HeapAlloc
				r.IdleReceiveCacheBytes, r.IdleReceiveCacheObjects = implCluster.SessionData(s).InternalConnCfg.InternalReceiveCache.Storage()
				r.Negotiated = step15Negotiated(s)
				var peak atomic.Uint64
				stop := make(chan struct{})
				joined := make(chan struct{})
				go func() {
					defer close(joined)
					ticker := time.NewTicker(100 * time.Microsecond)
					defer ticker.Stop()
					for {
						select {
						case <-ticker.C:
							m := step15Mem()
							for old := peak.Load(); m.HeapAlloc > old; old = peak.Load() {
								if peak.CompareAndSwap(old, m.HeapAlloc) {
									break
								}
							}
						case <-stop:
							return
						}
					}
				}()
				// Diagnostic retains source/final storage established before idle baseline.
				if _, _, e := run(0, func() {
					runtime.GC()
					m := step15Mem()
					r.PeakPostGCHeapBytes = max(r.PeakPostGCHeapBytes, m.HeapAlloc)
				}); e != nil {
					t.Error(e)
				}
				close(stop)
				<-joined
				r.PeakSampledHeapAllocBytes = peak.Load()
				runtime.GC()
				r.PostCloseHeapBytes = step15Mem().HeapAlloc
				r.RuntimeHeapBeforeConnect = baseRuntime
				r.SessionHeapAfterConnect = sessionHeap
				r.HeapAfterSetup = setupHeap
				r.PercentileSamples = len(firsts)
				r.WarmIdleDriverAndHarnessBytes = int64(r.IdleHeapBytes) - int64(baseRuntime) - r.ApplicationStorageBytes
				r.InclusivePeakDriverAndHarnessBytes = int64(r.PeakPostGCHeapBytes) - int64(baseRuntime) - r.ApplicationStorageBytes
				results = append(results, r)
				data, _ := json.Marshal(r)
				t.Log(string(data))
				runtime.KeepAlive(outputs)
			})
		}
	}
	data, e := json.MarshalIndent(results, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(outputDir, "results.json"), append(data, '\n'), 0644); e != nil {
		t.Fatal(e)
	}
	// Profiles are explicitly separate from latency runs and use the same payload,
	// API and concurrency shape; profile output is owned by this mode directory.
	for _, name := range []string{"heap", "allocs", "mutex", "block", "goroutine"} {
		f, e := os.Create(filepath.Join(outputDir, name+".pprof"))
		if e != nil {
			t.Fatal(e)
		}
		if e = pprof.Lookup(name).WriteTo(f, 0); e != nil {
			t.Error(e)
		}
		f.Close()
	}
	runtime.SetMutexProfileFraction(0)
	runtime.SetBlockProfileRate(0)
}

// Negotiated metadata is captured outside all normal measurement intervals.
func step15Negotiated(s *Session) []map[string]interface{} {
	p := implCluster.SessionData(s).InternalPool
	p.InternalMu.RLock()
	defer p.InternalMu.RUnlock()
	result := []map[string]interface{}{}
	for host, pool := range p.InternalHostConnPools {
		pool.InternalMu.RLock()
		picker := pool.InternalConnPicker
		count, missing := picker.Size()
		var conns []*driverConn
		switch v := picker.(type) {
		case *typedConnPicker:
			v.InternalMu.RLock()
			for _, shard := range v.InternalConns {
				conns = append(conns, shard...)
			}
			v.InternalMu.RUnlock()
		case *defaultConnPicker:
			v.InternalMu.RLock()
			conns = append(conns, v.InternalConns...)
			v.InternalMu.RUnlock()
		}
		pool.InternalMu.RUnlock()
		for _, c := range conns {
			compression := "none"
			if c.InternalCompressor != nil {
				compression = c.InternalCompressor.Name()
			}
			entry := map[string]interface{}{"host_id": host.String(), "host_connections": count, "missing_connections": missing, "shards": c.InternalScyllaSupported.ShardsCount(), "shard": c.InternalScyllaSupported.Shard(), "protocol": c.InternalVersion, "compression": compression, "metadata_id": c.InternalTracksResultMetadataID(), "authentication_fixture": *flagRunAuthTest, "routing": "token-aware/shuffle-replicas/round-robin-fallback"}
			if reader, ok := c.InternalR.(*connReader); ok {
				entry["remote"] = reader.InternalConn.RemoteAddr().String()
				entry["local"] = reader.InternalConn.LocalAddr().String()
				if encrypted, ok := reader.InternalConn.(interface{ ConnectionState() tls.ConnectionState }); ok {
					state := encrypted.ConnectionState()
					entry["tls_version"] = state.Version
					entry["tls_cipher"] = state.CipherSuite
					entry["tls_handshake_complete"] = state.HandshakeComplete
				}
			}
			result = append(result, entry)
		}
	}
	return result
}
