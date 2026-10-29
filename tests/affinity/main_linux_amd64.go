//go:build linux && amd64

// Copyright 2026 The Apache Software Foundation.
// Licensed under the Apache License, Version 2.0. See LICENSE and NOTICE
// in the repository root, or https://www.apache.org/licenses/LICENSE-2.0.

// A standalone synthetic cache/encoding/TCP experiment, not a gocql benchmark.
// Build with GO111MODULE=off; no new module or driver dependency is required.
package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

const incomingCPU = 49 // Linux asm-generic/socket.h.
const incomingNAPI = 56
const getCPUSyscall = 309 // Linux x86_64 asm/unistd_64.h.
const recordBytes = 256

type config struct {
	Mode, Cache, CPUs, Address, Listen string
	Pin, SocketCPU, Skew               bool
	MiB, Updates, Batch                int
	Warmup, Duration                   time.Duration
	SampleSeed                         uint64
}

// Each entry is 128 bytes, excluding map/index and mutex overhead.
type entry struct {
	data       [14]uint64
	key        uint64
	prev, next int32
}

type cache struct {
	mu         sync.Mutex
	shared     bool
	entries    []entry
	index      map[uint64]int32
	head, tail int32
}

func newCache(first, count int, shared bool) *cache {
	c := &cache{shared: shared, entries: make([]entry, count), index: make(map[uint64]int32, count), head: 0, tail: int32(count - 1)}
	for i := range c.entries {
		e := &c.entries[i]
		e.key = uint64(first + i)
		e.prev, e.next = int32(i-1), int32(i+1)
		if i == count-1 {
			e.next = -1
		}
		for j := range e.data {
			e.data[j] = (e.key+1)*0x9e3779b97f4a7c15 + uint64(j)
		}
		c.index[e.key] = int32(i)
	}
	return c
}

func (c *cache) lookup(key uint64, update bool, sequence uint64) [4]uint64 {
	if c.shared {
		c.mu.Lock()
		defer c.mu.Unlock()
	}
	i, ok := c.index[key]
	if !ok {
		panic("unexpected cache miss")
	}
	e := &c.entries[i]
	if i != c.head {
		c.entries[e.prev].next = e.next
		if e.next >= 0 {
			c.entries[e.next].prev = e.prev
		} else {
			c.tail = e.prev
		}
		e.prev, e.next = -1, c.head
		c.entries[c.head].prev = i
		c.head = i
	}
	if update {
		e.data[0] ^= sequence + 1
	}
	return [4]uint64{e.data[0], e.data[3], e.data[7], e.data[13]}
}

//go:noinline
func encode(dst []byte, key, sequence uint64, metadata [4]uint64) {
	for i, value := range metadata {
		binary.BigEndian.PutUint64(dst[i*8:], value^key^sequence)
	}
}

func affinity() ([]byte, error) {
	mask := make([]byte, 128)
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY, 0, uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return nil, errno
	}
	return mask, nil
}

func setAffinity(mask []byte) error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY, 0, uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return errno
	}
	return nil
}

func pin(cpu int) (func(), error) {
	runtime.LockOSThread()
	original, err := affinity()
	if err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	mask := make([]byte, len(original))
	if cpu < 0 || cpu/8 >= len(mask) || original[cpu/8]&(1<<uint(cpu%8)) == 0 {
		runtime.UnlockOSThread()
		return nil, fmt.Errorf("CPU %d outside allowed mask", cpu)
	}
	mask[cpu/8] = 1 << uint(cpu%8)
	if err := setAffinity(mask); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() {
		if err := setAffinity(original); err != nil {
			panic(err)
		}
		runtime.UnlockOSThread()
	}, nil
}

func currentCPU() (int, error) {
	var cpu uint32
	_, _, errno := syscall.RawSyscall(getCPUSyscall, uintptr(unsafe.Pointer(&cpu)), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(cpu), nil
}

func socketOption(conn *net.TCPConn, option int, set *int) (value int, err error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var opErr error
	err = raw.Control(func(fd uintptr) {
		if set != nil {
			opErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, option, *set)
		}
		if opErr == nil {
			value, opErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, option)
		}
	})
	if err != nil {
		return 0, err
	}
	return value, opErr
}

func cpuList(raw string) ([]int, error) {
	var cpus []int
	seen := map[int]bool{}
	for _, s := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n >= 1024 || seen[n] {
			return nil, fmt.Errorf("invalid CPU list %q", raw)
		}
		seen[n] = true
		cpus = append(cpus, n)
	}
	return cpus, nil
}

func writeAll(conn net.Conn, b []byte) error {
	for len(b) > 0 {
		n, err := conn.Write(b)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		b = b[n:]
	}
	return nil
}

type workerResult struct {
	Worker, TargetCPU                          int
	Operations, Checksum                       uint64
	CPUObservations                            map[int]uint64
	SampledCPUChanges                          uint64
	IncomingBefore, IncomingAfter, NAPIID      int
	IncomingAtSetup                            int
	SocketTargetSamples, SocketOtherSamples    uint64
	SocketAffinityStatus                       string
	LatencySamplesSeen, LatencySamplesRetained uint64
	BatchLatencyNS                             []int64 `json:"-"`
	Error                                      string  `json:",omitempty"`
}

type operation struct {
	cfg             config
	cache           *cache
	keys            []uint64
	sequence        uint64
	conn            *net.TCPConn
	send, receive   []byte
	checksum        uint64
	incomingAtSetup int
}

func (o *operation) batch(update bool) error {
	for i := 0; i < o.cfg.Batch; i++ {
		key := o.keys[o.sequence&uint64(len(o.keys)-1)]
		metadata := o.cache.lookup(key, update && o.sequence%100 < uint64(o.cfg.Updates), o.sequence)
		offset := 0
		if o.conn != nil {
			offset = i * recordBytes
		}
		encode(o.send[offset:], key, o.sequence, metadata)
		o.checksum += binary.BigEndian.Uint64(o.send[offset:])
		o.sequence++
	}
	if o.conn != nil {
		if err := writeAll(o.conn, o.send); err != nil {
			return err
		}
		if _, err := io.ReadFull(o.conn, o.receive); err != nil {
			return err
		}
		if !bytes.Equal(o.send, o.receive) {
			return fmt.Errorf("TCP echo mismatch")
		}
	}
	return nil
}

func makeOperation(cfg config, id, targetCPU, perWorker int, shared *cache) (*operation, error) {
	c := shared
	if cfg.Cache == "sharded" {
		c = newCache(id*perWorker, perWorker, false)
	}
	keyCount := 1
	for keyCount < perWorker {
		keyCount *= 2
	}
	keys := make([]uint64, keyCount)
	state := uint64(id+1) * 0x123456789abcdef
	for i := range keys {
		keys[i] = uint64(id*perWorker + i%perWorker)
	}
	for i := len(keys) - 1; i > 0; i-- {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		j := int(state % uint64(i+1))
		keys[i], keys[j] = keys[j], keys[i]
	}
	o := &operation{cfg: cfg, cache: c, keys: keys, send: make([]byte, recordBytes), incomingAtSetup: -1}
	if cfg.Mode == "tcp" {
		conn, err := net.DialTimeout("tcp4", cfg.Address, 5*time.Second)
		if err != nil {
			return nil, err
		}
		o.conn = conn.(*net.TCPConn)
		if err := o.conn.SetNoDelay(true); err != nil {
			o.conn.Close()
			return nil, err
		}
		if err := o.conn.SetDeadline(time.Now().Add(cfg.Warmup + cfg.Duration + 30*time.Second)); err != nil {
			o.conn.Close()
			return nil, err
		}
		if cfg.SocketCPU {
			value, err := socketOption(o.conn, incomingCPU, &targetCPU)
			if err != nil || value != targetCPU {
				o.conn.Close()
				return nil, fmt.Errorf("SO_INCOMING_CPU: value=%d err=%v", value, err)
			}
			o.incomingAtSetup = value
		}
		o.send, o.receive = make([]byte, cfg.Batch*recordBytes), make([]byte, cfg.Batch*recordBytes)
	}
	return o, nil
}

type gate struct {
	ready, warmed           sync.WaitGroup
	warmStart, measureStart chan struct{}
	warmStop, stop          atomic.Bool
	stopSignal              chan struct{}
	jobs                    []chan time.Time
	tokens                  chan struct{}
	producerDone            chan struct{}
}

// One random batch in each 256-batch window prevents phase locking to the
// power-of-two key stream. Algorithm R retains a bounded sample of the full
// measurement instead of keeping only its earliest latency observations.
type latencySampler struct {
	state, next, seen uint64
	samples           []int64
}

func newLatencySampler(seed uint64, capacity int) latencySampler {
	return latencySampler{state: seed, samples: make([]int64, 0, capacity)}
}

func (s *latencySampler) random() uint64 {
	s.state += 0x9e3779b97f4a7c15
	x := s.state
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func (s *latencySampler) selectBatch(batch uint64) bool {
	if batch&255 == 0 {
		s.next = batch + (s.random() & 255)
	}
	return batch == s.next
}

func (s *latencySampler) record(ns int64) {
	s.seen++
	if len(s.samples) < cap(s.samples) {
		s.samples = append(s.samples, ns)
		return
	}
	if index := s.random() % s.seen; index < uint64(len(s.samples)) {
		s.samples[index] = ns
	}
}

func socketAffinityStatus(requested bool, target, setup, before, after int, otherSamples uint64) string {
	if !requested {
		return "not-requested"
	}
	if setup != target {
		return "setup-failed"
	}
	if before != target || after != target || otherSamples > 0 {
		return "unsupported-observed-mismatch"
	}
	// Sampling can falsify a fixed assignment, but cannot prove it between probes.
	return "unverified-no-mismatch-observed"
}

func worker(cfg config, id, targetCPU, perWorker int, shared *cache, g *gate, done chan<- workerResult) {
	r := workerResult{Worker: id, TargetCPU: targetCPU, IncomingAtSetup: -1, IncomingBefore: -1, IncomingAfter: -1, NAPIID: -1, SocketAffinityStatus: "not-requested", CPUObservations: make(map[int]uint64)}
	sampler := newLatencySampler(cfg.SampleSeed^(uint64(id+1)*0xd1b54a32d192ed03), 65536)
	defer func() { done <- r }()
	for cpu := 0; cpu < 1024; cpu++ {
		r.CPUObservations[cpu] = 0
	}
	var o *operation
	var err error
	if cfg.Pin {
		var unpin func()
		unpin, err = pin(targetCPU)
		if err == nil {
			defer unpin()
		}
	}
	if err == nil {
		o, err = makeOperation(cfg, id, targetCPU, perWorker, shared)
	}
	if o != nil && o.conn != nil {
		defer o.conn.Close()
		r.IncomingAtSetup = o.incomingAtSetup
	}
	if err != nil {
		r.Error = err.Error()
	}
	g.ready.Done()
	<-g.warmStart
	if err == nil {
		for !g.warmStop.Load() {
			if err = o.batch(false); err != nil {
				r.Error = err.Error()
				break
			}
		}
	}
	g.warmed.Done()
	<-g.measureStart
	if err != nil {
		return
	}
	o.sequence, o.checksum = 0, 0
	if o.conn != nil {
		r.IncomingBefore, err = socketOption(o.conn, incomingCPU, nil)
	}
	if err != nil {
		r.Error = err.Error()
		return
	}
	previousCPU := -1
loop:
	for batch := uint64(0); !g.stop.Load(); batch++ {
		var queued time.Time
		if cfg.Skew {
			select {
			case queued = <-g.jobs[id]:
			case <-g.stopSignal:
				break loop
			}
			if g.stop.Load() {
				break loop
			}
		}
		if batch%256 == 0 {
			cpu, e := currentCPU()
			if e != nil {
				r.Error = e.Error()
				break
			}
			r.CPUObservations[cpu]++
			if previousCPU >= 0 && previousCPU != cpu {
				r.SampledCPUChanges++
			}
			previousCPU = cpu
			if cfg.Pin && cpu != targetCPU {
				r.Error = "pinned worker migrated"
				break
			}
		}
		var started time.Time
		if sampler.selectBatch(batch) {
			started = time.Now()
			if cfg.Skew {
				started = queued
			}
		}
		if err = o.batch(true); err != nil {
			r.Error = err.Error()
			break
		}
		r.Operations += uint64(cfg.Batch)
		if cfg.Skew {
			g.tokens <- struct{}{}
		}
		if !started.IsZero() {
			sampler.record(time.Since(started).Nanoseconds())
			// Outside the timed batch, observe the one-time socket option instead
			// of claiming it enforces receive placement throughout the run.
			if cfg.SocketCPU {
				value, e := socketOption(o.conn, incomingCPU, nil)
				if e != nil {
					r.Error = e.Error()
					break
				}
				if value == targetCPU {
					r.SocketTargetSamples++
				} else {
					r.SocketOtherSamples++
				}
			}
		}
	}
	r.BatchLatencyNS = sampler.samples
	r.LatencySamplesSeen, r.LatencySamplesRetained = sampler.seen, uint64(len(sampler.samples))
	r.Checksum = o.checksum
	if o.conn != nil {
		r.IncomingAfter, err = socketOption(o.conn, incomingCPU, nil)
		if err == nil {
			r.NAPIID, err = socketOption(o.conn, incomingNAPI, nil)
		}
		if err != nil {
			r.Error = err.Error()
		}
	}
	r.SocketAffinityStatus = socketAffinityStatus(cfg.SocketCPU, targetCPU, r.IncomingAtSetup, r.IncomingBefore, r.IncomingAfter, r.SocketOtherSamples)
}

type cpuTicks struct{ total, idle uint64 }

func readCPUs(cpus []int) map[int]cpuTicks {
	wanted := make(map[int]bool)
	for _, cpu := range cpus {
		wanted[cpu] = true
	}
	raw, err := os.ReadFile("/proc/stat")
	if err != nil {
		return nil
	}
	result := make(map[int]cpuTicks)
	for _, line := range strings.Split(string(raw), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 9 || !strings.HasPrefix(fields[0], "cpu") {
			continue
		}
		cpu, err := strconv.Atoi(strings.TrimPrefix(fields[0], "cpu"))
		if err != nil || !wanted[cpu] {
			continue
		}
		var ticks cpuTicks
		for i := 1; i <= 8; i++ {
			value, _ := strconv.ParseUint(fields[i], 10, 64)
			ticks.total += value
			if i == 4 || i == 5 {
				ticks.idle += value
			}
		}
		result[cpu] = ticks
	}
	return result
}

type result struct {
	Config                                                                   config
	GoVersion                                                                string
	Workers                                                                  []workerResult
	Seconds, OperationsPerSecond, ProcessCPUSeconds, CPUSecondsPerMillionOps float64
	Operations                                                               uint64
	Allocations, AllocatedBytes, GCs                                         uint64
	HeapBytes                                                                uint64
	VoluntarySwitches, InvoluntarySwitches                                   int64
	CPUOverallBusyPercent                                                    map[int]float64
	BatchP50NS, BatchP95NS, BatchP99NS                                       int64
	WorkerOperationsCV                                                       float64
	Error                                                                    string `json:",omitempty"`
}

func seconds(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }

func run(cfg config, cpus []int) (result, error) {
	r := result{Config: cfg, GoVersion: runtime.Version(), Workers: make([]workerResult, 0, len(cpus))}
	runtime.GOMAXPROCS(len(cpus))
	entries := (cfg.MiB * 1024 * 1024 / int(unsafe.Sizeof(entry{}))) / len(cpus) * len(cpus)
	if entries < len(cpus)*2 {
		return r, fmt.Errorf("cache too small")
	}
	var shared *cache
	if cfg.Cache == "shared" {
		shared = newCache(0, entries, true)
	}
	g := &gate{warmStart: make(chan struct{}), measureStart: make(chan struct{}), stopSignal: make(chan struct{})}
	if cfg.Skew {
		if len(cpus) < 2 {
			return r, fmt.Errorf("skew requires at least two workers")
		}
		g.tokens = make(chan struct{}, 64)
		g.producerDone = make(chan struct{})
		for range cpus {
			g.jobs = append(g.jobs, make(chan time.Time, 64))
		}
		for i := 0; i < 64; i++ {
			g.tokens <- struct{}{}
		}
		go func() {
			defer close(g.producerDone)
			<-g.measureStart
			for sequence := 0; ; sequence++ {
				select {
				case <-g.tokens:
				case <-g.stopSignal:
					return
				}
				target := 0
				if sequence%100 >= 80 {
					target = 1 + (sequence%100-80)%(len(cpus)-1)
				}
				select {
				case g.jobs[target] <- time.Now():
				case <-g.stopSignal:
					return
				}
			}
		}()
	}
	g.ready.Add(len(cpus))
	g.warmed.Add(len(cpus))
	done := make(chan workerResult, len(cpus))
	for id, cpu := range cpus {
		go worker(cfg, id, cpu, entries/len(cpus), shared, g, done)
	}
	g.ready.Wait()
	runtime.GC()
	close(g.warmStart)
	time.Sleep(cfg.Warmup)
	g.warmStop.Store(true)
	g.warmed.Wait()
	var before, after runtime.MemStats
	var usageBefore, usageAfter syscall.Rusage
	cpuBefore := readCPUs(cpus)
	runtime.ReadMemStats(&before)
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usageBefore); err != nil {
		return r, err
	}
	started := time.Now()
	close(g.measureStart)
	time.Sleep(cfg.Duration)
	g.stop.Store(true)
	close(g.stopSignal)
	if cfg.Skew {
		<-g.producerDone
	}
	for range cpus {
		wr := <-done
		r.Workers = append(r.Workers, wr)
		r.Operations += wr.Operations
		if wr.Error != "" {
			r.Error = wr.Error
		}
	}
	r.Seconds = time.Since(started).Seconds()
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usageAfter); err != nil {
		return r, err
	}
	runtime.ReadMemStats(&after)
	cpuAfter := readCPUs(cpus)
	var samples []int64
	for _, wr := range r.Workers {
		samples = append(samples, wr.BatchLatencyNS...)
		for cpu, count := range wr.CPUObservations {
			if count == 0 {
				delete(wr.CPUObservations, cpu)
			}
		}
	}
	r.ProcessCPUSeconds = seconds(usageAfter.Utime) + seconds(usageAfter.Stime) - seconds(usageBefore.Utime) - seconds(usageBefore.Stime)
	r.OperationsPerSecond = float64(r.Operations) / r.Seconds
	if r.Operations > 0 {
		r.CPUSecondsPerMillionOps = r.ProcessCPUSeconds / float64(r.Operations) * 1e6
	}
	r.Allocations, r.AllocatedBytes = after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc
	r.GCs, r.HeapBytes = uint64(after.NumGC-before.NumGC), after.HeapAlloc
	r.VoluntarySwitches, r.InvoluntarySwitches = usageAfter.Nvcsw-usageBefore.Nvcsw, usageAfter.Nivcsw-usageBefore.Nivcsw
	r.CPUOverallBusyPercent = make(map[int]float64)
	for _, cpu := range cpus {
		dt, di := cpuAfter[cpu].total-cpuBefore[cpu].total, cpuAfter[cpu].idle-cpuBefore[cpu].idle
		if dt > 0 {
			r.CPUOverallBusyPercent[cpu] = 100 * float64(dt-di) / float64(dt)
		}
	}
	sort.Slice(r.Workers, func(i, j int) bool { return r.Workers[i].Worker < r.Workers[j].Worker })
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	if len(samples) > 0 {
		r.BatchP50NS, r.BatchP95NS, r.BatchP99NS = samples[(len(samples)-1)*50/100], samples[(len(samples)-1)*95/100], samples[(len(samples)-1)*99/100]
	}
	mean, variance := float64(r.Operations)/float64(len(cpus)), float64(0)
	for _, wr := range r.Workers {
		variance += math.Pow(float64(wr.Operations)-mean, 2)
	}
	if mean > 0 {
		r.WorkerOperationsCV = math.Sqrt(variance/float64(len(cpus))) / mean
	}
	if r.Error != "" {
		return r, fmt.Errorf("worker: %s", r.Error)
	}
	return r, nil
}

func serve(cfg config) error {
	listener, err := net.Listen("tcp4", cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{"address": listener.Addr().String(), "pid": os.Getpid()}); err != nil {
		return err
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go func() {
			defer conn.Close()
			if err := conn.(*net.TCPConn).SetNoDelay(true); err != nil {
				return
			}
			buffer := make([]byte, cfg.Batch*recordBytes)
			for {
				if _, err := io.ReadFull(conn, buffer); err != nil {
					return
				}
				if err := writeAll(conn, buffer); err != nil {
					return
				}
			}
		}()
	}
}

func main() {
	var cfg config
	flag.StringVar(&cfg.Mode, "mode", "cpu", "cpu, tcp, or server")
	flag.StringVar(&cfg.Cache, "cache", "sharded", "shared or sharded")
	flag.StringVar(&cfg.CPUs, "cpus", "0,1,2,3", "one CPU target per worker; constrain whole process with taskset")
	flag.StringVar(&cfg.Address, "address", "", "echo server address")
	flag.StringVar(&cfg.Listen, "listen", "127.0.0.1:0", "server listen address")
	flag.BoolVar(&cfg.Pin, "pin", false, "lock each worker thread and set CPU affinity")
	flag.BoolVar(&cfg.SocketCPU, "socket-cpu", false, "one-time SO_INCOMING_CPU feasibility probe; does not enforce sustained receive affinity")
	flag.BoolVar(&cfg.Skew, "skew", false, "bounded 64-batch dispatcher sends 80% of batches to worker zero; latency includes queueing")
	flag.IntVar(&cfg.MiB, "mib", 1, "total entry storage MiB, excluding map/index overhead")
	flag.IntVar(&cfg.Updates, "updates", 0, "update percentage; logical lookups always touch LRU")
	flag.IntVar(&cfg.Batch, "batch", 32, "operations per batch / TCP pipeline depth")
	flag.DurationVar(&cfg.Warmup, "warmup", 2*time.Second, "read-only warmup")
	flag.DurationVar(&cfg.Duration, "duration", 10*time.Second, "measurement duration")
	flag.Uint64Var(&cfg.SampleSeed, "sample-seed", 1, "latency sampling seed; use the same seed for paired variants")
	flag.Parse()
	cpus, err := cpuList(cfg.CPUs)
	if err == nil && (cfg.MiB < 1 || cfg.MiB > 1024 || cfg.Batch < 1 || cfg.Batch > 4096 || cfg.Updates < 0 || cfg.Updates > 100 || cfg.Duration <= 0 || cfg.Warmup < 0) {
		err = fmt.Errorf("invalid numeric settings")
	}
	if err == nil && cfg.Mode != "cpu" && cfg.Mode != "tcp" && cfg.Mode != "server" {
		err = fmt.Errorf("invalid mode")
	}
	if err == nil && cfg.Cache != "shared" && cfg.Cache != "sharded" {
		err = fmt.Errorf("invalid cache")
	}
	if err == nil && cfg.SocketCPU && cfg.Mode != "tcp" {
		err = fmt.Errorf("socket-cpu requires tcp mode")
	}
	if err == nil && cfg.Mode == "server" {
		err = serve(cfg)
		if err == nil {
			return
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r, err := run(cfg, cpus)
	if e := json.NewEncoder(os.Stdout).Encode(r); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
