// Command driver-comparison runs both public adapters through this shared core.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	hdr "github.com/HdrHistogram/hdrhistogram-go"
)

var queueSize = flag.Int("queue-size", 1024, "bounded arrival queue capacity, at least four times concurrency")

var metadataRefresh = flag.Duration("metadata-refresh", 0, "v2 periodic metadata refresh; zero matches legacy event-driven metadata")

const keys = 100000
const partitions = 1024
const rowsPerPartition = 128
const pointRead = "SELECT k,r,p FROM bench_compare.point WHERE k=?"
const pageRead = "SELECT k,r,p FROM bench_compare.paged WHERE k=?"
const pointWrite = "INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)"
const pageWrite = "INSERT INTO bench_compare.paged (k,r,p) VALUES (?,?,?)"

type row struct {
	K, R int32
	P    []byte
}
type consumer func(*row) error
type worker interface {
	Read(context.Context, int32, consumer) error
	Write(context.Context, int32, int32, []byte) error
}
type adapter interface {
	Worker(bool, bool) (worker, error)
	Exec(context.Context, string) error
	Close() error
	Evidence() any
}

var incremental = flag.Bool("incremental", false, "explicit bounded incremental receive for streaming queries")

var payloads [2][256][]byte
var checksums [2][256][32]byte

func initialize() {
	for size := 0; size < 2; size++ {
		for id := 0; id < 256; id++ {
			b := make([]byte, 1024*(1+3*size))
			x := uint64(id + 1)
			for i := range b {
				x ^= x << 13
				x ^= x >> 7
				x ^= x << 17
				b[i] = byte(x)
			}
			payloads[size][id] = b
			checksums[size][id] = sha256.Sum256(b)
		}
	}
}
func variant(k, r int32) int { return int((uint32(k)*131 + uint32(r)*17) % 256) }
func payload(paged bool, k, r int32) []byte {
	s := 0
	if paged {
		s = 1
	}
	return payloads[s][variant(k, r)]
}
func validator(paged bool, k int32) consumer {
	count := int32(0)
	return func(v *row) error {
		s := 0
		if paged {
			s = 1
		}
		if v.K != k || v.R != count || len(v.P) != 1024*(1+3*s) || sha256.Sum256(v.P) != checksums[s][variant(k, count)] {
			return fmt.Errorf("invalid owned row key=%d row=%d got=%d/%d len=%d", k, count, v.K, v.R, len(v.P))
		}
		count++
		return nil
	}
}
func operation(ctx context.Context, w worker, scenario string, seq uint64) error {
	paged := scenario == "paged"
	space := uint64(keys)
	if paged {
		space = partitions
	}
	k := int32((seq * 7919) % space)
	if scenario == "write" {
		return w.Write(ctx, k, 0, payload(false, k, 0))
	}
	n := 0
	validate := validator(paged, k)
	err := w.Read(ctx, k, func(v *row) error { n++; return validate(v) })
	expected := 1
	if paged {
		expected = rowsPerPartition
	}
	if err == nil && n != expected {
		return fmt.Errorf("rows=%d expected=%d", n, expected)
	}
	return err
}
func check(a adapter, stream bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, paged := range []bool{false, true} {
		w, e := a.Worker(paged, stream)
		if e != nil {
			return e
		}
		k := int32(-7)
		n := 0
		if e = w.Read(ctx, k, func(*row) error { n++; return nil }); e != nil || n != 0 {
			return fmt.Errorf("missing: %d %v", n, e)
		}
		limit := 1
		if paged {
			limit = 128
		}
		for r := 0; r < limit; r++ {
			if e = w.Write(ctx, k, int32(r), payload(paged, k, int32(r))); e != nil {
				return e
			}
		}
		for r := 0; r < limit; r++ {
			if e = w.Write(ctx, k, int32(r), payload(paged, k, int32(r)+1)); e != nil {
				return e
			}
		}
		n = 0
		e = w.Read(ctx, k, func(v *row) error {
			s := 0
			if paged {
				s = 1
			}
			if v.K != k || v.R != int32(n) || sha256.Sum256(v.P) != checksums[s][variant(k, int32(n)+1)] {
				return errors.New("overwrite/readback failed")
			}
			n++
			return nil
		})
		if e != nil || n != limit {
			return fmt.Errorf("readback: %d %v", n, e)
		}
		if e = a.Exec(ctx, fmt.Sprintf("DELETE FROM bench_compare.%s WHERE k=-7", map[bool]string{false: "point", true: "paged"}[paged])); e != nil {
			return e
		}
	}
	return nil
}
func prepare(a adapter, scenario string) error {
	ctx := context.Background()
	table := "point"
	paged := scenario == "paged"
	if paged {
		table = "paged"
	}
	if err := a.Exec(ctx, "TRUNCATE bench_compare."+table); err != nil {
		return err
	}
	count := keys
	if paged {
		count = partitions * 128
	}
	loadStart := time.Now()
	loadRate := 20000
	if paged {
		loadRate = 5000
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for id := 0; id < 16; id++ {
		w, e := a.Worker(paged, false)
		if e != nil {
			return e
		}
		wg.Add(1)
		go func(id int, w worker) {
			defer wg.Done()
			for i := id; i < count; i += 16 {
				if delay := time.Until(loadStart.Add(time.Duration(i) * time.Second / time.Duration(loadRate))); delay > 0 {
					time.Sleep(delay)
				}
				k, r := int32(i), int32(0)
				if paged {
					k = int32(i / 128)
					r = int32(i % 128)
				}
				if e := w.Write(ctx, k, r, payload(paged, k, r)); e != nil {
					errs <- e
					return
				}
			}
		}(id, w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	return nil
}
func warmDataset(a adapter, scenario string, stream bool) error {
	count := keys
	if scenario == "paged" {
		count = partitions
	}
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for id := 0; id < 16; id++ {
		w, e := a.Worker(scenario == "paged", stream)
		if e != nil {
			return e
		}
		wg.Add(1)
		go func(id int, w worker) {
			defer wg.Done()
			s := scenario
			if s == "write" {
				s = "read"
			}
			for i := id; i < count; i += 16 {
				if e := operation(context.Background(), w, s, uint64(i)); e != nil {
					errs <- e
					return
				}
			}
		}(id, w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		return e
	}
	return nil
}

type hist struct {
	*hdr.Histogram
	Overflow int64
}

func histogram() *hist { return &hist{Histogram: hdr.New(1, 60000000, 3)} }
func (h *hist) record(d time.Duration) {
	us := max(int64(1), d.Microseconds())
	if e := h.RecordValue(us); e != nil {
		h.Overflow++
	}
}
func (h *hist) export() any {
	return map[string]any{"snapshot": h.Export(), "overflow": h.Overflow, "p90_us": h.ValueAtQuantile(90), "p95_us": h.ValueAtQuantile(95), "p99_us": h.ValueAtQuantile(99), "count": h.TotalCount()}
}

type stats struct {
	mu                                                                         sync.Mutex
	Latency, Dispatch, Queue, Schedule                                         *hist
	Admitted, Completed, InWindow, Errors, Timeouts, Offered, Rejected, Missed int64
	ErrorSamples                                                               []string
	QueueCapacity, PeakQueue, EndQueue                                         int
	AllocBytes, Mallocs, GC                                                    uint64
	CPUSeconds, Elapsed, Rate                                                  float64
	Start, End                                                                 string
	Drained                                                                    int64
	AffinityChecks                                                             int64
	AffinityError                                                              string
	Backlog                                                                    []int
}

func (s *stats) result() any {
	return map[string]any{"admitted": s.Admitted, "completed": s.Completed, "in_window": s.InWindow, "drained": s.Drained, "errors": s.Errors, "timeouts": s.Timeouts, "error_samples": s.ErrorSamples, "offered": s.Offered, "rejected": s.Rejected, "missed_schedules_over_1ms": s.Missed, "queue_capacity": s.QueueCapacity, "peak_queue": s.PeakQueue, "end_queue": s.EndQueue, "backlog_samples": s.Backlog, "latency": s.Latency.export(), "dispatch_latency": s.Dispatch.export(), "queue_latency": s.Queue.export(), "schedule_lag": s.Schedule.export(), "alloc_bytes": s.AllocBytes, "mallocs": s.Mallocs, "gc": s.GC, "cpu_seconds_including_drain": s.CPUSeconds, "duration_seconds": s.Elapsed, "throughput": float64(s.InWindow) / s.Elapsed, "target_rate": s.Rate, "start": s.Start, "end": s.End, "affinity_checks": s.AffinityChecks, "affinity_error": s.AffinityError}
}
func cpu() float64 {
	var r syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &r)
	return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
}
func affinity() error {
	files, e := filepath.Glob("/proc/self/task/*/status")
	if e != nil {
		return e
	}
	for _, p := range files {
		b, e := os.ReadFile(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return e
		}
		found := false
		for _, l := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(l, "Cpus_allowed_list:") {
				found = true
				if strings.TrimSpace(strings.TrimPrefix(l, "Cpus_allowed_list:")) != "8" {
					return fmt.Errorf("%s: %s", p, l)
				}
			}
		}
		if !found {
			return fmt.Errorf("missing affinity %s", p)
		}
	}
	return nil
}

type arrival struct {
	seq                 uint64
	scheduled, enqueued time.Time
}

func phase(workers []worker, scenario string, duration time.Duration, rate float64) (*stats, error) {
	s := &stats{Latency: histogram(), Dispatch: histogram(), Queue: histogram(), Schedule: histogram(), Elapsed: duration.Seconds(), Rate: rate}
	if e := affinity(); e != nil {
		return nil, e
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	c0 := cpu()
	start := time.Now()
	end := start.Add(duration)
	s.Start = start.UTC().Format(time.RFC3339Nano)
	s.End = end.UTC().Format(time.RFC3339Nano)
	ctx, cancel := context.WithDeadline(context.Background(), end.Add(15*time.Second))
	defer cancel()
	done := make(chan struct{})
	monitorDone := make(chan struct{})
	s.QueueCapacity = max(*queueSize, len(workers)*4)
	queue := make(chan arrival, s.QueueCapacity)
	go func() {
		defer close(monitorDone)
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				e := affinity()
				s.mu.Lock()
				s.AffinityChecks++
				if e != nil {
					s.AffinityError = e.Error()
				}
				s.Backlog = append(s.Backlog, len(queue))
				s.mu.Unlock()
			}
		}
	}()
	execute := func(w worker, a arrival) {
		dispatch := time.Now()
		opCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		e := operation(opCtx, w, scenario, a.seq)
		stop()
		finish := time.Now()
		s.mu.Lock()
		defer s.mu.Unlock()
		s.Completed++
		s.Latency.record(finish.Sub(a.scheduled))
		s.Dispatch.record(finish.Sub(dispatch))
		s.Queue.record(dispatch.Sub(a.enqueued))
		if e != nil {
			s.Errors++
			if errors.Is(e, context.DeadlineExceeded) || strings.Contains(strings.ToLower(e.Error()), "timeout") {
				s.Timeouts++
			}
			if len(s.ErrorSamples) < 8 {
				s.ErrorSamples = append(s.ErrorSamples, e.Error())
			}
		} else if !finish.After(end) {
			s.InWindow++
		} else {
			s.Drained++
		}
	}
	var wg sync.WaitGroup
	for id, w := range workers {
		wg.Add(1)
		go func(id int, w worker) {
			defer wg.Done()
			if rate > 0 {
				for a := range queue {
					execute(w, a)
				}
			} else {
				for seq := uint64(id); ; seq += uint64(len(workers)) {
					now := time.Now()
					if !now.Before(end) {
						return
					}
					s.mu.Lock()
					s.Admitted++
					s.Offered++
					s.mu.Unlock()
					execute(w, arrival{seq, now, now})
				}
			}
		}(id, w)
	}
	if rate > 0 {
		total := uint64(math.Ceil(duration.Seconds() * rate))
		for seq := uint64(0); seq < total; seq++ {
			scheduled := start.Add(time.Duration(float64(seq) / rate * 1e9))
			now := time.Now()
			if now.Before(scheduled) {
				time.Sleep(scheduled.Sub(now))
				now = time.Now()
			}
			if !now.Before(end) {
				s.mu.Lock()
				s.Offered += int64(total - seq)
				s.Rejected += int64(total - seq)
				s.Missed += int64(total - seq)
				s.mu.Unlock()
				break
			}
			s.mu.Lock()
			s.Offered++
			s.Schedule.record(now.Sub(scheduled))
			if now.Sub(scheduled) > time.Millisecond {
				s.Missed++
			}
			s.mu.Unlock()
			select {
			case queue <- arrival{seq, scheduled, now}:
				s.mu.Lock()
				s.Admitted++
				s.PeakQueue = max(s.PeakQueue, len(queue))
				s.mu.Unlock()
			default:
				s.mu.Lock()
				s.Rejected++
				s.mu.Unlock()
			}
		}
		if wait := time.Until(end); wait > 0 {
			time.Sleep(wait)
		}
		s.EndQueue = len(queue)
		close(queue)
	}
	wg.Wait()
	close(done)
	<-monitorDone
	runtime.ReadMemStats(&after)
	s.CPUSeconds = cpu() - c0
	s.AllocBytes = after.TotalAlloc - before.TotalAlloc
	s.Mallocs = after.Mallocs - before.Mallocs
	s.GC = uint64(after.NumGC - before.NumGC)
	if e := affinity(); e != nil {
		s.AffinityError = e.Error()
	}
	if s.AffinityError != "" {
		return s, errors.New(s.AffinityError)
	}
	return s, nil
}
func run() (resultErr error) {
	hosts := flag.String("hosts", "", "comma separated IPs")
	mode := flag.String("mode", "run", "schema, check, prepare, warm, run")
	scenario := flag.String("scenario", "read", "read, write, paged")
	stream := flag.Bool("stream", false, "v2 streaming")
	concurrency := flag.Int("concurrency", 1, "workers")
	warmup := flag.Duration("warmup", 30*time.Second, "warmup")
	duration := flag.Duration("duration", 120*time.Second, "measurement")
	rate := flag.Float64("rate", 0, "scheduled arrivals/s; zero means closed loop")
	output := flag.String("output", "", "JSON path")
	profilePrefix := flag.String("profile-prefix", "", "write measured-phase CPU and before/after heap profiles with this prefix")
	flag.Parse()
	if runtime.GOMAXPROCS(0) != 1 {
		return errors.New("GOMAXPROCS must be 1")
	}
	if e := affinity(); e != nil {
		return e
	}
	fmt.Fprintln(os.Stderr, "AFFINITY initial: all application threads restricted to CPU 8")
	defer func() {
		resultErr = errors.Join(resultErr, affinity())
		fmt.Fprintln(os.Stderr, "AFFINITY final: checked all application threads")
	}()

	if *concurrency < 1 || *queueSize < 1 || *rate < 0 || *duration <= 0 {
		return errors.New("invalid measurement configuration")
	}
	if *scenario != "read" && *scenario != "write" && *scenario != "paged" {
		return errors.New("invalid scenario")
	}
	initialize()
	a, e := connect(strings.Split(*hosts, ","))
	if e != nil {
		return e
	}
	defer func() { resultErr = errors.Join(resultErr, a.Close()) }()
	switch *mode {
	case "schema":
		for _, q := range []string{"CREATE KEYSPACE IF NOT EXISTS bench_compare WITH replication={'class':'NetworkTopologyStrategy','datacenter1':3} AND tablets={'enabled':false}", "CREATE TABLE IF NOT EXISTS bench_compare.point (k int PRIMARY KEY,r int,p blob)", "CREATE TABLE IF NOT EXISTS bench_compare.paged (k int,r int,p blob,PRIMARY KEY(k,r))"} {
			if e := a.Exec(context.Background(), q); e != nil {
				return e
			}
		}
		return nil
	case "check":
		return check(a, *stream)
	case "probe":
		for _, sc := range []string{"read", "write", "paged"} {
			w, err := a.Worker(sc == "paged", *stream)
			if err != nil {
				return err
			}
			if err := operation(context.Background(), w, sc, 0); err != nil {
				return err
			}
		}
		fmt.Printf("PROBE %d %v\n", os.Getpid(), a.Evidence())
		time.Sleep(8 * time.Second)
		return nil
	case "prepare":
		return prepare(a, *scenario)
	case "warm":
		return warmDataset(a, *scenario, *stream)
	case "run":
	default:
		return errors.New("invalid mode")
	}
	workers := make([]worker, *concurrency)
	for i := range workers {
		workers[i], e = a.Worker(*scenario == "paged", *stream)
		if e != nil {
			return e
		}
	}
	// Prime all prepared statements on routed hosts before timing.
	for i, w := range workers {
		if e := operation(context.Background(), w, *scenario, uint64(i)); e != nil {
			return e
		}
	}
	var warm *stats
	if *warmup > 0 {
		warm, e = phase(workers, *scenario, *warmup, *rate)
		if e != nil {
			return e
		}
		if warm.Errors > 0 {
			return fmt.Errorf("warmup errors: %v", warm.ErrorSamples)
		}
	}
	profiles, e := startProfiles(*profilePrefix)
	if e != nil {
		return e
	}
	measured, e := phase(workers, *scenario, *duration, *rate)
	profileErr := profiles.stop()
	e = errors.Join(e, profileErr)
	if e != nil {
		return e
	}
	result := map[string]any{"driver": driverName, "scenario": *scenario, "stream": *stream, "concurrency": *concurrency, "runtime": runtime.Version(), "metadata_refresh_ns": int64(*metadataRefresh), "gomaxprocs": runtime.GOMAXPROCS(0), "evidence": a.Evidence(), "measurement": measured.result()}
	if *profilePrefix != "" {
		result["profiles"] = map[string]any{"prefix": *profilePrefix, "scope": "measured phase including bounded drain; excludes setup, priming, warmup, and heap snapshots", "heap_sample_rate": runtime.MemProfileRate}
	}
	if warm != nil {
		result["warmup"] = warm.result()
	}
	b, e := json.MarshalIndent(result, "", "  ")
	if e != nil {
		return e
	}
	if *output != "" {
		if e = os.WriteFile(*output, append(b, '\n'), 0644); e != nil {
			return e
		}
	} else {
		fmt.Println(string(b))
	}
	if measured.Errors > 0 {
		return fmt.Errorf("measured errors: %v", measured.ErrorSamples)
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
