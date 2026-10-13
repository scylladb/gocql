//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var (
	traceSessionDurationCols = []rowsCol{{"duration", optNative(TypeInt)}}
	traceSessionCols         = []rowsCol{{"coordinator", optNative(TypeInet)}, {"duration", optNative(TypeInt)}}
	traceEventCols           = []rowsCol{
		{"event_id", optNative(TypeTimeUUID)},
		{"activity", optNative(TypeVarchar)},
		{"source", optNative(TypeInet)},
		{"source_elapsed", optNative(TypeInt)},
		{"thread", optNative(TypeVarchar)},
	}
)

type traceEvent struct {
	at       time.Time
	activity string
	source   net.IP
	elapsed  int32
	thread   string
}

// traceServer answers the system_traces queries the tracers make, as
// described by a traceConfig, and counts the duration lookups.
type traceServer struct {
	*controlTestServer
	durationLookups atomic.Int32
}

// traceConfig describes one traced session. readyAfter is how many duration
// lookups report it unfinished (duration 0) before reporting duration; fail
// makes every trace query fail.
type traceConfig struct {
	readyAfter  int32
	duration    int32
	coordinator net.IP
	events      []traceEvent
	fail        bool
}

func newTraceServer(t *testing.T, cfg traceConfig) *traceServer {
	t.Helper()
	ts := &traceServer{}
	ts.controlTestServer = newControlTestServer(t, nil)

	// The trace queries bind the session id, so the control connection
	// prepares them: answer PREPARE with one of these ids and EXECUTE with
	// the matching rows.
	const (
		durationID = 40 + iota
		coordinatorID
		eventsID
	)
	ts.setPrepareHook(func(query string, stream int, resp *framer, writeID func(uint64)) bool {
		var id uint64
		switch {
		case strings.Contains(query, "system_traces.events"):
			id = eventsID
		case strings.Contains(query, "system_traces.sessions") && strings.Contains(query, "coordinator"):
			id = coordinatorID
		case strings.Contains(query, "system_traces.sessions"):
			id = durationID
		default:
			return false
		}
		testPrepared{
			id:       id,
			keyspace: "system_traces", table: "sessions",
			cols:      []testPreparedCol{{name: "session_id", typ: TypeUUID}},
			pkIndexes: []uint16{0},
		}.write(resp, stream, writeID)
		return true
	})
	ts.setExecuteHook(func(id uint64, stream int, resp *framer) bool {
		if id < durationID || id > eventsID {
			return false
		}
		if cfg.fail {
			writeErrorResult(resp, stream, ErrCodeServer, "traces unavailable")
			return true
		}
		switch id {
		case eventsID:
			var rows [][][]byte
			for _, e := range cfg.events {
				u := UUIDFromTime(e.at)
				rows = append(rows, [][]byte{u[:], []byte(e.activity), e.source.To4(), int32Bytes(e.elapsed), []byte(e.thread)})
			}
			writeRows(resp, stream, traceEventCols, rows)
		case coordinatorID:
			writeRows(resp, stream, traceSessionCols, [][][]byte{{cfg.coordinator.To4(), int32Bytes(cfg.duration)}})
		case durationID:
			d := int32(0)
			if ts.durationLookups.Add(1) > cfg.readyAfter {
				d = cfg.duration
			}
			writeRows(resp, stream, traceSessionDurationCols, [][][]byte{{int32Bytes(d)}})
		}
		return true
	})
	return ts
}

func (ts *traceServer) session(t *testing.T) *Session {
	t.Helper()
	s, err := ts.cluster().CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

var (
	traceID     = []byte{0xde, 0xad, 0xbe, 0xef, 0, 0, 0x10, 0, 0x80, 0, 0, 0, 0, 0, 0, 1}
	traceStart  = time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	traceEvents = []traceEvent{
		{traceStart, "Parsing statement", net.IPv4(10, 0, 0, 1), 15, "Native-Transport-Requests-1"},
		{traceStart.Add(time.Millisecond), "Request complete", net.IPv4(10, 0, 0, 2), 420, "shard 3"},
	}
)

func TestTracerEnhanced(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{duration: 1500, coordinator: net.IPv4(10, 0, 0, 9), events: traceEvents})
	tracer := NewTracer(ts.session(t))

	tracer.Trace(traceID)
	tracer.Trace([]byte{1})
	if got := tracer.AllTraceIDs(); !reflect.DeepEqual(got, [][]byte{traceID, {1}}) {
		t.Fatalf("AllTraceIDs = %x, want [%x 01]", got, traceID)
	}

	ready, err := tracer.IsReady(traceID)
	if err != nil || !ready {
		t.Fatalf("IsReady = %v, %v; want true", ready, err)
	}

	coordinator, d, err := tracer.GetCoordinatorTime(traceID)
	if err != nil || coordinator != "10.0.0.9" || d != 1500*time.Microsecond {
		t.Fatalf("GetCoordinatorTime = %q, %v, %v; want 10.0.0.9, 1.5ms", coordinator, d, err)
	}

	activities, err := tracer.GetActivities(traceID)
	if err != nil {
		t.Fatal(err)
	}
	want := []TraceEntry{
		{Timestamp: traceStart, Activity: "Parsing statement", Source: "10.0.0.1", Elapsed: 15, Thread: "Native-Transport-Requests-1"},
		{Timestamp: traceStart.Add(time.Millisecond), Activity: "Request complete", Source: "10.0.0.2", Elapsed: 420, Thread: "shard 3"},
	}
	if len(activities) != len(want) {
		t.Fatalf("GetActivities = %+v, want %+v", activities, want)
	}
	for i := range want {
		got := activities[i]
		if !got.Timestamp.Equal(want[i].Timestamp) || got.Activity != want[i].Activity || got.Source != want[i].Source ||
			got.Elapsed != want[i].Elapsed || got.Thread != want[i].Thread {
			t.Fatalf("activity %d = %+v, want %+v", i, got, want[i])
		}
	}
}

// A session whose duration is still 0 has not finished tracing.
func TestTracerEnhancedNotReady(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{readyAfter: 1, duration: 10})
	tracer := NewTracer(ts.session(t))

	ready, err := tracer.IsReady(traceID)
	if err != nil || ready {
		t.Fatalf("first IsReady = %v, %v; want false", ready, err)
	}
	ready, err = tracer.IsReady(traceID)
	if err != nil || !ready {
		t.Fatalf("second IsReady = %v, %v; want true", ready, err)
	}
}

func TestTracerEnhancedErrors(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{fail: true})
	tracer := NewTracer(ts.session(t))

	if ready, err := tracer.IsReady(traceID); err == nil || ready {
		t.Fatalf("IsReady = %v, %v; want an error", ready, err)
	}
	if _, _, err := tracer.GetCoordinatorTime(traceID); err == nil {
		t.Fatal("GetCoordinatorTime: want an error")
	}
	if a, err := tracer.GetActivities(traceID); err == nil || a != nil {
		t.Fatalf("GetActivities = %v, %v; want nil and an error", a, err)
	}
}

// syncBuffer lets a test read what TraceWriter wrote.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestTraceWriter(t *testing.T) {
	t.Parallel()

	// Unfinished on the first two lookups: the writer must keep polling.
	ts := newTraceServer(t, traceConfig{readyAfter: 2, duration: 1500, coordinator: net.IPv4(10, 0, 0, 9), events: traceEvents})
	var out syncBuffer
	w := NewTraceWriter(ts.session(t), &out)
	w.SetSleepInterval(time.Millisecond)

	w.Trace(traceID)

	want := "Tracing session deadbeef000010008000000000000001 (coordinator: 10.0.0.9, duration: 1.5ms):\n" +
		"2026/10/13 12:00:00: Parsing statement [Native-Transport-Requests-1] (source: 10.0.0.1, elapsed: 15)\n" +
		"2026/10/13 12:00:00.001: Request complete [shard 3] (source: 10.0.0.2, elapsed: 420)\n"
	got := out.String()
	// event timestamps are formatted in the local time zone
	got = strings.ReplaceAll(got, traceStart.Local().Format("2006/01/02 15:04:05"), "2026/10/13 12:00:00")
	if got != want {
		t.Fatalf("trace output:\n%s\nwant:\n%s", got, want)
	}
	if n := ts.durationLookups.Load(); n != 3 {
		t.Fatalf("duration looked up %d times, want 3", n)
	}
}

// After maxAttempts unfinished lookups the writer says the trace is incomplete
// and prints what there is.
func TestTraceWriterIncomplete(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{readyAfter: 100, duration: 5, coordinator: net.IPv4(10, 0, 0, 9)})
	var out syncBuffer
	w := NewTraceWriter(ts.session(t), &out)
	w.SetMaxAttempts(2)
	w.SetSleepInterval(time.Millisecond)

	w.Trace(traceID)

	if !strings.HasPrefix(out.String(), "Error: failed to wait tracing to complete. !!! Tracing is incomplete !!!\nTracing session") {
		t.Fatalf("trace output = %q, want the incomplete warning before the session line", out.String())
	}
	if n := ts.durationLookups.Load(); n != 2 {
		t.Fatalf("duration looked up %d times, want 2", n)
	}
}

// A non-positive attempt limit still makes one lookup.
func TestTraceWriterAttemptFloor(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{readyAfter: 100, duration: 5, coordinator: net.IPv4(10, 0, 0, 9)})
	var out syncBuffer
	w := NewTraceWriter(ts.session(t), &out)
	w.SetMaxAttempts(0)

	w.Trace(traceID)

	if n := ts.durationLookups.Load(); n != 1 {
		t.Fatalf("duration looked up %d times, want 1", n)
	}
}

func TestTraceWriterError(t *testing.T) {
	t.Parallel()

	ts := newTraceServer(t, traceConfig{fail: true})
	var out syncBuffer
	w := NewTraceWriter(ts.session(t), &out)

	w.Trace(traceID)

	if got := out.String(); !strings.HasPrefix(got, "Error: traces unavailable") || strings.Contains(got, "Tracing session") {
		t.Fatalf("trace output = %q, want only the error", got)
	}
}
