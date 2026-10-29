package gocql

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"time"
)

// Tracer is the interface implemented by query tracers. Tracers have the
// ability to obtain a detailed event log of all events that happened during
// the execution of a query from Cassandra. Gathering this information might
// be essential for debugging and optimizing queries, but this feature should
// not be used on production systems with very high load.
type Tracer interface {
	Trace(traceId []byte)
}

type TraceWriter struct {
	session *Session
	w       io.Writer
	mu      sync.Mutex

	maxAttempts   int
	sleepInterval time.Duration
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(session *Session, w io.Writer) *TraceWriter {
	return &TraceWriter{session: session, w: w, maxAttempts: 5, sleepInterval: 3 * time.Millisecond}
}

func (t *TraceWriter) SetMaxAttempts(maxAttempts int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.maxAttempts = maxAttempts
}

func (t *TraceWriter) SetSleepInterval(sleepInterval time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sleepInterval = sleepInterval
}

func (t *TraceWriter) Trace(traceId []byte) {

	t.mu.Lock()
	defer t.mu.Unlock()
	tracer := NewTracer(t.session)
	attempts := t.maxAttempts
	if attempts <= 0 {
		attempts = 1
	}
	ready := false
	for i := 0; i < attempts; i++ {
		var err error
		ready, err = tracer.IsReady(traceId)
		if err != nil {
			fmt.Fprintln(t.w, "Error:", err)
			return
		}
		if ready || i == attempts-1 {
			break
		}
		timer := time.NewTimer(t.sleepInterval)
		ctx := v2SchemaContext(t.session)
		select {
		case <-ctx.Done():
			timer.Stop()
			fmt.Fprintln(t.w, "Error:", ctx.Err())
			return
		case <-timer.C:
		}
	}
	if !ready {
		fmt.Fprintln(t.w, "Error: failed to wait tracing to complete. !!! Tracing is incomplete !!!")
	}
	coordinator, duration, err := tracer.GetCoordinatorTime(traceId)
	if err != nil {
		fmt.Fprintln(t.w, "Error:", err)
		return
	}
	fmt.Fprintf(t.w, "Tracing session %016x (coordinator: %s, duration: %v):\n", traceId, coordinator, duration)
	activities, err := tracer.GetActivities(traceId)
	if err != nil {
		fmt.Fprintln(t.w, "Error:", err)
		return
	}
	for _, e := range activities {
		fmt.Fprintf(t.w, "%s: %s [%s] (source: %s, elapsed: %d)\n", e.Timestamp.Format("2006/01/02 15:04:05.999999"), e.Activity, e.Thread, e.Source, e.Elapsed)
	}

}

type TracerEnhanced struct {
	session  *Session
	traceIDs [][]byte
	mu       sync.Mutex
}

func NewTracer(session *Session) *TracerEnhanced {
	return &TracerEnhanced{session: session}
}

func (t *TracerEnhanced) Trace(traceId []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.traceIDs = append(t.traceIDs, traceId)
}

func (t *TracerEnhanced) AllTraceIDs() [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.traceIDs
}

func (t *TracerEnhanced) IsReady(traceId []byte) (bool, error) {
	rows, err := readV2TraceRows(t.session, traceId, "SELECT duration FROM system_traces.sessions WHERE session_id = ?", OneColumn(NullableCodec(Int32)))
	if err != nil {
		return false, err
	}
	return len(rows) > 0 && rows[0].Valid && rows[0].Value > 0, nil
}

func (t *TracerEnhanced) GetCoordinatorTime(traceId []byte) (string, time.Duration, error) {
	type row struct {
		coordinator netip.Addr
		duration    Nullable[int32]
	}
	decoder := Row(Column("coordinator", InetCodec, func(r *row) *netip.Addr { return &r.coordinator }), Column("duration", NullableCodec(Int32), func(r *row) *Nullable[int32] { return &r.duration }))
	rows, err := readV2TraceRows(t.session, traceId, "SELECT coordinator, duration FROM system_traces.sessions WHERE session_id = ?", decoder)
	if err != nil {
		return "", 0, err
	}
	if len(rows) == 0 {
		return "", 0, ErrNoRows
	}
	return rows[0].coordinator.String(), time.Duration(rows[0].duration.Value) * time.Microsecond, nil
}

type TraceEntry struct {
	Timestamp time.Time
	Activity  string
	Source    string
	Thread    string
	Elapsed   int
}

func (t *TracerEnhanced) GetActivities(traceId []byte) ([]TraceEntry, error) {
	decoder := Row(Column("event_id", TimeUUIDCodec, func(r *traceActivityRow) *TimeUUID { return &r.id }), Column("activity", TextCodec, func(r *traceActivityRow) *Text { return &r.activity }), Column("source", InetCodec, func(r *traceActivityRow) *netip.Addr { return &r.source }), Column("source_elapsed", NullableCodec(Int32), func(r *traceActivityRow) *Nullable[int32] { return &r.elapsed }), Column("thread", TextCodec, func(r *traceActivityRow) *Text { return &r.thread }))
	rows, err := readV2TraceRows(t.session, traceId, "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?", decoder)
	if err != nil {
		return nil, err
	}
	out := make([]TraceEntry, len(rows))
	for i, r := range rows {
		out[i] = TraceEntry{Timestamp: UUID(r.id).Time(), Activity: r.activity.String(), Source: r.source.String(), Thread: r.thread.String(), Elapsed: int(r.elapsed.Value)}
	}
	return out, nil
}

func readV2TraceRows[R any](s *Session, traceID []byte, cql string, decoder RowDecoder[R]) ([]R, error) {
	if s == nil || s.Closed() {
		return nil, ErrClosed
	}
	if len(traceID) != 16 {
		return nil, typeValidation("trace ID", "must contain 16 bytes")
	}
	if s.control == nil {
		return nil, errNoControl
	}
	backend, ok := s.control.(v2SystemQueryer)
	if !ok {
		return nil, typeValidation("tracing", "control connection does not support typed requests")
	}
	var id UUID
	copy(id[:], traceID)
	typ, err := ScalarType(TypeUUID)
	if err != nil {
		return nil, err
	}
	ctx := v2SchemaContext(s)
	timeout := s.cfg.MetadataSchemaRequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return readV2SystemRowsVia(ctx, 4, timeout, backend.queryV2System, Statement[UUID, R]{CQL: cql, Params: OneParam(UUIDCodec), Rows: decoder}, NewColumns(ColumnSpec{Name: "session_id", Type: typ}), id)
}
