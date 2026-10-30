// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"net/netip"
	"time"
)

func (s *InternalSessionRuntime) Trace(ctx context.Context, id UUID) (implCluster.TraceInfo, error) {
	result := implCluster.TraceInfo{ID: id}
	if ctx == nil {
		return result, typeValidation("trace context", "nil context")
	}
	if s == nil || s.Closed() {
		return result, ErrClosed
	}
	profile := defaultProfile()
	if s.InternalProfile != nil {
		profile = s.InternalProfile.Snapshot()
	}
	if profile.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, profile.Timeout)
		defer cancel()
	}
	uuidType, err := ScalarType(TypeUUID)
	if err != nil {
		return result, err
	}
	params := NewColumns(ColumnSpec{Name: "session_id", Type: uuidType})
	sessionStatement := Statement[UUID, InternalTraceSessionRow]{CQL: "SELECT coordinator, started_at, duration FROM system_traces.sessions WHERE session_id = ?", Params: OneParam(UUIDCodec), Rows: Row(
		Column("coordinator", NullableCodec(InetCodec), func(r *InternalTraceSessionRow) *Nullable[netip.Addr] { return &r.InternalCoordinator }),
		Column("started_at", NullableCodec(TimestampCodec), func(r *InternalTraceSessionRow) *Nullable[Timestamp] { return &r.InternalStarted }),
		Column("duration", NullableCodec(Int32), func(r *InternalTraceSessionRow) *Nullable[int32] { return &r.InternalDuration }),
	)}
	bound, err := BindUnpreparedOwned(sessionStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { bound.Close(); _ = bound.Wait(ctx) }()
	options := Options{Consistency: Option[Consistency]{Value: One, Set: true}, Idempotent: true, Timeout: Option[time.Duration]{Set: true}}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rows, err := Run(ctx, s.Session, bound, options)
		if err != nil {
			return result, err
		}
		ready := false
		if rows.Next() {
			row := rows.Row()
			if row.InternalCoordinator.Valid && row.InternalStarted.Valid && row.InternalDuration.Valid {
				result.Coordinator = row.InternalCoordinator.Value
				result.Started = time.UnixMilli(int64(row.InternalStarted.Value))
				result.Duration = time.Duration(row.InternalDuration.Value) * time.Microsecond
				ready = true
			}
			if rows.Next() {
				_ = rows.Close()
				return result, &ProtocolError{Stage: "trace", Err: fmt.Errorf("duplicate tracing session")}
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return result, err
		}
		if ready {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	eventsStatement := Statement[UUID, InternalTraceActivityRow]{CQL: "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?", Params: OneParam(UUIDCodec), Rows: Row(
		Column("event_id", TimeUUIDCodec, func(r *InternalTraceActivityRow) *TimeUUID { return &r.InternalId }),
		Column("activity", TextCodec, func(r *InternalTraceActivityRow) *Text { return &r.InternalActivity }),
		Column("source", InetCodec, func(r *InternalTraceActivityRow) *netip.Addr { return &r.InternalSource }),
		Column("source_elapsed", NullableCodec(Int32), func(r *InternalTraceActivityRow) *Nullable[int32] { return &r.InternalElapsed }),
		Column("thread", TextCodec, func(r *InternalTraceActivityRow) *Text { return &r.InternalThread }),
	)}
	eventsBound, err := BindUnpreparedOwned(eventsStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { eventsBound.Close(); _ = eventsBound.Wait(ctx) }()
	rows, err := Run(ctx, s.Session, eventsBound, options)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		r := rows.Row()
		e := implCluster.TraceEvent{ID: UUID(r.InternalId), Activity: r.InternalActivity.String(), Source: r.InternalSource, Thread: r.InternalThread.String()}
		if r.InternalElapsed.Valid {
			e.Elapsed = time.Duration(r.InternalElapsed.Value) * time.Microsecond
		}
		result.Events = append(result.Events, e)
	}
	return result, rows.Err()
}
