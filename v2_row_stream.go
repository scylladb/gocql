/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// StreamOptions captures execution defaults once; streaming is pull-driven.
type StreamOptions struct {
	Options Options
	Limits  StreamLimits
}
type StreamInfo struct {
	Result               Result
	DeliveredRows, Pages int64
	OutputStarted        bool
}

// RowStream owns one configured pool slot and writes only during NextInto.
// Output belongs to the caller and remains valid after Close.
type RowStream[R any] struct {
	attemptMetadata *streamResponse
	closeDone       chan struct{}
	retryTarget     *Target
	active          sync.Mutex
	mu              sync.Mutex
	advancing       atomic.Bool
	closing         atomic.Bool
	ctx             context.Context
	cancel          context.CancelFunc
	stop            func() bool
	session         *Session
	request         *executionRequest
	limits          StreamLimits
	decoder         StreamRowDecoder[R]
	plan            StreamRowPlan[R]
	columns         Columns
	response        *streamResponse
	reservation     *exclusiveConn
	prepared        *hostPrepared
	pageCancel      context.CancelFunc
	state           PageState
	history         pagingHistory
	info            StreamInfo
	err             error
	terminal        bool
	cleaned         bool
}

func RunStream[B, R any](ctx context.Context, session *Session, bound *Bound[B], decoder StreamRowDecoder[R], options StreamOptions) (*RowStream[R], error) {
	if decoder == nil {
		return nil, typeValidation("stream decoder", "nil decoder")
	}
	limits, err := checkedStreamLimits(options.Limits)
	if err != nil {
		return nil, err
	}
	if options.Options.Prefetch.Set && options.Options.Prefetch.Value != 0 {
		return nil, typeValidation("stream prefetch", "must be zero")
	}
	options.Options.Prefetch = Option[int]{Set: true, Value: 0}
	runctx, cancel, request, err := admitExecution(ctx, session, bound, options.Options)
	if err != nil {
		return nil, err
	}
	request.options.profile.Speculation = nil
	cursor := &RowStream[R]{ctx: runctx, cancel: cancel, session: session, request: request, limits: limits, decoder: decoder, state: StartPage(), history: pagingHistory{maxCount: session.typedRuntime().config.Limits.MaxPagingStates, maxBytes: session.typedRuntime().config.Limits.MaxPagingStateBytes}}
	// Construction owns active until headers/metadata have selected a checked plan.
	cursor.active.Lock()
	err = cursor.loadPage()
	if err == nil {
		cursor.stop = context.AfterFunc(runctx, func() {
			cause := runctx.Err()
			if session.Closed() {
				cause = ErrClosed
			}
			cursor.closeWithError(cause)
		})
	}
	cursor.active.Unlock()
	if err != nil {
		cursor.fail(err)
		cursor.active.Lock()
		cursor.cleanup(false)
		cursor.active.Unlock()
		return nil, &ExecutionError{Err: err, Result: cursor.Info().Result}
	}
	request.logical.installStop(func() { cursor.closeWithError(ErrClosed) })
	return cursor, nil
}

func (s *RowStream[R]) setError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && s.session.Closed() {
		err = ErrClosed
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}
func (s *RowStream[R]) fail(err error) {
	if errors.Is(err, context.Canceled) && s.session.Closed() {
		err = ErrClosed
	}
	s.mu.Lock()
	if !s.terminal && s.err == nil {
		s.err = err
	}
	s.terminal = true
	s.mu.Unlock()
}

func (s *RowStream[R]) Err() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}
func (s *RowStream[R]) Info() StreamInfo {
	if s == nil {
		return StreamInfo{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info := s.info
	info.Result = cloneResult(info.Result)
	return info
}
func (s *RowStream[R]) Columns() Columns {
	if s == nil {
		return Columns{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.columns
}
func (s *RowStream[R]) Close() error {
	if s == nil {
		return nil
	}
	return s.closeWithError(nil)
}
func (s *RowStream[R]) closeWithError(err error) error {
	s.mu.Lock()
	if s.closeDone == nil {
		s.closeDone = make(chan struct{})
	}
	done := s.closeDone
	if s.closing.Load() {
		s.mu.Unlock()
		<-done
		return s.Err()
	}
	s.closing.Store(true)
	if !s.terminal && s.err == nil && err != nil {
		s.err = err
	}
	s.terminal = true
	s.mu.Unlock()
	defer close(done)
	// Publish termination before cancellation so our own cancellation callback
	// cannot turn successful explicit Close into a context error. Every caller
	// joins the same completion, including callback and session shutdown paths.
	s.cancel()
	s.active.Lock()
	s.cleanup(false)
	s.active.Unlock()
	return s.Err()
}

// active is held; no driver mutex is held while calling application code.
func (s *RowStream[R]) cleanup(reusable bool) {
	if s.cleaned {
		return
	}
	s.cleaned = true
	if s.stop != nil {
		s.stop()
	}
	if s.reservation != nil {
		if err := s.reservation.close(reusable); err != nil {
			s.setError(err)
		}
		s.reservation = nil
	}
	if s.pageCancel != nil {
		s.pageCancel()
		s.pageCancel = nil
	}
	if s.response != nil {
		s.response.releaseMetadata()
		s.response = nil
	}
	if s.attemptMetadata != nil {
		s.attemptMetadata.releaseMetadata()
		s.attemptMetadata = nil
	}
	s.history.clear()
	s.cancel()
	s.request.logical.finish(s.Info().Result, s.Err())
	s.request.logical.releaseInput()
}
func (s *RowStream[R]) NextInto(dst *R) bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	terminalBefore := s.terminal
	s.mu.Unlock()
	if terminalBefore {
		return false
	}
	if !s.advancing.CompareAndSwap(false, true) {
		s.mu.Lock()
		active := !s.terminal
		if active && s.err == nil {
			s.err = typeValidation("stream advance", "concurrent NextInto")
		}
		s.mu.Unlock()
		if active {
			s.cancel()
		}
		return false
	}
	defer s.advancing.Store(false)
	s.active.Lock()
	defer s.active.Unlock()
	s.mu.Lock()
	terminal := s.terminal
	s.mu.Unlock()
	if terminal || s.closing.Load() {
		return false
	}
	if err := s.ctx.Err(); err != nil {
		s.fail(err)
		s.cleanup(false)
		return false
	}
	if dst == nil {
		s.fail(typeValidation("stream destination", "nil destination"))
		s.cleanup(false)
		return false
	}
	for {
		if s.response.index < s.response.rowCount {
			s.mu.Lock()
			s.info.OutputStarted = true
			s.mu.Unlock()
			ok, err := decodeStreamRow(s.response, s.plan, dst)
			if err != nil {
				s.fail(err)
				s.cleanup(false)
				return false
			}
			if ok {
				s.mu.Lock()
				s.info.DeliveredRows++
				s.info.Result.Rows = s.info.DeliveredRows
				s.mu.Unlock()
				return true
			}
		} else if !s.response.terminal {
			// Finish an empty response without touching a destination or Reset.
			err := s.reservation.withIO(s.response.ctx, func() error {
				if err := s.response.body.RequireEnd(); err != nil {
					return err
				}
				s.response.terminal = true
				return s.reservation.finishResponse()
			})
			if err != nil {
				s.fail(err)
				s.cleanup(false)
				return false
			}
		}
		next := completePageState()
		if s.response.metadata.pagingState != nil {
			next = RestorePageState(s.response.metadata.pagingState)
		}
		if !next.More() {
			s.mu.Lock()
			s.terminal = true
			s.mu.Unlock()
			s.cleanup(true)
			return false
		}
		if err := s.history.add(next); err != nil {
			s.fail(err)
			s.cleanup(false)
			return false
		}
		s.state = next
		if s.pageCancel != nil {
			s.pageCancel()
			s.pageCancel = nil
		}
		if err := s.loadPage(); err != nil {
			s.fail(err)
			s.cleanup(false)
			return false
		}
	}
}

func (s *RowStream[R]) acquire(plan QueryPlan) error {
	request := s.request
	var target Target
	if s.retryTarget != nil {
		target = *s.retryTarget
		s.retryTarget = nil
	} else if request.connection != nil {
		target = Target{HostID: request.connection.host.hostUUID()}
	} else if plan != nil {
		var ok bool
		target, ok = plan.Next()
		if !ok {
			return ErrNoConnections
		}
	} else if request.routing.HostID.Set {
		target = Target{HostID: request.routing.HostID.Value, ShardID: request.routing.ShardID, StrictShard: request.routing.ShardID.Set}
	} else {
		hosts := s.session.hostSource.getHostsList()
		for _, host := range hosts {
			if host != nil && host.IsUp() && !s.session.cfg.filterHost(host) {
				target.HostID = host.hostUUID()
				break
			}
		}
		if target.HostID.IsEmpty() {
			return ErrNoConnections
		}
	}
	s.session.pool.mu.RLock()
	pool := s.session.pool.hostConnPools[target.HostID]
	s.session.pool.mu.RUnlock()
	if pool == nil || pool.host == nil || !pool.host.IsUp() || s.session.cfg.filterHost(pool.host) {
		return ErrNoConnections
	}
	for {
		var reservation *exclusiveConn
		var err error
		if request.connection != nil {
			reservation, err = request.connection.reserveExclusiveWithLimits(s.ctx, s.limits)
		} else {
			reservation, err = pool.reserveStreamWithLimits(s.ctx, target, request.routing, s.limits)
		}
		if err != nil {
			return err
		}
		s.reservation = reservation
		if request.prepared == nil {
			return nil
		}
		request.prepared.mu.RLock()
		prepared := request.prepared.hosts[reservation.conn.host.hostUUID()]
		request.prepared.mu.RUnlock()
		if prepared != nil && prepared.epoch.current() {
			s.prepared = prepared
			if s.plan == nil || !s.columns.Equal(prepared.columns) {
				plan, err := s.decoder.Compile(prepared.columns)
				if err != nil {
					return err
				}
				if plan == nil {
					return typeValidation("stream plan", "decoder returned nil plan")
				}
				s.plan = plan
				s.mu.Lock()
				s.columns = prepared.columns
				s.mu.Unlock()
			}
			return nil
		}
		// Preparation uses shared I/O before any execution submission. Return the
		// idle reservation before entering that path; no decoder/output has run.
		conn := reservation.conn
		if err = reservation.close(true); err != nil {
			return err
		}
		s.reservation = nil
		s.prepared, err = s.session.prepareOnConn(s.ctx, conn, request.prepared, request.values, false)
		if err != nil {
			return err
		}
	}
}
func (s *RowStream[R]) wireRequest(consistency Consistency) (*attemptRequest, error) {
	r := s.request
	conn := s.reservation.conn
	opts := r.options.options
	if conn.version < 5 && r.keyspace != "" && r.keyspace != s.session.cfg.Keyspace && !qualifiedForKeyspace(r.statement, r.keyspace) {
		return nil, typeValidation("keyspace", "protocol v3/v4 requires the session keyspace")
	}
	if conn.version < 4 && len(opts.CustomPayload) > 0 {
		return nil, typeValidation("payload", "requires protocol v4")
	}
	if opts.NowInSeconds.Set && conn.version < 5 {
		return nil, typeValidation("now in seconds", "requires protocol v5")
	}
	for _, cell := range r.values.cells {
		if conn.version < 4 && cell.state == Unset {
			return nil, typeValidation("unset", "requires protocol v4")
		}
	}
	w := &attemptRequest{statement: r.statement, values: r.values.framed, valueCount: len(r.values.cells), consistency: consistency, pageSize: int(r.options.profile.PageSize), keyspace: r.keyspace, trace: opts.Trace, payload: opts.CustomPayload, tabletVersionBlock: r.tabletVersionBlock}
	if conn.version < 5 {
		w.keyspace = ""
	}
	if s.state.More() {
		w.pagingState = s.state.token
	}
	if r.options.profile.SerialConsistency.Set {
		w.serialConsistency = r.options.profile.SerialConsistency.Value
	}
	if opts.Timestamp.Set {
		w.timestamp = opts.Timestamp.Value
		w.defaultTimestamp = true
	}
	if opts.NowInSeconds.Set {
		v := opts.NowInSeconds.Value
		w.nowInSeconds = &v
	}
	if s.prepared != nil {
		w.preparedID = s.prepared.id
		w.resultMetadataID = s.prepared.metadataID
		w.skipMetadata = !opts.IncludeMetadata && metadataIDTracked(conn.tracksResultMetadataID(), s.prepared.metadataID)
	}
	return w, nil
}
func (s *RowStream[R]) loadPage() error {
	request := s.request
	metadata := s.session.Metadata()
	request.tabletFeedback = s.session.captureTabletFeedback(request.routing, metadata)
	request.tabletVersionBlock = metadata.tabletVersionBlock(request.routing)
	var plan QueryPlan
	if policy := request.options.profile.Routing; policy != nil {
		if err := policy.Validate(metadata); err != nil {
			return err
		}
		plan = policy.Plan(request.routing, metadata)
	}
	pagectx := s.ctx
	if timeout := request.options.profile.PageTimeout; timeout > 0 {
		pagectx, s.pageCancel = context.WithTimeout(s.ctx, timeout)
	}
	consistency := request.options.profile.Consistency
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := pagectx.Err(); err != nil {
			return err
		}
		if s.reservation == nil {
			if err := s.acquire(plan); err != nil {
				return errors.Join(lastErr, err)
			}
		}
		if s.prepared != nil && !s.prepared.epoch.current() {
			if s.Info().OutputStarted {
				return &SchemaChangedError{Statement: request.statement, Previous: request.values.columns, Current: request.prepared.parameters}
			}
			conn := s.reservation.conn
			if err := s.reservation.close(true); err != nil {
				return err
			}
			s.reservation = nil
			prepared, err := s.session.prepareOnConn(pagectx, conn, request.prepared, request.values, false)
			if err != nil {
				return err
			}
			s.prepared = prepared
			s.retryTarget = &Target{HostID: conn.host.hostUUID(), ShardID: Option[uint32]{Set: conn.scyllaSupported.nrShards > 0, Value: uint32(conn.scyllaSupported.shard)}, StrictShard: conn.scyllaSupported.nrShards > 0}
			if err := s.acquire(plan); err != nil {
				return err
			}
		}
		wireRequest, err := s.wireRequest(consistency)
		if err != nil {
			return err
		}
		wireRequest.lease = request.owner.writerLease(request.values.framed)
		observed := uint64(0)
		if request.prepared != nil {
			observed = request.prepared.dependencies.tracker.start()
		}
		started := time.Now()
		response, err := s.reservation.exchange(pagectx, wireRequest)
		wireRequest.lease.Release()
		conn := s.reservation.conn
		event := AttemptEvent{RequestID: request.logical.id, Attempt: attempt, Target: Target{HostID: conn.host.hostUUID()}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)}
		if conn.scyllaSupported.nrShards > 0 {
			event.Target.ShardID = Option[uint32]{Set: true, Value: uint32(conn.scyllaSupported.shard)}
		}
		rt := s.session.typedRuntime()
		rt.attempts.Add(1)
		if observer := request.logical.observer; observer != nil {
			s.session.observeSafely(func() { observer.ObserveAttempt(event) })
		}
		if policy := request.options.profile.Routing; policy != nil {
			s.session.observeSafely(func() { policy.Observe(event) })
		}
		s.mu.Lock()
		s.info.Result.Attempts++
		s.info.Result.Latency += event.Latency
		s.info.Result.HostID = event.Target.HostID
		s.info.Result.AchievedConsistency = consistency
		// Result extensions describe the latest attempt, including terminal server
		// errors after output. The metadata parser already owns their bytes.
		s.info.Result.Warnings = nil
		s.info.Result.TraceIDs = nil
		s.info.Result.CustomPayload = nil
		if response != nil {
			s.info.Result.Warnings = response.info.Warnings
			s.info.Result.TraceIDs = response.info.TraceIDs
			s.info.Result.CustomPayload = response.info.CustomPayload
		}
		s.mu.Unlock()
		// Latest failed-attempt extensions remain budgeted until replacement or
		// terminal cleanup, including while another attempt allocates metadata.
		if s.attemptMetadata != nil {
			s.attemptMetadata.releaseMetadata()
			s.attemptMetadata = nil
		}
		if err == nil {
			page := &resultMetadataObservation{kind: response.kind, columns: response.columns, columnCount: response.columnCount, noMetadata: response.metadata.noMetadata, metadataChanged: response.metadata.metadataChanged, metadataID: response.metadata.metadataID}
			err = selectPreparedMetadata(request.prepared, conn.host.hostUUID(), s.prepared, wireRequest.resultMetadataID, conn.tracksResultMetadataID(), page, observed)
			if err == nil {
				if s.plan == nil || !s.columns.Equal(page.columns) {
					s.plan, err = s.decoder.Compile(page.columns)
					if err == nil && s.plan == nil {
						err = typeValidation("stream plan", "decoder returned nil plan")
					}
				}
				if err == nil {
					err = response.installColumns(page.columns)
				}
			}
			if err == nil {
				prior := s.response
				s.response = response
				s.mu.Lock()
				s.columns = page.columns
				s.info.Pages++
				s.mu.Unlock()
				if prior != nil {
					prior.releaseMetadata()
				}
				return nil
			}
		}
		if response != nil {
			s.attemptMetadata = response
		}
		lastErr = err
		// OutputStarted is set before Reset, so no decoder/partial row ever replays.
		if s.Info().OutputStarted {
			return err
		}
		var unprepared *RequestErrUnprepared
		if attempt == 1 && request.prepared != nil && errors.As(err, &unprepared) && bytes.Equal(unprepared.StatementId, wireRequest.preparedID) {
			if closeErr := s.reservation.close(true); closeErr != nil {
				return closeErr
			}
			s.reservation = nil
			s.retryTarget = &Target{HostID: conn.host.hostUUID(), ShardID: Option[uint32]{Set: conn.scyllaSupported.nrShards > 0, Value: uint32(conn.scyllaSupported.shard)}, StrictShard: conn.scyllaSupported.nrShards > 0}
			s.prepared, err = s.session.prepareOnConnExpected(pagectx, conn, request.prepared, request.values, true, s.prepared)
			if err != nil {
				return err
			}
			rt.retries.Add(1)
			continue
		}
		decision := RetryDecision{}
		if retry := request.options.profile.Retry; retry != nil {
			decision = retry.Decide(AttemptError{Err: err, Attempt: attempt, Idempotent: request.options.options.Idempotent, Consistency: consistency, Conditional: request.routing.Conditional, BytesWritten: attemptBytesWritten(err)})
		}
		if decision.Action == StopRetry {
			return err
		}
		if decision.Action != RetrySame && decision.Action != RetryNext {
			return typeValidation("stream retry", "requires retry same, next or stop")
		}
		if decision.Delay < 0 {
			return typeValidation("retry delay", "negative delay")
		}
		if decision.Consistency.Set {
			if !validConsistency(decision.Consistency.Value) {
				return typeValidation("retry consistency", "invalid consistency")
			}
			consistency = decision.Consistency.Value
		}
		if decision.Action == RetrySame && s.reservation.closing.Load() {
			s.retryTarget = &Target{HostID: conn.host.hostUUID(), ShardID: Option[uint32]{Set: conn.scyllaSupported.nrShards > 0, Value: uint32(conn.scyllaSupported.shard)}, StrictShard: conn.scyllaSupported.nrShards > 0}
		}
		if decision.Action == RetryNext || s.reservation.closing.Load() {
			s.reservation.close(false)
			s.reservation = nil
			s.prepared = nil
		}
		if decision.Delay > 0 {
			timer := time.NewTimer(decision.Delay)
			select {
			case <-pagectx.Done():
				timer.Stop()
				return pagectx.Err()
			case <-timer.C:
			}
		}
		rt.retries.Add(1)
	}
}
