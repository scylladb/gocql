// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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

package execution

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implRouting "github.com/scylladb/gocql/v2/internal/routing"
)

import (
	model "github.com/scylladb/gocql/v2/internal/model"

	"bytes"
	"context"
	"errors"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"

	"time"
	// StreamOptions captures execution defaults once; streaming is pull-driven.
)

type StreamOptions struct {
	Options Options
	Limits  StreamLimits
	// Incremental reserves an exclusive connection and bounds receive scratch.
	// The default buffers a complete page on normal multiplexed connections.
	Incremental bool
}
type StreamInfo struct {
	Result               Result
	DeliveredRows, Pages int64
	OutputStarted        bool
}

// RowStream writes only during NextInto. Incremental mode owns a pool slot.
// Output belongs to the caller and remains valid after Close.
type RowStream[R any] struct {
	privateStorage streamHandleStorage[R]
}

func RunStream[B, R any](ctx context.Context, session *implCluster.Session, bound *Bound[B], decoder StreamRowDecoder[R], options StreamOptions) (*RowStream[R], error) {
	return runStreamWithCache(ctx, session, bound, decoder, options, nil, nil)
}
func runStreamWithCache[B, R any](ctx context.Context, session *implCluster.Session, bound *Bound[B], decoder StreamRowDecoder[R], options StreamOptions, plans *streamPlanCache[R], storage *RowStreamStorage[R]) (*RowStream[R], error) {
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
	runctx, cancel, request, err := InternalAdmitExecution(ctx, session, bound, options.Options)
	if err != nil {
		return nil, err
	}
	request.InternalOptions.InternalProfile.Speculation = nil
	var cursor *RowStream[R]
	if storage != nil {
		cursor = new(RowStream[R])
		cursor.privateStorage.RowStreamStorage = storage
		cursor.privateStorage.endpoint = new(streamTerminal)
	} else {
		holder := new(struct {
			handle RowStream[R]
			state  RowStreamStorage[R]
		})
		cursor = &holder.handle
		cursor.privateStorage.RowStreamStorage = &holder.state
	}
	cursor.privateStorage.cursor = executionCursor(request)
	cursor.privateStorage.InternalCtx = runctx
	cursor.privateStorage.InternalCancel = cancel
	cursor.privateStorage.InternalSession = session
	cursor.privateStorage.InternalRequest = request
	cursor.privateStorage.limits = limits
	cursor.privateStorage.InternalDecoder = decoder
	cursor.privateStorage.state = StartPage()
	cursor.privateStorage.InternalHistory = pagingHistory{InternalMaxCount: InternalRuntimeFor(session).InternalConfig.Limits.MaxPagingStates, InternalMaxBytes: InternalRuntimeFor(session).InternalConfig.Limits.MaxPagingStateBytes}
	cursor.privateStorage.pageBuffered = !options.Incremental
	cursor.privateStorage.bufferedPlans = plans
	cursor.privateStorage.InternalActive.Lock()
	err = InternalRowStreamLoadPage(cursor)
	if hook := streamCancellationHook(ctx, runctx); err == nil && hook != nil {
		owner := request.InternalOwner
		owner.InternalRetain()
		cursor.privateStorage.callbackOwner = owner
		cursor.privateStorage.stop = context.AfterFunc(hook, func() {
			defer owner.InternalRelease()
			// Parent callback order is unspecified. Let standard propagation set
			// the execution cause before Close calls its explicit CancelFunc.
			if hook != runctx && runctx.Err() == nil {
				<-runctx.Done()
			}
			cause := runctx.Err()
			if cause == nil {
				cause = hook.Err()
			}
			if session.Closed() {
				cause = ErrClosed
			}
			InternalRowStreamCloseWithError(cursor, cause)
		})
	}
	cursor.unlockActive()
	if err != nil {
		InternalRowStreamFail(cursor, err)
		cursor.privateStorage.InternalActive.Lock()
		InternalRowStreamCleanup(cursor, false)
		cursor.unlockActive()
		return nil, &ExecutionError{Err: err, Result: cursor.Info().Result}
	}
	request.InternalLogical.installStop(func() { InternalRowStreamCloseWithError(cursor, ErrClosed) })
	return cursor, nil
}

func (s *RowStream[R]) setError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) && s.privateStorage.InternalSession.Closed() {
		err = ErrClosed
	}
	s.privateStorage.mu.Lock()
	if s.privateStorage.err == nil {
		s.privateStorage.err = err
	}
	s.privateStorage.mu.Unlock()
}
func InternalRowStreamFail[R any](s *RowStream[R], err error) {
	if errors.Is(err, context.Canceled) && s.privateStorage.InternalSession.Closed() {
		err = ErrClosed
	}
	s.privateStorage.mu.Lock()
	if !s.privateStorage.terminal && s.privateStorage.err == nil {
		s.privateStorage.err = err
	}
	s.privateStorage.terminal = true
	s.privateStorage.mu.Unlock()
}

func (s *RowStream[R]) Err() error {
	if !s.acquireStorage() {
		_, err, _ := s.terminalInfo()
		return err
	}
	defer s.privateStorage.cursor.release()
	if s == nil {
		return nil
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	return s.privateStorage.err
}
func (s *RowStream[R]) Info() StreamInfo {
	if !s.acquireStorage() {
		info, _, _ := s.terminalInfo()
		return info
	}
	defer s.privateStorage.cursor.release()
	if s == nil {
		return StreamInfo{}
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	info := s.privateStorage.info
	info.Result = cloneResult(info.Result)
	return info
}
func (s *RowStream[R]) Columns() Columns {
	if !s.acquireStorage() {
		_, _, columns := s.terminalInfo()
		return columns
	}
	defer s.privateStorage.cursor.release()
	if s == nil {
		return Columns{}
	}
	s.privateStorage.mu.Lock()
	defer s.privateStorage.mu.Unlock()
	return s.privateStorage.columns
}
func (s *RowStream[R]) Close() error {
	if s == nil {
		return nil
	}
	return InternalRowStreamCloseWithError(s, nil)
}
func InternalRowStreamCloseWithError[R any](s *RowStream[R], err error) error {
	if !s.acquireStorage() {
		_, terminalErr, _ := s.terminalInfo()
		return terminalErr
	}
	defer s.privateStorage.cursor.release()
	s.privateStorage.mu.Lock()
	if s.privateStorage.closeDone == nil {
		s.privateStorage.closeDone = make(chan struct{})
	}
	done := s.privateStorage.closeDone
	if s.privateStorage.InternalClosing.Load() {
		s.privateStorage.mu.Unlock()
		<-done
		return s.Err()
	}
	s.privateStorage.InternalClosing.Store(true)
	if !s.privateStorage.terminal && s.privateStorage.err == nil && err != nil {
		s.privateStorage.err = err
	}
	s.privateStorage.terminal = true
	s.privateStorage.mu.Unlock()
	// Publish termination before cancellation so our own cancellation callback
	// cannot turn successful explicit Close into a context error. Close joins
	// active decoding; Query.Wait additionally joins request observers.
	s.privateStorage.InternalCancel()
	s.privateStorage.InternalActive.Lock()
	InternalRowStreamCleanup(s, false)
	// Observers can close their own cursor, so publish completed decoding before
	// invoking them, outside the active gate. The input owner still pins them.
	close(done)
	s.unlockActive()
	return s.Err()
}

// active is held; application notification is deferred until unlockActive.
func InternalRowStreamCleanup[R any](s *RowStream[R], reusable bool) {
	if s.privateStorage.cleaned {
		return
	}
	s.privateStorage.cleaned = true
	s.snapshotTerminal()
	if s.privateStorage.bufferedPage != nil {
		s.privateStorage.bufferedPage.Owner.Release()
		s.privateStorage.bufferedPage = nil
		s.privateStorage.bufferedBody = nil
		s.privateStorage.bufferedReader = nil
	}
	if s.privateStorage.stop != nil {
		if s.privateStorage.stop() {
			s.privateStorage.callbackOwner.InternalRelease()
		}
		s.privateStorage.stop = nil
		s.privateStorage.callbackOwner = nil
	}
	if s.privateStorage.InternalReservation != nil {
		if err := s.privateStorage.InternalReservation.InternalClose(reusable); err != nil {
			s.setError(err)
		}
		s.privateStorage.InternalReservation = nil
	}
	if s.privateStorage.pageCancel != nil {
		s.privateStorage.pageCancel()
		s.privateStorage.pageCancel = nil
	}
	if s.privateStorage.response != nil {
		s.privateStorage.response.InternalReleaseMetadata()
		s.privateStorage.response = nil
	}
	if s.privateStorage.attemptMetadata != nil {
		s.privateStorage.attemptMetadata.InternalReleaseMetadata()
		s.privateStorage.attemptMetadata = nil
	}
	s.privateStorage.InternalHistory.InternalClear()
	s.privateStorage.InternalCancel()
	s.privateStorage.pendingFinish = s.privateStorage.InternalRequest.InternalLogical
}

func (s *RowStream[R]) unlockActive() {
	logical := s.privateStorage.pendingFinish
	s.privateStorage.pendingFinish = nil
	var result Result
	var err error
	if logical != nil {
		result, err = s.Info().Result, s.Err()
	}
	s.privateStorage.InternalActive.Unlock()
	if logical != nil {
		logical.finishAndRelease(result, err)
	}
}
func (s *RowStream[R]) NextInto(dst *R) bool {
	if !s.acquireStorage() {
		return false
	}
	defer s.privateStorage.cursor.release()
	if s == nil {
		return false
	}
	s.privateStorage.mu.Lock()
	terminalBefore := s.privateStorage.terminal
	s.privateStorage.mu.Unlock()
	if terminalBefore {
		return false
	}
	if !s.privateStorage.advancing.CompareAndSwap(false, true) {
		s.privateStorage.mu.Lock()
		active := !s.privateStorage.terminal
		if active && s.privateStorage.err == nil {
			s.privateStorage.err = typeValidation("stream advance", "concurrent NextInto")
		}
		s.privateStorage.mu.Unlock()
		if active {
			s.privateStorage.InternalCancel()
		}
		return false
	}
	defer s.privateStorage.advancing.Store(false)
	s.privateStorage.InternalActive.Lock()
	defer s.unlockActive()
	s.privateStorage.mu.Lock()
	terminal := s.privateStorage.terminal
	s.privateStorage.mu.Unlock()
	if terminal || s.privateStorage.InternalClosing.Load() {
		return false
	}
	if err := s.privateStorage.InternalCtx.Err(); err != nil {
		InternalRowStreamFail(s, err)
		InternalRowStreamCleanup(s, false)
		return false
	}
	if dst == nil {
		InternalRowStreamFail(s, typeValidation("stream destination", "nil destination"))
		InternalRowStreamCleanup(s, false)
		return false
	}
	if s.privateStorage.pageBuffered {
		return s.nextBuffered(dst)
	}
	for {
		if s.privateStorage.response.InternalIndex < s.privateStorage.response.InternalRowCount {
			s.privateStorage.mu.Lock()
			s.privateStorage.info.OutputStarted = true
			s.privateStorage.mu.Unlock()
			ok, err := decodeStreamRow(s.privateStorage.response, s.privateStorage.plan, dst)
			if err != nil {
				InternalRowStreamFail(s, err)
				InternalRowStreamCleanup(s, false)
				return false
			}
			if ok {
				s.privateStorage.mu.Lock()
				s.privateStorage.info.DeliveredRows++
				s.privateStorage.info.Result.Rows = s.privateStorage.info.DeliveredRows
				s.privateStorage.mu.Unlock()
				return true
			}
		} else if !s.privateStorage.response.InternalTerminal {
			// Finish an empty response without touching a destination or Reset.
			err := s.privateStorage.InternalReservation.InternalWithIO(s.privateStorage.response.InternalCtx, func() error {
				if err := s.privateStorage.response.InternalBody.RequireEnd(); err != nil {
					return err
				}
				s.privateStorage.response.InternalTerminal = true
				return s.privateStorage.InternalReservation.InternalFinishResponse()
			})
			if err != nil {
				InternalRowStreamFail(s, err)
				InternalRowStreamCleanup(s, false)
				return false
			}
		}
		next := completePageState()
		if s.privateStorage.response.InternalMetadata.PagingState != nil {
			next = RestorePageState(s.privateStorage.response.InternalMetadata.PagingState)
		}
		if !next.More() {
			s.privateStorage.mu.Lock()
			s.privateStorage.terminal = true
			s.privateStorage.mu.Unlock()
			InternalRowStreamCleanup(s, true)
			return false
		}
		if err := s.privateStorage.InternalHistory.InternalAdd(next); err != nil {
			InternalRowStreamFail(s, err)
			InternalRowStreamCleanup(s, false)
			return false
		}
		s.privateStorage.state = next
		if s.privateStorage.pageCancel != nil {
			s.privateStorage.pageCancel()
			s.privateStorage.pageCancel = nil
		}
		if err := InternalRowStreamLoadPage(s); err != nil {
			InternalRowStreamFail(s, err)
			InternalRowStreamCleanup(s, false)
			return false
		}
	}
}

func (s *RowStream[R]) acquire(plan QueryPlan) error {
	request := s.privateStorage.InternalRequest
	var target Target
	if s.privateStorage.retryTarget != nil {
		target = *s.privateStorage.retryTarget
		s.privateStorage.retryTarget = nil
	} else if request.connection != nil {
		target = Target{HostID: implEndpoint.InternalHostInfoHostUUID(request.connection.InternalHost)}
	} else if plan != nil {
		var ok bool
		target, ok = plan.Next()
		if !ok {
			return ErrNoConnections
		}
	} else if request.InternalRouting.HostID.Set {
		target = Target{HostID: request.InternalRouting.HostID.Value, ShardID: request.InternalRouting.ShardID, StrictShard: request.InternalRouting.ShardID.Set}
	} else {
		hosts := implCluster.SessionData(s.privateStorage.InternalSession).InternalHostSource.InternalGetHostsList()
		for _, host := range hosts {
			if host != nil && host.IsUp() && !implCluster.InternalClusterSettingsFilterHost(&implCluster.SessionData(s.privateStorage.InternalSession).InternalCfg, host) {
				target.HostID = implEndpoint.InternalHostInfoHostUUID(host)
				break
			}
		}
		if target.HostID.IsEmpty() {
			return ErrNoConnections
		}
	}
	implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalMu.RLock()
	pool := implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalHostConnPools[target.HostID]
	implCluster.SessionData(s.privateStorage.InternalSession).InternalPool.InternalMu.RUnlock()
	if pool == nil || pool.InternalHost == nil || !pool.InternalHost.IsUp() || implCluster.InternalClusterSettingsFilterHost(&implCluster.SessionData(s.privateStorage.InternalSession).InternalCfg, pool.InternalHost) {
		return ErrNoConnections
	}
	for {
		var reservation *exclusiveConn
		var err error
		if request.connection != nil {
			reservation, err = request.connection.InternalReserveExclusiveWithLimits(s.privateStorage.InternalCtx, s.privateStorage.limits)
		} else {
			reservation, err = pool.InternalReserveStreamWithLimits(s.privateStorage.InternalCtx, target, request.InternalRouting, s.privateStorage.limits)
		}
		if err != nil {
			return err
		}
		s.privateStorage.InternalReservation = reservation
		if request.InternalPrepared == nil {
			return nil
		}
		request.InternalPrepared.InternalMu.RLock()
		prepared := request.InternalPrepared.InternalHosts[implEndpoint.InternalHostInfoHostUUID(reservation.InternalConn.InternalHost)]
		request.InternalPrepared.InternalMu.RUnlock()
		if prepared != nil && prepared.InternalEpoch.InternalCurrent() {
			s.privateStorage.prepared = prepared
			if s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(prepared.InternalColumns) {
				plan, err := s.privateStorage.InternalDecoder.Compile(prepared.InternalColumns)
				if err != nil {
					return err
				}
				if plan == nil {
					return typeValidation("stream plan", "decoder returned nil plan")
				}
				s.privateStorage.plan = plan
				s.privateStorage.mu.Lock()
				s.privateStorage.columns = prepared.InternalColumns
				s.privateStorage.mu.Unlock()
			}
			return nil
		}
		// Preparation uses shared I/O before any execution submission. Return the
		// idle reservation before entering that path; no decoder/output has run.
		conn := reservation.InternalConn
		if err = reservation.InternalClose(true); err != nil {
			return err
		}
		s.privateStorage.InternalReservation = nil
		s.privateStorage.prepared, err = InternalRuntimeFor(s.privateStorage.InternalSession).InternalPrepareOnConn(s.privateStorage.InternalCtx, conn, request.InternalPrepared, request.InternalValues, false)
		if err != nil {
			return err
		}
	}
}
func (s *RowStream[R]) wireRequest(consistency Consistency) (*attemptRequest, error) {
	r := s.privateStorage.InternalRequest
	conn := s.privateStorage.InternalReservation.InternalConn
	opts := r.InternalOptions.InternalOptions
	if conn.InternalVersion < 5 && r.keyspace != "" && r.keyspace != implCluster.SessionData(s.privateStorage.InternalSession).InternalCfg.Keyspace && !InternalQualifiedForKeyspace(r.InternalStatement, r.keyspace) {
		return nil, typeValidation("keyspace", "protocol v3/v4 requires the session keyspace")
	}
	if conn.InternalVersion < 4 && len(opts.CustomPayload) > 0 {
		return nil, typeValidation("payload", "requires protocol v4")
	}
	if opts.NowInSeconds.Set && conn.InternalVersion < 5 {
		return nil, typeValidation("now in seconds", "requires protocol v5")
	}
	for _, cell := range r.InternalValues.Cells {
		if conn.InternalVersion < 4 && cell.State == Unset {
			return nil, typeValidation("unset", "requires protocol v4")
		}
	}
	w := &attemptRequest{Statement: r.InternalStatement, Values: r.InternalValues.Framed, ValueCount: len(r.InternalValues.Cells), Consistency: consistency, PageSize: int(r.InternalOptions.InternalProfile.PageSize), Keyspace: r.keyspace, Trace: opts.Trace, Payload: opts.CustomPayload, TabletVersionBlock: r.tabletVersionBlock}
	if conn.InternalVersion < 5 {
		w.Keyspace = ""
	}
	if s.privateStorage.state.More() {
		w.PagingState = model.PageToken(s.privateStorage.state)
	}
	if r.InternalOptions.InternalProfile.SerialConsistency.Set {
		w.SerialConsistency = r.InternalOptions.InternalProfile.SerialConsistency.Value
	}
	if opts.Timestamp.Set {
		w.Timestamp = opts.Timestamp.Value
		w.DefaultTimestamp = true
	}
	if opts.NowInSeconds.Set {
		v := opts.NowInSeconds.Value
		w.NowInSeconds = &v
	}
	if s.privateStorage.prepared != nil {
		w.PreparedID = s.privateStorage.prepared.InternalId
		w.ResultMetadataID = s.privateStorage.prepared.InternalMetadataID
		w.SkipMetadata = !opts.IncludeMetadata && metadataIDTracked(conn.InternalTracksResultMetadataID(), s.privateStorage.prepared.InternalMetadataID)
	}
	return w, nil
}
func InternalRowStreamLoadPage[R any](s *RowStream[R]) error {
	if s.privateStorage.pageBuffered {
		return s.loadBufferedPage()
	}
	request := s.privateStorage.InternalRequest
	metadata := s.privateStorage.InternalSession.Metadata()
	request.tabletFeedback = implCluster.InternalSessionCaptureTabletFeedback(s.privateStorage.InternalSession, request.InternalRouting, metadata)
	request.tabletVersionBlock = implRouting.InternalMetadataSnapshotTabletVersionBlock(metadata, request.InternalRouting)
	var plan QueryPlan
	if policy := request.InternalOptions.InternalProfile.Routing; policy != nil {
		if err := policy.Validate(metadata); err != nil {
			return err
		}
		plan = policy.Plan(request.InternalRouting, metadata)
	}
	pagectx := s.privateStorage.InternalCtx
	if timeout := request.InternalOptions.InternalProfile.PageTimeout; timeout > 0 {
		pagectx, s.privateStorage.pageCancel = context.WithTimeout(s.privateStorage.InternalCtx, timeout)
	}
	consistency := request.InternalOptions.InternalProfile.Consistency
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := pagectx.Err(); err != nil {
			return err
		}
		if s.privateStorage.InternalReservation == nil {
			if err := s.acquire(plan); err != nil {
				return errors.Join(lastErr, err)
			}
		}
		if s.privateStorage.prepared != nil && !s.privateStorage.prepared.InternalEpoch.InternalCurrent() {
			// Reconcile before submitting a continuation page. This prepares
			// and validates the same canonical input, then sends the next page
			// state; it never replays a page that has exposed caller output.
			if s.Info().OutputStarted && !s.privateStorage.state.More() {
				return &SchemaChangedError{Statement: request.InternalStatement, Previous: request.InternalValues.Columns, Current: request.InternalPrepared.InternalParameters}
			}
			conn := s.privateStorage.InternalReservation.InternalConn
			if err := s.privateStorage.InternalReservation.InternalClose(true); err != nil {
				return err
			}
			s.privateStorage.InternalReservation = nil
			prepared, err := InternalRuntimeFor(s.privateStorage.InternalSession).InternalPrepareOnConn(pagectx, conn, request.InternalPrepared, request.InternalValues, false)
			if err != nil {
				return err
			}
			s.privateStorage.prepared = prepared
			s.privateStorage.retryTarget = &Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
			if err := s.acquire(plan); err != nil {
				return err
			}
		}
		wireRequest, err := s.wireRequest(consistency)
		if err != nil {
			return err
		}
		wireRequest.Lease = request.InternalOwner.InternalWriterLease(request.InternalValues.Framed)
		observed := uint64(0)
		if request.InternalPrepared != nil {
			observed = request.InternalPrepared.InternalDependencies.InternalTracker.InternalStart()
		}
		started := time.Now()
		response, err := s.privateStorage.InternalReservation.InternalExchange(pagectx, wireRequest)
		wireRequest.Lease.Release()
		conn := s.privateStorage.InternalReservation.InternalConn
		event := AttemptEvent{RequestID: request.InternalLogical.id, Attempt: attempt, Target: Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)}
		if implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0 {
			event.Target.ShardID = Option[uint32]{Set: true, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}
		}
		rt := InternalRuntimeFor(s.privateStorage.InternalSession)
		rt.attempts.Add(1)
		if observer := request.InternalLogical.observer; observer != nil {
			implCluster.InternalSessionObserveSafely(s.privateStorage.InternalSession, func() { observer.ObserveAttempt(event) })
		}
		if policy := request.InternalOptions.InternalProfile.Routing; policy != nil {
			implCluster.InternalSessionObserveSafely(s.privateStorage.InternalSession, func() { policy.Observe(event) })
		}
		s.privateStorage.mu.Lock()
		s.privateStorage.info.Result.Attempts++
		s.privateStorage.info.Result.Latency += event.Latency
		s.privateStorage.info.Result.HostID = event.Target.HostID
		s.privateStorage.info.Result.AchievedConsistency = consistency

		// Result extensions describe the latest attempt, including terminal server
		// errors after output. The metadata parser already owns their bytes.
		s.privateStorage.info.Result.Warnings = nil
		s.privateStorage.info.Result.TraceIDs = nil
		s.privateStorage.info.Result.CustomPayload = nil
		if response != nil {
			s.privateStorage.info.Result.Warnings = response.InternalInfo.Warnings
			s.privateStorage.info.Result.TraceIDs = response.InternalInfo.TraceIDs
			s.privateStorage.info.Result.CustomPayload = response.InternalInfo.CustomPayload
		}
		s.privateStorage.mu.Unlock()
		// Latest failed-attempt extensions remain budgeted until replacement or
		// terminal cleanup, including while another attempt allocates metadata.
		if s.privateStorage.attemptMetadata != nil {
			s.privateStorage.attemptMetadata.InternalReleaseMetadata()
			s.privateStorage.attemptMetadata = nil
		}
		if err == nil {
			page := &InternalResultMetadataObservation{kind: response.InternalKind, columns: response.InternalColumns, columnCount: response.InternalColumnCount, noMetadata: response.InternalMetadata.NoMetadata, metadataChanged: response.InternalMetadata.MetadataChanged, metadataID: response.InternalMetadata.MetadataID}
			err = InternalSelectPreparedMetadata(request.InternalPrepared, implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), s.privateStorage.prepared, wireRequest.ResultMetadataID, conn.InternalTracksResultMetadataID(), page, observed)
			if err == nil {
				if s.privateStorage.plan == nil || !s.privateStorage.columns.Equal(page.columns) {
					s.privateStorage.plan, err = s.privateStorage.InternalDecoder.Compile(page.columns)
					if err == nil && s.privateStorage.plan == nil {
						err = typeValidation("stream plan", "decoder returned nil plan")
					}
				}
				if err == nil {
					if s.privateStorage.response == nil {
						err = response.InternalInstallColumns(page.columns)
					} else {
						err = response.InternalInstallColumnsReusing(page.columns, s.privateStorage.response)
					}
				}
			}
			if err == nil {
				prior := s.privateStorage.response
				s.privateStorage.response = response
				s.privateStorage.mu.Lock()
				s.privateStorage.columns = page.columns
				s.privateStorage.info.Pages++
				s.privateStorage.mu.Unlock()
				if prior != nil {
					prior.InternalReleaseMetadata()
				}
				return nil
			}
		}
		if response != nil {
			s.privateStorage.attemptMetadata = response
		}
		lastErr = err
		// OutputStarted is set before Reset, so no decoder/partial row ever replays.
		if s.Info().OutputStarted {
			return err
		}
		var unprepared *RequestErrUnprepared
		if attempt == 1 && request.InternalPrepared != nil && errors.As(err, &unprepared) && bytes.Equal(unprepared.StatementId, wireRequest.PreparedID) {
			if closeErr := s.privateStorage.InternalReservation.InternalClose(true); closeErr != nil {
				return closeErr
			}
			s.privateStorage.InternalReservation = nil
			s.privateStorage.retryTarget = &Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
			s.privateStorage.prepared, err = InternalRuntimeFor(s.privateStorage.InternalSession).InternalPrepareOnConnExpected(pagectx, conn, request.InternalPrepared, request.InternalValues, true, s.privateStorage.prepared)
			if err != nil {
				return err
			}
			rt.retries.Add(1)
			continue
		}
		decision := RetryDecision{}
		if retry := request.InternalOptions.InternalProfile.Retry; retry != nil {
			decision = retry.Decide(AttemptError{Err: err, Attempt: attempt, Idempotent: request.InternalOptions.InternalOptions.Idempotent, Consistency: consistency, Conditional: request.InternalRouting.Conditional, BytesWritten: InternalAttemptBytesWritten(err)})
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
		if decision.Action == RetrySame && s.privateStorage.InternalReservation.InternalClosing.Load() {
			s.privateStorage.retryTarget = &Target{HostID: implEndpoint.InternalHostInfoHostUUID(conn.InternalHost), ShardID: Option[uint32]{Set: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0, Value: uint32(implEndpoint.ScyllaConnectionFeaturesData(&conn.InternalScyllaSupported).InternalShard)}, StrictShard: implEndpoint.ScyllaHostFeaturesData(&conn.InternalScyllaSupported.ScyllaHostFeatures).InternalNrShards > 0}
		}
		if decision.Action == RetryNext || s.privateStorage.InternalReservation.InternalClosing.Load() {
			s.privateStorage.InternalReservation.InternalClose(false)
			s.privateStorage.InternalReservation = nil
			s.privateStorage.prepared = nil
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
