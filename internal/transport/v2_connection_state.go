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

package transport

import (
	"context"
	"errors"
	"fmt"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"sync"
	"sync/atomic"
	"time"
)

var InternalErrOrphanBudget = errors.New("gocql: abandoned stream budget exceeded")

// All admission state is protected by Conn.mu. Notifications allocate only
// while an exclusive acquisition/shared waiter exists, never per ordinary call.
func (c *Conn) changeLocked() {
	if c.sharedChanged != nil {
		close(c.sharedChanged)
		c.sharedChanged = nil
	}
}

func (c *Conn) changedLocked() <-chan struct{} {
	if c.sharedChanged == nil {
		c.sharedChanged = make(chan struct{})
	}
	return c.sharedChanged
}

func (c *Conn) beginShared(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		c.InternalMu.Lock()
		if c.InternalClosed {
			c.InternalMu.Unlock()
			return ErrConnectionClosed
		}
		if !c.InternalExclusiveFlag.Load() {
			c.InternalSharedOps++
			c.InternalMu.Unlock()
			return nil
		}
		changed := c.changedLocked()
		c.InternalMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (c *Conn) endShared() {
	c.InternalMu.Lock()
	c.InternalSharedOps--
	c.changeLocked()
	c.InternalMu.Unlock()
}

func (c *Conn) markOrphan(call *InternalCallReq) bool {
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()

	if c.InternalClosed || c.InternalCalls[call.InternalStreamID] != call || call.orphaned {
		return false
	}
	call.orphaned = true
	c.orphans++
	budget := 1024
	if c.InternalCfg != nil && c.InternalCfg.TypedConfig != nil {
		budget = c.InternalCfg.TypedConfig.Pool.MaxOrphanedStreams
	}
	budget = min(budget, c.InternalStreams.NumStreams-1)
	return c.orphans > budget
}

// pauseSharedReader is called only at a complete native frame/v5 segment
// boundary. It acknowledges exclusive ownership before the acquiring goroutine
// can read and acknowledges shared resumption before admission reopens.
func (c *Conn) pauseSharedReader(ctx context.Context) bool {
	c.InternalMu.Lock()
	if !c.pauseWanted {
		c.InternalMu.Unlock()
		return false
	}
	ack, resume, resumed := c.readerPaused, c.readerResume, c.readerResumed
	close(ack)
	c.InternalMu.Unlock()
	select {
	case <-resume:
	case <-ctx.Done():
	}
	c.InternalMu.Lock()
	close(resumed)
	c.InternalMu.Unlock()
	return true
}

type InternalExclusiveConn struct {
	cancelMu               sync.Mutex
	InternalIoGeneration   uint64
	ioActive, ioWatching   bool
	ioDone                 <-chan struct{}
	ioStop                 func() bool
	terminalCause          error
	InternalConn           *Conn
	InternalCtx            context.Context
	stop                   func() bool
	active                 sync.Mutex
	InternalClosing        atomic.Bool
	InternalDone           chan struct{}
	closeErr               error
	InternalComplete       bool
	closed                 bool
	InternalStream         int
	InternalLimits         StreamLimits
	InternalMetadataBudget *streamMetadataBudget
	InternalUnits          *InternalVerifiedStreamUnits
}

func (c *Conn) InternalReserveExclusive(ctx context.Context) (*InternalExclusiveConn, error) {
	return c.InternalReserveExclusiveWithLimits(ctx, StreamLimits{})
}

func (c *Conn) InternalReserveExclusiveWithLimits(ctx context.Context, limits StreamLimits) (_ *InternalExclusiveConn, err error) {
	limits, err = InternalCheckedStreamLimits(limits)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, typeValidation("stream context", "nil context")
	}
	reader, ok := c.InternalR.(*connReader)
	if !ok || reader.InternalConn == nil || reader.InternalR != nil {
		return nil, &StreamCapabilityError{Protocol: int(c.InternalVersion), Reason: "connection has no transferable unbuffered reader"}
	}
	if c.InternalVersion < 3 || c.InternalVersion > 5 || c.InternalVersion < 5 && c.InternalCompressor != nil {
		return nil, &StreamCapabilityError{Protocol: int(c.InternalVersion), Reason: "frame compression has no incremental receive transform"}
	}
	if c.InternalVersion == 5 && c.InternalCompressor != nil && c.InternalCompressor.Name() != "lz4" {
		return nil, &StreamCapabilityError{Protocol: int(c.InternalVersion), Reason: "v5 streaming requires raw or LZ4 segments"}
	}
	c.InternalMu.Lock()
	if c.InternalClosed {
		c.InternalMu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.InternalCfg != nil && c.InternalCfg.InternalIsControlConn {
		c.InternalMu.Unlock()
		return nil, &StreamCapabilityError{Protocol: int(c.InternalVersion), Reason: "control connection cannot be reserved"}
	}
	if c.InternalExclusiveFlag.Load() {
		c.InternalMu.Unlock()
		return nil, ErrNoStreams
	}
	c.InternalExclusiveFlag.Store(true)
	c.changeLocked()
	c.InternalMu.Unlock()
	acquired := false
	defer func() {
		if !acquired {
			c.restoreSharedReader(reader)
		}
	}()
	for {
		c.InternalMu.Lock()
		if c.InternalClosed {
			c.InternalMu.Unlock()
			return nil, ErrConnectionClosed
		}
		if c.InternalSharedOps == 0 && len(c.InternalCalls) == 0 && c.InternalStreams.InUse() == 0 {
			c.pauseWanted = true
			c.readerPaused = make(chan struct{})
			c.readerResume = make(chan struct{})
			c.readerResumed = make(chan struct{})
			paused := c.readerPaused
			c.InternalMu.Unlock()
			if err = reader.InternalRequestPause(true); err != nil {
				return nil, err
			}
			select {
			case <-paused:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.InternalCtx.Done():
				return nil, ErrConnectionClosed
			}
			break
		}
		changed := c.changedLocked()
		c.InternalMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.InternalCtx.Done():
			return nil, ErrConnectionClosed
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	stream, ok := c.InternalStreams.GetStream()
	if !ok {
		return nil, ErrNoStreams
	}
	r := &InternalExclusiveConn{InternalConn: c, InternalCtx: ctx, InternalStream: stream, InternalDone: make(chan struct{}), InternalComplete: true}

	if err = r.InternalInitStream(limits); err != nil {
		c.InternalStreams.Clear(stream)
		return nil, err
	}
	r.stop = context.AfterFunc(ctx, func() { r.cancelLifetime(ctx.Err()) })
	acquired = true
	return r, nil
}

func (c *Conn) restoreSharedReader(reader *connReader) {
	c.InternalMu.Lock()
	paused, resume, resumed := c.readerPaused, c.readerResume, c.readerResumed
	if c.pauseWanted {
		c.pauseWanted = false
		if resume != nil {
			close(resume)
		}
	}
	c.InternalMu.Unlock()
	reader.InternalRequestPause(false)
	if paused != nil {
		select {
		case <-paused:
			select {
			case <-resumed:
			case <-c.InternalCtx.Done():
			}
		default:
		}
	}
	c.InternalMu.Lock()
	c.InternalExclusiveFlag.Store(false)
	c.changeLocked()
	c.InternalMu.Unlock()
	if pool, ok := c.InternalErrorHandler.(InternalPoolOwner); ok {
		pool.InternalNotifyReservationChanged()
	}
}

func (r *InternalExclusiveConn) InternalClose(reusable bool) error {
	if r == nil {
		return nil
	}

	if !r.InternalClosing.CompareAndSwap(false, true) {
		<-r.InternalDone
		return r.closeErr
	}
	return r.closeOwned(reusable)
}

// closeOwned is called only after winning the terminal generation gate.
func (r *InternalExclusiveConn) closeOwned(reusable bool) error {
	defer close(r.InternalDone)
	if r.stop != nil {
		r.stop()
	}

	if !reusable {
		r.InternalConn.InternalCloseWithError(ErrConnectionClosed)
	}
	r.active.Lock()
	defer r.active.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.stopIOContext()
	if reusable && r.InternalComplete && r.InternalUnits != nil {
		if err := r.drainVerifiedControl(); err != nil {
			r.closeErr = err
			r.InternalConn.InternalCloseWithError(err)
			reusable = false
		}
	}
	if r.stop != nil {
		r.stop()
	}
	r.InternalConn.InternalStreams.Clear(r.InternalStream)
	reader := r.InternalConn.InternalR.(*connReader)
	if reusable && (!r.InternalComplete || r.InternalUnits != nil && !r.InternalUnits.boundary()) {
		r.closeErr = fmt.Errorf("gocql: incomplete streaming response")
		r.InternalConn.InternalCloseWithError(r.closeErr)
		reusable = false
	}
	if r.InternalUnits != nil {
		r.InternalUnits.InternalData = nil
		r.InternalUnits.InternalOffset = 0
		r.InternalUnits.InternalUnits.Close()
		r.InternalUnits = nil
	}
	r.InternalConn.restoreSharedReader(reader)
	return r.closeErr
}

func (c *Conn) typedHeartbeat(ctx context.Context) {
	config := c.InternalCfg.TypedConfig
	interval := config.KeepaliveInterval
	if interval == 0 {
		return
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		callCtx := ctx
		cancel := func() {}
		if config.KeepaliveTimeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, config.KeepaliveTimeout)
		}
		err := c.beginShared(callCtx)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return
			}
			timer.Reset(interval)
			continue
		}
		done := make(chan struct{})
		stop := context.AfterFunc(callCtx, func() { c.InternalCloseWithError(callCtx.Err()); close(done) })
		f, err := c.execAdmitted(callCtx, &writeOptionsFrame{}, nil, 0, true)
		if !stop() {
			<-done
		}
		cancel()
		c.endShared()
		if err == nil {
			var result frame
			result, err = f.ParseFrame()
			f.Release()
			if err == nil {
				if _, ok := result.(*frm.SupportedFrame); !ok {
					err = fmt.Errorf("gocql: unexpected heartbeat response %T", result)
				}
			}
		}
		if err == nil {
			failures = 0
		} else {
			failures++
			if c.Closed() {
				return
			}
		}
		if failures > 5 {
			c.InternalCloseWithError(fmt.Errorf("gocql: heartbeat failed"))
			return
		}
		timer.Reset(interval)
	}
}

func (c *Conn) InternalOpeningContext(parent context.Context) (context.Context, context.CancelFunc) {
	if !c.InternalOpeningDeadline.IsZero() {
		return context.WithDeadline(parent, c.InternalOpeningDeadline)
	}
	return context.WithCancel(parent)
}

func (c *Conn) InternalValidateOpeningTarget(parent context.Context) error {
	ctx, cancel := c.InternalOpeningContext(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.InternalValidateTarget != nil {
		if err := c.InternalValidateTarget(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Setup cancellation interrupts socket I/O before joining accepted calls. Its
// returned barrier is idempotent and must finish before publishing a connection.
func (c *Conn) InternalSetupCancellation(ctx context.Context) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.InternalCloseWithError(ctx.Err()); close(done) })
	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		if !stop() {
			<-done
		}
	}
}

func (r *InternalExclusiveConn) cancelLifetime(cause error) {
	r.cancelMu.Lock()
	won := r.InternalClosing.CompareAndSwap(false, true)
	if won {
		r.terminalCause = cause
	}
	r.cancelMu.Unlock()
	if won {
		r.closeOwned(false)
	}
}

// Caller holds active. Register at most once per actual page/cancel domain,
// not once per row. A callback claims termination while holding the generation
// gate, before it can wait on active I/O; old page callbacks cannot affect new IO.
func (r *InternalExclusiveConn) watchIOContext(ctx context.Context) error {
	done := ctx.Done()
	if r.ioWatching && r.ioDone == done {
		return nil
	}
	r.cancelMu.Lock()
	if r.ioStop != nil {
		r.ioStop()
		r.ioStop = nil
	}
	r.InternalIoGeneration++
	generation := r.InternalIoGeneration
	r.ioActive = true
	r.ioWatching = true
	r.ioDone = done
	if done != nil && done != r.InternalCtx.Done() {
		r.ioStop = context.AfterFunc(ctx, func() { r.InternalCancelIOGeneration(generation, ctx.Err()) })
	}
	r.cancelMu.Unlock()
	if r.InternalClosing.Load() {
		return ErrClosed
	}
	return ctx.Err()
}

func (r *InternalExclusiveConn) stopIOContext() {
	r.cancelMu.Lock()
	r.ioActive = false
	r.ioWatching = false
	r.InternalIoGeneration++
	r.ioDone = nil
	if r.ioStop != nil {
		r.ioStop()
		r.ioStop = nil
	}
	r.cancelMu.Unlock()
}

func (r *InternalExclusiveConn) cancellationCause() error {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	return r.terminalCause
}

func (r *InternalExclusiveConn) InternalCancelIOGeneration(generation uint64, cause error) {
	r.cancelMu.Lock()
	won := r.ioActive && r.InternalIoGeneration == generation && r.InternalClosing.CompareAndSwap(false, true)
	if won {
		r.terminalCause = cause
	}
	r.cancelMu.Unlock()
	if won {
		r.closeOwned(false)
	}
}

// requestPause interrupts only the idle first-header-byte wait. If a frame
// races the request, its header/body complete normally before reader ack.
func (r *InternalConnReader) InternalRequestPause(pause bool) error {
	r.deadlineMu.Lock()
	defer r.deadlineMu.Unlock()
	r.pause.Store(pause)
	if r.InternalConn == nil {
		return nil
	}
	if pause && r.InternalDisarm.Load() {
		return r.InternalConn.SetReadDeadline(time.Now())
	}
	if !pause {
		return r.InternalConn.SetReadDeadline(time.Time{})
	}
	return nil
}
