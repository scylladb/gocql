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
	"context"
	"errors"
	"fmt"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"sync"
	"sync/atomic"
	"time"
)

var errOrphanBudget = errors.New("gocql: abandoned stream budget exceeded")

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
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return ErrConnectionClosed
		}
		if !c.exclusiveFlag.Load() {
			c.sharedOps++
			c.mu.Unlock()
			return nil
		}
		changed := c.changedLocked()
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func (c *Conn) endShared() { c.mu.Lock(); c.sharedOps--; c.changeLocked(); c.mu.Unlock() }
func (c *Conn) markOrphan(call *callReq) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Pointer identity is safe here: exec still owns this call until its epilogue.
	if c.closed || c.calls[call.streamID] != call || call.orphaned {
		return false
	}
	call.orphaned = true
	c.orphans++
	budget := 1024
	if c.cfg != nil && c.cfg.TypedConfig != nil {
		budget = c.cfg.TypedConfig.Pool.MaxOrphanedStreams
	}
	budget = min(budget, c.streams.NumStreams-1)
	return c.orphans > budget
}

// pauseSharedReader is called only at a complete native frame/v5 segment
// boundary. It acknowledges exclusive ownership before the acquiring goroutine
// can read and acknowledges shared resumption before admission reopens.
func (c *Conn) pauseSharedReader(ctx context.Context) bool {
	c.mu.Lock()
	if !c.pauseWanted {
		c.mu.Unlock()
		return false
	}
	ack, resume, resumed := c.readerPaused, c.readerResume, c.readerResumed
	close(ack)
	c.mu.Unlock()
	select {
	case <-resume:
	case <-ctx.Done():
	}
	c.mu.Lock()
	close(resumed)
	c.mu.Unlock()
	return true
}

type exclusiveConn struct {
	cancelMu             sync.Mutex
	ioGeneration         uint64
	ioActive, ioWatching bool
	ioDone               <-chan struct{}
	ioStop               func() bool
	terminalCause        error
	conn                 *Conn
	ctx                  context.Context
	stop                 func() bool
	active               sync.Mutex
	closing              atomic.Bool
	done                 chan struct{}
	closeErr             error
	complete             bool
	closed               bool
	stream               int
	limits               StreamLimits
	metadataBudget       *streamMetadataBudget
	units                *verifiedStreamUnits
}

func (c *Conn) reserveExclusive(ctx context.Context) (*exclusiveConn, error) {
	return c.reserveExclusiveWithLimits(ctx, StreamLimits{})
}
func (c *Conn) reserveExclusiveWithLimits(ctx context.Context, limits StreamLimits) (_ *exclusiveConn, err error) {
	limits, err = checkedStreamLimits(limits)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		return nil, typeValidation("stream context", "nil context")
	}
	reader, ok := c.r.(*connReader)
	if !ok || reader.conn == nil || reader.r != nil {
		return nil, &StreamCapabilityError{Protocol: int(c.version), Reason: "connection has no transferable unbuffered reader"}
	}
	if c.version < 3 || c.version > 5 || c.version < 5 && c.compressor != nil {
		return nil, &StreamCapabilityError{Protocol: int(c.version), Reason: "frame compression has no incremental receive transform"}
	}
	if c.version == 5 && c.compressor != nil && c.compressor.Name() != "lz4" {
		return nil, &StreamCapabilityError{Protocol: int(c.version), Reason: "v5 streaming requires raw or LZ4 segments"}
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrConnectionClosed
	}
	if c.cfg != nil && c.cfg.isControlConn {
		c.mu.Unlock()
		return nil, &StreamCapabilityError{Protocol: int(c.version), Reason: "control connection cannot be reserved"}
	}
	if c.exclusiveFlag.Load() {
		c.mu.Unlock()
		return nil, ErrNoStreams
	}
	c.exclusiveFlag.Store(true)
	c.changeLocked()
	c.mu.Unlock()
	acquired := false
	defer func() {
		if !acquired {
			c.restoreSharedReader(reader)
		}
	}()
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrConnectionClosed
		}
		if c.sharedOps == 0 && len(c.calls) == 0 && c.streams.InUse() == 0 {
			c.pauseWanted = true
			c.readerPaused = make(chan struct{})
			c.readerResume = make(chan struct{})
			c.readerResumed = make(chan struct{})
			paused := c.readerPaused
			c.mu.Unlock()
			if err = reader.requestPause(true); err != nil {
				return nil, err
			}
			select {
			case <-paused:
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-c.ctx.Done():
				return nil, ErrConnectionClosed
			}
			break
		}
		changed := c.changedLocked()
		c.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, ErrConnectionClosed
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	stream, ok := c.streams.GetStream()
	if !ok {
		return nil, ErrNoStreams
	}
	r := &exclusiveConn{conn: c, ctx: ctx, stream: stream, done: make(chan struct{}), complete: true}
	// Closing the socket interrupts an active synchronous decoder/write. The
	// public owner joins active below before allowing caller storage to return.
	if err = r.initStream(limits); err != nil {
		c.streams.Clear(stream)
		return nil, err
	}
	r.stop = context.AfterFunc(ctx, func() { r.cancelLifetime(ctx.Err()) })
	acquired = true
	return r, nil
}
func (c *Conn) restoreSharedReader(reader *connReader) {
	c.mu.Lock()
	paused, resume, resumed := c.readerPaused, c.readerResume, c.readerResumed
	if c.pauseWanted {
		c.pauseWanted = false
		if resume != nil {
			close(resume)
		}
	}
	c.mu.Unlock()
	reader.requestPause(false)
	if paused != nil {
		select {
		case <-paused:
			select {
			case <-resumed:
			case <-c.ctx.Done():
			}
		default:
		}
	}
	c.mu.Lock()
	c.exclusiveFlag.Store(false)
	c.changeLocked()
	c.mu.Unlock()
	if pool, ok := c.errorHandler.(*hostConnPool); ok {
		pool.signalReservation()
	}
}
func (r *exclusiveConn) close(reusable bool) error {
	if r == nil {
		return nil
	}
	// Win generation ownership before touching the connection. A delayed close
	// from a released cursor must not affect newer shared requests.
	if !r.closing.CompareAndSwap(false, true) {
		<-r.done
		return r.closeErr
	}
	return r.closeOwned(reusable)
}

// closeOwned is called only after winning the terminal generation gate.
func (r *exclusiveConn) closeOwned(reusable bool) error {
	defer close(r.done)
	if r.stop != nil {
		r.stop()
	}
	// Interruption happens before joining the active synchronous I/O owner.
	if !reusable {
		r.conn.closeWithError(ErrConnectionClosed)
	}
	r.active.Lock()
	defer r.active.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.stopIOContext()
	if reusable && r.complete && r.units != nil {
		if err := r.drainVerifiedControl(); err != nil {
			r.closeErr = err
			r.conn.closeWithError(err)
			reusable = false
		}
	}
	if r.stop != nil {
		r.stop()
	}
	r.conn.streams.Clear(r.stream)
	reader := r.conn.r.(*connReader)
	if reusable && (!r.complete || r.units != nil && !r.units.boundary()) {
		r.closeErr = fmt.Errorf("gocql: incomplete streaming response")
		r.conn.closeWithError(r.closeErr)
		reusable = false
	}
	if r.units != nil {
		r.units.data = nil
		r.units.offset = 0
		r.units.units.Close()
		r.units = nil
	}
	r.conn.restoreSharedReader(reader)
	return r.closeErr
}

// requestPause interrupts only the idle first-header-byte wait. If a frame
// races the request, its header/body complete normally before reader ack.
func (r *connReader) requestPause(pause bool) error {
	r.deadlineMu.Lock()
	defer r.deadlineMu.Unlock()
	r.pause.Store(pause)
	if r.conn == nil {
		return nil
	}
	if pause && r.disarm.Load() {
		return r.conn.SetReadDeadline(time.Now())
	}
	if !pause {
		return r.conn.SetReadDeadline(time.Time{})
	}
	return nil
}

func (c *Conn) typedHeartbeat(ctx context.Context) {
	config := c.cfg.TypedConfig
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
		stop := context.AfterFunc(callCtx, func() { c.closeWithError(callCtx.Err()); close(done) })
		f, err := c.execAdmitted(callCtx, &writeOptionsFrame{}, nil, 0, true)
		if !stop() {
			<-done
		}
		cancel()
		c.endShared()
		if err == nil {
			var result frame
			result, err = f.parseFrame()
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
			c.closeWithError(fmt.Errorf("gocql: heartbeat failed"))
			return
		}
		timer.Reset(interval)
	}
}

func (c *Conn) openingContext(parent context.Context) (context.Context, context.CancelFunc) {
	if !c.openingDeadline.IsZero() {
		return context.WithDeadline(parent, c.openingDeadline)
	}
	return context.WithCancel(parent)
}
func (c *Conn) validateOpeningTarget(parent context.Context) error {
	ctx, cancel := c.openingContext(parent)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.validateTarget != nil {
		if err := c.validateTarget(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// Setup cancellation interrupts socket I/O before joining accepted calls. Its
// returned barrier is idempotent and must finish before publishing a connection.
func (c *Conn) setupCancellation(ctx context.Context) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { c.closeWithError(ctx.Err()); close(done) })
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

func (r *exclusiveConn) cancelLifetime(cause error) {
	r.cancelMu.Lock()
	won := r.closing.CompareAndSwap(false, true)
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
func (r *exclusiveConn) watchIOContext(ctx context.Context) error {
	done := ctx.Done()
	if r.ioWatching && r.ioDone == done {
		return nil
	}
	r.cancelMu.Lock()
	if r.ioStop != nil {
		r.ioStop()
		r.ioStop = nil
	}
	r.ioGeneration++
	generation := r.ioGeneration
	r.ioActive = true
	r.ioWatching = true
	r.ioDone = done
	if done != nil && done != r.ctx.Done() {
		r.ioStop = context.AfterFunc(ctx, func() { r.cancelIOGeneration(generation, ctx.Err()) })
	}
	r.cancelMu.Unlock()
	if r.closing.Load() {
		return ErrClosed
	}
	return ctx.Err()
}
func (r *exclusiveConn) stopIOContext() {
	r.cancelMu.Lock()
	r.ioActive = false
	r.ioWatching = false
	r.ioGeneration++
	r.ioDone = nil
	if r.ioStop != nil {
		r.ioStop()
		r.ioStop = nil
	}
	r.cancelMu.Unlock()
}
func (r *exclusiveConn) cancellationCause() error {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	return r.terminalCause
}

func (r *exclusiveConn) cancelIOGeneration(generation uint64, cause error) {
	r.cancelMu.Lock()
	won := r.ioActive && r.ioGeneration == generation && r.closing.CompareAndSwap(false, true)
	if won {
		r.terminalCause = cause
	}
	r.cancelMu.Unlock()
	if won {
		r.closeOwned(false)
	}
}
