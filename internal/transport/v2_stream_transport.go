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
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	"errors"
	"fmt"
	"github.com/scylladb/gocql/v2/internal/codec"
	"io"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type StreamLimits struct {
	MaxMetadataBytes             int64
	ReadAheadBytes, DiscardBytes int
	MaxTransformBytes            int64
}

func InternalCheckedStreamLimits(l StreamLimits) (StreamLimits, error) {
	if l.MaxMetadataBytes < 0 || l.ReadAheadBytes < 0 || l.DiscardBytes < 0 || l.MaxTransformBytes < 0 {
		return l, typeValidation("stream limits", "negative budget")
	}
	if l.MaxMetadataBytes == 0 {
		l.MaxMetadataBytes = 1 << 20
	}
	if l.ReadAheadBytes == 0 {
		l.ReadAheadBytes = 32 << 10
	}
	if l.DiscardBytes == 0 {
		l.DiscardBytes = 32 << 10
	}
	if l.MaxTransformBytes == 0 {
		l.MaxTransformBytes = segment.DefaultUnitStorageBytes
	}
	if l.MaxTransformBytes < 16 {
		return l, typeValidation("stream limits", "transform budget below framing size")
	}
	return l, nil
}

// verifiedStreamUnits exposes only one validated v5 unit. Native headers/cells
// may cross non-self-contained units; self-contained units never split a frame.
type InternalVerifiedStreamUnits struct {
	InternalUnits                              *segment.UnitReader
	InternalData                               []byte
	InternalOffset                             int
	self, InternalFrameActive, InternalStarted bool
	InternalCopies                             int64
}

func (s *InternalVerifiedStreamUnits) beginFrame() {
	s.InternalFrameActive = true
	s.InternalStarted = false
}
func (s *InternalVerifiedStreamUnits) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.InternalOffset == len(s.InternalData) {
		if s.InternalFrameActive && s.InternalStarted && s.self {
			return 0, fmt.Errorf("gocql: frame exceeds self-contained segment")
		}
		h, b, err := s.InternalUnits.Next()
		if err != nil {
			return 0, err
		}
		if s.InternalFrameActive && s.InternalStarted && h.IsSelfContained {
			return 0, fmt.Errorf("gocql: split frame continued by self-contained segment")
		}
		s.InternalData = b
		s.InternalOffset = 0
		s.self = h.IsSelfContained
		if len(b) == 0 {
			return 0, fmt.Errorf("gocql: empty v5 segment")
		}
	}
	s.InternalStarted = true
	n := copy(p, s.InternalData[s.InternalOffset:])
	s.InternalOffset += n
	s.InternalCopies += int64(n)
	return n, nil
}
func (s *InternalVerifiedStreamUnits) endFrame() error {
	if !s.self && s.InternalOffset != len(s.InternalData) {
		return fmt.Errorf("gocql: non-self-contained segment has trailing frame bytes")
	}
	s.InternalFrameActive = false
	return nil
}
func (s *InternalVerifiedStreamUnits) boundary() bool {
	return !s.InternalFrameActive && s.InternalOffset == len(s.InternalData)
}

type InternalStreamResponse struct {
	InternalCtx                     context.Context
	reservation                     *InternalExclusiveConn
	InternalBody                    *wire.Stream
	InternalMetadata                parsedMetadata
	InternalColumns                 Columns
	InternalColumnCount             int
	InternalRowCount, InternalIndex int32
	InternalKind                    int
	InternalInfo                    Result
	InternalBudget                  *streamMetadataCursor
	InternalReader                  *StreamRowReader
	InternalTerminal                bool
}

// InternalWorkingStorageBytes accounts live driver byte storage, excluding
// caller output, source fixtures and standard-library TLS record storage.
// Metadata uses its conservative simultaneous allocation charge; unit aliases
// are counted once and raw production ConnReader has no read-ahead allocation.
func (r *InternalStreamResponse) InternalWorkingStorageBytes() int64 {
	if r == nil {
		return 0
	}
	n := r.InternalBody.WorkingStorageBytes()
	if r.reservation.InternalMetadataBudget != nil {
		n += r.reservation.InternalMetadataBudget.Bytes()
	}
	n += codec.InternalStreamReaderWorkingStorageBytes(r.InternalReader)
	if r.reservation.InternalUnits != nil {
		n += r.reservation.InternalUnits.InternalUnits.StorageBytes()
	}
	if reader, ok := r.reservation.InternalConn.InternalR.(*InternalConnReader); ok && reader.InternalR != nil {
		n += int64(reader.InternalR.Size())
	}
	return n
}

func (r *InternalStreamResponse) InternalReleaseMetadata() {
	if r == nil {
		return
	}
	if r.InternalBudget != nil {
		r.InternalBudget.Release()
		r.InternalBudget = nil
	}
	r.InternalBody = nil
	r.InternalTerminal = true
	r.InternalColumns = Columns{}
	r.InternalMetadata = parsedMetadata{}
	r.InternalReader = nil
	r.InternalInfo = Result{}
}
func (r *InternalExclusiveConn) InternalInitStream(limits StreamLimits) error {
	l, err := InternalCheckedStreamLimits(limits)
	if err != nil {
		return err
	}
	r.InternalLimits = l
	r.InternalConn.InternalMu.Lock()
	if r.InternalConn.InternalStreamBudget == nil {
		r.InternalConn.InternalStreamBudget = &streamMetadataBudget{Limit: l.MaxMetadataBytes}
	}
	r.InternalMetadataBudget = r.InternalConn.InternalStreamBudget
	r.InternalConn.InternalMu.Unlock()
	if err := r.InternalMetadataBudget.SetLimit(l.MaxMetadataBytes); err != nil {
		return err
	}
	if r.InternalConn.InternalVersion == 5 {
		units, err := segment.NewUnitReader(r.InternalCtx, r.InternalConn.InternalR, r.InternalConn.InternalSegCompressor, segment.UnitLimits{MaxStorageBytes: l.MaxTransformBytes, MaxDecodedBytes: int(min(r.InternalConn.typedProtocolLimits().MaxDecompressedBytes, segment.MaxPayloadSize))})
		if err != nil {
			return err
		}
		r.InternalUnits = &InternalVerifiedStreamUnits{InternalUnits: units}
	}
	r.InternalComplete = true
	return nil
}
func (r *InternalExclusiveConn) receiveSource() io.Reader {
	if r.InternalUnits != nil {
		return r.InternalUnits
	}
	return r.InternalConn.InternalR
}
func (r *InternalExclusiveConn) InternalFinishResponse() error {
	if r.InternalUnits != nil {
		if err := r.InternalUnits.endFrame(); err != nil {
			return err
		}
	}
	r.InternalComplete = true
	r.stopIOContext()
	return nil
}
func (r *InternalExclusiveConn) InternalWithIO(ctx context.Context, fn func() error) error {
	if ctx == nil {
		return typeValidation("stream context", "nil context")
	}
	if r.InternalClosing.Load() {
		return ErrClosed
	}
	r.active.Lock()
	defer r.active.Unlock()
	if r.InternalClosing.Load() || r.closed || r.InternalConn.InternalCtx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := fn()
	if r.InternalClosing.Load() {
		if cause := r.cancellationCause(); cause != nil {
			return cause
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = r.InternalCtx.Err()
	}
	return err
}

// exchange installs exclusive receive ownership before writing. It returns
// after checked extensions/metadata and row count, without consuming row data.
// Runtime owns metadata selection/reprepare/paging and calls decodeStreamRow.
// Every row read uses the page context supplied here plus reservation lifetime.
func (r *InternalExclusiveConn) InternalExchange(ctx context.Context, request *attemptRequest) (response *InternalStreamResponse, err error) {
	err = r.InternalWithIO(ctx, func() error {
		var pending *streamMetadataCursor
		defer func() {
			if pending != nil {
				pending.Release()
			}
		}()
		if request == nil {
			return typeValidation("stream request", "nil attempt")
		}
		if !r.InternalComplete {
			return &ProtocolError{Stage: "stream exchange", Err: errors.New("prior response incomplete")}
		}
		if r.InternalLimits.MaxMetadataBytes == 0 {
			if err := r.InternalInitStream(StreamLimits{}); err != nil {
				return err
			}
		}
		if request.Lease != nil {
			if !request.Lease.Retain() {
				return ErrReleased
			}
			defer request.Lease.Release()
		}
		owners := make([]*wire.Lease, 0, len(request.Payload))
		defer func() {
			for _, o := range owners {
				o.Release()
			}
		}()
		for _, e := range request.Payload {
			if e.Value.Valid && cqltype.BytesLease(e.Value.Value) != nil {
				if !cqltype.BytesLease(e.Value.Value).Retain() {
					return ErrReleased
				}
				owners = append(owners, cqltype.BytesLease(e.Value.Value))
			}
		}
		packet, err := r.InternalConn.InternalBuildTypedRequest(request, r.InternalStream)
		if err != nil {
			return err
		}
		writer, ok := r.InternalConn.InternalW.(spanContextWriter)
		if !ok {
			return &StreamCapabilityError{Protocol: int(r.InternalConn.InternalVersion), Reason: "writer lacks span support"}
		}
		if err := r.watchIOContext(ctx); err != nil {
			return err
		}
		r.InternalComplete = false
		written, err := writer.InternalWriteSpans(ctx, packet)
		if err != nil {
			return runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: written > 0})
		}
		for {
			if r.InternalUnits != nil {
				r.InternalUnits.beginFrame()
			}
			limits := r.InternalConn.typedProtocolLimits()
			header, body, err := frm.ReadStreamEnvelope(ctx, r.receiveSource(), r.InternalConn.InternalVersion, wire.StreamLimits{MaxBytes: min(limits.MaxFrameBytes, limits.MaxDecompressedBytes), MaxDepth: limits.MaxMetadataDepth + 2, DiscardBytes: r.InternalLimits.DiscardBytes}, r.InternalConn.typedResponseExtensionFlags())
			if err != nil {
				return err
			}
			cursor := &streamMetadataCursor{Scope: body, Budget: r.InternalMetadataBudget}
			pending = cursor
			p := &typedParser{Cursor: cursor, Proto: r.InternalConn.InternalVersion, MetadataID: r.InternalConn.InternalTracksResultMetadataID(), MaxDepth: limits.MaxMetadataDepth}
			info := p.StreamExtensions(header)
			if p.Err != nil {
				return p.Err
			}
			if header.Op == frm.OpEvent {
				event := p.StreamEvent(header)
				if p.Err != nil {
					return p.Err
				}
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if r.InternalUnits != nil {
					if err := r.InternalUnits.endFrame(); err != nil {
						return err
					}
				}
				r.InternalComplete = true
				pending = nil
				r.InternalComplete = false
				if err := r.InternalConn.InternalDispatchEventOwned(event, cursor.Release); err != nil {
					return err
				}
				r.InternalComplete = false
				continue
			}
			if header.Stream != r.InternalStream {
				return &ProtocolError{Stage: "stream response", Err: fmt.Errorf("unexpected stream %d", header.Stream)}
			}
			if header.Op == frm.OpError {
				serverErr := p.ServerError(header, r.InternalConn.InternalFramers.InternalDefaults.rateLimitingErrorCode)
				if p.Err != nil {
					return p.Err
				}
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if err := r.InternalFinishResponse(); err != nil {
					return err
				}
				response = &InternalStreamResponse{InternalCtx: ctx, reservation: r, InternalBody: body, InternalBudget: cursor, InternalInfo: info, InternalTerminal: true}
				pending = nil
				return serverErr
			}
			if header.Op != frm.OpResult {
				return &ProtocolError{Stage: "stream response", Err: fmt.Errorf("unexpected opcode %d", header.Op)}
			}
			result := &InternalStreamResponse{InternalCtx: ctx, reservation: r, InternalBody: body, InternalBudget: cursor, InternalInfo: info, InternalKind: p.I32()}
			switch result.InternalKind {
			case frm.ResultKindRows:
				result.InternalColumns, result.InternalColumnCount = p.MetadataFields(&result.InternalMetadata, false, 0)
				result.InternalRowCount = int32(p.I32())
				if result.InternalRowCount < 0 {
					return &ProtocolError{Stage: "stream rows", Err: wire.ErrLength}
				}
				if _, err := wire.CheckProduct(int64(result.InternalRowCount), int64(result.InternalColumnCount), body.Remaining()/4); err != nil {
					return err
				}
			case frm.ResultKindVoid:
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if err := r.InternalFinishResponse(); err != nil {
					return err
				}
				result.InternalTerminal = true
			default:
				return &StreamCapabilityError{Protocol: int(r.InternalConn.InternalVersion), Reason: "streaming execution requires ROWS or VOID result"}
			}
			if p.Err != nil {
				return p.Err
			}
			response = result
			pending = nil
			return nil
		}
	})
	if err != nil && !r.InternalComplete {
		r.InternalClose(false)
	}
	return response, err
}

// Settle columns using the attempt's exact immutable metadata before exposing
// a row reader. Full response metadata is never replaced by a newer cache view.
func (r *InternalStreamResponse) InternalInstallColumns(columns Columns) error {
	if columns.Len() != r.InternalColumnCount {
		return typeValidation("stream columns", "metadata count differs")
	}
	reader, err := newStreamRowReader(columns, r.InternalBody)
	if err != nil {
		return err
	}
	r.InternalColumns = columns
	r.InternalReader = reader
	return nil
}

// Continuation pages may share internal immutable validation plans. The codec
// requires exact metadata ownership identity and allocates fresh page state.
func (r *InternalStreamResponse) InternalInstallColumnsReusing(columns Columns, prior *InternalStreamResponse) error {
	if columns.Len() != r.InternalColumnCount {
		return typeValidation("stream columns", "metadata count differs")
	}
	var previous *StreamRowReader
	if prior != nil {
		previous = prior.InternalReader
	}
	reader, err := codec.NewStreamRowReaderReusingValidation(columns, r.InternalBody, previous)
	if err != nil {
		return err
	}
	r.InternalColumns = columns
	r.InternalReader = reader
	return nil
}
func InternalDecodeStreamRow[R any](response *InternalStreamResponse, plan StreamRowPlan[R], dst *R) (ok bool, err error) {
	if response == nil {
		return false, typeValidation("stream response", "nil response")
	}
	pageCtx := response.InternalCtx
	if pageCtx == nil {
		return false, typeValidation("stream context", "response has no page context")
	}
	err = response.reservation.InternalWithIO(pageCtx, func() error {
		if response.InternalTerminal {
			return nil
		}
		if response.InternalIndex == response.InternalRowCount && response.reservation.InternalComplete {
			response.InternalTerminal = true
			return nil
		}
		if dst == nil {
			return typeValidation("destination", "nil stream destination")
		}
		if response.InternalReader == nil || plan == nil {
			return typeValidation("stream plan", "metadata/plan not installed")
		}
		if response.InternalIndex == response.InternalRowCount {
			if err := response.InternalBody.RequireEnd(); err != nil {
				return err
			}
			if err := response.reservation.InternalFinishResponse(); err != nil {
				return err
			}
			response.InternalTerminal = true
			return nil
		}
		codec.BeginStreamRow(response.InternalReader)
		plan.Reset(dst)
		if err := plan.DecodeInto(response.InternalReader, dst); err != nil {
			return err
		}
		if err := response.InternalReader.RequireEnd(); err != nil {
			return err
		}
		response.InternalIndex++
		ok = true
		// Exact final framing is checked before publication of the final row.
		if response.InternalIndex == response.InternalRowCount {
			if err := response.InternalBody.RequireEnd(); err != nil {
				return err
			}
			if err := response.reservation.InternalFinishResponse(); err != nil {
				return err
			}
			response.InternalTerminal = true
		}
		return nil
	})
	if err != nil {
		ok = false
		response.reservation.InternalClose(false)
	}
	return ok, err
}

// One bounded event worker per connection replaces unbounded goroutine-per-event
// dispatch. Ownership charges transfer with the queued event until consumption.
type InternalConnEvent struct {
	value   frame
	release func()
}

func (c *Conn) dispatchEvent(event frame) error { return c.InternalDispatchEventOwned(event, nil) }
func (c *Conn) InternalDispatchEventOwned(event frame, release func()) error {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	reject := func(err error) error {
		if release != nil {
			release()
		}
		return err
	}
	if c.InternalHooks == nil {
		return reject(nil)
	}
	if c.eventsClosed || c.InternalCtx.Err() != nil {
		return reject(ErrClosed)
	}
	c.eventOnce.Do(func() {
		c.eventQueue = make(chan InternalConnEvent, 32)
		c.InternalMu.Lock()
		c.eventDone = make(chan struct{})
		done := c.eventDone
		c.InternalMu.Unlock()
		coordinator := c.InternalHooks
		if !coordinator.InternalStartConnectionWorker(func() {
			defer close(done)
			defer func() {
				c.eventMu.Lock()
				defer c.eventMu.Unlock()
				c.eventsClosed = true
				for {
					select {
					case e := <-c.eventQueue:
						if e.release != nil {
							e.release()
						}
					default:
						return
					}
				}
			}()
			for {
				select {
				case <-coordinator.InternalConnectionWorkersDone():
					return
				case <-c.InternalCtx.Done():
					return
				case e := <-c.eventQueue:
					retained := coordinator.InternalHandleConnectionEvent(c.InternalCtx, e.value, e.release)
					if !retained && e.release != nil {
						e.release()
					}
				}
			}
		}) {
			c.eventsClosed = true
			close(done)
		}
	})
	if c.eventsClosed {
		return reject(ErrClosed)
	}
	select {
	case c.eventQueue <- InternalConnEvent{event, release}:
		return nil
	default:
		return reject(&ProtocolError{Stage: "event queue", Err: errors.New("bounded event dispatch overflow")})
	}
}

// Higher-level shutdown joins this worker after canceling its context and
// releasing any event consumer locks. It must not be called by that worker.
func (c *Conn) InternalWaitEventDispatch() {
	c.InternalMu.Lock()
	done := c.eventDone
	c.InternalMu.Unlock()
	if done != nil {
		<-done
	}
}

// Drain only control envelopes already present in the validated final unit.
// No read of another socket unit is allowed when releasing a completed result.
func (r *InternalExclusiveConn) drainVerifiedControl() error {
	for r.InternalUnits != nil && r.InternalUnits.InternalOffset < len(r.InternalUnits.InternalData) {
		if !r.InternalUnits.self {
			return &ProtocolError{Stage: "stream trailing unit", Err: wire.ErrTrailingBytes}
		}
		r.InternalUnits.beginFrame()
		limits := r.InternalConn.typedProtocolLimits()
		h, body, err := frm.ReadStreamEnvelope(r.InternalCtx, r.InternalUnits, r.InternalConn.InternalVersion, wire.StreamLimits{MaxBytes: min(limits.MaxFrameBytes, limits.MaxDecompressedBytes), MaxDepth: limits.MaxMetadataDepth + 2, DiscardBytes: r.InternalLimits.DiscardBytes}, r.InternalConn.typedResponseExtensionFlags())
		if err != nil {
			return err
		}
		cursor := &streamMetadataCursor{Scope: body, Budget: r.InternalMetadataBudget}
		p := &typedParser{Cursor: cursor, Proto: r.InternalConn.InternalVersion, MaxDepth: limits.MaxMetadataDepth}
		p.StreamExtensions(h)
		if h.Op != frm.OpEvent || h.Stream != -1 {
			cursor.Release()
			return &ProtocolError{Stage: "stream trailing frame", Err: fmt.Errorf("unexpected opcode %d/stream %d", h.Op, h.Stream)}
		}
		event := p.StreamEvent(h)
		if p.Err != nil {
			cursor.Release()
			return p.Err
		}
		if err := body.RequireEnd(); err != nil {
			cursor.Release()
			return err
		}
		if err := r.InternalUnits.endFrame(); err != nil {
			cursor.Release()
			return err
		}
		if err := r.InternalConn.InternalDispatchEventOwned(event, cursor.Release); err != nil {
			return err
		}
	}
	return nil
}
