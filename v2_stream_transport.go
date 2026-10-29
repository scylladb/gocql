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
	"io"
	"net"
	"sync"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type StreamLimits struct {
	MaxMetadataBytes             int64
	ReadAheadBytes, DiscardBytes int
	MaxTransformBytes            int64
}

func checkedStreamLimits(l StreamLimits) (StreamLimits, error) {
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

// The budget is shared by all live metadata/control objects of a reserved
// connection. Replacing an object does not reset accounting before release.
type streamMetadataBudget struct {
	mu          sync.Mutex
	limit, live int64
}

func (b *streamMetadataBudget) reserve(n int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.limit-b.live {
		return &ProtocolError{Stage: "stream metadata budget", Err: wire.ErrLimit}
	}
	b.live += n
	return nil
}

type streamMetadataCursor struct {
	scope    *wire.Stream
	budget   *streamMetadataBudget
	used     int64
	released sync.Once
}

func (b *streamMetadataBudget) release(n int64) { b.mu.Lock(); b.live -= n; b.mu.Unlock() }
func (b *streamMetadataBudget) bytes() int64    { b.mu.Lock(); defer b.mu.Unlock(); return b.live }
func (b *streamMetadataBudget) setLimit(limit int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.live > limit {
		return wire.ErrLimit
	}
	b.limit = limit
	return nil
}
func (c *streamMetadataCursor) release() { c.released.Do(func() { c.budget.release(c.used) }) }
func (c *streamMetadataCursor) reserve(n int64) error {
	if err := c.budget.reserve(n); err != nil {
		return err
	}
	c.used += n
	return nil
}
func (c *streamMetadataCursor) copy(n int64) ([]byte, error) {
	if n < 0 || n > c.scope.Remaining() {
		return nil, io.ErrUnexpectedEOF
	}
	if err := c.reserve(n); err != nil {
		return nil, err
	}
	b := make([]byte, int(n))
	if err := c.scope.ReadFull(b); err != nil {
		return nil, err
	}
	return b, nil
}
func (c *streamMetadataCursor) Take(n int64) (wire.Chain, error) {
	b, err := c.copy(n)
	if err != nil {
		return wire.Chain{}, err
	}
	return wire.NewChain(wire.Borrow(b))
}
func (c *streamMetadataCursor) Uint8() (uint8, error)   { return c.scope.Uint8() }
func (c *streamMetadataCursor) Uint16() (uint16, error) { return c.scope.Uint16() }
func (c *streamMetadataCursor) Uint32() (uint32, error) { return c.scope.Uint32() }
func (c *streamMetadataCursor) Uint64() (uint64, error) { return c.scope.Uint64() }
func (c *streamMetadataCursor) Remaining() int64        { return c.scope.Remaining() }
func (c *streamMetadataCursor) RequireEnd() error       { return c.scope.RequireEnd() }

// verifiedStreamUnits exposes only one validated v5 unit. Native headers/cells
// may cross non-self-contained units; self-contained units never split a frame.
type verifiedStreamUnits struct {
	units                      *segment.UnitReader
	data                       []byte
	offset                     int
	self, frameActive, started bool
	copies                     int64
}

func (s *verifiedStreamUnits) beginFrame() { s.frameActive = true; s.started = false }
func (s *verifiedStreamUnits) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.offset == len(s.data) {
		if s.frameActive && s.started && s.self {
			return 0, fmt.Errorf("gocql: frame exceeds self-contained segment")
		}
		h, b, err := s.units.Next()
		if err != nil {
			return 0, err
		}
		if s.frameActive && s.started && h.IsSelfContained {
			return 0, fmt.Errorf("gocql: split frame continued by self-contained segment")
		}
		s.data = b
		s.offset = 0
		s.self = h.IsSelfContained
		if len(b) == 0 {
			return 0, fmt.Errorf("gocql: empty v5 segment")
		}
	}
	s.started = true
	n := copy(p, s.data[s.offset:])
	s.offset += n
	s.copies += int64(n)
	return n, nil
}
func (s *verifiedStreamUnits) endFrame() error {
	if !s.self && s.offset != len(s.data) {
		return fmt.Errorf("gocql: non-self-contained segment has trailing frame bytes")
	}
	s.frameActive = false
	return nil
}
func (s *verifiedStreamUnits) boundary() bool { return !s.frameActive && s.offset == len(s.data) }

type streamResponse struct {
	ctx             context.Context
	reservation     *exclusiveConn
	body            *wire.Stream
	metadata        parsedMetadata
	columns         Columns
	columnCount     int
	rowCount, index int32
	kind            int
	info            Result
	budget          *streamMetadataCursor
	reader          *StreamRowReader
	terminal        bool
}

func (r *streamResponse) releaseMetadata() {
	if r == nil {
		return
	}
	if r.budget != nil {
		r.budget.release()
		r.budget = nil
	}
	r.body = nil
	r.terminal = true
	r.columns = Columns{}
	r.metadata = parsedMetadata{}
	r.reader = nil
	r.info = Result{}
}
func (r *exclusiveConn) initStream(limits StreamLimits) error {
	l, err := checkedStreamLimits(limits)
	if err != nil {
		return err
	}
	r.limits = l
	r.conn.mu.Lock()
	if r.conn.streamBudget == nil {
		r.conn.streamBudget = &streamMetadataBudget{limit: l.MaxMetadataBytes}
	}
	r.metadataBudget = r.conn.streamBudget
	r.conn.mu.Unlock()
	if err := r.metadataBudget.setLimit(l.MaxMetadataBytes); err != nil {
		return err
	}
	if r.conn.version == 5 {
		units, err := segment.NewUnitReader(r.ctx, r.conn.r, r.conn.segCompressor, segment.UnitLimits{MaxStorageBytes: l.MaxTransformBytes, MaxDecodedBytes: int(min(r.conn.typedProtocolLimits().MaxDecompressedBytes, segment.MaxPayloadSize))})
		if err != nil {
			return err
		}
		r.units = &verifiedStreamUnits{units: units}
	}
	r.complete = true
	return nil
}
func (r *exclusiveConn) receiveSource() io.Reader {
	if r.units != nil {
		return r.units
	}
	return r.conn.r
}
func (r *exclusiveConn) finishResponse() error {
	if r.units != nil {
		if err := r.units.endFrame(); err != nil {
			return err
		}
	}
	r.complete = true
	r.stopIOContext()
	return nil
}
func (r *exclusiveConn) withIO(ctx context.Context, fn func() error) error {
	if ctx == nil {
		return typeValidation("stream context", "nil context")
	}
	if r.closing.Load() {
		return ErrClosed
	}
	r.active.Lock()
	defer r.active.Unlock()
	if r.closing.Load() || r.closed || r.conn.ctx.Err() != nil {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := fn()
	if r.closing.Load() {
		if cause := r.cancellationCause(); cause != nil {
			return cause
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		err = r.ctx.Err()
	}
	return err
}

// exchange installs exclusive receive ownership before writing. It returns
// after checked extensions/metadata and row count, without consuming row data.
// Runtime owns metadata selection/reprepare/paging and calls decodeStreamRow.
// Every row read uses the page context supplied here plus reservation lifetime.
func (r *exclusiveConn) exchange(ctx context.Context, request *attemptRequest) (response *streamResponse, err error) {
	err = r.withIO(ctx, func() error {
		var pending *streamMetadataCursor
		defer func() {
			if pending != nil {
				pending.release()
			}
		}()
		if request == nil {
			return typeValidation("stream request", "nil attempt")
		}
		if !r.complete {
			return &ProtocolError{Stage: "stream exchange", Err: errors.New("prior response incomplete")}
		}
		if r.limits.MaxMetadataBytes == 0 {
			if err := r.initStream(StreamLimits{}); err != nil {
				return err
			}
		}
		if request.lease != nil {
			if !request.lease.Retain() {
				return ErrReleased
			}
			defer request.lease.Release()
		}
		owners := make([]*wire.Lease, 0, len(request.payload))
		defer func() {
			for _, o := range owners {
				o.Release()
			}
		}()
		for _, e := range request.payload {
			if e.Value.Valid && e.Value.Value.lease != nil {
				if !e.Value.Value.lease.Retain() {
					return ErrReleased
				}
				owners = append(owners, e.Value.Value.lease)
			}
		}
		packet, err := r.conn.buildTypedRequest(request, r.stream)
		if err != nil {
			return err
		}
		writer, ok := r.conn.w.(spanContextWriter)
		if !ok {
			return &StreamCapabilityError{Protocol: int(r.conn.version), Reason: "writer lacks span support"}
		}
		if err := r.watchIOContext(ctx); err != nil {
			return err
		}
		r.complete = false
		written, err := writer.writeSpans(ctx, packet)
		if err != nil {
			return &QueryError{err: err, potentiallyExecuted: written > 0}
		}
		for {
			if r.units != nil {
				r.units.beginFrame()
			}
			limits := r.conn.typedProtocolLimits()
			header, body, err := frm.ReadStreamEnvelope(ctx, r.receiveSource(), r.conn.version, wire.StreamLimits{MaxBytes: min(limits.MaxFrameBytes, limits.MaxDecompressedBytes), MaxDepth: limits.MaxMetadataDepth + 2, DiscardBytes: r.limits.DiscardBytes})
			if err != nil {
				return err
			}
			cursor := &streamMetadataCursor{scope: body, budget: r.metadataBudget}
			pending = cursor
			p := &typedParser{cursor: cursor, proto: r.conn.version, metadataID: r.conn.tracksResultMetadataID(), maxDepth: limits.MaxMetadataDepth}
			info := p.streamExtensions(header)
			if p.err != nil {
				return p.err
			}
			if header.Op == frm.OpEvent {
				event := p.streamEvent(header)
				if p.err != nil {
					return p.err
				}
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if r.units != nil {
					if err := r.units.endFrame(); err != nil {
						return err
					}
				}
				r.complete = true
				pending = nil
				r.complete = false
				if err := r.conn.dispatchEventOwned(event, cursor.release); err != nil {
					return err
				}
				r.complete = false
				continue
			}
			if header.Stream != r.stream {
				return &ProtocolError{Stage: "stream response", Err: fmt.Errorf("unexpected stream %d", header.Stream)}
			}
			if header.Op == frm.OpError {
				serverErr := p.serverError(header, r.conn.framers.defaults.rateLimitingErrorCode)
				if p.err != nil {
					return p.err
				}
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if err := r.finishResponse(); err != nil {
					return err
				}
				response = &streamResponse{ctx: ctx, reservation: r, body: body, budget: cursor, info: info, terminal: true}
				pending = nil
				return serverErr
			}
			if header.Op != frm.OpResult {
				return &ProtocolError{Stage: "stream response", Err: fmt.Errorf("unexpected opcode %d", header.Op)}
			}
			result := &streamResponse{ctx: ctx, reservation: r, body: body, budget: cursor, info: info, kind: p.i32()}
			switch result.kind {
			case frm.ResultKindRows:
				result.columns, result.columnCount = p.metadataFields(&result.metadata, false, 0)
				result.rowCount = int32(p.i32())
				if result.rowCount < 0 {
					return &ProtocolError{Stage: "stream rows", Err: wire.ErrLength}
				}
				if _, err := wire.CheckProduct(int64(result.rowCount), int64(result.columnCount), body.Remaining()/4); err != nil {
					return err
				}
			case frm.ResultKindVoid:
				if err := body.RequireEnd(); err != nil {
					return err
				}
				if err := r.finishResponse(); err != nil {
					return err
				}
				result.terminal = true
			default:
				return &StreamCapabilityError{Protocol: int(r.conn.version), Reason: "streaming execution requires ROWS or VOID result"}
			}
			if p.err != nil {
				return p.err
			}
			response = result
			pending = nil
			return nil
		}
	})
	if err != nil && !r.complete {
		r.close(false)
	}
	return response, err
}

// Settle columns using the attempt's exact immutable metadata before exposing
// a row reader. Full response metadata is never replaced by a newer cache view.
func (r *streamResponse) installColumns(columns Columns) error {
	if columns.Len() != r.columnCount {
		return typeValidation("stream columns", "metadata count differs")
	}
	reader, err := newStreamRowReader(columns, r.body)
	if err != nil {
		return err
	}
	r.columns = columns
	r.reader = reader
	return nil
}
func decodeStreamRow[R any](response *streamResponse, plan StreamRowPlan[R], dst *R) (ok bool, err error) {
	if response == nil {
		return false, typeValidation("stream response", "nil response")
	}
	pageCtx := response.ctx
	if pageCtx == nil {
		return false, typeValidation("stream context", "response has no page context")
	}
	err = response.reservation.withIO(pageCtx, func() error {
		if response.terminal {
			return nil
		}
		if response.index == response.rowCount && response.reservation.complete {
			response.terminal = true
			return nil
		}
		if dst == nil {
			return typeValidation("destination", "nil stream destination")
		}
		if response.reader == nil || plan == nil {
			return typeValidation("stream plan", "metadata/plan not installed")
		}
		if response.index == response.rowCount {
			if err := response.body.RequireEnd(); err != nil {
				return err
			}
			if err := response.reservation.finishResponse(); err != nil {
				return err
			}
			response.terminal = true
			return nil
		}
		response.reader.index = 0
		plan.Reset(dst)
		if err := plan.DecodeInto(response.reader, dst); err != nil {
			return err
		}
		if err := response.reader.RequireEnd(); err != nil {
			return err
		}
		response.index++
		ok = true
		// Exact final framing is checked before publication of the final row.
		if response.index == response.rowCount {
			if err := response.body.RequireEnd(); err != nil {
				return err
			}
			if err := response.reservation.finishResponse(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		ok = false
		response.reservation.close(false)
	}
	return ok, err
}

// One bounded event worker per connection replaces unbounded goroutine-per-event
// dispatch. Ownership charges transfer with the queued event until consumption.
type connEvent struct {
	value   frame
	release func()
}

func (c *Conn) dispatchEvent(event frame) error { return c.dispatchEventOwned(event, nil) }
func (c *Conn) dispatchEventOwned(event frame, release func()) error {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	reject := func(err error) error {
		if release != nil {
			release()
		}
		return err
	}
	if c.session == nil {
		return reject(nil)
	}
	if c.eventsClosed || c.ctx.Err() != nil {
		return reject(ErrClosed)
	}
	c.eventOnce.Do(func() {
		c.eventQueue = make(chan connEvent, 32)
		c.mu.Lock()
		c.eventDone = make(chan struct{})
		done := c.eventDone
		c.mu.Unlock()
		go func() {
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
				case <-c.ctx.Done():
					return
				case e := <-c.eventQueue:
					c.session.handleEvent(e.value)
					if e.release != nil {
						e.release()
					}
				}
			}
		}()
	})
	select {
	case c.eventQueue <- connEvent{event, release}:
		return nil
	default:
		return reject(&ProtocolError{Stage: "event queue", Err: errors.New("bounded event dispatch overflow")})
	}
}
func (p *typedParser) streamEvent(h frm.FrameHeader) frame {
	p.reserveAllocation(128)
	switch kind := p.str(); kind {
	case "TOPOLOGY_CHANGE", "STATUS_CHANGE":
		change := p.str()
		n := int(p.u8())
		if n != 4 && n != 16 {
			p.check(errors.New("invalid event address width"))
			return nil
		}
		address := net.IP(p.copy(n))
		port := p.i32()
		if port < 0 || port > 65535 {
			p.check(errors.New("invalid event port"))
			return nil
		}
		if kind == "TOPOLOGY_CHANGE" {
			return &frm.TopologyChangeEventFrame{FrameHeader: h, Change: change, Host: address, Port: port}
		}
		return &frm.StatusChangeEventFrame{FrameHeader: h, Change: change, Host: address, Port: port}
	case "CLIENT_ROUTES_CHANGE":
		return &frm.ClientRoutesChanged{FrameHeader: h, ChangeType: p.str(), ConnectionIDs: p.strings(), HostIDs: p.strings()}
	case "SCHEMA_CHANGE":
		change, target, keyspace := p.str(), p.str(), p.str()
		switch target {
		case "KEYSPACE":
			return &frm.SchemaChangeKeyspace{FrameHeader: h, Change: change, Keyspace: keyspace}
		case "TABLE":
			return &frm.SchemaChangeTable{FrameHeader: h, Change: change, Keyspace: keyspace, Object: p.str()}
		case "TYPE":
			return &frm.SchemaChangeType{FrameHeader: h, Change: change, Keyspace: keyspace, Object: p.str()}
		case "FUNCTION":
			return &frm.SchemaChangeFunction{FrameHeader: h, Change: change, Keyspace: keyspace, Name: p.str(), Args: p.strings()}
		case "AGGREGATE":
			return &frm.SchemaChangeAggregate{FrameHeader: h, Change: change, Keyspace: keyspace, Name: p.str(), Args: p.strings()}
		default:
			p.check(fmt.Errorf("gocql: unsupported event target %q", target))
		}
	default:
		p.check(fmt.Errorf("gocql: unsupported event kind %q", kind))
	}
	return nil
}

// Higher-level shutdown joins this worker after canceling its context and
// releasing any event consumer locks. It must not be called by that worker.
func (c *Conn) waitEventDispatch() {
	c.mu.Lock()
	done := c.eventDone
	c.mu.Unlock()
	if done != nil {
		<-done
	}
}

func (p *typedParser) streamExtensions(h frm.FrameHeader) Result {
	info := Result{}
	if h.Flags&frm.FlagTracing != 0 {
		b := p.copy(16)
		if p.err == nil {
			p.reserveAllocation(16)
			var id UUID
			copy(id[:], b)
			info.TraceIDs = []UUID{id}
		}
	}
	if h.Flags&frm.FlagWarning != 0 {
		info.Warnings = p.strings()
	}
	if h.Flags&frm.FlagCustomPayload != 0 {
		count := int(p.u16())
		p.reserveAllocation(int64(count) * 80)
		if p.err == nil {
			info.CustomPayload = make([]PayloadEntry, count)
			for i := range info.CustomPayload {
				e := &info.CustomPayload[i]
				e.Key = p.str()
				b := p.bytes()
				if b != nil {
					e.Value = Nullable[Bytes]{Valid: true, Value: BorrowBytes(b)}
				}
			}
		}
	}
	return info
}

// Drain only control envelopes already present in the validated final unit.
// No read of another socket unit is allowed when releasing a completed result.
func (r *exclusiveConn) drainVerifiedControl() error {
	for r.units != nil && r.units.offset < len(r.units.data) {
		if !r.units.self {
			return &ProtocolError{Stage: "stream trailing unit", Err: wire.ErrTrailingBytes}
		}
		r.units.beginFrame()
		limits := r.conn.typedProtocolLimits()
		h, body, err := frm.ReadStreamEnvelope(r.ctx, r.units, r.conn.version, wire.StreamLimits{MaxBytes: min(limits.MaxFrameBytes, limits.MaxDecompressedBytes), MaxDepth: limits.MaxMetadataDepth + 2, DiscardBytes: r.limits.DiscardBytes})
		if err != nil {
			return err
		}
		cursor := &streamMetadataCursor{scope: body, budget: r.metadataBudget}
		p := &typedParser{cursor: cursor, proto: r.conn.version, maxDepth: limits.MaxMetadataDepth}
		p.streamExtensions(h)
		if h.Op != frm.OpEvent || h.Stream != -1 {
			cursor.release()
			return &ProtocolError{Stage: "stream trailing frame", Err: fmt.Errorf("unexpected opcode %d/stream %d", h.Op, h.Stream)}
		}
		event := p.streamEvent(h)
		if p.err != nil {
			cursor.release()
			return p.err
		}
		if err := body.RequireEnd(); err != nil {
			cursor.release()
			return err
		}
		if err := r.units.endFrame(); err != nil {
			cursor.release()
			return err
		}
		if err := r.conn.dispatchEventOwned(event, cursor.release); err != nil {
			return err
		}
	}
	return nil
}
