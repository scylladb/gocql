package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

// executeTyped issues one attempt with the connection's negotiated protocol.
// Canonical values are borrowed unchanged; retries and metadata repair belong to
// the execution layer. A received page owns its storage independently of Conn.
func (c *Conn) executeTyped(ctx context.Context, req *attemptRequest) (*responsePage, error) {
	if err := c.beginShared(ctx); err != nil {
		return nil, &QueryError{err: err}
	}
	defer c.endShared()
	if err := ctx.Err(); err != nil {
		return nil, &QueryError{err: err}
	}
	if req == nil {
		return nil, &QueryError{err: fmt.Errorf("gocql: nil typed attempt")}
	}
	if req.lease != nil {
		if !req.lease.Retain() {
			return nil, &QueryError{err: ErrReleased}
		}
		// The writer's synchronous completion is an explicit input lifetime boundary.
		defer req.lease.Release()
	}
	// Custom payload views can belong to a previously received page. Each
	// accepted attempt holds those leases just like canonical bound values.
	var payloadOwners []*wire.Lease
	defer func() {
		for _, owner := range payloadOwners {
			owner.Release()
		}
	}()
	for _, entry := range req.payload {
		if entry.Value.Valid && entry.Value.Value.lease != nil {
			owner := entry.Value.Value.lease
			if !owner.Retain() {
				return nil, &QueryError{err: ErrReleased}
			}
			payloadOwners = append(payloadOwners, owner)
		}
	}
	stream, ok := c.streams.GetStream()
	if !ok {
		return nil, &QueryError{err: ErrNoStreams}
	}
	call := getCallReq(stream)
	call.typed = true
	if c.streamObserver != nil {
		call.streamObserverContext = c.streamObserver.StreamContext(ctx)
	}
	if err := c.addCall(call); err != nil {
		c.streams.Clear(stream)
		call.finishExec()
		putCallReq(call)
		return nil, &QueryError{err: err}
	}
	var stopWaiting, releaseStream, recycleCall bool
	var closeErr error
	defer func() {
		if closeErr != nil {
			c.closeWithError(closeErr)
		}
	}()
	defer func() {
		if stopWaiting {
			close(call.timeout)
		}
		call.finishExec()
		if releaseStream {
			c.releaseStream(call)
		}
		if recycleCall {
			putCallReq(call)
		}
	}()
	failBeforeWrite := func(err error) (*responsePage, error) {
		stopWaiting = true
		if c.removeCallIfOpen(stream) {
			releaseStream = true
			recycleCall = true
		}
		return nil, &QueryError{err: err}
	}
	packet, err := c.buildTypedRequest(req, stream)
	if err != nil {
		return failBeforeWrite(err)
	}
	writer, ok := c.w.(spanContextWriter)
	if !ok {
		return failBeforeWrite(fmt.Errorf("gocql: connection writer does not support immutable spans"))
	}
	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{Host: c.host})
	}
	n, err := writer.writeSpans(ctx, packet)
	if err != nil {
		stopWaiting = true
		if n == 0 && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			if c.removeCallIfOpen(stream) {
				releaseStream = true
				recycleCall = true
			}
		} else {
			closeErr = err
		}
		return nil, &QueryError{err: err, potentiallyExecuted: n > 0}
	}
	var timeoutCh <-chan time.Time
	if req.timeout > 0 {
		if call.timer == nil {
			call.timer = time.NewTimer(req.timeout)
		} else {
			if !call.timer.Stop() {
				select {
				case <-call.timer.C:
				default:
				}
			}
			call.timer.Reset(req.timeout)
		}
		timeoutCh = call.timer.C
	}
	select {
	case resp := <-call.resp:
		stopWaiting = true
		if resp.removedFromCalls {
			releaseStream = true
			recycleCall = true
		}
		if resp.err != nil {
			return resp.page, &QueryError{err: resp.err, potentiallyExecuted: true}
		}
		return resp.page, nil
	case <-timeoutCh:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = errOrphanBudget
		}
		return nil, &QueryError{err: ErrTimeoutNoResponse, potentiallyExecuted: true, timeout: req.timeout, inFlight: c.streams.InUse()}
	case <-ctx.Done():
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = errOrphanBudget
		}
		return nil, &QueryError{err: ctx.Err(), potentiallyExecuted: true, timeout: req.timeout, inFlight: c.streams.InUse()}
	case <-c.ctx.Done():
		stopWaiting = true
		return nil, &QueryError{err: ErrConnectionClosed, potentiallyExecuted: true}
	}
}

// typedEnvelope owns only newly encoded protocol fields. Borrowed values never
// enter the slab, and Freeze cannot relocate any published bytes.
type typedEnvelope struct {
	b   *wire.Builder
	err error
}

func newTypedEnvelope() *typedEnvelope { return &typedEnvelope{b: wire.NewBuilder(1024)} }
func (e *typedEnvelope) check(err error) {
	if e.err == nil {
		e.err = err
	}
}
func (e *typedEnvelope) u8(v byte)    { e.check(e.b.Uint8(v)) }
func (e *typedEnvelope) u16(v uint16) { e.check(e.b.Uint16(v)) }
func (e *typedEnvelope) u32(v uint32) { e.check(e.b.Uint32(v)) }
func (e *typedEnvelope) u64(v uint64) { e.check(e.b.Uint64(v)) }
func (e *typedEnvelope) chain(c wire.Chain) {
	e.check(c.Each(func(s wire.Span) error { return e.b.Borrow(s) }))
}
func (e *typedEnvelope) str(s string, long bool) {
	if (!long && len(s) > math.MaxUint16) || int64(len(s)) > math.MaxInt32 {
		e.check(fmt.Errorf("gocql: oversized protocol string"))
		return
	}
	if long {
		e.u32(uint32(len(s)))
	} else {
		e.u16(uint16(len(s)))
	}
	e.check(e.b.Borrow(wire.BorrowString(s)))
}
func (e *typedEnvelope) bytes(p []byte, short bool) {
	if short {
		if len(p) > math.MaxUint16 {
			e.check(fmt.Errorf("gocql: oversized short bytes"))
			return
		}
		e.u16(uint16(len(p)))
	} else {
		if int64(len(p)) > math.MaxInt32 {
			e.check(ErrFrameTooBig)
			return
		}
		e.u32(uint32(len(p)))
	}
	e.check(e.b.Borrow(wire.Borrow(p)))
}
func (e *typedEnvelope) freeze() (wire.Chain, error) {
	if e.err != nil {
		return wire.Chain{}, e.err
	}
	return e.b.Freeze()
}

func validateTypedValues(c wire.Chain, count int, proto byte) error {
	if count < 0 || count > math.MaxUint16 {
		return fmt.Errorf("gocql: invalid bound value count %d", count)
	}
	cursor := wire.NewCursor(c)
	for i := 0; i < count; i++ {
		n, err := cursor.Uint32()
		if err != nil {
			return err
		}
		switch int32(n) {
		case -1:
		case -2:
			if proto < 4 {
				return fmt.Errorf("gocql: unset requires protocol v4")
			}
		default:
			if int32(n) < 0 {
				return fmt.Errorf("gocql: invalid bound length %d", int32(n))
			}
			if err := cursor.Skip(int64(n)); err != nil {
				return err
			}
		}
	}
	return cursor.RequireEnd()
}

func (c *Conn) buildTypedRequest(req *attemptRequest, stream int) (wire.Chain, error) {
	proto := c.version
	if proto < 3 || proto > 5 {
		return wire.Chain{}, fmt.Errorf("gocql: unsupported typed protocol %d", proto)
	}
	if (req.keyspace != "" || req.nowInSeconds != nil) && proto < 5 {
		return wire.Chain{}, fmt.Errorf("gocql: keyspace and now_in_seconds overrides require protocol v5")
	}
	if req.pageSize < 0 || int64(req.pageSize) > math.MaxInt32 {
		return wire.Chain{}, fmt.Errorf("gocql: invalid page size")
	}
	if err := validateTypedValues(req.values, req.valueCount, proto); err != nil {
		return wire.Chain{}, err
	}
	e := newTypedEnvelope()
	flags := c.framers.defaults.flags & frm.FlagBetaProtocol
	if req.trace {
		flags |= frm.FlagTracing
	}
	if len(req.payload) > 0 {
		if proto < 4 {
			return wire.Chain{}, fmt.Errorf("gocql: custom payload requires protocol v4")
		}
		if len(req.payload) > math.MaxUint16 {
			return wire.Chain{}, fmt.Errorf("gocql: too many payload entries")
		}
		flags |= frm.FlagCustomPayload
		e.u16(uint16(len(req.payload)))
		for _, entry := range req.payload {
			e.str(entry.Key, false)
			if !entry.Value.Valid {
				e.u32(math.MaxUint32)
				continue
			}
			value := entry.Value.Value
			if err := value.checkAlive(); err != nil {
				return wire.Chain{}, err
			}
			if value.Len() > math.MaxInt32 {
				return wire.Chain{}, ErrFrameTooBig
			}
			e.u32(uint32(value.Len()))
			e.chain(value.chain)
		}
	}
	op := frm.OpQuery
	switch {
	case req.prepare:
		op = frm.OpPrepare
		e.str(req.statement, true)
		if proto >= 5 {
			var f uint32
			if req.keyspace != "" {
				f = frm.FlagWithPreparedKeyspace
			}
			e.u32(f)
		}
		if req.keyspace != "" {
			e.str(req.keyspace, false)
		}
	case req.batch != nil:
		op = frm.OpBatch
		if len(req.batch.items) > math.MaxUint16 {
			return wire.Chain{}, fmt.Errorf("gocql: too many batch entries")
		}
		e.u8(byte(req.batch.kind))
		e.u16(uint16(len(req.batch.items)))
		for _, item := range req.batch.items {
			if err := validateTypedValues(item.values, item.valueCount, proto); err != nil {
				return wire.Chain{}, err
			}
			if len(item.preparedID) > 0 {
				e.u8(1)
				e.bytes(item.preparedID, true)
			} else {
				e.u8(0)
				e.str(item.statement, true)
			}
			e.u16(uint16(item.valueCount))
			e.chain(item.values)
		}
		c.writeTypedParams(e, req, true)
	default:
		if len(req.preparedID) > 0 {
			op = frm.OpExecute
			e.bytes(req.preparedID, true)
			if proto >= 5 || c.framers.defaults.scyllaUseMetadataID {
				e.bytes(req.resultMetadataID, true)
			}
		} else {
			e.str(req.statement, true)
		}
		c.writeTypedParams(e, req, false)
		if op == frm.OpExecute && c.framers.defaults.tabletsRoutingV2 {
			e.u8(req.tabletVersionBlock)
		}
	}
	body, err := e.freeze()
	if err != nil {
		return wire.Chain{}, err
	}
	if body.Len() > c.typedProtocolLimits().MaxFrameBytes {
		return wire.Chain{}, ErrFrameTooBig
	}
	if proto < 5 && c.compressor != nil {
		var out []byte
		switch c.compressor.Name() {
		case "lz4":
			out = make([]byte, 4)
			binary.BigEndian.PutUint32(out, uint32(body.Len()))
			out, err = wirecompress.AppendLZ4(out, body)
		case "snappy":
			out, err = wirecompress.AppendSnappy(nil, body)
		default:
			err = fmt.Errorf("gocql: span encoder unavailable for compressor %q", c.compressor.Name())
		}
		if err != nil {
			return wire.Chain{}, err
		}
		body, _ = wire.NewChain(wire.Borrow(out))
		flags |= frm.FlagCompress
	}
	if body.Len() > c.typedProtocolLimits().MaxFrameBytes {
		return wire.Chain{}, ErrFrameTooBig
	}
	header := newTypedEnvelope()
	header.u8(proto)
	header.u8(flags)
	header.u16(uint16(stream))
	header.u8(byte(op))
	header.u32(uint32(body.Len()))
	header.chain(body)
	frame, err := header.freeze()
	if err != nil || proto < 5 {
		return frame, err
	}
	if c.segCompressor != nil && c.compressor.Name() != "lz4" {
		return wire.Chain{}, fmt.Errorf("gocql: v5 spans require LZ4 segment compression")
	}
	var spans []wire.Span
	selfContained := frame.Len() <= segment.MaxPayloadSize
	for offset := int64(0); offset < frame.Len(); {
		size := min(int64(segment.MaxPayloadSize), frame.Len()-offset)
		part, _ := frame.Slice(offset, size)
		encoded, err := segment.EncodeSpans(part, selfContained, c.segCompressor != nil)
		if err != nil {
			return wire.Chain{}, err
		}
		_ = encoded.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
		offset += size
	}
	return wire.NewChain(spans...)
}
func (c *Conn) writeTypedParams(e *typedEnvelope, r *attemptRequest, batch bool) {
	e.u16(uint16(r.consistency))
	var flags uint32
	if !batch {
		if r.valueCount > 0 {
			flags |= frm.FlagValues
		}
		if r.skipMetadata {
			flags |= frm.FlagSkipMetaData
		}
		if r.pageSize > 0 {
			flags |= frm.FlagPageSize
		}
		if r.pagingState != nil {
			flags |= frm.FlagWithPagingState
		}
	}
	if r.serialConsistency != 0 {
		flags |= frm.FlagWithSerialConsistency
	}
	if r.defaultTimestamp {
		flags |= frm.FlagDefaultTimestamp
	}
	if r.keyspace != "" {
		flags |= frm.FlagWithKeyspace
	}
	if r.nowInSeconds != nil {
		flags |= frm.FlagWithNowInSeconds
	}
	if c.version >= 5 {
		e.u32(flags)
	} else {
		e.u8(byte(flags))
	}
	if !batch {
		if r.valueCount > 0 {
			e.u16(uint16(r.valueCount))
			e.chain(r.values)
		}
		if r.pageSize > 0 {
			e.u32(uint32(r.pageSize))
		}
		if r.pagingState != nil {
			e.bytes(r.pagingState, false)
		}
	}
	if r.serialConsistency != 0 {
		e.u16(uint16(r.serialConsistency))
	}
	if r.defaultTimestamp {
		e.u64(uint64(r.timestamp))
	}
	if r.keyspace != "" {
		e.str(r.keyspace, false)
	}
	if r.nowInSeconds != nil {
		e.u32(uint32(*r.nowInSeconds))
	}
}

func (c *Conn) typedProtocolLimits() ProtocolLimits {
	limits := ProtocolLimits{MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
	if c.cfg != nil {
		configured := c.cfg.TypedLimits
		if configured.MaxFrameBytes > 0 {
			limits.MaxFrameBytes = min(configured.MaxFrameBytes, int64(frm.MaxFrameSize))
		}
		if configured.MaxDecompressedBytes > 0 {
			limits.MaxDecompressedBytes = min(configured.MaxDecompressedBytes, int64(frm.MaxFrameSize))
		}
		if configured.MaxMetadataDepth > 0 {
			limits.MaxMetadataDepth = min(configured.MaxMetadataDepth, 128)
		}
	}
	return limits
}
