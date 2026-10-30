package gocql

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"context"
	"github.com/scylladb/gocql/v2/internal/protocol"

	"errors"
	"fmt"

	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"

	"github.com/scylladb/gocql/v2/internal/wire"
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
	if req.Lease != nil {
		if !req.Lease.Retain() {
			return nil, &QueryError{err: ErrReleased}
		}
		// The writer's synchronous completion is an explicit input lifetime boundary.
		defer req.Lease.Release()
	}
	// Custom payload views can belong to a previously received page. Each
	// accepted attempt holds those leases just like canonical bound values.
	var payloadOwners []*wire.Lease
	defer func() {
		for _, owner := range payloadOwners {
			owner.Release()
		}
	}()
	for _, entry := range req.Payload {
		if entry.Value.Valid && cqltype.BytesLease(entry.Value.Value) != nil {
			owner := cqltype.BytesLease(entry.Value.Value)
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
	if req.Timeout > 0 {
		if call.timer == nil {
			call.timer = time.NewTimer(req.Timeout)
		} else {
			if !call.timer.Stop() {
				select {
				case <-call.timer.C:
				default:
				}
			}
			call.timer.Reset(req.Timeout)
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
		return nil, &QueryError{err: ErrTimeoutNoResponse, potentiallyExecuted: true, timeout: req.Timeout, inFlight: c.streams.InUse()}
	case <-ctx.Done():
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = errOrphanBudget
		}
		return nil, &QueryError{err: ctx.Err(), potentiallyExecuted: true, timeout: req.Timeout, inFlight: c.streams.InUse()}
	case <-c.ctx.Done():
		stopWaiting = true
		return nil, &QueryError{err: ErrConnectionClosed, potentiallyExecuted: true}
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

func (c *Conn) buildTypedRequest(req *attemptRequest, stream int) (wire.Chain, error) {
	return c.nativeProtocol().BuildTypedRequest(req, stream)
}
func (c *Conn) writeTypedParams(e *typedEnvelope, req *attemptRequest, batch bool) {
	c.nativeProtocol().WriteTypedParams(e, req, batch)
}

func (c *Conn) nativeProtocol() protocol.Settings {
	limits := c.typedProtocolLimits()
	p := protocol.Settings{Protocol: c.version, Flags: c.framers.defaults.flags, MetadataID: c.framers.defaults.scyllaUseMetadataID, TabletsRoutingV2: c.framers.defaults.tabletsRoutingV2, MaxFrameBytes: limits.MaxFrameBytes, MaxDecompressedBytes: limits.MaxDecompressedBytes, MaxMetadataDepth: limits.MaxMetadataDepth, FlagLWT: c.framers.defaults.flagLWT, RateLimitingErrorCode: c.framers.defaults.rateLimitingErrorCode, SegmentCompression: c.segCompressor != nil}
	if c.compressor != nil {
		p.Compression = c.compressor.Name()
	}
	return p
}
