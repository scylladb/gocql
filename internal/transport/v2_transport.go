// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"

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
func (c *Conn) InternalExecuteTyped(ctx context.Context, req *attemptRequest) (*InternalResponsePage, error) {
	if err := c.beginShared(ctx); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	defer c.endShared()
	if err := ctx.Err(); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	if req == nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: fmt.Errorf("gocql: nil typed attempt")})
	}
	if req.Lease != nil {
		if !req.Lease.Retain() {
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrReleased})
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
				return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrReleased})
			}
			payloadOwners = append(payloadOwners, owner)
		}
	}
	stream, ok := c.InternalStreams.GetStream()
	if !ok {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrNoStreams})
	}
	call := InternalGetCallReq(stream)
	call.InternalTyped = true
	if c.InternalStreamObserver != nil {
		call.streamObserverContext = c.InternalStreamObserver.StreamContext(ctx)
	}
	if err := c.addCall(call); err != nil {
		c.InternalStreams.Clear(stream)
		call.finishExec()
		InternalPutCallReq(call)
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	var stopWaiting, releaseStream, recycleCall bool
	var closeErr error
	defer func() {
		if closeErr != nil {
			c.InternalCloseWithError(closeErr)
		}
	}()
	defer func() {
		if stopWaiting {
			close(call.InternalTimeout)
		}
		call.finishExec()
		if releaseStream {
			c.releaseStream(call)
		}
		if recycleCall {
			InternalPutCallReq(call)
		}
	}()
	failBeforeWrite := func(err error) (*InternalResponsePage, error) {
		stopWaiting = true
		if c.InternalRemoveCallIfOpen(stream) {
			releaseStream = true
			recycleCall = true
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	frameRequest := *req
	framing := req.Framing
	if framing != nil && (c.InternalVersion != 4 || c.InternalCompressor != nil || !framing.TryAcquire()) {
		framing = nil
	}
	frameRequest.Framing = framing
	framingTransferred := false
	defer func() {
		if framing != nil && !framingTransferred {
			framing.Release()
		}
	}()
	packet, err := c.InternalBuildTypedRequest(&frameRequest, stream)
	if err != nil {
		return failBeforeWrite(err)
	}
	writer, ok := c.InternalW.(spanContextWriter)
	if !ok {
		return failBeforeWrite(fmt.Errorf("gocql: connection writer does not support immutable spans"))
	}
	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{Host: c.InternalHost})
	}
	var n int64
	if c.InternalCoalesceWait == 0 {
		// Hold both input and shared-connection admission until the persistent
		// writer finishes, even if cancellation returns to the calling goroutine.
		if err := c.beginShared(ctx); err != nil {
			return failBeforeWrite(err)
		}
		if req.Lease != nil && !req.Lease.Retain() {
			c.endShared()
			return failBeforeWrite(ErrReleased)
		}
		ownedPayloads := payloadOwners
		payloadOwners = nil
		inputLease := req.Lease
		ownedFraming := framing
		lease := wire.NewLease(packet, func() {
			if ownedFraming != nil {
				ownedFraming.Release()
			}
			if inputLease != nil {
				inputLease.Release()
			}
			for _, owner := range ownedPayloads {
				owner.Release()
			}
			c.endShared()
		})
		framingTransferred = true
		n, err = c.submitTyped(ctx, packet, lease)
	} else {
		n, err = writer.InternalWriteSpans(ctx, packet)
	}
	if err != nil {
		stopWaiting = true
		var pending *pendingWriteError
		if errors.As(err, &pending) {
			if c.markOrphan(call) {
				closeErr = InternalErrOrphanBudget
			}
		} else if n == 0 && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			if c.InternalRemoveCallIfOpen(stream) {
				releaseStream = true
				recycleCall = true
			}
		} else {
			closeErr = err
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: n > 0})
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
	case resp := <-call.InternalResp:
		stopWaiting = true
		if resp.removedFromCalls {
			releaseStream = true
			recycleCall = true
		}
		if resp.InternalErr != nil {
			return resp.InternalPage, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: resp.InternalErr, InternalPotentiallyExecuted: true})
		}
		return resp.InternalPage, nil
	case <-timeoutCh:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-ctx.Done():
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err(), InternalPotentiallyExecuted: true, InternalTimeout: req.Timeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-c.InternalCtx.Done():
		stopWaiting = true
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrConnectionClosed, InternalPotentiallyExecuted: true})
	}
}

func (c *Conn) typedProtocolLimits() ProtocolLimits {
	limits := ProtocolLimits{MaxFrameBytes: frm.MaxFrameSize, MaxDecompressedBytes: frm.MaxFrameSize, MaxMetadataDepth: 64}
	if c.InternalCfg != nil {
		configured := c.InternalCfg.TypedLimits
		if configured.MaxFrameBytes > 0 {
			limits.MaxFrameBytes = configured.MaxFrameBytes
		}
		if configured.MaxDecompressedBytes > 0 {
			limits.MaxDecompressedBytes = configured.MaxDecompressedBytes
		}
		if configured.MaxMetadataDepth > 0 {
			limits.MaxMetadataDepth = min(configured.MaxMetadataDepth, 128)
		}
	}
	return limits
}

func (c *Conn) InternalBuildTypedRequest(req *attemptRequest, stream int) (wire.Chain, error) {
	return c.nativeProtocol().BuildTypedRequest(req, stream)
}
func (c *Conn) writeTypedParams(e *typedEnvelope, req *attemptRequest, batch bool) {
	c.nativeProtocol().WriteTypedParams(e, req, batch)
}

func (c *Conn) nativeProtocol() protocol.Settings {
	limits := c.typedProtocolLimits()
	p := protocol.Settings{MetadataReuse: &c.typedMetadataReuse, Protocol: c.InternalVersion, Flags: c.InternalFramers.InternalDefaults.InternalFlags, MetadataID: c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID, TabletsRoutingV2: c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2, MaxFrameBytes: limits.MaxFrameBytes, MaxDecompressedBytes: limits.MaxDecompressedBytes, MaxMetadataDepth: limits.MaxMetadataDepth, FlagLWT: c.InternalFramers.InternalDefaults.InternalFlagLWT, RateLimitingErrorCode: c.InternalFramers.InternalDefaults.rateLimitingErrorCode, SegmentCompression: c.InternalSegCompressor != nil}
	if c.InternalCompressor != nil {
		p.Compression = c.InternalCompressor.Name()
	}
	return p
}
