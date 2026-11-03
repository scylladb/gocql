// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"sync"

	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type streamPlanEntry[R any] struct {
	validation *codec.StreamValidationCache
	plan       StreamRowPlan[R]
}
type streamPlanCache[R any] struct {
	mu                  sync.Mutex
	entries             [8]streamPlanEntry[R]
	next, retainedBytes int
	opaqueValidation    *codec.StreamValidationCache
	opaquePlan          StreamRowPlan[R]
}

func (c *streamPlanCache[R]) find(columns Columns, limits wire.StreamLimits) (*codec.StreamValidationCache, StreamRowPlan[R]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, entry := range c.entries {
		if entry.validation.Matches(columns, limits) {
			if entry.plan == nil && c.opaqueValidation == entry.validation {
				return entry.validation, c.opaquePlan
			}
			return entry.validation, entry.plan
		}
	}
	return nil, nil
}
func (c *streamPlanCache[R]) put(validation *codec.StreamValidationCache, plan StreamRowPlan[R]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Preserve the original single-current-plan behavior for custom codecs.
	// Arbitrary application plan storage must not multiply across eight shapes.
	c.opaqueValidation, c.opaquePlan = nil, nil
	if validation == nil {
		return
	}
	storedPlan := plan
	if !codec.CacheableStreamRowPlan(plan) {
		storedPlan = nil
		c.opaqueValidation, c.opaquePlan = validation, plan
	}
	for i := range c.entries {
		if c.entries[i].validation == validation {
			c.entries[i].plan = storedPlan
			return
		}
	}
	for {
		slot := c.next % len(c.entries)
		old := c.entries[slot]
		c.retainedBytes -= old.validation.RetainedBytes()
		c.entries[slot] = streamPlanEntry[R]{}
		c.next++
		if c.retainedBytes+validation.RetainedBytes() <= codec.StreamValidationCacheMaxBytes {
			c.entries[slot] = streamPlanEntry[R]{validation: validation, plan: storedPlan}
			c.retainedBytes += validation.RetainedBytes()
			return
		}
	}
}

// StreamQuery reuses owned parameters and an immutable streaming decoder plan.
// Page buffering is the default; Incremental explicitly bounds receive storage.
type StreamQuery[P, R any] struct {
	query   *Query[P, Unit]
	decoder StreamRowDecoder[R]
	options StreamOptions
	plans   streamPlanCache[R]
	state   RowStreamStorage[R]
}

func NewStreamQuery[P, B, R any](prepared *Prepared[P, B], decoder StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	if prepared == nil || prepared.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	if decoder == nil {
		return nil, typeValidation("stream decoder", "nil decoder")
	}
	if _, err := checkedStreamLimits(options.Limits); err != nil {
		return nil, err
	}
	if options.Options.Prefetch.Set && options.Options.Prefetch.Value != 0 {
		return nil, typeValidation("stream prefetch", "must be zero")
	}
	p := &Prepared[P, Unit]{privateStorage: PreparedStorage[P, Unit]{InternalState: prepared.privateStorage.InternalState, InternalKind: prepared.privateStorage.InternalKind, InternalParams: prepared.privateStorage.InternalParams, InternalRows: InternalExecRowsDecoder{}}}
	q, err := NewQuery(p, options.Options)
	if err != nil {
		return nil, err
	}
	options.Options = q.options
	result := &StreamQuery[P, R]{query: q, decoder: decoder, options: options}
	q.state.retireExtra = func() { result.state = RowStreamStorage[R]{} }
	return result, nil
}

// NewUnpreparedStreamQuery uses the same owned input and receive-mode contract
// for a statement executed without server preparation.
func NewUnpreparedStreamQuery[P, B, R any](session *implCluster.Session, statement Statement[P, B], columns Columns, decoder StreamRowDecoder[R], options StreamOptions) (*StreamQuery[P, R], error) {
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	if decoder == nil {
		return nil, typeValidation("stream decoder", "nil decoder")
	}
	if _, err := checkedStreamLimits(options.Limits); err != nil {
		return nil, err
	}
	if options.Options.Prefetch.Set && options.Options.Prefetch.Value != 0 {
		return nil, typeValidation("stream prefetch", "must be zero")
	}
	q, err := NewUnpreparedQuery(session, Statement[P, Unit]{Kind: statement.Kind, CQL: statement.CQL, Params: statement.Params, Rows: InternalExecRowsDecoder{}}, columns, options.Options)
	if err != nil {
		return nil, err
	}
	options.Options = q.options
	result := &StreamQuery[P, R]{query: q, decoder: decoder, options: options}
	q.state.retireExtra = func() { result.state = RowStreamStorage[R]{} }
	return result, nil
}

func (q *StreamQuery[P, R]) Run(ctx context.Context, value P) (*RowStream[R], error) {
	if q == nil {
		return nil, typeValidation("query", "nil query")
	}
	if err := q.query.capture(ctx, value); err != nil {
		return nil, err
	}
	defer q.query.state.owner.InternalRelease()
	return runStreamWithCache(ctx, q.query.state.runtime.Session, &q.query.bound, q.decoder, q.options, &q.plans, &q.state)
}
func (q *StreamQuery[P, R]) Close() {
	if q != nil {
		q.query.Close()
	}
}
func (q *StreamQuery[P, R]) Wait(ctx context.Context) error {
	if q == nil {
		return typeValidation("query", "nil query")
	}
	return q.query.Wait(ctx)
}

func (s *RowStream[R]) loadBufferedPage() error {
	p := &s.privateStorage
	response, err := InternalRuntimeFor(p.InternalSession).executePage(p.InternalCtx, p.InternalRequest, s.privateStorage.state)
	if err != nil {
		return err
	}
	// Release the preceding page before retaining this one. Validation is shared
	// only for identical immutable column storage; changed metadata is rechecked.
	previous := p.bufferedReader
	if p.bufferedPage != nil {
		p.bufferedPage.Owner.Release()
	}
	p.bufferedPage, p.bufferedIndex = response, 0
	limits := wire.StreamLimits{DiscardBytes: p.limits.DiscardBytes}
	configured := InternalRuntimeFor(p.InternalSession).InternalConfig.Limits
	if configured.MaxMetadataDepth > 0 {
		limits.MaxDepth = min(configured.MaxMetadataDepth, 128) + 2
	}
	if configured.MaxFrameBytes > 0 {
		limits.MaxBytes = configured.MaxFrameBytes
	}
	if configured.MaxDecompressedBytes > 0 && (limits.MaxBytes == 0 || configured.MaxDecompressedBytes < limits.MaxBytes) {
		limits.MaxBytes = configured.MaxDecompressedBytes
	}
	body, err := wire.NewStream(p.InternalCtx, wire.NewCursor(response.Rows), response.Rows.Len(), limits)
	if err != nil {
		return err
	}
	cache := p.bufferedPlans
	var validation *codec.StreamValidationCache
	var plan StreamRowPlan[R]
	if cache != nil {
		validation, plan = cache.find(response.Columns, body.Limits())
	}
	var reader *codec.StreamRowReader
	if validation != nil {
		reader, err = codec.NewStreamRowReaderUsingValidation(response.Columns, body, validation)
	} else {
		reader, err = codec.NewStreamRowReaderReusingValidation(response.Columns, body, previous)
	}
	if err != nil {
		return err
	}
	p.bufferedBody, p.bufferedReader = body, reader
	if plan == nil {
		plan, err = p.InternalDecoder.Compile(response.Columns)
		if err != nil {
			return err
		}
		if plan == nil {
			return typeValidation("stream plan", "decoder returned nil plan")
		}
		if cache != nil {
			if validation == nil {
				validation = codec.CaptureStreamValidation(reader)
			}
			cache.put(validation, plan)
		}
	}
	p.plan = plan
	p.mu.Lock()
	p.columns = response.Columns
	prior := p.info.Result
	p.info.Result = response.Result
	p.info.Result.Attempts += prior.Attempts
	p.info.Result.Latency += prior.Latency
	p.info.Result.Rows = p.info.DeliveredRows
	p.info.Pages++
	p.mu.Unlock()
	return nil
}

// InternalActive serializes this with Close and the cancellation callback.
func (s *RowStream[R]) nextBuffered(dst *R) bool {
	p := &s.privateStorage
	fail := func(err error) bool { InternalRowStreamFail(s, err); InternalRowStreamCleanup(s, false); return false }
	for {
		page := p.bufferedPage
		if p.bufferedIndex < page.RowCount {
			codec.BeginStreamRow(p.bufferedReader)
			p.mu.Lock()
			p.info.OutputStarted = true
			p.mu.Unlock()
			p.plan.Reset(dst)
			if err := p.plan.DecodeInto(p.bufferedReader, dst); err != nil {
				return fail(err)
			}
			if err := p.bufferedReader.RequireEnd(); err != nil {
				return fail(err)
			}
			p.bufferedIndex++
			if p.bufferedIndex == page.RowCount {
				if err := p.bufferedBody.RequireEnd(); err != nil {
					return fail(err)
				}
			}
			if err := p.InternalCtx.Err(); err != nil {
				return fail(err)
			}
			p.mu.Lock()
			p.info.DeliveredRows++
			p.info.Result.Rows = p.info.DeliveredRows
			p.mu.Unlock()
			return true
		}
		if err := p.bufferedBody.RequireEnd(); err != nil {
			return fail(err)
		}
		next := page.State
		if !next.More() {
			p.mu.Lock()
			p.terminal = true
			p.mu.Unlock()
			InternalRowStreamCleanup(s, true)
			return false
		}
		if err := p.InternalHistory.InternalAdd(next); err != nil {
			return fail(err)
		}
		s.privateStorage.state = next
		if err := s.loadBufferedPage(); err != nil {
			return fail(err)
		}
	}
}
