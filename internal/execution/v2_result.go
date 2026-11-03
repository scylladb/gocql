// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"fmt"
	"github.com/scylladb/gocql/v2/internal/codec"

	"github.com/scylladb/gocql/v2/internal/wire"
	"sync"
)

type PageLease struct {
	privateStorage PageLeaseStorage
}

func (l *PageLease) Close() {
	if l != nil {
		l.privateStorage.once.Do(func() { l.privateStorage.owner.Release() })
	}
}

type Page[R any] struct {
	privateStorage pageHandleStorage[R]
}

func InternalNewPage[R any](response *responsePage, decoder RowDecoder[R], release func()) (*Page[R], error) {
	return InternalNewPageWithPlans(response, decoder, nil, release)
}
func InternalNewPageWithPlans[R any](response *responsePage, decoder RowDecoder[R], plans *InternalResultPlanCache[R], release func()) (*Page[R], error) {
	return newPageWithStorage(response, decoder, plans, release, nil, false)
}

func newPageWithStorage[R any](response *responsePage, decoder RowDecoder[R], plans *InternalResultPlanCache[R], release func(), storage *queryResultStorage[R], internal bool) (*Page[R], error) {
	if response == nil {
		return nil, typeValidation("response", "nil response")
	}
	if decoder == nil {
		response.Owner.Release()
		return nil, typeValidation("rows", "nil row decoder")
	}
	var plan RowPlan[R]
	var err error
	if plans == nil {
		plan, err = compileRowPlan(decoder, response.Columns)
	} else {
		plan, err = plans.InternalSelectPlan(response, decoder)
	}
	if err != nil {
		response.Owner.Release()
		return nil, err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return nil, &ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	var page *Page[R]
	if storage != nil {
		storage.page = PageStorage[R]{}
		if internal {
			storage.internalPage = Page[R]{}
			page = &storage.internalPage
		} else {
			page = new(Page[R])
			page.privateStorage.endpoint = new(pageTerminal)
		}
		page.privateStorage.PageStorage = &storage.page
		wire.ResetCursor(&storage.pageCursor, response.Rows)
		page.privateStorage.InternalCursor = &storage.pageCursor
	} else {
		holder := new(struct {
			handle Page[R]
			state  PageStorage[R]
		})
		page = &holder.handle
		page.privateStorage.PageStorage = &holder.state
		page.privateStorage.InternalCursor = wire.NewCursor(response.Rows)
	}
	page.privateStorage.InternalResponse = response
	page.privateStorage.InternalPlan = plan
	page.privateStorage.InternalRelease = release
	return page, nil
}
func (p *Page[R]) Next() bool {
	ok, stopped := false, false
	if p != nil && p.privateStorage.PageStorage != nil {
		ok, stopped = p.privateStorage.cursor.acquireAdvance()
	}
	if !ok {
		return false
	}
	defer p.privateStorage.cursor.release()
	if stopped {
		_ = p.closeWithError(ErrClosed)
		return false
	}
	if p == nil {
		return false
	}
	p.privateStorage.mu.Lock()
	defer p.unlockAfterAdvance()
	return p.advance(nil, false)
}
func (p *Page[R]) NextInto(dst *R) bool {
	ok, stopped := false, false
	if p != nil && p.privateStorage.PageStorage != nil {
		ok, stopped = p.privateStorage.cursor.acquireAdvance()
	}
	if !ok {
		if p == nil || p.privateStorage.PageStorage == nil {
			InternalClearDestination(dst)
		}
		return false
	}
	defer p.privateStorage.cursor.release()
	if stopped {
		if !codec.PreservesRowDestination(p.privateStorage.InternalPlan) {
			InternalClearDestination(dst)
		}
		_ = p.closeWithError(ErrClosed)
		return false
	}
	if p == nil {
		InternalClearDestination(dst)
		return false
	}
	p.privateStorage.mu.Lock()
	defer p.unlockAfterAdvance()
	ok = p.advance(dst, true)
	if !ok && !codec.PreservesRowDestination(p.privateStorage.InternalPlan) {
		InternalClearDestination(dst)
	}
	return ok
}
func InternalClearDestination[R any](dst *R) {
	if dst != nil {
		var zero R
		*dst = zero
	}
}
func (p *Page[R]) advance(dst *R, into bool) bool {
	p.privateStorage.valid = false
	p.privateStorage.destination = nil
	if p.privateStorage.closed || p.privateStorage.InternalExhausted || p.privateStorage.err != nil {
		return false
	}
	if into && dst == nil {
		p.privateStorage.err = typeValidation("destination", "nil row destination")
		return false
	}
	if p.privateStorage.session != nil && p.privateStorage.session.Closed() {
		p.privateStorage.err = ErrClosed
		p.closeLocked()
		return false
	}
	if p.privateStorage.InternalCtx != nil && p.privateStorage.InternalCtx.Err() != nil {
		p.privateStorage.err = p.privateStorage.InternalCtx.Err()
		p.closeLocked()
		return false
	}
	if p.privateStorage.InternalIndex >= p.privateStorage.InternalResponse.RowCount {
		p.privateStorage.InternalExhausted = true
		p.privateStorage.reader = RowReader{}
		InternalClearDestination(&p.privateStorage.current)
		if err := p.privateStorage.InternalCursor.RequireEnd(); err != nil {
			p.privateStorage.err = &ProtocolError{Stage: "rows", Err: err}
		}
		return false
	}
	p.privateStorage.reader = codec.RowReaderFromCursor(p.privateStorage.InternalResponse.Columns, p.privateStorage.InternalCursor, p.privateStorage.InternalResponse.Owner)
	var err error
	if into {
		// The plan receives the actual application destination, never a row temporary.
		if !codec.PreservesRowDestination(p.privateStorage.InternalPlan) {
			InternalClearDestination(dst)
		}
		err = p.privateStorage.InternalPlan.DecodeInto(&p.privateStorage.reader, dst)
	} else {
		p.privateStorage.current, err = p.privateStorage.InternalPlan.Decode(&p.privateStorage.reader)
	}
	if err == nil {
		err = finishRowReader(&p.privateStorage.reader)
	}
	if err == nil && p.privateStorage.InternalCtx != nil {
		err = p.privateStorage.InternalCtx.Err()
	}
	if err == nil && p.privateStorage.session != nil && p.privateStorage.session.Closed() {
		err = ErrClosed
	}
	if err != nil {
		p.privateStorage.err = err
		InternalClearDestination(&p.privateStorage.current)
		return false
	}
	p.privateStorage.InternalIndex++
	p.privateStorage.valid = true
	if into {
		p.privateStorage.destination = dst
		InternalClearDestination(&p.privateStorage.current)
	}
	return true
}
func (p *Page[R]) currentRow() R {
	if p.privateStorage.destination != nil {
		return *p.privateStorage.destination
	}
	return p.privateStorage.current
}
func (p *Page[R]) Row() R {
	if !p.acquire() {
		var zero R
		return zero
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		panic(ErrNoRows)
	}
	p.privateStorage.mu.Lock()
	defer p.privateStorage.mu.Unlock()
	if !p.privateStorage.valid || p.privateStorage.closed {
		panic(ErrNoRows)
	}
	return p.currentRow()
}
func (p *Page[R]) Err() error {
	if !p.acquire() {
		_, err, _, _ := p.terminalInfo()
		return err
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		return nil
	}
	p.privateStorage.mu.Lock()
	defer p.privateStorage.mu.Unlock()
	return p.privateStorage.err
}
func (p *Page[R]) Close() error {
	return p.closeWithError(nil)
}

func (p *Page[R]) closeWithError(cause error) error {
	if !p.acquire() {
		_, err, _, _ := p.terminalInfo()
		return err
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		return nil
	}
	if p.privateStorage.cancel != nil {
		p.privateStorage.cancel()
	}
	p.privateStorage.mu.Lock()
	defer p.unlockAfterAdvance()
	if !p.privateStorage.closed && p.privateStorage.err == nil && cause != nil {
		p.privateStorage.err = cause
	}
	p.closeLocked()
	return p.privateStorage.err
}
func (p *Page[R]) closeLocked() {
	if p.privateStorage.closed {
		return
	}
	p.privateStorage.closed = true
	p.snapshotTerminal()
	p.privateStorage.valid = false
	p.privateStorage.destination = nil
	InternalClearDestination(&p.privateStorage.current)
	p.privateStorage.reader = RowReader{}
	p.privateStorage.InternalResponse.Owner.Release()
	if p.privateStorage.InternalRelease != nil {
		p.privateStorage.pendingRelease = p.privateStorage.InternalRelease
		p.privateStorage.InternalRelease = nil
	}
}
func (p *Page[R]) Retain() (*PageLease, error) {
	if !p.acquire() {
		return nil, ErrNoRows
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		return nil, ErrNoRows
	}
	p.privateStorage.mu.Lock()
	defer p.privateStorage.mu.Unlock()
	if !p.privateStorage.valid || p.privateStorage.closed {
		return nil, ErrNoRows
	}
	if !p.privateStorage.InternalResponse.Owner.Retain() {
		return nil, ErrReleased
	}
	return func() *PageLease {
		opaqueStorageValuex := new(PageLease)
		opaqueStorageValuex.privateStorage.owner = p.privateStorage.InternalResponse.Owner
		return opaqueStorageValuex
	}(), nil
}
func (p *Page[R]) Clone() (R, error) {
	if !p.acquire() {
		var zero R
		return zero, ErrNoRows
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		var zero R
		return zero, ErrNoRows
	}
	p.privateStorage.mu.Lock()
	defer p.privateStorage.mu.Unlock()
	if !p.privateStorage.valid || p.privateStorage.closed {
		var zero R
		return zero, ErrNoRows
	}
	return p.privateStorage.InternalPlan.Clone(p.currentRow()), nil
}
func (p *Page[R]) State() PageState {
	if !p.acquire() {
		_, _, _, state := p.terminalInfo()
		return state
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		return completePageState()
	}
	return p.privateStorage.InternalResponse.State
}
func (p *Page[R]) Info() Result {
	if !p.acquire() {
		info, _, _, _ := p.terminalInfo()
		return info
	}
	defer p.privateStorage.cursor.release()
	if p == nil {
		return Result{}
	}
	return cloneResult(p.privateStorage.InternalResponse.Result)
}

type InternalPrefetchJob struct {
	state  PageState
	future *InternalPageFuture
}

type Rows[R any] struct {
	privateStorage rowsHandleStorage[R]
}

func (r *Rows[R]) Next() bool {
	ok, stopped := false, false
	if r != nil && r.privateStorage.RowsStorage != nil {
		ok, stopped = r.privateStorage.cursor.acquireAdvance()
	}
	if !ok {
		return false
	}
	defer r.privateStorage.cursor.release()
	if stopped {
		_ = r.closeWithError(ErrClosed)
		return false
	}
	if r == nil {
		return false
	}
	r.privateStorage.mu.Lock()
	defer r.unlockAfterAdvance()
	return r.advance(nil, false)
}
func (r *Rows[R]) NextInto(dst *R) bool {
	ok, stopped := false, false
	if r != nil && r.privateStorage.RowsStorage != nil {
		ok, stopped = r.privateStorage.cursor.acquireAdvance()
	}
	if !ok {
		if r == nil || r.privateStorage.RowsStorage == nil {
			InternalClearDestination(dst)
		}
		return false
	}
	defer r.privateStorage.cursor.release()
	if stopped {
		if !codec.PreservesRowDestination(r.privateStorage.InternalPage.privateStorage.InternalPlan) {
			InternalClearDestination(dst)
		}
		_ = r.closeWithError(ErrClosed)
		return false
	}
	if r == nil {
		InternalClearDestination(dst)
		return false
	}
	r.privateStorage.mu.Lock()
	defer r.unlockAfterAdvance()
	ok = r.advance(dst, true)
	if !ok && (r.privateStorage.InternalPage == nil || !codec.PreservesRowDestination(r.privateStorage.InternalPage.privateStorage.InternalPlan)) {
		InternalClearDestination(dst)
	}
	return ok
}
func (r *Rows[R]) advance(dst *R, into bool) bool {
	if r == nil || r.privateStorage.closed || r.privateStorage.err != nil {
		return false
	}
	r.privateStorage.valid = false
	if into && dst == nil {
		r.privateStorage.err = typeValidation("destination", "nil row destination")
		r.closeLocked()
		return false
	}
	for {
		if r.privateStorage.rejectColumns && r.privateStorage.InternalPage != nil && r.privateStorage.InternalPage.Columns().Len() != 0 && r.privateStorage.terminalErr == nil {
			r.privateStorage.terminalErr = typeValidation("Exec", "result columns require Run")
		}
		if r.privateStorage.session != nil && r.privateStorage.session.Closed() {
			r.privateStorage.err = ErrClosed
			r.closeLocked()
			return false
		}
		if err := r.privateStorage.InternalCtx.Err(); err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		var advanced bool
		if into {
			advanced = r.privateStorage.InternalPage.advance(dst, true)
			if !advanced && !codec.PreservesRowDestination(r.privateStorage.InternalPage.privateStorage.InternalPlan) {
				InternalClearDestination(dst)
			}
		} else {
			advanced = r.privateStorage.InternalPage.advance(nil, false)
		}
		if advanced {
			if err := r.privateStorage.InternalCtx.Err(); err != nil {
				r.privateStorage.err = err
				r.closeLocked()
				return false
			}
			if r.privateStorage.session != nil && r.privateStorage.session.Closed() {
				r.privateStorage.err = ErrClosed
				r.closeLocked()
				return false
			}
			r.privateStorage.valid = true
			return true
		}
		if err := r.privateStorage.InternalPage.Err(); err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		state := r.privateStorage.InternalPage.State()
		_ = r.privateStorage.InternalPage.Close()
		if !state.More() {
			if r.privateStorage.err == nil {
				r.privateStorage.err = r.privateStorage.terminalErr
			}
			r.closeLocked()
			return false
		}
		if r.privateStorage.nextErr != nil {
			r.privateStorage.err = r.privateStorage.nextErr
			r.closeLocked()
			return false
		}
		if r.privateStorage.future == nil {
			if err := r.privateStorage.InternalHistory.InternalAdd(state); err != nil {
				r.privateStorage.err = err
				r.closeLocked()
				return false
			}
		}
		var response *responsePage
		var err error
		if r.privateStorage.future != nil {
			response, err = r.privateStorage.future.InternalTake(r.privateStorage.InternalCtx)
			r.privateStorage.future = nil
		} else {
			response, err = InternalRuntimeFor(r.privateStorage.session).executePage(r.privateStorage.InternalCtx, r.privateStorage.InternalRequest, state)
		}
		if err != nil {
			mergeResult(&r.privateStorage.info, InternalExecutionErrorResult(err))
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		mergeResult(&r.privateStorage.info, response.Result)
		r.privateStorage.InternalPage, err = newPageWithStorage(response, r.privateStorage.decoder, r.privateStorage.plans, nil, r.privateStorage.results, true)
		if err != nil {
			r.privateStorage.err = err
			r.closeLocked()
			return false
		}
		r.startPrefetch()
	}
}
func (r *Rows[R]) Row() R {
	if !r.acquire() {
		var zero R
		return zero
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		panic(ErrNoRows)
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		panic(ErrNoRows)
	}
	return r.privateStorage.InternalPage.currentRow()
}
func (r *Rows[R]) Err() error {
	if !r.acquire() {
		_, err, _ := r.terminalInfo()
		return err
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		return nil
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	return r.privateStorage.err
}
func (r *Rows[R]) Close() error {
	return r.closeWithError(nil)
}

func (r *Rows[R]) closeWithError(cause error) error {
	if !r.acquire() {
		_, err, _ := r.terminalInfo()
		return err
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		return nil
	}
	if r.privateStorage.cancel != nil {
		r.privateStorage.cancel()
	}
	r.privateStorage.mu.Lock()
	defer r.unlockAfterAdvance()
	if !r.privateStorage.closed && r.privateStorage.err == nil && cause != nil {
		r.privateStorage.err = cause
	}
	r.closeLocked()
	return r.privateStorage.err
}
func (r *Rows[R]) closeLocked() {
	if r.privateStorage.closed {
		return
	}
	r.privateStorage.closed = true
	r.snapshotTerminal()
	r.privateStorage.valid = false
	if r.privateStorage.InternalPage != nil {
		if err := r.privateStorage.InternalPage.Close(); r.privateStorage.err == nil {
			r.privateStorage.err = err
		}
	}
	if r.privateStorage.cancel != nil {
		r.privateStorage.cancel()
	}
	if r.privateStorage.future != nil {
		r.privateStorage.future.InternalClose()
		r.privateStorage.future = nil
	}
	r.privateStorage.InternalHistory.InternalClear()
	if r.privateStorage.InternalRelease != nil {
		r.privateStorage.pendingRelease = r.privateStorage.InternalRelease
		r.privateStorage.InternalRelease = nil
	}
}
func (r *Rows[R]) Retain() (*PageLease, error) {
	if !r.acquire() {
		return nil, ErrNoRows
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		return nil, ErrNoRows
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		return nil, ErrNoRows
	}
	return r.privateStorage.InternalPage.Retain()
}
func (r *Rows[R]) Clone() (R, error) {
	if !r.acquire() {
		var zero R
		return zero, ErrNoRows
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		var z R
		return z, ErrNoRows
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if !r.privateStorage.valid || r.privateStorage.closed {
		var z R
		return z, ErrNoRows
	}
	return r.privateStorage.InternalPage.Clone()
}
func (r *Rows[R]) Info() Result {
	if !r.acquire() {
		info, _, _ := r.terminalInfo()
		return info
	}
	defer r.privateStorage.cursor.release()
	if r == nil {
		return Result{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	return cloneResult(r.privateStorage.info)
}

type InternalResultPlanCache[R any] struct {
	InternalMu   sync.Mutex
	entry        *typedResultMetadata
	columns      Columns
	InternalPlan RowPlan[R]
	revision     uint64
}

func (c *InternalResultPlanCache[R]) InternalSelectPlan(response *responsePage, decoder RowDecoder[R]) (RowPlan[R], error) {
	entry := response.InternalMetadata
	if entry == nil {
		entry = &typedResultMetadata{InternalColumns: response.Columns}
		response.InternalMetadata = entry
	}
	c.InternalMu.Lock()
	if c.InternalPlan != nil && c.entry == entry {
		plan := c.InternalPlan
		c.InternalMu.Unlock()
		return plan, nil
	}
	previous, columns := c.InternalPlan, c.columns
	c.revision++
	revision := c.revision
	c.InternalMu.Unlock()
	var plan RowPlan[R]
	var err error
	if previous != nil && columns.Equal(response.Columns) {
		plan = previous
	} else {
		plan, err = compileRowPlan(decoder, response.Columns)
	}
	if err != nil {
		return nil, err
	}
	if entry.InternalTracker != nil {
		entry.InternalTracker.InternalMu.RLock()
		defer entry.InternalTracker.InternalMu.RUnlock()
	}
	c.InternalMu.Lock()
	if c.revision == revision && entry.InternalEpoch.InternalCurrent() {
		c.entry = entry
		c.columns = response.Columns
		c.InternalPlan = plan
	}
	c.InternalMu.Unlock()
	return plan, nil
}

// pageFuture transfers one prefetched response without making Close wait for I/O.
// A closed future releases a late response before retiring its input admission.
type InternalPageFuture struct {
	mu           sync.Mutex
	InternalDone chan struct{}
	response     *responsePage
	err          error
	closed       bool
}

func (f *InternalPageFuture) InternalComplete(response *responsePage, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		if response != nil {
			response.Owner.Release()
		}
	} else {
		f.response = response
		f.err = err
	}
	close(f.InternalDone)
}
func (f *InternalPageFuture) InternalTake(ctx context.Context) (*responsePage, error) {
	select {
	case <-ctx.Done():
		f.InternalClose()
		return nil, ctx.Err()
	case <-f.InternalDone:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	response := f.response
	f.response = nil
	return response, f.err
}
func (f *InternalPageFuture) InternalClose() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	if f.response != nil {
		f.response.Owner.Release()
		f.response = nil
	}
}
func (r *Rows[R]) startPrefetch() {
	if r.privateStorage.InternalRequest.InternalOptions.InternalProfile.Prefetch == 0 || r.privateStorage.InternalPage == nil || r.privateStorage.closed {
		return
	}
	state := r.privateStorage.InternalPage.State()
	if !state.More() {
		return
	}
	if err := r.privateStorage.InternalHistory.InternalAdd(state); err != nil {
		r.privateStorage.nextErr = err
		return
	}
	future := &InternalPageFuture{InternalDone: make(chan struct{})}
	r.privateStorage.future = future
	if r.privateStorage.prefetchJobs == nil {
		perfAdd(4, 1)
		jobs := make(chan InternalPrefetchJob, 1)
		r.privateStorage.prefetchJobs = jobs
		r.privateStorage.InternalRequest.InternalOwner.InternalRetain()
		go func() {
			defer r.privateStorage.InternalRequest.InternalOwner.InternalRelease()
			for {
				select {
				case <-r.privateStorage.InternalCtx.Done():
					return
				case job := <-jobs:
					response, err := InternalRuntimeFor(r.privateStorage.session).executePage(r.privateStorage.InternalCtx, r.privateStorage.InternalRequest, job.state)
					job.future.InternalComplete(response, err)
				}
			}
		}()
	}
	r.privateStorage.prefetchJobs <- InternalPrefetchJob{state: state, future: future}
}

// Columns returns the actual physical result metadata, including for empty pages.
func (p *Page[R]) Columns() Columns {
	if !p.acquire() {
		_, _, columns, _ := p.terminalInfo()
		return columns
	}
	defer p.privateStorage.cursor.release()
	if p == nil || p.privateStorage.InternalResponse == nil {
		return Columns{}
	}
	return p.privateStorage.InternalResponse.Columns
}

// Columns returns the current page's actual physical result metadata.
func (r *Rows[R]) Columns() Columns {
	if !r.acquire() {
		_, _, columns := r.terminalInfo()
		return columns
	}
	defer r.privateStorage.cursor.release()
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if r == nil {
		return Columns{}
	}
	r.privateStorage.mu.Lock()
	defer r.privateStorage.mu.Unlock()
	if r.privateStorage.InternalPage == nil {
		return Columns{}
	}
	return r.privateStorage.InternalPage.Columns()
}

type InternalExecRowsDecoder struct{}
type InternalExecRowsPlan struct{ count int }

func (InternalExecRowsDecoder) Compile(columns Columns) (RowPlan[Unit], error) {
	return InternalExecRowsPlan{count: columns.Len()}, nil
}
func (InternalExecRowsDecoder) Clone(value Unit) Unit { return value }
func (p InternalExecRowsPlan) Decode(reader *RowReader) (Unit, error) {
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return Unit{}, err
		}
	}
	return Unit{}, nil
}
func (InternalExecRowsPlan) Clone(value Unit) Unit { return value }

func (p InternalExecRowsPlan) DecodeInto(reader *RowReader, dst *Unit) error {
	if dst == nil {
		return typeValidation("destination", "nil row destination")
	}
	*dst = Unit{}
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return err
		}
	}
	return nil
}

// Completion observers may inspect their iterator; invoke them after unlocking.
func (p *Page[R]) unlockAfterAdvance() {
	release := p.privateStorage.pendingRelease
	p.privateStorage.pendingRelease = nil
	p.privateStorage.mu.Unlock()
	if release != nil {
		release()
	}
}
func (r *Rows[R]) unlockAfterAdvance() {
	release := r.privateStorage.pendingRelease
	r.privateStorage.pendingRelease = nil
	r.privateStorage.mu.Unlock()
	if release != nil {
		release()
	}
}
