package gocql

import (
	model "github.com/scylladb/gocql/v2/internal/model"

	"bytes"
	"context"
	"fmt"
	"github.com/scylladb/gocql/v2/internal/codec"
	"hash/maphash"
	"sync"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type PageLease struct {
	once  sync.Once
	owner *wire.Lease
}

func (l *PageLease) Close() {
	if l != nil {
		l.once.Do(func() { l.owner.Release() })
	}
}

type Page[R any] struct {
	pendingRelease func()
	mu             sync.Mutex
	destination    *R
	ctx            context.Context
	cancel         context.CancelFunc
	session        *Session
	response       *responsePage
	plan           RowPlan[R]
	cursor         *wire.Cursor
	reader         RowReader
	index          int32
	current        R
	valid          bool
	err            error
	closed         bool
	exhausted      bool
	release        func()
}

func newPage[R any](response *responsePage, decoder RowDecoder[R], release func()) (*Page[R], error) {
	return newPageWithPlans(response, decoder, nil, release)
}
func newPageWithPlans[R any](response *responsePage, decoder RowDecoder[R], plans *resultPlanCache[R], release func()) (*Page[R], error) {
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
		plan, err = plans.selectPlan(response, decoder)
	}
	if err != nil {
		response.Owner.Release()
		return nil, err
	}
	if response.RowCount < 0 {
		response.Owner.Release()
		return nil, &ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	return &Page[R]{response: response, plan: plan, cursor: wire.NewCursor(response.Rows), release: release}, nil
}
func (p *Page[R]) Next() bool {
	if p == nil {
		return false
	}
	p.mu.Lock()
	defer p.unlockAfterAdvance()
	return p.advance(nil, false)
}
func (p *Page[R]) NextInto(dst *R) bool {
	if p == nil {
		clearDestination(dst)
		return false
	}
	p.mu.Lock()
	defer p.unlockAfterAdvance()
	ok := p.advance(dst, true)
	if !ok {
		clearDestination(dst)
	}
	return ok
}
func clearDestination[R any](dst *R) {
	if dst != nil {
		var zero R
		*dst = zero
	}
}
func (p *Page[R]) advance(dst *R, into bool) bool {
	p.valid = false
	p.destination = nil
	if p.closed || p.exhausted || p.err != nil {
		return false
	}
	if into && dst == nil {
		p.err = typeValidation("destination", "nil row destination")
		return false
	}
	if p.session != nil && p.session.Closed() {
		p.err = ErrClosed
		p.closeLocked()
		return false
	}
	if p.ctx != nil && p.ctx.Err() != nil {
		p.err = p.ctx.Err()
		p.closeLocked()
		return false
	}
	if p.index >= p.response.RowCount {
		p.exhausted = true
		p.reader = RowReader{}
		clearDestination(&p.current)
		if err := p.cursor.RequireEnd(); err != nil {
			p.err = &ProtocolError{Stage: "rows", Err: err}
		}
		return false
	}
	p.reader = codec.RowReaderFromCursor(p.response.Columns, p.cursor, p.response.Owner)
	var err error
	if into {
		// The plan receives the actual application destination, never a row temporary.
		clearDestination(dst)
		err = p.plan.DecodeInto(&p.reader, dst)
	} else {
		p.current, err = p.plan.Decode(&p.reader)
	}
	if err == nil {
		err = finishRowReader(&p.reader)
	}
	if err == nil && p.ctx != nil {
		err = p.ctx.Err()
	}
	if err == nil && p.session != nil && p.session.Closed() {
		err = ErrClosed
	}
	if err != nil {
		p.err = err
		clearDestination(&p.current)
		return false
	}
	p.index++
	p.valid = true
	if into {
		p.destination = dst
		clearDestination(&p.current)
	}
	return true
}
func (p *Page[R]) currentRow() R {
	if p.destination != nil {
		return *p.destination
	}
	return p.current
}
func (p *Page[R]) Row() R {
	if p == nil {
		panic(ErrNoRows)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid || p.closed {
		panic(ErrNoRows)
	}
	return p.currentRow()
}
func (p *Page[R]) Err() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}
func (p *Page[R]) Close() error {
	if p == nil {
		return nil
	}
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Lock()
	defer p.unlockAfterAdvance()
	p.closeLocked()
	return p.err
}
func (p *Page[R]) closeLocked() {
	if p.closed {
		return
	}
	p.closed = true
	p.valid = false
	p.destination = nil
	clearDestination(&p.current)
	p.reader = RowReader{}
	p.response.Owner.Release()
	if p.release != nil {
		p.pendingRelease = p.release
		p.release = nil
	}
}
func (p *Page[R]) Retain() (*PageLease, error) {
	if p == nil {
		return nil, ErrNoRows
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid || p.closed {
		return nil, ErrNoRows
	}
	if !p.response.Owner.Retain() {
		return nil, ErrReleased
	}
	return &PageLease{owner: p.response.Owner}, nil
}
func (p *Page[R]) Clone() (R, error) {
	if p == nil {
		var zero R
		return zero, ErrNoRows
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.valid || p.closed {
		var zero R
		return zero, ErrNoRows
	}
	return p.plan.Clone(p.currentRow()), nil
}
func (p *Page[R]) State() PageState {
	if p == nil {
		return completePageState()
	}
	return p.response.State
}
func (p *Page[R]) Info() Result {
	if p == nil {
		return Result{}
	}
	return cloneResult(p.response.Result)
}

type prefetchJob struct {
	state  PageState
	future *pageFuture
}

type Rows[R any] struct {
	prefetchJobs   chan prefetchJob
	pendingRelease func()
	mu             sync.Mutex
	plans          *resultPlanCache[R]
	history        pagingHistory
	nextErr        error
	rejectColumns  bool
	terminalErr    error
	future         *pageFuture

	ctx     context.Context
	cancel  context.CancelFunc
	session *Session
	request *executionRequest
	decoder RowDecoder[R]
	page    *Page[R]
	info    Result
	err     error
	closed  bool
	valid   bool
	release func()
}

func (r *Rows[R]) Next() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.unlockAfterAdvance()
	return r.advance(nil, false)
}
func (r *Rows[R]) NextInto(dst *R) bool {
	if r == nil {
		clearDestination(dst)
		return false
	}
	r.mu.Lock()
	defer r.unlockAfterAdvance()
	ok := r.advance(dst, true)
	if !ok {
		clearDestination(dst)
	}
	return ok
}
func (r *Rows[R]) advance(dst *R, into bool) bool {
	if r == nil || r.closed || r.err != nil {
		return false
	}
	r.valid = false
	if into && dst == nil {
		r.err = typeValidation("destination", "nil row destination")
		r.closeLocked()
		return false
	}
	for {
		if r.rejectColumns && r.page != nil && r.page.Columns().Len() != 0 && r.terminalErr == nil {
			r.terminalErr = typeValidation("Exec", "result columns require Run")
		}
		if r.session != nil && r.session.Closed() {
			r.err = ErrClosed
			r.closeLocked()
			return false
		}
		if err := r.ctx.Err(); err != nil {
			r.err = err
			r.closeLocked()
			return false
		}
		var advanced bool
		if into {
			advanced = r.page.NextInto(dst)
		} else {
			advanced = r.page.Next()
		}
		if advanced {
			if err := r.ctx.Err(); err != nil {
				r.err = err
				r.closeLocked()
				return false
			}
			if r.session != nil && r.session.Closed() {
				r.err = ErrClosed
				r.closeLocked()
				return false
			}
			r.valid = true
			return true
		}
		if err := r.page.Err(); err != nil {
			r.err = err
			r.closeLocked()
			return false
		}
		state := r.page.State()
		_ = r.page.Close()
		if !state.More() {
			if r.err == nil {
				r.err = r.terminalErr
			}
			r.closeLocked()
			return false
		}
		if r.nextErr != nil {
			r.err = r.nextErr
			r.closeLocked()
			return false
		}
		if r.future == nil {
			if err := r.history.add(state); err != nil {
				r.err = err
				r.closeLocked()
				return false
			}
		}
		var response *responsePage
		var err error
		if r.future != nil {
			response, err = r.future.take(r.ctx)
			r.future = nil
		} else {
			response, err = r.session.executePage(r.ctx, r.request, state)
		}
		if err != nil {
			mergeResult(&r.info, executionErrorResult(err))
			r.err = err
			r.closeLocked()
			return false
		}
		mergeResult(&r.info, response.Result)
		r.page, err = newPageWithPlans(response, r.decoder, r.plans, nil)
		if err != nil {
			r.err = err
			r.closeLocked()
			return false
		}
		r.startPrefetch()
	}
}
func (r *Rows[R]) Row() R {
	if r == nil {
		panic(ErrNoRows)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.valid || r.closed {
		panic(ErrNoRows)
	}
	return r.page.Row()
}
func (r *Rows[R]) Err() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}
func (r *Rows[R]) Close() error {
	if r == nil {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Lock()
	defer r.unlockAfterAdvance()
	r.closeLocked()
	return r.err
}
func (r *Rows[R]) closeLocked() {
	if r.closed {
		return
	}
	r.closed = true
	r.valid = false
	if r.page != nil {
		if err := r.page.Close(); r.err == nil {
			r.err = err
		}
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.future != nil {
		r.future.close()
		r.future = nil
	}
	r.history.clear()
	if r.release != nil {
		r.pendingRelease = r.release
		r.release = nil
	}
}
func (r *Rows[R]) Retain() (*PageLease, error) {
	if r == nil {
		return nil, ErrNoRows
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.valid || r.closed {
		return nil, ErrNoRows
	}
	return r.page.Retain()
}
func (r *Rows[R]) Clone() (R, error) {
	if r == nil {
		var z R
		return z, ErrNoRows
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.valid || r.closed {
		var z R
		return z, ErrNoRows
	}
	return r.page.Clone()
}
func (r *Rows[R]) Info() Result {
	if r == nil {
		return Result{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneResult(r.info)
}

// pagingHistory owns references to the same immutable tokens sent on the wire.
// Hash collisions compare full bytes; count and payload budgets are independent.
// This nongeneric state machine is also used by the incremental streaming pager.
type pagingHistoryEntry struct {
	state PageState
	next  *pagingHistoryEntry
}
type pagingHistory struct {
	seed     maphash.Seed
	entries  map[uint64]pagingHistoryEntry
	count    int
	bytes    int64
	maxCount int
	maxBytes int64
}

func (h *pagingHistory) add(state PageState) error {
	if !state.More() {
		return nil
	}
	if h.entries == nil {
		h.seed = maphash.MakeSeed()
		h.entries = make(map[uint64]pagingHistoryEntry)
	}
	hash := maphash.Bytes(h.seed, model.PageToken(state))
	entry, exists := h.entries[hash]
	if exists {
		if bytes.Equal(model.PageToken(entry.state), model.PageToken(state)) {
			return ErrPagingNoProgress
		}
		for p := entry.next; p != nil; p = p.next {
			if bytes.Equal(model.PageToken(p.state), model.PageToken(state)) {
				return ErrPagingNoProgress
			}
		}
	}
	maxCount, maxBytes := h.maxCount, h.maxBytes
	if maxCount == 0 {
		maxCount = 65536
	}
	if maxBytes == 0 {
		maxBytes = 16 << 20
	}
	if h.count >= maxCount || int64(len(model.PageToken(state))) > maxBytes-h.bytes {
		return ErrPagingStateLimit
	}
	candidate := pagingHistoryEntry{state: state}
	if exists {
		previous := entry
		candidate.next = &previous
	}
	h.entries[hash] = candidate
	h.count++
	h.bytes += int64(len(model.PageToken(state)))
	return nil
}
func (h *pagingHistory) clear() { h.entries = nil; h.count = 0; h.bytes = 0 }

// resultPlanCache is scoped to one application decoder/handle, not CQL. Only
// its current shape is retained; live pages own previous selected plans.
type typedResultMetadata struct {
	tracker *preparedSchemaTracker
	columns Columns
	epoch   preparedSchemaStamp
}
type resultPlanCache[R any] struct {
	mu       sync.Mutex
	entry    *typedResultMetadata
	columns  Columns
	plan     RowPlan[R]
	revision uint64
}

func (c *resultPlanCache[R]) selectPlan(response *responsePage, decoder RowDecoder[R]) (RowPlan[R], error) {
	entry := response.metadata
	if entry == nil {
		entry = &typedResultMetadata{columns: response.Columns}
		response.metadata = entry
	}
	c.mu.Lock()
	if c.plan != nil && c.entry == entry {
		plan := c.plan
		c.mu.Unlock()
		return plan, nil
	}
	previous, columns := c.plan, c.columns
	c.revision++
	revision := c.revision
	c.mu.Unlock()
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
	if entry.tracker != nil {
		entry.tracker.mu.RLock()
		defer entry.tracker.mu.RUnlock()
	}
	c.mu.Lock()
	if c.revision == revision && entry.epoch.current() {
		c.entry = entry
		c.columns = response.Columns
		c.plan = plan
	}
	c.mu.Unlock()
	return plan, nil
}

// pageFuture transfers one prefetched response without making Close wait for I/O.
// A closed future releases a late response before retiring its input admission.
type pageFuture struct {
	mu       sync.Mutex
	done     chan struct{}
	response *responsePage
	err      error
	closed   bool
}

func (f *pageFuture) complete(response *responsePage, err error) {
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
	close(f.done)
}
func (f *pageFuture) take(ctx context.Context) (*responsePage, error) {
	select {
	case <-ctx.Done():
		f.close()
		return nil, ctx.Err()
	case <-f.done:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	response := f.response
	f.response = nil
	return response, f.err
}
func (f *pageFuture) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	if f.response != nil {
		f.response.Owner.Release()
		f.response = nil
	}
}
func (r *Rows[R]) startPrefetch() {
	if r.request.options.profile.Prefetch == 0 || r.page == nil || r.closed {
		return
	}
	state := r.page.State()
	if !state.More() {
		return
	}
	if err := r.history.add(state); err != nil {
		r.nextErr = err
		return
	}
	future := &pageFuture{done: make(chan struct{})}
	r.future = future
	if r.prefetchJobs == nil {
		jobs := make(chan prefetchJob, 1)
		r.prefetchJobs = jobs
		r.request.owner.retain()
		go func() {
			defer r.request.owner.release()
			for {
				select {
				case <-r.ctx.Done():
					return
				case job := <-jobs:
					response, err := r.session.executePage(r.ctx, r.request, job.state)
					job.future.complete(response, err)
				}
			}
		}()
	}
	r.prefetchJobs <- prefetchJob{state: state, future: future}
}

// Columns returns the actual physical result metadata, including for empty pages.
func (p *Page[R]) Columns() Columns {
	if p == nil || p.response == nil {
		return Columns{}
	}
	return p.response.Columns
}

// Columns returns the current page's actual physical result metadata.
func (r *Rows[R]) Columns() Columns {
	if r == nil {
		return Columns{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.page == nil {
		return Columns{}
	}
	return r.page.Columns()
}

type execRowsDecoder struct{}
type execRowsPlan struct{ count int }

func (execRowsDecoder) Compile(columns Columns) (RowPlan[Unit], error) {
	return execRowsPlan{count: columns.Len()}, nil
}
func (execRowsDecoder) Clone(value Unit) Unit { return value }
func (p execRowsPlan) Decode(reader *RowReader) (Unit, error) {
	for i := 0; i < p.count; i++ {
		if err := reader.SkipColumn(); err != nil {
			return Unit{}, err
		}
	}
	return Unit{}, nil
}
func (execRowsPlan) Clone(value Unit) Unit { return value }

func (p execRowsPlan) DecodeInto(reader *RowReader, dst *Unit) error {
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
	release := p.pendingRelease
	p.pendingRelease = nil
	p.mu.Unlock()
	if release != nil {
		release()
	}
}
func (r *Rows[R]) unlockAfterAdvance() {
	release := r.pendingRelease
	r.pendingRelease = nil
	r.mu.Unlock()
	if release != nil {
		release()
	}
}
