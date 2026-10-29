package gocql

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type Result struct {
	HostID              UUID
	Attempts            int
	Latency             time.Duration
	Rows                int64
	Warnings            []string
	TraceIDs            []UUID
	CustomPayload       []PayloadEntry
	AchievedConsistency Consistency
}

func cloneResult(result Result) Result {
	result.Warnings = append([]string(nil), result.Warnings...)
	result.TraceIDs = append([]UUID(nil), result.TraceIDs...)
	result.CustomPayload = append([]PayloadEntry(nil), result.CustomPayload...)
	return result
}
func mergeResult(dst *Result, src Result) {
	dst.HostID = src.HostID
	dst.Attempts += src.Attempts
	dst.Latency += src.Latency
	dst.Rows += src.Rows
	dst.Warnings = append(dst.Warnings, src.Warnings...)
	dst.TraceIDs = append(dst.TraceIDs, src.TraceIDs...)
	dst.CustomPayload = append(dst.CustomPayload, src.CustomPayload...)
	dst.AchievedConsistency = src.AchievedConsistency
}

type PageState struct {
	token []byte
	kind  uint8
}

func StartPage() PageState { return PageState{} }
func RestorePageState(token []byte) PageState {
	return PageState{token: append([]byte{}, token...), kind: 1}
}
func (p PageState) More() bool     { return p.kind == 1 }
func (p PageState) Complete() bool { return p.kind == 2 }
func (p PageState) Bytes() []byte {
	if p.kind != 1 {
		return nil
	}
	return append([]byte{}, p.token...)
}
func completePageState() PageState { return PageState{kind: 2} }

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
	response *responsePage
	plan     RowPlan[R]
	cursor   *wire.Cursor
	reader   RowReader
	index    int32
	current  R
	valid    bool
	err      error
	closed   bool
	release  func()
}

func newPage[R any](response *responsePage, decoder RowDecoder[R], release func()) (*Page[R], error) {
	if response == nil {
		return nil, typeValidation("response", "nil response")
	}
	if decoder == nil {
		response.owner.Release()
		return nil, typeValidation("rows", "nil row decoder")
	}
	plan, err := compileRowPlan(decoder, response.columns)
	if err != nil {
		response.owner.Release()
		return nil, err
	}
	if response.rowCount < 0 {
		response.owner.Release()
		return nil, &ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	return &Page[R]{response: response, plan: plan, cursor: wire.NewCursor(response.rows), release: release}, nil
}
func (p *Page[R]) Next() bool {
	if p == nil || p.closed || p.err != nil {
		return false
	}
	p.valid = false
	if p.index >= p.response.rowCount {
		if err := p.cursor.RequireEnd(); err != nil {
			p.err = &ProtocolError{Stage: "rows", Err: err}
		}
		return false
	}
	p.reader = RowReader{columns: p.response.columns, cursor: p.cursor, lease: p.response.owner}
	reader := &p.reader
	row, err := p.plan.Decode(reader)
	if err == nil {
		err = finishRowReader(reader)
	}
	if err != nil {
		p.err = err
		return false
	}
	p.index++
	p.current = row
	p.valid = true
	return true
}
func (p *Page[R]) Row() R {
	if p == nil || !p.valid || p.closed {
		panic(ErrNoRows)
	}
	return p.current
}
func (p *Page[R]) Err() error {
	if p == nil {
		return nil
	}
	return p.err
}
func (p *Page[R]) Close() error {
	if p == nil {
		return nil
	}
	if !p.closed {
		p.closed = true
		p.valid = false
		var zero R
		p.current = zero
		p.response.owner.Release()
		if p.release != nil {
			p.release()
			p.release = nil
		}
	}
	return p.err
}
func (p *Page[R]) Retain() (*PageLease, error) {
	if p == nil || !p.valid || p.closed {
		return nil, ErrNoRows
	}
	if !p.response.owner.Retain() {
		return nil, ErrReleased
	}
	return &PageLease{owner: p.response.owner}, nil
}
func (p *Page[R]) Clone() (R, error) {
	if p == nil || !p.valid || p.closed {
		var zero R
		return zero, ErrNoRows
	}
	return p.plan.Clone(p.current), nil
}
func (p *Page[R]) State() PageState {
	if p == nil {
		return completePageState()
	}
	return p.response.state
}
func (p *Page[R]) Info() Result {
	if p == nil {
		return Result{}
	}
	return cloneResult(p.response.result)
}

type Rows[R any] struct {
	rejectColumns bool
	terminalErr   error
	future        *pageFuture

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
	seen    map[string]struct{}
	release func()
}

func (r *Rows[R]) Next() bool {
	if r == nil || r.closed || r.err != nil {
		return false
	}
	r.valid = false
	for {
		if r.rejectColumns && r.page != nil && r.page.Columns().Len() != 0 && r.terminalErr == nil {
			r.terminalErr = typeValidation("Exec", "result columns require Run")
		}
		if err := r.ctx.Err(); err != nil {
			r.err = err
			_ = r.Close()
			return false
		}
		if r.page.Next() {
			r.valid = true
			return true
		}
		if err := r.page.Err(); err != nil {
			r.err = err
			_ = r.Close()
			return false
		}
		state := r.page.State()
		_ = r.page.Close()
		if !state.More() {
			if r.err == nil {
				r.err = r.terminalErr
			}
			_ = r.Close()
			return false
		}
		key := string(state.token)
		if _, ok := r.seen[key]; ok {
			r.err = ErrPagingNoProgress
			_ = r.Close()
			return false
		}
		r.seen[key] = struct{}{}
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
			_ = r.Close()
			return false
		}
		mergeResult(&r.info, response.result)
		r.page, err = newPage(response, r.decoder, nil)
		if err != nil {
			r.err = err
			_ = r.Close()
			return false
		}
		r.startPrefetch()
	}
}
func (r *Rows[R]) Row() R {
	if r == nil || !r.valid || r.closed {
		panic(ErrNoRows)
	}
	return r.page.Row()
}
func (r *Rows[R]) Err() error {
	if r == nil {
		return nil
	}
	return r.err
}
func (r *Rows[R]) Close() error {
	if r == nil {
		return nil
	}
	if !r.closed {
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
		if r.release != nil {
			r.release()
			r.release = nil
		}
	}
	return r.err
}
func (r *Rows[R]) Retain() (*PageLease, error) {
	if r == nil || !r.valid || r.closed {
		return nil, ErrNoRows
	}
	return r.page.Retain()
}
func (r *Rows[R]) Clone() (R, error) {
	if r == nil || !r.valid || r.closed {
		var zero R
		return zero, ErrNoRows
	}
	return r.page.Clone()
}
func (r *Rows[R]) Info() Result {
	if r == nil {
		return Result{}
	}
	return cloneResult(r.info)
}

type CASResult[R any] struct {
	Applied  bool
	Previous Option[R]
}
type casRows[R any] struct{ decoder RowDecoder[R] }
type casRowPlan[R any] struct {
	previous RowPlan[R]
	columns  Columns
}

func CASRows[R any](decoder RowDecoder[R]) RowDecoder[CASResult[R]] {
	return casRows[R]{decoder: decoder}
}
func (c casRows[R]) Compile(columns Columns) (RowPlan[CASResult[R]], error) {
	first, ok := columns.At(0)
	if !ok || first.Name != "[applied]" {
		return nil, typeValidation("CAS result", "first column must be [applied]")
	}
	if err := Bool.CheckType(first.Type); err != nil {
		return nil, err
	}
	_, previous, err := splitColumns(columns, 1)
	if err != nil {
		return nil, err
	}
	p := casRowPlan[R]{columns: previous}
	if previous.Len() > 0 {
		if c.decoder == nil {
			return nil, typeValidation("CAS result", "nil previous-value decoder")
		}
		p.previous, err = compileRowPlan(c.decoder, previous)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}
func (p casRowPlan[R]) Decode(reader *RowReader) (CASResult[R], error) {
	var result CASResult[R]
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return CASResult[R]{}, err
	}
	return result, nil
}
func (p casRowPlan[R]) DecodeInto(reader *RowReader, dst *CASResult[R]) error {
	if dst == nil {
		return typeValidation("destination", "nil CAS destination")
	}
	*dst = CASResult[R]{}
	if err := readColumnInto(reader, Bool, &dst.Applied); err != nil {
		return err
	}
	if p.previous == nil {
		return nil
	}
	columns, index := reader.columns, reader.index
	reader.columns, reader.index = p.columns, 0
	err := p.previous.DecodeInto(reader, &dst.Previous.Value)
	if err == nil {
		err = finishRowReader(reader)
	}
	reader.columns, reader.index = columns, index
	if err != nil {
		return err
	}
	reader.index += p.columns.Len()
	dst.Previous.Set = true
	return nil
}
func (p casRowPlan[R]) Clone(value CASResult[R]) CASResult[R] {
	if value.Previous.Set {
		value.Previous.Value = p.previous.Clone(value.Previous.Value)
	}
	return value
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
			response.owner.Release()
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
		f.response.owner.Release()
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
	if _, seen := r.seen[string(state.token)]; seen {
		return
	}
	future := &pageFuture{done: make(chan struct{})}
	r.future = future
	r.request.owner.retain()
	go func() {
		defer r.request.owner.release()
		response, err := r.session.executePage(r.ctx, r.request, state)
		future.complete(response, err)
	}()
}

func (c casRows[R]) Clone(value CASResult[R]) CASResult[R] {
	if value.Previous.Set {
		value.Previous.Value = c.decoder.Clone(value.Previous.Value)
	}
	return value
}

// Columns returns the actual physical result metadata, including for empty pages.
func (p *Page[R]) Columns() Columns {
	if p == nil || p.response == nil {
		return Columns{}
	}
	return p.response.columns
}

// Columns returns the current page's actual physical result metadata.
func (r *Rows[R]) Columns() Columns {
	if r == nil || r.page == nil {
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
