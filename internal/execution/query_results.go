// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"github.com/scylladb/gocql/v2/internal/wire"
	"sync"
)

type queryResultStorage[R any] struct {
	rows         RowsStorage[R]
	page         PageStorage[R]
	pageCursor   wire.Cursor
	internalPage Page[R]
}

type rowsTerminal struct {
	mu      sync.Mutex
	info    Result
	err     error
	columns Columns
}
type pageTerminal struct {
	mu      sync.Mutex
	info    Result
	err     error
	columns Columns
	state   PageState
}

type rowsHandleStorage[R any] struct {
	*RowsStorage[R]
	cursor   queryCursor
	endpoint *rowsTerminal
}
type pageHandleStorage[R any] struct {
	*PageStorage[R]
	cursor   queryCursor
	endpoint *pageTerminal
}

func (r *Rows[R]) acquire() bool {
	return r != nil && r.privateStorage.RowsStorage != nil && r.privateStorage.cursor.acquire()
}
func (p *Page[R]) acquire() bool {
	return p != nil && p.privateStorage.PageStorage != nil && p.privateStorage.cursor.acquire()
}
func (r *Rows[R]) terminalInfo() (Result, error, Columns) {
	if r == nil || r.privateStorage.endpoint == nil {
		return Result{}, nil, Columns{}
	}
	t := r.privateStorage.endpoint
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneResult(t.info), t.err, t.columns
}
func (p *Page[R]) terminalInfo() (Result, error, Columns, PageState) {
	if p == nil || p.privateStorage.endpoint == nil {
		return Result{}, nil, Columns{}, completePageState()
	}
	t := p.privateStorage.endpoint
	t.mu.Lock()
	defer t.mu.Unlock()
	return cloneResult(t.info), t.err, t.columns, t.state
}
func (r *Rows[R]) snapshotTerminal() {
	if t := r.privateStorage.endpoint; t != nil {
		t.mu.Lock()
		t.info = cloneResult(r.privateStorage.info)
		t.err = r.privateStorage.err
		if r.privateStorage.InternalPage != nil && r.privateStorage.InternalPage.privateStorage.InternalResponse != nil {
			t.columns = r.privateStorage.InternalPage.privateStorage.InternalResponse.Columns
		}
		t.mu.Unlock()
	}
}
func (p *Page[R]) snapshotTerminal() {
	if t := p.privateStorage.endpoint; t != nil {
		t.mu.Lock()
		t.info = cloneResult(p.privateStorage.InternalResponse.Result)
		t.err = p.privateStorage.err
		t.columns = p.privateStorage.InternalResponse.Columns
		t.state = p.privateStorage.InternalResponse.State
		t.mu.Unlock()
	}
}

func (r *Rows[R]) stopExecution(logical *InternalLogicalExecution) {
	if !r.acquire() {
		return
	}
	defer r.privateStorage.cursor.release()
	r.privateStorage.mu.Lock()
	if r.privateStorage.valid && !r.privateStorage.closed {
		r.snapshotTerminal()
		if r.privateStorage.endpoint != nil {
			r.privateStorage.endpoint.mu.Lock()
			r.privateStorage.endpoint.err = ErrClosed
			r.privateStorage.endpoint.mu.Unlock()
		}
		if r.privateStorage.future != nil {
			r.privateStorage.future.InternalClose()
			r.privateStorage.future = nil
		}
		info := cloneResult(r.privateStorage.info)
		r.privateStorage.mu.Unlock()
		logical.finishAndRelease(info, ErrClosed)
		return
	}
	r.privateStorage.mu.Unlock()
	_ = r.closeWithError(ErrClosed)
}
func (p *Page[R]) stopExecution(logical *InternalLogicalExecution) {
	if !p.acquire() {
		return
	}
	defer p.privateStorage.cursor.release()
	p.privateStorage.mu.Lock()
	if p.privateStorage.valid && !p.privateStorage.closed {
		p.snapshotTerminal()
		if p.privateStorage.endpoint != nil {
			p.privateStorage.endpoint.mu.Lock()
			p.privateStorage.endpoint.err = ErrClosed
			p.privateStorage.endpoint.mu.Unlock()
		}
		info := cloneResult(p.privateStorage.InternalResponse.Result)
		p.privateStorage.mu.Unlock()
		logical.finishAndRelease(info, ErrClosed)
		return
	}
	p.privateStorage.mu.Unlock()
	_ = p.closeWithError(ErrClosed)
}

type streamTerminal struct {
	mu      sync.Mutex
	info    StreamInfo
	err     error
	columns Columns
}
type streamHandleStorage[R any] struct {
	*RowStreamStorage[R]
	cursor   queryCursor
	endpoint *streamTerminal
}

func (s *RowStream[R]) acquireStorage() bool {
	return s != nil && s.privateStorage.RowStreamStorage != nil && s.privateStorage.cursor.acquire()
}
func (s *RowStream[R]) terminalInfo() (StreamInfo, error, Columns) {
	if s == nil || s.privateStorage.endpoint == nil {
		return StreamInfo{}, nil, Columns{}
	}
	t := s.privateStorage.endpoint
	t.mu.Lock()
	defer t.mu.Unlock()
	info := t.info
	info.Result = cloneResult(info.Result)
	return info, t.err, t.columns
}
func (s *RowStream[R]) snapshotTerminal() {
	t := s.privateStorage.endpoint
	if t == nil {
		return
	}
	s.privateStorage.mu.Lock()
	info := s.privateStorage.info
	info.Result = cloneResult(info.Result)
	err, columns := s.privateStorage.err, s.privateStorage.columns
	s.privateStorage.mu.Unlock()
	t.mu.Lock()
	t.info, t.err, t.columns = info, err, columns
	t.mu.Unlock()
}
