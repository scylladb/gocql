// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "context"
import "sync"
import "github.com/scylladb/gocql/v2/internal/wire"

// inputOwner serializes close against admission. Each admitted operation and
// transport writer has an independent reference; result leases never use it.
type InternalInputOwner struct {
	mu              sync.Mutex
	InternalClosed  bool
	InternalRefs    int
	done            chan struct{}
	InternalOnDrain func()
	onIdle          func()
}

func InternalNewInputOwner() *InternalInputOwner {
	return &InternalInputOwner{done: make(chan struct{})}
}
func (o *InternalInputOwner) InternalAdmit() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.InternalClosed {
		return ErrClosed
	}
	o.InternalRefs++
	return nil
}
func (o *InternalInputOwner) InternalRetain() { o.mu.Lock(); o.InternalRefs++; o.mu.Unlock() }
func (o *InternalInputOwner) retainIfLive() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.InternalRefs == 0 {
		return false
	}
	o.InternalRefs++
	return true
}
func (o *InternalInputOwner) InternalRelease() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.InternalRefs <= 0 {
		panic("gocql: unbalanced input owner release")
	}
	o.InternalRefs--
	if o.InternalRefs == 0 && o.onIdle != nil {
		o.onIdle()
	}
	if o.InternalClosed && o.InternalRefs == 0 {
		if o.InternalOnDrain != nil {
			o.InternalOnDrain()
			o.InternalOnDrain = nil
		}
		if o.done != nil {
			close(o.done)
		}
	}
}
func (o *InternalInputOwner) InternalClose() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.InternalClosed {
		return
	}
	o.InternalClosed = true
	if o.InternalRefs == 0 {
		if o.InternalOnDrain != nil {
			o.InternalOnDrain()
			o.InternalOnDrain = nil
		}
		if o.done != nil {
			close(o.done)
		}
	}
}
func (o *InternalInputOwner) InternalWait(ctx context.Context) error {
	o.mu.Lock()
	closed := o.InternalClosed
	if !closed {
		o.mu.Unlock()
		return typeValidation("binding", "Wait requires Close first")
	}
	if o.InternalRefs == 0 {
		o.mu.Unlock()
		return nil
	}
	if o.done == nil {
		o.done = make(chan struct{})
	}
	done := o.done
	o.mu.Unlock()
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (o *InternalInputOwner) InternalWriterLease(chain wire.Chain) *wire.Lease {
	o.InternalRetain()
	return wire.NewLease(chain, o.InternalRelease)
}

type InternalExecutionRequest struct {
	query              *queryState
	tabletFeedback     tabletFeedbackStamp
	tabletVersionBlock byte
	connection         *Conn
	InternalStatement  string
	keyspace           string
	InternalPrepared   *InternalPreparedState
	InternalValues     encodedValues
	InternalOwner      *inputOwner
	InternalRouting    RoutingInfo
	InternalOptions    InternalResolvedOptions
	batch              *InternalBatchState
	InternalLogical    *InternalLogicalExecution
}

type ExecutionError struct {
	Err    error
	Result Result
}

func (e *ExecutionError) Error() string { return "gocql: request: " + e.Err.Error() }
func (e *ExecutionError) Unwrap() error { return e.Err }
