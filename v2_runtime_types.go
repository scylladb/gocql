package gocql

import (
	"context"
	"errors"
	"sync"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type responsePage struct {
	responseData
	metadata *typedResultMetadata
}

type executionRequest struct {
	tabletFeedback     tabletFeedbackStamp
	tabletVersionBlock byte
	connection         *Conn
	statement          string
	keyspace           string
	prepared           *preparedState
	values             encodedValues
	owner              *inputOwner
	routing            RoutingInfo
	options            resolvedOptions
	batch              *batchState
	logical            *logicalExecution
}

var (
	ErrClosed           = errors.New("gocql: closed")
	ErrPagingStateLimit = errors.New("gocql: paging state history limit exceeded")
	ErrPagingComplete   = errors.New("gocql: completed page cannot be resumed")
	ErrPagingNoProgress = errors.New("gocql: paging made no progress")
	ErrNoRows           = errors.New("gocql: no rows")
	ErrMultipleRows     = errors.New("gocql: multiple rows")
)

// SchemaChangedError requires preparing and binding again after incompatible metadata.
type SchemaChangedError struct {
	Statement         string
	Previous, Current Columns
}

func (e *SchemaChangedError) Error() string {
	return "gocql: bound parameter schema changed for " + e.Statement
}

type ExecutionError struct {
	Err    error
	Result Result
}

func (e *ExecutionError) Error() string { return "gocql: request: " + e.Err.Error() }
func (e *ExecutionError) Unwrap() error { return e.Err }

// inputOwner serializes close against admission. Each admitted operation and
// transport writer has an independent reference; result leases never use it.
type inputOwner struct {
	mu      sync.Mutex
	closed  bool
	refs    int
	done    chan struct{}
	onDrain func()
}

func newInputOwner() *inputOwner { return &inputOwner{done: make(chan struct{})} }
func (o *inputOwner) admit() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	o.refs++
	return nil
}
func (o *inputOwner) retain() { o.mu.Lock(); o.refs++; o.mu.Unlock() }
func (o *inputOwner) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.refs <= 0 {
		panic("gocql: unbalanced input owner release")
	}
	o.refs--
	if o.closed && o.refs == 0 {
		if o.onDrain != nil {
			o.onDrain()
			o.onDrain = nil
		}
		close(o.done)
	}
}
func (o *inputOwner) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	o.closed = true
	if o.refs == 0 {
		if o.onDrain != nil {
			o.onDrain()
			o.onDrain = nil
		}
		close(o.done)
	}
}
func (o *inputOwner) wait(ctx context.Context) error {
	o.mu.Lock()
	closed := o.closed
	done := o.done
	o.mu.Unlock()
	if !closed {
		return typeValidation("binding", "Wait requires Close first")
	}
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
func (o *inputOwner) writerLease(chain wire.Chain) *wire.Lease {
	o.retain()
	return wire.NewLease(chain, o.release)
}
