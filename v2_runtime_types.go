package gocql

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Option distinguishes an explicit zero value from an inherited setting.
type Option[T any] struct {
	Value T
	Set   bool
}

type PayloadEntry struct {
	Key   string
	Value Nullable[Bytes]
}

type encodedCell struct {
	typ   DataType
	state CellState
	body  wire.Chain
}
type encodedValues struct {
	columns Columns
	cells   []encodedCell
	framed  wire.Chain
}

// attemptRequest contains only immutable protocol inputs. The transport retains
// lease until every accepted writer has stopped accessing its spans.
type attemptRequest struct {
	statement                      string
	preparedID, resultMetadataID   []byte
	values                         wire.Chain
	valueCount                     int
	lease                          *wire.Lease
	consistency, serialConsistency Consistency
	pageSize                       int
	pagingState                    []byte
	timestamp                      int64
	defaultTimestamp, skipMetadata bool
	keyspace                       string
	nowInSeconds                   *int32
	trace                          bool
	timeout                        time.Duration
	prepare                        bool
	batch                          *typedBatchRequest
	payload                        []PayloadEntry
}
type typedBatchRequest struct {
	kind  BatchKind
	items []typedBatchItem
}
type typedBatchItem struct {
	statement  string
	preparedID []byte
	values     wire.Chain
	valueCount int
}
type responsePage struct {
	metadataChanged                     bool
	owner                               *wire.Lease
	rows                                wire.Chain
	columns                             Columns
	rowCount                            int32
	columnCount                         int
	lwt                                 bool
	keyspace                            string
	schemaChange                        bool
	pagingState, metadataID, preparedID []byte
	noMetadata                          bool
	bindColumns                         Columns
	pkIndexes                           []int
	kind                                int
	traceID                             []byte
	payload                             []PayloadEntry
	warnings                            []string
	state                               PageState
	result                              Result
}

type executionRequest struct {
	connection *Conn
	statement  string
	keyspace   string
	prepared   *preparedState
	values     encodedValues
	owner      *inputOwner
	routing    RoutingInfo
	options    resolvedOptions
	batch      *batchState
	logical    *logicalExecution
}

var (
	ErrClosed           = errors.New("gocql: closed")
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
