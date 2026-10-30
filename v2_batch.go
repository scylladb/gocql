package gocql

import (
	"bytes"
	"context"
	"fmt"
)

// BatchKind selects native protocol batch semantics.
type BatchKind byte

// BatchItem retains an immutable binding descriptor without admitting execution.
type BatchItem struct{ request *executionRequest }
type batchState struct {
	kind  BatchKind
	items []executionRequest
}

func BatchEntry[R any](bound *Bound[R]) (BatchItem, error) {
	if bound == nil || bound.request.owner == nil {
		return BatchItem{}, typeValidation("batch item", "nil binding")
	}
	return BatchItem{request: &bound.request}, nil
}
func RunBatch[R any](ctx context.Context, s *Session, kind BatchKind, items []BatchItem, decoder RowDecoder[R], options Options) (*Page[R], error) {
	if options.PageSize.Set {
		return nil, &UnsupportedSettingError{Setting: "batch page size", Reason: "native BATCH responses do not support paging"}
	}
	if options.Prefetch.Set && options.Prefetch.Value != 0 {
		return nil, &UnsupportedSettingError{Setting: "batch prefetch", Reason: "native BATCH responses do not support paging"}
	}
	if len(items) == 0 || len(items) > 65535 {
		return nil, typeValidation("batch", "requires 1 to 65535 entries")
	}
	if kind != LoggedBatch && kind != UnloggedBatch && kind != CounterBatch {
		return nil, typeValidation("batch kind", "unknown kind")
	}
	if decoder == nil {
		return nil, typeValidation("batch rows", "nil decoder")
	}
	state := &batchState{kind: kind, items: make([]executionRequest, len(items))}
	routing := RoutingInfo{Kind: OrdinaryStatement}
	admitted := make([]*inputOwner, 0, len(items))
	for i, item := range items {
		if item.request != nil && item.request.prepared != nil && item.request.prepared.session != s {
			return nil, typeValidation("batch session", "prepared statement belongs to another session")
		}
		if item.request == nil {
			return nil, typeValidation("batch item", "nil descriptor")
		}
		if len(item.request.values.cells) > 65535 {
			return nil, typeValidation("batch values", "maximum count is 65535")
		}
		state.items[i] = *item.request
		keyspace := item.request.routing.Keyspace
		if keyspace != "" {
			if routing.Keyspace != "" && keyspace != routing.Keyspace {
				return nil, typeValidation("batch keyspace", "members use different keyspaces")
			}
			routing.Keyspace = keyspace
		}
		if !routing.Token.Set && item.request.routing.Token.Set {
			routing.Token = item.request.routing.Token
			routing.Table = item.request.routing.Table
		}
		if item.request.routing.Kind == ConditionalStatement {
			routing.Kind = ConditionalStatement
		} else if routing.Kind == OrdinaryStatement && item.request.routing.Kind == UnknownStatement {
			routing.Kind = UnknownStatement
		}
		routing.Conditional = routing.Kind != OrdinaryStatement
	}
	for _, item := range state.items {
		if err := item.owner.admit(); err != nil {
			for _, owner := range admitted {
				owner.release()
			}
			return nil, err
		}
		admitted = append(admitted, item.owner)
	}
	owner := newInputOwner()
	owner.onDrain = func() {
		for _, member := range admitted {
			member.release()
		}
	}
	bound := &Bound[R]{request: executionRequest{owner: owner, routing: routing, batch: state}, rows: decoder}
	page, err := RunPage(ctx, s, bound, StartPage(), options)
	owner.close()
	return page, err
}
func ExecBatch(ctx context.Context, s *Session, kind BatchKind, items []BatchItem, options Options) (Result, error) {
	page, err := RunBatch(ctx, s, kind, items, execRowsDecoder{}, options)
	if err != nil {
		return executionErrorResult(err), err
	}
	defer page.Close()
	for page.Next() {
	}
	if page.Err() == nil && page.Columns().Len() > 0 {
		page.err = typeValidation("ExecBatch", "result columns require RunBatch")
	}
	return page.Info(), page.Err()
}
func (s *Session) prepareBatchAttempt(ctx context.Context, conn *Conn, batch *batchState) (*typedBatchRequest, error) {
	request := &typedBatchRequest{kind: batch.kind, items: make([]typedBatchItem, len(batch.items))}
	for i, item := range batch.items {
		entry := typedBatchItem{statement: item.statement, values: item.values.framed, valueCount: len(item.values.cells)}
		if item.prepared != nil {
			prepared, err := s.prepareOnConn(ctx, conn, item.prepared, item.values, false)
			if err != nil {
				return nil, err
			}
			entry.preparedID = prepared.id
		}
		if conn.version < 4 {
			for _, cell := range item.values.cells {
				if cell.state == Unset {
					return nil, typeValidation("unset", "requires protocol v4")
				}
			}
		}
		request.items[i] = entry
	}
	return request, nil
}
func (s *Session) repairBatch(ctx context.Context, conn *Conn, batch *batchState, id []byte) error {
	for _, item := range batch.items {
		if item.prepared == nil {
			continue
		}
		item.prepared.mu.RLock()
		cached := item.prepared.hosts[conn.host.hostUUID()]
		item.prepared.mu.RUnlock()
		if cached != nil && bytes.Equal(cached.id, id) {
			_, err := s.prepareOnConn(ctx, conn, item.prepared, item.values, true)
			return err
		}
	}
	return &ProtocolError{Stage: "batch unprepared", Err: fmt.Errorf("unknown statement ID")}
}
