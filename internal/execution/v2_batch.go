// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"bytes"
	"context"
	"fmt"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	// BatchItem retains an immutable binding descriptor without admitting
)

type BatchItem struct{ privateStorage BatchItemStorage }
type InternalBatchState struct {
	kind  BatchKind
	items []InternalExecutionRequest
}

// Public batches capture option payloads as well as their already owned items.
// Cancellation can return before transport cleanup without retaining caller data.
func RunOwnedBatch[R any](ctx context.Context, s *implCluster.Session, kind BatchKind, items []BatchItem, decoder RowDecoder[R], options Options) (*Page[R], error) {
	options, err := cloneQueryOptions(options)
	if err != nil {
		return nil, err
	}
	return RunBatch(ctx, s, kind, items, decoder, options)
}
func ExecOwnedBatch(ctx context.Context, s *implCluster.Session, kind BatchKind, items []BatchItem, options Options) (Result, error) {
	options, err := cloneQueryOptions(options)
	if err != nil {
		return Result{}, err
	}
	return ExecBatch(ctx, s, kind, items, options)
}

func BatchEntry[R any](bound *Bound[R]) (BatchItem, error) {
	if bound == nil || bound.privateStorage.InternalRequest.InternalOwner == nil {
		return *func() *BatchItem {
			opaqueStorageValue := new(BatchItem)
			return opaqueStorageValue
		}(), typeValidation("batch item", "nil binding")
	}
	return *func() *BatchItem {
		opaqueStorageValuex := new(BatchItem)
		opaqueStorageValuex.privateStorage.InternalRequest = &bound.privateStorage.InternalRequest
		return opaqueStorageValuex
	}(), nil
}
func RunBatch[R any](ctx context.Context, s *implCluster.Session, kind BatchKind, items []BatchItem, decoder RowDecoder[R], options Options) (*Page[R], error) {
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
	state := &InternalBatchState{kind: kind, items: make([]InternalExecutionRequest, len(items))}
	routing := RoutingInfo{Kind: OrdinaryStatement}
	admitted := make([]*inputOwner, 0, len(items))
	for i, item := range items {
		if item.privateStorage.InternalRequest != nil && item.privateStorage.InternalRequest.InternalPrepared != nil && item.privateStorage.InternalRequest.InternalPrepared.InternalSession != s {
			return nil, typeValidation("batch session", "prepared statement belongs to another session")
		}
		if item.privateStorage.InternalRequest == nil {
			return nil, typeValidation("batch item", "nil descriptor")
		}
		if len(item.privateStorage.InternalRequest.InternalValues.Cells) > 65535 {
			return nil, typeValidation("batch values", "maximum count is 65535")
		}
		state.items[i] = *item.privateStorage.InternalRequest
		keyspace := item.privateStorage.InternalRequest.InternalRouting.Keyspace
		if keyspace != "" {
			if routing.Keyspace != "" && keyspace != routing.Keyspace {
				return nil, typeValidation("batch keyspace", "members use different keyspaces")
			}
			routing.Keyspace = keyspace
		}
		if !routing.Token.Set && item.privateStorage.InternalRequest.InternalRouting.Token.Set {
			routing.Token = item.privateStorage.InternalRequest.InternalRouting.Token
			routing.Table = item.privateStorage.InternalRequest.InternalRouting.Table
		}
		if item.privateStorage.InternalRequest.InternalRouting.Kind == ConditionalStatement {
			routing.Kind = ConditionalStatement
		} else if routing.Kind == OrdinaryStatement && item.privateStorage.InternalRequest.InternalRouting.Kind == UnknownStatement {
			routing.Kind = UnknownStatement
		}
		routing.Conditional = routing.Kind != OrdinaryStatement
	}
	for _, item := range state.items {
		if err := item.InternalOwner.InternalAdmit(); err != nil {
			for _, owner := range admitted {
				owner.InternalRelease()
			}
			return nil, err
		}
		admitted = append(admitted, item.InternalOwner)
	}
	owner := newInputOwner()
	owner.InternalOnDrain = func() {
		for _, member := range admitted {
			member.InternalRelease()
		}
	}
	bound := func() *Bound[R] {
		opaqueStorageValuexx := new(Bound[R])
		opaqueStorageValuexx.privateStorage.InternalRequest = InternalExecutionRequest{InternalOwner: owner, InternalRouting: routing, batch: state}
		opaqueStorageValuexx.privateStorage.InternalRows = decoder
		return opaqueStorageValuexx
	}()
	page, err := RunPage(ctx, s, bound, StartPage(), options)
	owner.InternalClose()
	return page, err
}
func ExecBatch(ctx context.Context, s *implCluster.Session, kind BatchKind, items []BatchItem, options Options) (Result, error) {
	page, err := RunBatch(ctx, s, kind, items, InternalExecRowsDecoder{}, options)
	if err != nil {
		return InternalExecutionErrorResult(err), err
	}
	defer page.Close()
	for page.Next() {
	}
	if page.Err() == nil && page.Columns().Len() > 0 {
		page.privateStorage.err = typeValidation("ExecBatch", "result columns require RunBatch")
	}
	return page.Info(), page.Err()
}
func (s *InternalSessionRuntime) prepareBatchAttempt(ctx context.Context, conn *Conn, batch *InternalBatchState) (*typedBatchRequest, error) {
	request := &typedBatchRequest{Kind: batch.kind, Items: make([]typedBatchItem, len(batch.items))}
	for i, item := range batch.items {
		entry := typedBatchItem{Statement: item.InternalStatement, Values: item.InternalValues.Framed, ValueCount: len(item.InternalValues.Cells)}
		if item.InternalPrepared != nil {
			prepared, err := s.InternalPrepareOnConn(ctx, conn, item.InternalPrepared, item.InternalValues, false)
			if err != nil {
				return nil, err
			}
			entry.PreparedID = prepared.InternalId
		}
		if conn.InternalVersion < 4 {
			for _, cell := range item.InternalValues.Cells {
				if cell.State == Unset {
					return nil, typeValidation("unset", "requires protocol v4")
				}
			}
		}
		request.Items[i] = entry
	}
	return request, nil
}
func (s *InternalSessionRuntime) repairBatch(ctx context.Context, conn *Conn, batch *InternalBatchState, id []byte) error {
	for _, item := range batch.items {
		if item.InternalPrepared == nil {
			continue
		}
		item.InternalPrepared.InternalMu.RLock()
		cached := item.InternalPrepared.InternalHosts[implEndpoint.InternalHostInfoHostUUID(conn.InternalHost)]
		item.InternalPrepared.InternalMu.RUnlock()
		if cached != nil && bytes.Equal(cached.InternalId, id) {
			_, err := s.InternalPrepareOnConn(ctx, conn, item.InternalPrepared, item.InternalValues, true)
			return err
		}
	}
	return &ProtocolError{Stage: "batch unprepared", Err: fmt.Errorf("unknown statement ID")}
}
