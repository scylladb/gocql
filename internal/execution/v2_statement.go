// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	"sync"
	"sync/atomic"
)

type PrepareOptions struct {
	Keyspace       string
	HostID         Option[UUID]
	Trace          bool
	privateStorage PrepareOptionsStorage
}
type PreparedInfo struct {
	Keyspace, Table     string
	PartitionKeyIndexes []int
	Kind                StatementKind
	Conditional         bool
	Generation          uint64
}
type InternalHostPrepared struct {
	InternalEpoch                  preparedSchemaStamp
	InternalResult                 *typedResultMetadata
	InternalRevision               uint64
	InternalMetadataChanged        bool
	InternalId, InternalMetadataID []byte
	InternalColumns                Columns
	InternalGeneration             uint64
}
type InternalPreparedState struct {
	InternalConflictDirty               atomic.Bool
	conflictMu                          sync.Mutex
	InternalConflictEpoch               atomic.Uint64
	InternalConflictRefresh             atomic.Bool
	InternalResultHost                  UUID
	InternalResult                      *typedResultMetadata
	InternalMu                          sync.RWMutex
	InternalSession                     *implCluster.Session
	InternalProtocol                    uint8
	InternalStatement, InternalKeyspace string
	InternalParameters, InternalColumns Columns
	InternalInfo                        PreparedInfo
	InternalHosts                       map[UUID]*InternalHostPrepared
	InternalPreparing                   map[UUID]*InternalHostPrepareFlight
	InternalDependencies                *preparedDependencies
	InternalPartitioner                 string
	InternalInitialEpoch                preparedSchemaStamp
}
type InternalHostPrepareFlight struct {
	started         uint64
	InternalWaiters atomic.Int32
	done            chan struct{}
	previous        *InternalHostPrepared
	epoch           preparedSchemaStamp
	result          *InternalHostPrepared
	err             error
}
type Prepared[P, R any] struct {
	privateStorage PreparedStorage[P, R]
}
type Bound[R any] struct {
	privateStorage BoundStorage[R]
}

func Prepare[P, R any](ctx context.Context, s *implCluster.Session, statement Statement[P, R], options PrepareOptions) (*Prepared[P, R], error) {
	if ctx == nil || s == nil {
		return nil, typeValidation("prepare", "nil context or session")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, ErrClosed
	}
	if options.HostID.Set && options.HostID.Value.IsEmpty() {
		return nil, typeValidation("prepare host", "empty UUID")
	}
	if options.privateStorage.InternalConnection != nil && !implCluster.InternalConnectionOwnedBy(options.privateStorage.InternalConnection, s) {
		return nil, typeValidation("prepare connection", "connection belongs to another session")
	}
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	keyspace := options.Keyspace
	if keyspace == "" {
		keyspace = implCluster.SessionData(s).InternalCfg.Keyspace
	}
	state, err := InternalRuntimeFor(s).InternalPrepareTyped(ctx, statement.CQL, keyspace, options)
	if err != nil {
		return nil, err
	}
	state.InternalMu.RLock()
	parameters, columns := state.InternalParameters, state.InternalColumns
	confirmedKind := state.InternalInfo.Kind
	state.InternalMu.RUnlock()
	plan, err := compileParamPlan(statement.Params, parameters)
	if err != nil {
		return nil, err
	}
	kind := statement.Kind
	if confirmedKind == ConditionalStatement || kind == UnknownStatement {
		kind = confirmedKind
	}
	var rows RowPlan[R]
	// Cassandra omits conditional result metadata from PREPARE. Unknown and
	// conditional statements therefore defer absent result shapes until EXECUTE;
	// actual metadata still selects and validates the plan before any row output.
	// A confirmed ordinary statement keeps eager zero-column validation.
	if columns.Len() != 0 || kind == OrdinaryStatement {
		rows, err = compileRowPlan(statement.Rows, columns)
		if err != nil {
			return nil, err
		}
	}
	return func() *Prepared[P, R] {
		opaqueStorageValue := new(Prepared[P, R])
		opaqueStorageValue.privateStorage.InternalKind = kind
		opaqueStorageValue.privateStorage.InternalState = state
		opaqueStorageValue.privateStorage.InternalParams = plan
		opaqueStorageValue.privateStorage.InternalRows = statement.Rows
		opaqueStorageValue.privateStorage.InternalPlans = &InternalResultPlanCache[R]{columns: columns, InternalPlan: rows}
		return opaqueStorageValue
	}(), nil
}

func (p *Prepared[P, R]) Parameters() Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return Columns{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	return p.privateStorage.InternalState.InternalParameters
}
func (p *Prepared[P, R]) Columns() Columns {
	if p == nil || p.privateStorage.InternalState == nil {
		return Columns{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	return p.privateStorage.InternalState.InternalColumns
}
func (p *Prepared[P, R]) Info() PreparedInfo {
	if p == nil || p.privateStorage.InternalState == nil {
		return PreparedInfo{}
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	defer p.privateStorage.InternalState.InternalMu.RUnlock()
	v := p.privateStorage.InternalState.InternalInfo
	v.Kind = p.privateStorage.InternalKind
	v.Conditional = p.privateStorage.InternalKind == ConditionalStatement
	v.PartitionKeyIndexes = append([]int(nil), v.PartitionKeyIndexes...)
	return v
}
func BindBorrowed[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return InternalBindPrepared(p, value, false)
}
func BindOwned[P, R any](p *Prepared[P, R], value P) (*Bound[R], error) {
	return InternalBindPrepared(p, value, true)
}
func InternalBindPrepared[P, R any](p *Prepared[P, R], value P, owned bool) (*Bound[R], error) {
	if p == nil || p.privateStorage.InternalState == nil {
		return nil, typeValidation("prepared", "nil handle")
	}
	p.privateStorage.InternalState.InternalMu.RLock()
	columns := p.privateStorage.InternalState.InternalParameters
	info := p.privateStorage.InternalState.InternalInfo
	statement, keyspace := p.privateStorage.InternalState.InternalStatement, p.privateStorage.InternalState.InternalKeyspace
	p.privateStorage.InternalState.InternalMu.RUnlock()
	allowUnset := p.privateStorage.InternalState.InternalProtocol >= 4
	values, err := encodeParameters(p.privateStorage.InternalParams, columns, value, owned, allowUnset)
	if err != nil {
		return nil, err
	}
	token, err := computeRoutingToken(values.Cells, info.PartitionKeyIndexes, implCluster.InternalSessionCapturedRoutingPartitioner(p.privateStorage.InternalState.InternalSession, p.privateStorage.InternalState.InternalPartitioner))
	if err != nil {
		return nil, err
	}
	return func() *Bound[R] {
		opaqueStorageValuex := new(Bound[R])
		opaqueStorageValuex.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: statement, keyspace: keyspace, InternalPrepared: p.privateStorage.InternalState, InternalValues: values, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Keyspace: info.Keyspace, Table: info.Table, Token: token, Kind: p.privateStorage.InternalKind, Conditional: p.privateStorage.InternalKind != OrdinaryStatement}}
		opaqueStorageValuex.privateStorage.InternalRows = p.privateStorage.InternalRows
		opaqueStorageValuex.privateStorage.plans = p.privateStorage.InternalPlans
		return opaqueStorageValuex
	}(), nil
}
func BindUnpreparedBorrowed[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return InternalBindUnprepared(s, columns, value, false)
}
func BindUnpreparedOwned[P, R any](s Statement[P, R], columns Columns, value P) (*Bound[R], error) {
	return InternalBindUnprepared(s, columns, value, true)
}
func InternalBindUnprepared[P, R any](s Statement[P, R], columns Columns, value P, owned bool) (*Bound[R], error) {
	if err := validateStatement(s); err != nil {
		return nil, err
	}
	plan, err := compileParamPlan(s.Params, columns)
	if err != nil {
		return nil, err
	}
	values, err := encodeParameters(plan, columns, value, owned, true)
	if err != nil {
		return nil, err
	}
	return func() *Bound[R] {
		opaqueStorageValuexx := new(Bound[R])
		opaqueStorageValuexx.privateStorage.InternalRequest = InternalExecutionRequest{InternalStatement: s.CQL, InternalValues: values, InternalOwner: newInputOwner(), InternalRouting: RoutingInfo{Kind: s.Kind, Conditional: s.Kind != OrdinaryStatement}}
		opaqueStorageValuexx.privateStorage.InternalRows = s.Rows
		opaqueStorageValuexx.privateStorage.plans = &InternalResultPlanCache[R]{}
		return opaqueStorageValuexx
	}(), nil
}

func (b *Bound[R]) Close() {
	if b != nil && b.privateStorage.InternalRequest.InternalOwner != nil {
		b.privateStorage.InternalRequest.InternalOwner.InternalClose()
	}
}
func (b *Bound[R]) Wait(ctx context.Context) error {
	if b == nil || b.privateStorage.InternalRequest.InternalOwner == nil {
		return typeValidation("binding", "nil binding")
	}
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	return b.privateStorage.InternalRequest.InternalOwner.InternalWait(ctx)
}
func (b *Bound[R]) Routing() RoutingInfo {
	if b == nil {
		return RoutingInfo{}
	}
	return b.privateStorage.InternalRequest.InternalRouting
}
