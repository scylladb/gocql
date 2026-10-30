// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	routing "github.com/scylladb/gocql/v2/internal/routing"

	model "github.com/scylladb/gocql/v2/internal/model"

	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"sort"

	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/tablets"
)

type TraceInfo struct {
	ID          UUID
	Coordinator netip.Addr
	Started     time.Time
	Duration    time.Duration
	Events      []TraceEvent
}
type TraceEvent struct {
	ID       UUID
	Activity string
	Source   netip.Addr
	Elapsed  time.Duration
	Thread   string
}

type InternalV2LatencySummary struct{ p50, p95, p99 time.Duration }

type InternalV2PeripheralState struct {
	metricsOnce                   sync.Once
	InternalSnapshot              atomic.Pointer[MetadataSnapshot]
	publishMu                     sync.Mutex
	workers                       sync.WaitGroup
	hostMu                        sync.Mutex
	InternalPublicationRevision   uint64
	InternalMu                    sync.Mutex
	InternalSubscribers           map[*Subscription]struct{}
	closed                        bool
	InternalConfig                Config
	InternalSchemaEpoch           map[string]uint64
	schemaRevision                map[string]uint64
	InternalTableRevision         map[metadataTable]uint64
	schemaDropped                 map[string]bool
	InternalRetryScheduled        atomic.Bool
	schemaRetryNeeded             atomic.Bool
	InternalPreparedRefreshScopes map[string]uint64
	preparedRefreshVersion        uint64
	InternalSchemaReconciled      atomic.Uint64
	periodicOnce                  sync.Once
	schemaGeneration              atomic.Uint64
	InternalPlacementEpoch        atomic.Uint64
	InternalFeedbackOnce          sync.Once
	InternalFeedbackMu            sync.Mutex
	InternalFeedback              map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback
	feedbackWake                  chan struct{}
	InternalDone                  chan struct{}
	latency                       [1024]atomic.Int64
	latencyCount                  atomic.Uint64
	latencySummary                atomic.Pointer[InternalV2LatencySummary]
	orphans                       atomic.Uint64
	schemaStale                   map[string]bool
	refreshDiagnostics            []error
	routeDiagnostic               error
	InternalFeedbackResync        bool
	knownTabletTables             map[metadataTable]bool
}

func InternalSessionPeripheral(s *Session) *InternalV2PeripheralState {
	if s == nil {
		return nil
	}
	if p := s.privateStorage.v2Peripheral.Load(); p != nil {
		return p
	}
	p := &InternalV2PeripheralState{InternalSubscribers: make(map[*Subscription]struct{}), InternalSchemaEpoch: make(map[string]uint64), schemaRevision: make(map[string]uint64), InternalTableRevision: make(map[metadataTable]uint64), schemaDropped: make(map[string]bool), schemaStale: make(map[string]bool), InternalFeedback: make(map[InternalV2TabletFeedbackKey]InternalV2PendingTabletFeedback), feedbackWake: make(chan struct{}, 1), knownTabletTables: make(map[metadataTable]bool), InternalDone: make(chan struct{})}
	if s.privateStorage.v2Peripheral.CompareAndSwap(nil, p) {
		return p
	}
	return s.privateStorage.v2Peripheral.Load()
}
func (s *Session) initV2Peripheral(config Config) error {
	InternalSessionConfigureV2Peripheral(s, config)
	p := InternalSessionPeripheral(s)
	p.InternalFeedbackOnce.Do(func() { p.InternalStartWorker(func() { InternalSessionRunV2TabletFeedback(s, p) }) })
	p.metricsOnce.Do(func() {
		p.InternalStartWorker(func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-p.InternalDone:
					return
				case <-ticker.C:
					p.InternalPublishLatencySummary()
				}
			}
		})
	})
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Snapshot().Routing; policy != nil {
			if err := policy.Validate(s.Metadata()); err != nil {
				return err
			}
		}
	}
	InternalSessionObserveV2Lifecycle(s, LifecycleEvent{Kind: "session", Stage: "ready", At: time.Now()})
	if config.Metadata.PeriodicRefresh > 0 {
		InternalSessionPeripheral(s).periodicOnce.Do(func() {
			InternalSessionPeripheral(s).InternalStartWorker(func() { s.runV2PeriodicRefresh(config.Metadata) })
		})
	}
	return nil
}
func InternalSessionConfigureV2Peripheral(s *Session, config Config) {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	p.InternalConfig = config
	p.InternalMu.Unlock()
}
func (s *Session) Metadata() *MetadataSnapshot {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return routing.NewMetadataSnapshot(routing.SnapshotState{})
	}
	if snapshot := p.InternalSnapshot.Load(); snapshot != nil {
		return snapshot
	}
	for {
		InternalSessionPublishV2Metadata(s)
		if snapshot := p.InternalSnapshot.Load(); snapshot != nil {
			return snapshot
		}
	}
}
func (s *Session) Metrics() MetricsSnapshot {
	if s == nil {
		return MetricsSnapshot{}
	}
	var result MetricsSnapshot
	if owner := InternalCurrentExecution(s); owner != nil {
		result = owner.MetricsSnapshot()
	}
	p := InternalSessionPeripheral(s)
	result.Orphans = p.orphans.Load()
	if summary := p.latencySummary.Load(); summary != nil {
		result.LatencyP50 = summary.p50
		result.LatencyP95 = summary.p95
		result.LatencyP99 = summary.p99
	}
	return result
}
func InternalSessionPublishV2Metadata(s *Session) bool {
	for attempt := 0; attempt < 8; attempt++ {
		if s.tryPublishV2Metadata() {
			return true
		}
	}
	return false
}
func (s *Session) tryPublishV2Metadata() bool {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return false
	}
	old := p.InternalSnapshot.Load()
	p.InternalMu.Lock()
	revision := p.InternalPublicationRevision
	stale := maps.Clone(p.schemaStale)
	config := p.InternalConfig
	diagnostics := append([]error(nil), p.refreshDiagnostics...)
	if p.routeDiagnostic != nil {
		diagnostics = append(diagnostics, p.routeDiagnostic)
	}
	p.InternalMu.Unlock()
	m := routing.NewMetadataSnapshot(routing.SnapshotState{InternalGeneration: 1, InternalKeyspaces: make(map[string]*KeyspaceMetadata), InternalRanges: make(map[string][]TokenRange), InternalTablets: make(map[metadataTable][]TokenRange)})
	routing.SnapshotData(m).InternalDiagnostics = diagnostics
	if old != nil {
		routing.SnapshotData(m).InternalGeneration = routing.SnapshotData(old).InternalGeneration + 1
	}
	var hosts []*HostInfo
	var topologyRevision uint64
	if s.privateStorage.InternalHostSource != nil {
		s.privateStorage.InternalHostSource.mu.RLock()
		topologyRevision = s.privateStorage.InternalHostSource.revision
		routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, s.privateStorage.InternalHostSource.diagnostics...)
		for _, h := range s.privateStorage.InternalHostSource.InternalHosts {
			hosts = append(hosts, h)
		}
		s.privateStorage.InternalHostSource.mu.RUnlock()
	}
	var schemaSource *map[string]*KeyspaceMetadata
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil {
		schemaSource = s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load()
	}

	replicaHosts := make([]*HostInfo, 0, len(hosts))
	validTokens := true
	for _, h := range hosts {
		snapshot, err := InternalSnapshotV2Host(s, h)
		if err != nil {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
			validTokens = false
		}
		routing.SnapshotData(m).InternalHosts = append(routing.SnapshotData(m).InternalHosts, snapshot)
		if routing.SnapshotData(m).InternalPartitioner == "" {
			routing.SnapshotData(m).InternalPartitioner = h.Partitioner()
		}
		replicaHosts = append(replicaHosts, func() *HostInfo {
			opaqueStorageValue := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = snapshot.ID
			implEndpoint.HostInfoData(opaqueStorageValue).InternalDataCenter = snapshot.Datacenter
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRack = snapshot.Rack
			implEndpoint.HostInfoData(opaqueStorageValue).InternalTokens = append([]string(nil), h.Tokens()...)
			return opaqueStorageValue
		}())
	}
	sort.Slice(routing.SnapshotData(m).InternalHosts, func(i, j int) bool {
		return routing.SnapshotData(m).InternalHosts[i].ID.String() < routing.SnapshotData(m).InternalHosts[j].ID.String()
	})
	if len(hosts) > 0 && len(routing.SnapshotData(m).InternalHosts) == 0 && old != nil {
		return false
	}
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil {
		md := s.privateStorage.InternalMetadataDescriber.InternalMetadata
		if schemaSource != nil {
			routing.SnapshotData(m).InternalSchemaSources = *schemaSource
			for name, ks := range *schemaSource {
				if ks != nil {
					if old != nil && routing.SnapshotData(old).InternalSchemaSources[name] == ks && len(implMetadata.KeyspaceData(ks).InternalTablesInvalidated) == 0 {
						routing.SnapshotData(m).InternalKeyspaces[name] = routing.SnapshotData(old).InternalKeyspaces[name]
					} else {
						routing.SnapshotData(m).InternalKeyspaces[name] = cloneV2Keyspace(ks)
					}
				}
			}
		}
		if md.privateStorage.InternalTabletsMetadata != nil && (old == nil || routing.SnapshotData(old).InternalPlacements == nil) {
			md.privateStorage.InternalTabletsMetadata.ForEach(func(ks, table string, entries tablets.TabletEntryList) bool {
				r := make([]TokenRange, len(entries))
				for i, e := range entries {
					r[i] = TokenRange{Start: e.FirstToken(), End: e.LastToken()}
					for _, replica := range e.ReplicasUnsafe() {
						r[i].Replicas = append(r[i].Replicas, Target{HostID: UUID(replica.HostUUIDValue()), ShardID: Option[uint32]{Value: uint32(replica.ShardID()), Set: replica.ShardID() >= 0}})
					}
				}
				routing.SnapshotData(m).InternalTablets[metadataTable{InternalKeyspace: ks, InternalTable: table}] = r
				return true
			})
		}
	}
	if old != nil {
		for name, prior := range routing.SnapshotData(old).InternalKeyspaces {
			if routing.SnapshotData(m).InternalKeyspaces[name] == nil && stale[name] {
				routing.SnapshotData(m).InternalKeyspaces[name] = cloneV2Keyspace(prior)
			}
			if current := routing.SnapshotData(m).InternalKeyspaces[name]; current != nil {
				for table := range implMetadata.KeyspaceData(current).InternalTablesInvalidated {
					if priorTable := prior.Tables[table]; priorTable != nil {
						copyKS := cloneV2Keyspace(prior)
						current.Tables[table] = copyKS.Tables[table]
						for name, view := range copyKS.Views {
							if view.BaseTableName == table {
								current.Views[name] = view
							}
						}
						for name, index := range copyKS.Indexes {
							if index.TableName == table {
								current.Indexes[name] = index
							}
						}
					}
				}
			}
		}
		for table := range routing.SnapshotData(old).InternalTablets {
			if ks := routing.SnapshotData(m).InternalKeyspaces[table.InternalKeyspace]; ks != nil {
				if _, ok := ks.Tables[table.InternalTable]; ok {
					if _, known := routing.SnapshotData(m).InternalTablets[table]; !known {
						routing.SnapshotData(m).InternalTablets[table] = nil
					}
				}
			}
		}
	}
	p.InternalFeedbackMu.Lock()
	for table := range p.knownTabletTables {
		if _, _, err := routing.InternalMetadataSnapshotLookupRanges(m, table.InternalKeyspace, table.InternalTable); err != nil {
			delete(p.knownTabletTables, table)
			continue
		}
		if ks := routing.SnapshotData(m).InternalKeyspaces[table.InternalKeyspace]; ks != nil {
			if _, exists := ks.Tables[table.InternalTable]; exists {
				if _, known := routing.SnapshotData(m).InternalTablets[table]; !known {
					routing.SnapshotData(m).InternalTablets[table] = nil
				}
			}
		}
	}
	p.InternalFeedbackMu.Unlock()
	// Reuse the established replication-strategy placement on private cold-path
	// descriptors. Invalid tokens were rejected before ring construction.
	if routing.SnapshotData(m).InternalPartitioner != "" && validTokens {
		partitioner := routing.SnapshotData(m).InternalPartitioner
		if config.Partitioners[routing.SnapshotData(m).InternalPartitioner] != nil {
			partitioner = "Murmur3Partitioner"
		}
		ring, err := newTokenRing(partitioner, replicaHosts)
		if err != nil {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
		} else if _, ok := ring.InternalPartitioner.(murmur3Partitioner); ok {
			for ks, meta := range routing.SnapshotData(m).InternalKeyspaces {
				ranges, err := InternalBuildV2VnodeRanges(ring, meta)
				if err != nil {
					routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
					continue
				}
				routing.SnapshotData(m).InternalRanges[ks] = ranges
			}
		} else {
			routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, typeValidation("partitioner", "no configured int64 partitioner for %q", routing.SnapshotData(m).InternalPartitioner))
		}
	}
	routing.SnapshotData(m).InternalPlacementEpoch = p.InternalPlacementEpoch.Load()
	routing.SnapshotData(m).InternalPlacementSignal = &p.InternalPlacementEpoch
	routing.SnapshotData(m).InternalPlacements = reconcileTabletPlacement(old, m)
	routing.SnapshotData(m).InternalHostIndex = buildRoutingHostIndex(routing.SnapshotData(m).InternalHosts)
	if config.Metadata.Mode != MetadataFull {
		routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, typeValidation("partitioner", "table partitioners outside loaded metadata disable token routing"))
	}
	for keyspace, ks := range routing.SnapshotData(m).InternalKeyspaces {
		for table, t := range ks.Tables {
			if t != nil && t.Options.Partitioner != "" && config.Partitioners[t.Options.Partitioner] == nil {
				routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, typeValidation("partitioner", "unknown partitioner %q for %s.%s; token routing disabled", t.Options.Partitioner, keyspace, table))
			}
		}
	}

	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Snapshot().Routing; policy != nil {
			if err := policy.Validate(m); err != nil {
				routing.SnapshotData(m).InternalDiagnostics = append(routing.SnapshotData(m).InternalDiagnostics, err)
			}
		}
	}
	// All potentially user-provided hooks above run before the publication gate.
	// A concurrent tombstone/source replacement rejects this entire stale build.
	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.InternalPublicationRevision != revision || p.InternalSnapshot.Load() != old {
		return false
	}
	if s.privateStorage.InternalHostSource != nil {
		s.privateStorage.InternalHostSource.mu.RLock()
		defer s.privateStorage.InternalHostSource.mu.RUnlock()
		if s.privateStorage.InternalHostSource.revision != topologyRevision {
			return false
		}
	}
	if s.privateStorage.InternalMetadataDescriber != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata != nil && s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalKeyspaceMap.Load() != schemaSource {
		return false
	}
	InternalSessionUpdatePreparedSchemaSnapshot(s, old, m)
	p.InternalSnapshot.Store(m)
	return true
}
func InternalSnapshotV2Host(s *Session, h *HostInfo) (HostSnapshot, error) {
	out, tokenErr := implEndpoint.SnapshotHost(h)
	if h == nil {
		return out, tokenErr
	}
	if s != nil {
		if cfg, ok := s.v2PoolConfig(); ok {
			if path, _ := connectivity.UnixContact(*cfg); path != "" {
				out.Address = netip.AddrPort{}
			}
		}
	}
	if s != nil {
		out.Enabled = !InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, h)
		if s.privateStorage.InternalPool != nil {
			pool, ok := s.privateStorage.InternalPool.InternalGetPool(h)
			out.Connected = ok && pool.Size() > 0
		}
	}
	return out, tokenErr
}
func InternalSessionObserveV2Lifecycle(s *Session, event LifecycleEvent) {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return
	}
	p.InternalMu.Lock()
	observer := p.InternalConfig.LifecycleObserver
	p.InternalMu.Unlock()
	if observer == nil {
		return
	}
	InternalSessionObserveSafely(s, func() { observer.ObserveLifecycle(event) })
}

// RefreshMetadata waits for the shared topology refresh under the caller's
// context. Metadata queries themselves use the session metadata timeout.
func (s *Session) RefreshMetadata(ctx context.Context) error {
	if ctx == nil {
		return typeValidation("context", "nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.Closed() {
		return ErrClosed
	}
	if s.privateStorage.InternalRingRefresher == nil {
		return errNoControl
	}
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	var topologyErr error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-s.privateStorage.InternalRingRefresher.RefreshNow():
		if !ok {
			return ErrClosed
		}
		topologyErr = err
	}
	var routeErr error
	if s.privateStorage.InternalClientRoutesHandler != nil {
		routeErr = s.privateStorage.InternalClientRoutesHandler.refreshContext(ctx)
	}
	schemaErr := InternalSessionRefreshV2AllSchema(s, ctx)
	if topologyErr != nil || routeErr != nil {
		return errors.Join(&PartialRefreshError{Generation: s.Metadata().Generation(), Topology: topologyErr, ClientRoutes: routeErr}, schemaErr)
	}
	return schemaErr
}

// routingPartitioner chooses the configured server/table partitioner. Unknown
// names deliberately leave routing tokenless; hashing with a guessed algorithm
// would send requests to the wrong replicas.
func (s *Session) routingPartitioner(keyspace, table string) TokenPartitioner {
	return InternalSessionCapturedRoutingPartitioner(s, InternalSessionPreparedPartitionerName(s, keyspace, table))
}

// Trace reads tracing tables through the same typed QUERY/codec path as user
// requests. Readiness polling is governed by one context across every round.
func (s *Session) Trace(ctx context.Context, id UUID) (TraceInfo, error) {
	result := TraceInfo{ID: id}
	if ctx == nil {
		return result, typeValidation("trace context", "nil context")
	}
	if s == nil || s.Closed() {
		return result, ErrClosed
	}
	owner := InternalCurrentExecution(s)
	if owner == nil {
		return result, ErrNoConnections
	}
	return owner.Trace(ctx, id)
}

// readV2SystemRows is the context-bound typed control-query seam. It detaches
// metadata values before releasing each page and never invokes legacy Scan.
func InternalReadV2SystemRows[P, R any](ctx context.Context, conn *Conn, statement Statement[P, R], columns Columns, value P) ([]R, error) {
	if conn == nil {
		return nil, typeValidation("system query", "nil connection")
	}
	return InternalReadV2SystemRowsVia(ctx, conn.InternalVersion, conn.InternalGetSystemRequestState().InternalTimeout, conn.InternalExecuteV2System, statement, columns, value)
}
func InternalReadV2SystemRowsVia[P, R any](ctx context.Context, version byte, timeout time.Duration, execute func(context.Context, *attemptRequest) (*responsePage, error), statement Statement[P, R], columns Columns, value P) ([]R, error) {
	if ctx == nil || execute == nil {
		return nil, typeValidation("system query", "nil context or connection")
	}
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	return implMetadata.ReadRows(ctx, version, timeout, func(ctx context.Context, request *attemptRequest) (*model.ResponseData, error) {
		response, err := execute(ctx, request)
		if response == nil {
			return nil, err
		}
		return &response.InternalResponseData, err
	}, implMetadata.Query[P, R]{CQL: statement.CQL, Params: statement.Params, Rows: statement.Rows}, columns, value)
}

type InternalAgreementPeer struct {
	id      Nullable[UUID]
	version Nullable[UUID]
}

func (s *Session) awaitV2SchemaAgreement(ctx context.Context) error {
	if ctx == nil {
		return typeValidation("schema agreement context", "nil context")
	}
	if s == nil || s.Closed() {
		return ErrClosed
	}
	return s.awaitV2SchemaAgreementUsing(ctx, nil, UUID{})
}
func InternalSessionAwaitV2SchemaAgreementOn(s *Session, ctx context.Context, conn *Conn) error {
	if ctx == nil || conn == nil || conn.InternalHost == nil {
		return typeValidation("schema agreement", "nil context or coordinator")
	}
	if s == nil || s.Closed() {
		return ErrClosed
	}
	return s.awaitV2SchemaAgreementUsing(ctx, conn, implEndpoint.InternalHostInfoHostUUID(conn.InternalHost))
}
func (s *Session) awaitV2SchemaAgreementUsing(ctx context.Context, coordinator *Conn, required UUID) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	p.InternalMu.Lock()
	options := p.InternalConfig.Metadata
	p.InternalMu.Unlock()
	timeout := options.AgreementTimeout
	if timeout == 0 {
		timeout = s.privateStorage.InternalCfg.MaxWaitSchemaAgreement
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	interval := options.AgreementPoll
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.Closed() {
			return ErrClosed
		}
		// Retain one immutable scope for this check. A missing/down enabled host
		// prevents agreement; each retry takes a fresh snapshot.
		scope := make(map[UUID][]*Conn)
		for _, h := range routing.SnapshotData(s.Metadata()).InternalHosts {
			if h.Enabled {
				scope[h.ID] = s.v2AgreementConnections(h.ID)
			}
		}
		if coordinator != nil {
			scope[required] = append([]*Conn{coordinator}, scope[required]...)
		}
		if len(scope) == 0 {
			return &NoHostsError{Operation: "schema agreement", Causes: []error{ErrNoConnections}}
		}
		type observation struct {
			version UUID
			err     error
		}
		results := make(chan observation, len(scope))
		// Fixed worker count bounds goroutines even with large topologies.
		jobs := make(chan struct {
			id    UUID
			conns []*Conn
		}, len(scope))
		for id, conns := range scope {
			jobs <- struct {
				id    UUID
				conns []*Conn
			}{id, conns}
		}
		close(jobs)
		var workers sync.WaitGroup
		for i := 0; i < min(16, len(scope)); i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for job := range jobs {
					v, err := InternalReadV2AgreementHost(ctx, job.id, job.conns)
					results <- observation{v, err}
				}
			}()
		}
		workers.Wait()
		close(results)
		complete := true
		versions := make(map[UUID]bool)
		var terminal error
		for result := range results {
			if result.err != nil {
				complete = false
				if InternalTerminalV2AgreementError(result.err) {
					terminal = result.err
				}
			} else {
				versions[result.version] = true
			}
		}
		if terminal != nil {
			return terminal
		}
		if complete && len(versions) == 1 {
			return nil
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func InternalReadV2AgreementHost(ctx context.Context, id UUID, conns []*Conn) (UUID, error) {
	stmt := Statement[Unit, InternalAgreementPeer]{CQL: "SELECT host_id, schema_version FROM system.local WHERE key = 'local'", Params: NoParams, Rows: Row(Column("host_id", NullableCodec(UUIDCodec), func(r *InternalAgreementPeer) *Nullable[UUID] { return &r.id }), Column("schema_version", NullableCodec(UUIDCodec), func(r *InternalAgreementPeer) *Nullable[UUID] { return &r.version }))}
	var last error = ErrNoConnections
	for _, conn := range conns {
		if conn == nil || conn.Closed() {
			continue
		}
		timeout := time.Duration(0)
		if conn.InternalCfg != nil && conn.InternalCfg.TypedConfig != nil {
			timeout = conn.InternalCfg.TypedConfig.Metadata.RequestTimeout
		}
		rows, err := InternalReadV2SystemRowsVia(ctx, conn.InternalVersion, timeout, conn.InternalExecuteV2System, stmt, NewColumns(), Unit{})
		if err != nil {
			if InternalTerminalV2AgreementError(err) {
				return UUID{}, err
			}
			last = err
			continue
		}
		if len(rows) != 1 || !rows[0].id.Valid || !rows[0].version.Valid || rows[0].id.Value != id || rows[0].version.Value.IsEmpty() {
			return UUID{}, typeValidation("schema agreement", "missing/mismatched host identity or version")
		}
		return rows[0].version.Value, nil
	}
	return UUID{}, last
}
func InternalTerminalV2AgreementError(err error) bool {
	var protocol *ProtocolError
	var validation *ValidationError
	if errors.As(err, &protocol) || errors.As(err, &validation) {
		return true
	}
	var server RequestError
	if errors.As(err, &server) {
		switch server.GetCode() {
		case ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeReadTimeout, ErrCodeWriteTimeout, ErrCodeUnprepared:
			return false
		default:
			return true
		}
	}
	return false
}
func (s *Session) v2AgreementConnections(id UUID) []*Conn {
	if s.privateStorage.InternalPool == nil {
		return nil
	}
	s.privateStorage.InternalPool.InternalMu.RLock()
	pool := s.privateStorage.InternalPool.InternalHostConnPools[id]
	s.privateStorage.InternalPool.InternalMu.RUnlock()
	if pool == nil {
		return nil
	}
	pool.InternalMu.RLock()
	defer pool.InternalMu.RUnlock()
	if pool.InternalClosed {
		return nil
	}
	var out []*Conn
	switch picker := pool.InternalConnPicker.(type) {
	case *typedConnPicker:
		picker.InternalMu.RLock()
		for _, cs := range picker.InternalConns {
			out = append(out, cs...)
		}
		out = append(out, picker.InternalExcess...)
		picker.InternalMu.RUnlock()
	case *defaultConnPicker:
		picker.InternalMu.RLock()
		out = append(out, picker.InternalConns...)
		picker.InternalMu.RUnlock()
	case *scyllaConnPicker:
		picker.InternalMu.RLock()
		out = append(out, picker.InternalConns...)
		out = append(out, picker.InternalExcessConns...)
		picker.InternalMu.RUnlock()
	}
	return out
}

type InternalV2SystemQueryer interface {
	InternalQueryV2System(context.Context, *attemptRequest) (*responsePage, error)
}

func (c *InternalControlConn) InternalQueryV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	h := c.InternalGetConn()
	if h == nil || h.InternalConn == nil {
		return nil, ErrNoConnections
	}
	conn, ok := h.InternalConn.(*Conn)
	if !ok {
		return nil, typeValidation("system query", "connection does not support typed requests")
	}
	return conn.InternalExecuteV2System(ctx, request)
}

func InternalSessionRefreshV2AllSchema(s *Session, ctx context.Context) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.InternalMu.Lock()
	config := p.InternalConfig
	epochs := maps.Clone(p.InternalSchemaEpoch)
	revisions := maps.Clone(p.schemaRevision)
	p.InternalMu.Unlock()
	if config.DefaultProfile != nil && config.Metadata.Mode == MetadataDisabled {
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	rows, err := queryV2SchemaRows(ctx, InternalNewSchemaSource(s), `SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces`, nil, v2DecodeKeyspace())
	if err != nil {
		return err
	}
	keyspaces := make(map[string]*KeyspaceMetadata, len(rows))
	allowed := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, name := range config.Metadata.Keyspaces {
		allowed[name] = true
	}
	for _, row := range rows {
		if row.InternalName == "" {
			return typeValidation("keyspace schema", "missing keyspace identity")
		}
		if _, ok := keyspaces[row.InternalName]; ok {
			return typeValidation("keyspace schema", "duplicate identity %q", row.InternalName)
		}
		if len(allowed) > 0 && !allowed[row.InternalName] {
			continue
		}
		keyspaces[row.InternalName] = v2KeyspaceFromRow(row.InternalName, row)
	}
	if err := readV2ScyllaKeyspaces(ctx, InternalNewSchemaSource(s), keyspaces, ""); err != nil {
		return err
	}

	failures := make(map[string]error)

	for name, base := range keyspaces {
		for attempt := 0; attempt < 8; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			p.InternalMu.Lock()
			epoch := p.InternalSchemaEpoch[name]
			revision := p.schemaRevision[name]
			tables := p.captureTableRevisionsLocked(name)
			p.InternalMu.Unlock()
			if attempt > 0 {
				base, err = getV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
				if errors.Is(err, ErrKeyspaceDoesNotExist) {
					if s.dropV2KeyspaceIfCurrent(name, epoch, revision) {
						delete(failures, name)
						break
					}
					continue
				}
				if err != nil {
					failures[name] = err
					break
				}
			}
			var ks *KeyspaceMetadata
			if config.DefaultProfile != nil && config.Metadata.Mode == MetadataMinimal {
				ks = base
				ks.Tables = make(map[string]*TableMetadata)
			} else {
				ks, err = fetchV2Keyspace(ctx, InternalNewSchemaSource(s), base)
			}
			if err != nil {
				// A DROP can occur after the keyspace enumeration but before its
				// schema reads. Confirm absence before treating it as stale data.
				dropped, probeErr := s.confirmV2DroppedKeyspace(ctx, name)
				if dropped {
					delete(failures, name)
					break
				}
				if probeErr != nil && ctx.Err() != nil {
					return ctx.Err()
				}
			} else {
				// Empty table/type result sets are legal, so they cannot by
				// themselves reveal a keyspace dropped during this fetch.
				current, checkErr := getV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
				if errors.Is(checkErr, ErrKeyspaceDoesNotExist) {
					if s.dropV2KeyspaceIfCurrent(name, epoch, revision) {
						delete(failures, name)
						break
					}
					continue
				}
				if checkErr != nil {
					failures[name] = checkErr
					break
				}
				if current.StrategyClass != base.StrategyClass || !compareInterfaceMaps(current.StrategyOptions, base.StrategyOptions) {
					failures[name] = typeValidation("schema refresh", "replication changed during refresh")
					continue
				}
			}
			p.InternalMu.Lock()
			changed := p.InternalSchemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables)
			if changed {
				p.InternalMu.Unlock()
				failures[name] = fmt.Errorf("schema changed during refresh")
				continue
			}
			if err != nil {
				p.InternalMu.Unlock()
				failures[name] = err
				break
			}
			if s.privateStorage.InternalMetadataDescriber != nil {
				s.invalidateV2ChangedTablets(name, ks)
				s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalSet(name, ks)
				p.schemaRevision[name]++
				delete(p.schemaStale, name)
				delete(p.schemaDropped, name)
			}
			p.InternalMu.Unlock()
			delete(failures, name)
			break
		}
	}

	if s.privateStorage.InternalMetadataDescriber != nil {
		for name := range s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGet() {
			if len(allowed) > 0 && !allowed[name] {
				continue
			}
			if _, exists := keyspaces[name]; exists {
				continue
			}
			p.InternalMu.Lock()
			if p.InternalSchemaEpoch[name] == epochs[name] && p.schemaRevision[name] == revisions[name] {
				p.schemaRevision[name]++
				s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
				delete(p.schemaStale, name)
				s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
			}
			p.InternalMu.Unlock()
		}
	}
	p.InternalMu.Lock()
	p.refreshDiagnostics = nil
	for name, cause := range failures {
		p.refreshDiagnostics = append(p.refreshDiagnostics, fmt.Errorf("keyspace %s: %w", name, cause))
	}
	p.InternalMu.Unlock()
	p.schemaGeneration.Add(1)
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	if len(failures) > 0 {
		s.scheduleV2SchemaRetry()
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: failures}
	}
	return nil
}
func InternalSessionRefreshV2Keyspace(s *Session, ctx context.Context, name string) error {
	p := InternalSessionPeripheral(s)
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	return s.refreshV2KeyspaceAttempt(ctx, name, 0)
}
func (s *Session) refreshV2KeyspaceAttempt(ctx context.Context, name string, attempt int) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaEpoch[name]
	revision := p.schemaRevision[name]
	tables := p.captureTableRevisionsLocked(name)
	config := p.InternalConfig
	p.InternalMu.Unlock()
	if config.DefaultProfile != nil && (config.Metadata.Mode == MetadataDisabled || len(config.Metadata.Keyspaces) > 0 && !slices.Contains(config.Metadata.Keyspaces, name)) {
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	base, err := getV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
	if errors.Is(err, ErrKeyspaceDoesNotExist) {
		p.InternalMu.Lock()
		if p.InternalSchemaEpoch[name] == epoch && p.schemaRevision[name] == revision {
			p.schemaRevision[name]++
			s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
			delete(p.schemaStale, name)
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
		}
		p.InternalMu.Unlock()
		if !InternalSessionPublishV2Metadata(s) {
			return s.v2MetadataPublicationError()
		}
		return err
	}
	if err != nil {
		return s.v2SchemaRefreshFailure(name, err)
	}
	var ks *KeyspaceMetadata
	if config.DefaultProfile != nil && config.Metadata.Mode == MetadataMinimal {
		ks = base
		ks.Tables = make(map[string]*TableMetadata)
	} else {
		ks, err = fetchV2Keyspace(ctx, InternalNewSchemaSource(s), base)
	}
	if err != nil {
		return s.v2SchemaRefreshFailure(name, err)
	}
	p.InternalMu.Lock()
	if p.InternalSchemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables) {
		p.InternalMu.Unlock()
		if attempt < 7 {
			return s.refreshV2KeyspaceAttempt(ctx, name, attempt+1)
		}
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: fmt.Errorf("schema changed during refresh")}}
	}
	s.invalidateV2ChangedTablets(name, ks)
	s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalSet(name, ks)
	p.schemaRevision[name]++
	delete(p.schemaStale, name)
	delete(p.schemaDropped, name)
	p.schemaGeneration.Add(1)
	p.InternalMu.Unlock()
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	return nil
}
func (s *Session) scheduleV2SchemaRetry() {
	InternalSessionPeripheral(s).schemaRetryNeeded.Store(true)
	InternalSessionStartV2SchemaRetry(s)
}
func InternalSessionStartV2SchemaRetry(s *Session) {
	p := InternalSessionPeripheral(s)
	if !p.InternalRetryScheduled.CompareAndSwap(false, true) {
		return
	}
	if !p.InternalStartWorker(func() {
		ctx := v2SchemaContext(InternalNewSchemaSource(s))
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-p.InternalDone:
			p.InternalRetryScheduled.Store(false)
			return
		case <-ctx.Done():
			p.InternalRetryScheduled.Store(false)
			return
		case <-timer.C:
		}
		p.InternalMu.Lock()
		closed := p.closed
		timeout := p.InternalConfig.Metadata.RequestTimeout
		scopes := maps.Clone(p.InternalPreparedRefreshScopes)
		p.InternalMu.Unlock()
		if closed {
			p.InternalRetryScheduled.Store(false)
			return
		}
		ctx, cancel := InternalExecutionContext(ctx, timeout)
		if p.schemaRetryNeeded.Swap(false) {
			if err := InternalSessionRefreshV2AllSchema(s, ctx); err != nil {
				p.schemaRetryNeeded.Store(true)
			}
		}
		for keyspace, version := range scopes {
			if err := s.reconcileV2PreparedScope(ctx, keyspace); err == nil {
				p.InternalMu.Lock()
				if p.InternalPreparedRefreshScopes[keyspace] == version {
					delete(p.InternalPreparedRefreshScopes, keyspace)
				}
				p.InternalMu.Unlock()
				p.InternalSchemaReconciled.Add(1)
			}
		}
		cancel()
		p.InternalRetryScheduled.Store(false)
		p.InternalMu.Lock()
		pending := len(p.InternalPreparedRefreshScopes) > 0
		p.InternalMu.Unlock()
		if (pending || p.schemaRetryNeeded.Load()) && !s.Closed() {
			InternalSessionStartV2SchemaRetry(s)
		}
	}) {
		p.InternalRetryScheduled.Store(false)
	}
}

// typedSchemaGeneration deliberately excludes topology and tablet-cache updates.
// Those changes must not invalidate every prepared statement.
func InternalSessionTypedSchemaGeneration(s *Session) uint64 {
	p := InternalSessionPeripheral(s)
	if p == nil {
		return 0
	}
	return p.schemaGeneration.Load() + 1
}

func InternalSessionRecordV2Latency(s *Session, latency time.Duration) {
	if s == nil || latency < 0 {
		return
	}
	p := InternalSessionPeripheral(s)
	index := p.latencyCount.Add(1) - 1
	p.latency[index%uint64(len(p.latency))].Store(int64(latency))

}
func (p *InternalV2PeripheralState) InternalPublishLatencySummary() {
	count := min(p.latencyCount.Load(), uint64(len(p.latency)))
	if count == 0 {
		return
	}
	samples := make([]time.Duration, int(count))
	for i := range samples {
		samples[i] = time.Duration(p.latency[i].Load())
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	percentile := func(percent int) time.Duration { return samples[(len(samples)*percent+99)/100-1] }
	p.latencySummary.Store(&InternalV2LatencySummary{p50: percentile(50), p95: percentile(95), p99: percentile(99)})
}

func InternalSessionRecordV2Orphan(s *Session) {
	if s != nil {
		InternalSessionPeripheral(s).orphans.Add(1)
	}
}

func InternalSessionMarkV2SchemaStale(s *Session, keyspace string, dropped bool) {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	if dropped {
		delete(p.schemaStale, keyspace)
	} else {
		p.schemaStale[keyspace] = true
	}
	p.InternalMu.Unlock()
}
func (s *Session) invalidateV2ChangedTablets(keyspace string, current *KeyspaceMetadata) {
	if s.privateStorage.InternalMetadataDescriber == nil {
		return
	}
	previous, _ := s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspace)
	if previous == nil {
		if m := InternalSessionPeripheral(s).InternalSnapshot.Load(); m != nil {
			previous = routing.SnapshotData(m).InternalKeyspaces[keyspace]
		}
	}
	if previous == nil {
		return
	}
	if !compareInterfaceMaps(previous.StrategyOptions, current.StrategyOptions) || previous.StrategyClass != current.StrategyClass {
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(keyspace)
		return
	}
	for name, table := range previous.Tables {
		next := current.Tables[name]
		if next == nil || !table.Equals(next) {
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithTable(keyspace, name)
		}
	}
}

func (s *Session) dropV2KeyspaceIfCurrent(name string, epoch uint64, revision ...uint64) bool {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.InternalSchemaEpoch[name] != epoch || len(revision) > 0 && p.schemaRevision[name] != revision[0] {
		return false
	}
	p.schemaRevision[name]++
	if s.privateStorage.InternalMetadataDescriber != nil {
		s.privateStorage.InternalMetadataDescriber.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(name)
		s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithKeyspace(name)
	}
	delete(p.schemaStale, name)
	p.schemaDropped[name] = true
	return true
}
func (s *Session) confirmV2DroppedKeyspace(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaEpoch[name]
	revision := p.schemaRevision[name]
	p.InternalMu.Unlock()
	_, err := getV2KeyspaceMetadata(ctx, InternalNewSchemaSource(s), name)
	if errors.Is(err, ErrKeyspaceDoesNotExist) {
		return s.dropV2KeyspaceIfCurrent(name, epoch, revision), nil
	}
	return false, err
}

func (s *Session) runV2PeriodicRefresh(options MetadataOptions) {
	ticker := time.NewTicker(options.PeriodicRefresh)
	defer ticker.Stop()
	p := InternalSessionPeripheral(s)
	for {
		select {
		case <-p.InternalDone:
			return
		case <-ticker.C:
		}
		ctx := v2SchemaContext(InternalNewSchemaSource(s))
		timeout := options.RequestTimeout
		ctx, cancel := InternalExecutionContext(ctx, timeout)
		err := s.RefreshMetadata(ctx)
		cancel()
		if err != nil && s.privateStorage.InternalLogger != nil {
			s.privateStorage.InternalLogger.Printf("gocql: periodic metadata refresh: %v", err)
		}
	}
}

func (s *Session) v2MetadataDepthLimit() int {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	limit := p.InternalConfig.Limits.MaxMetadataDepth
	p.InternalMu.Unlock()
	if limit <= 0 {
		return 64
	}
	return limit
}

// startWorker serializes admission with shutdown; Add can never race Wait.
func (p *InternalV2PeripheralState) InternalStartWorker(fn func()) bool {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	go func() { defer p.workers.Done(); fn() }()
	return true
}

func (p *InternalV2PeripheralState) admitMetadataWork() bool {
	p.InternalMu.Lock()
	defer p.InternalMu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	return true
}

func (p *InternalV2PeripheralState) captureTableRevisionsLocked(keyspace string) map[metadataTable]uint64 {
	out := make(map[metadataTable]uint64)
	for key, revision := range p.InternalTableRevision {
		if key.InternalKeyspace == keyspace {
			out[key] = revision
		}
	}
	return out
}
func (p *InternalV2PeripheralState) tableRevisionsCurrentLocked(keyspace string, before map[metadataTable]uint64) bool {
	for key, revision := range p.InternalTableRevision {
		if key.InternalKeyspace == keyspace && before[key] != revision {
			return false
		}
	}
	return true
}

func (s *Session) v2SchemaRefreshFailure(name string, cause error) error {
	p := InternalSessionPeripheral(s)
	p.InternalMu.Lock()
	if !p.schemaDropped[name] {
		p.schemaStale[name] = true
	}
	p.refreshDiagnostics = []error{fmt.Errorf("keyspace %s: %w", name, cause)}
	p.InternalMu.Unlock()
	InternalSessionPublishV2Metadata(s)
	s.scheduleV2SchemaRetry()
	return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: cause}}
}

func (s *Session) v2MetadataPublicationError() error {
	return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{"<publication>": fmt.Errorf("metadata sources changed throughout publication; refresh again")}}
}
