package gocql

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/tablets"
	"golang.org/x/sync/errgroup"
)

type HostSnapshot struct {
	ID                     UUID
	Address                netip.AddrPort
	Datacenter, Rack       string
	Tokens                 []int64
	Enabled, Connected, Up bool
	Shards                 uint32
}
type TokenRange struct {
	Start, End int64
	Replicas   []Target
}
type MetadataSnapshot struct {
	hostIndex       *routingHostIndex
	placements      *tabletPlacementTree
	placementEpoch  uint64
	placementSignal *atomic.Uint64
	generation      uint64
	hosts           []HostSnapshot
	keyspaces       map[string]*KeyspaceMetadata
	schemaSources   map[string]*KeyspaceMetadata
	ranges          map[string][]TokenRange
	tablets         map[metadataTable][]TokenRange
	partitioner     string
	diagnostics     []error
}
type metadataTable struct{ keyspace, table string }

func (m *MetadataSnapshot) Generation() uint64 {
	if m == nil {
		return 0
	}
	return m.generation
}
func (m *MetadataSnapshot) Hosts() []HostSnapshot {
	if m == nil {
		return nil
	}
	out := append([]HostSnapshot(nil), m.hosts...)
	for i := range out {
		out[i].Tokens = append([]int64(nil), out[i].Tokens...)
	}
	return out
}
func (m *MetadataSnapshot) Keyspace(name string) (*KeyspaceMetadata, bool) {
	if m == nil {
		return nil, false
	}
	ks, ok := m.keyspaces[name]
	if !ok {
		return nil, false
	}
	return cloneV2Keyspace(ks), true
}
func (m *MetadataSnapshot) Replicas(keyspace, table string, token int64) ([]Target, error) {
	if _, _, err := m.lookupRanges(keyspace, table); err != nil {
		return nil, err
	}
	r, _ := m.placementReplicas(keyspace, table, token)
	return append([]Target(nil), r...), nil
}
func (m *MetadataSnapshot) TokenRanges(keyspace, table string) ([]TokenRange, error) {
	if root, ok := m.tabletRoot(metadataTable{keyspace, table}); ok {
		if _, _, err := m.lookupRanges(keyspace, table); err != nil {
			return nil, err
		}
		var out []TokenRange
		placementEach(root, func(_ int64, r *tabletRange) {
			v := r.span
			v.Replicas = append([]Target(nil), v.Replicas...)
			out = append(out, v)
		})
		return out, nil
	}
	r, _, e := m.lookupRanges(keyspace, table)
	if e != nil {
		return nil, e
	}
	out := append([]TokenRange(nil), r...)
	for i := range out {
		out[i].Replicas = append([]Target(nil), out[i].Replicas...)
	}
	return out, nil
}
func (m *MetadataSnapshot) lookupRanges(keyspace, table string) ([]TokenRange, bool, error) {
	if m == nil {
		return nil, false, ErrNoKeyspace
	}
	ks, ok := m.keyspaces[keyspace]
	if !ok {
		return nil, false, fmt.Errorf("%s: %w", keyspace, ErrNoKeyspace)
	}
	if table != "" {
		if _, ok := ks.Tables[table]; !ok {
			if _, ok := ks.Views[table]; !ok {
				return nil, false, fmt.Errorf("%s.%s: %w", keyspace, table, ErrNoTable)
			}
		}
		if r, ok := m.tablets[metadataTable{keyspace, table}]; ok {
			return r, true, nil
		}
	}
	return m.ranges[keyspace], false, nil
}

// Diagnostics reports routing/metadata omissions without substituting fabricated tokens.
func (m *MetadataSnapshot) Diagnostics() []error {
	if m == nil {
		return nil
	}
	return append([]error(nil), m.diagnostics...)
}

type MetricsSnapshot struct {
	Requests, Attempts, Errors, Retries, Speculations, Cancelled, Orphans uint64
	InFlight                                                              int64
	LatencyP50, LatencyP95, LatencyP99                                    time.Duration
}
type LifecycleObserver interface{ ObserveLifecycle(LifecycleEvent) }
type LifecycleEvent struct {
	Kind, Stage string
	HostID      UUID
	Stream      int16
	At          time.Time
	Err         error
}
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

type v2PeripheralState struct {
	snapshot               atomic.Pointer[MetadataSnapshot]
	publishMu              sync.Mutex
	workers                sync.WaitGroup
	hostMu                 sync.Mutex
	publicationRevision    uint64
	mu                     sync.Mutex
	subscribers            map[*Subscription]struct{}
	closed                 bool
	config                 Config
	schemaEpoch            map[string]uint64
	schemaRevision         map[string]uint64
	tableRevision          map[metadataTable]uint64
	schemaDropped          map[string]bool
	retryScheduled         atomic.Bool
	schemaRetryNeeded      atomic.Bool
	preparedRefreshScopes  map[string]uint64
	preparedRefreshVersion uint64
	schemaReconciled       atomic.Uint64
	periodicOnce           sync.Once
	schemaGeneration       atomic.Uint64
	placementEpoch         atomic.Uint64
	feedbackOnce           sync.Once
	feedbackMu             sync.Mutex
	feedback               map[v2TabletFeedbackKey]v2PendingTabletFeedback
	feedbackWake           chan struct{}
	done                   chan struct{}
	latencyMu              sync.Mutex
	latency                [1024]time.Duration
	latencyCount           uint64
	orphans                atomic.Uint64
	schemaStale            map[string]bool
	refreshDiagnostics     []error
	routeDiagnostic        error
	feedbackResync         bool
	knownTabletTables      map[metadataTable]bool
}

func (s *Session) peripheral() *v2PeripheralState {
	if s == nil {
		return nil
	}
	if p := s.v2Peripheral.Load(); p != nil {
		return p
	}
	p := &v2PeripheralState{subscribers: make(map[*Subscription]struct{}), schemaEpoch: make(map[string]uint64), schemaRevision: make(map[string]uint64), tableRevision: make(map[metadataTable]uint64), schemaDropped: make(map[string]bool), schemaStale: make(map[string]bool), feedback: make(map[v2TabletFeedbackKey]v2PendingTabletFeedback), feedbackWake: make(chan struct{}, 1), knownTabletTables: make(map[metadataTable]bool), done: make(chan struct{})}
	if s.v2Peripheral.CompareAndSwap(nil, p) {
		return p
	}
	return s.v2Peripheral.Load()
}
func (s *Session) initV2Peripheral(config Config) error {
	s.configureV2Peripheral(config)
	p := s.peripheral()
	p.feedbackOnce.Do(func() { p.startWorker(func() { s.runV2TabletFeedback(p) }) })
	if !s.publishV2Metadata() {
		return s.v2MetadataPublicationError()
	}
	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Snapshot().Routing; policy != nil {
			if err := policy.Validate(s.Metadata()); err != nil {
				return err
			}
		}
	}
	s.observeV2Lifecycle(LifecycleEvent{Kind: "session", Stage: "ready", At: time.Now()})
	if config.Metadata.PeriodicRefresh > 0 {
		s.peripheral().periodicOnce.Do(func() { s.peripheral().startWorker(func() { s.runV2PeriodicRefresh(config.Metadata) }) })
	}
	return nil
}
func (s *Session) configureV2Peripheral(config Config) {
	p := s.peripheral()
	p.mu.Lock()
	p.config = config
	p.mu.Unlock()
}
func (s *Session) Metadata() *MetadataSnapshot {
	p := s.peripheral()
	if p == nil {
		return &MetadataSnapshot{}
	}
	if snapshot := p.snapshot.Load(); snapshot != nil {
		return snapshot
	}
	for {
		s.publishV2Metadata()
		if snapshot := p.snapshot.Load(); snapshot != nil {
			return snapshot
		}
	}
}
func (s *Session) Metrics() MetricsSnapshot {
	if s == nil {
		return MetricsSnapshot{}
	}
	result := s.typedRuntime().metricsSnapshot()
	p := s.peripheral()
	result.Orphans = p.orphans.Load()
	p.latencyMu.Lock()
	count := min(p.latencyCount, uint64(len(p.latency)))
	latencies := append([]time.Duration(nil), p.latency[:int(count)]...)
	p.latencyMu.Unlock()
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		percentile := func(percent int) time.Duration { index := (len(latencies)*percent+99)/100 - 1; return latencies[index] }
		result.LatencyP50 = percentile(50)
		result.LatencyP95 = percentile(95)
		result.LatencyP99 = percentile(99)
	}
	return result
}
func (s *Session) publishV2Metadata() bool {
	for attempt := 0; attempt < 8; attempt++ {
		if s.tryPublishV2Metadata() {
			return true
		}
	}
	return false
}
func (s *Session) tryPublishV2Metadata() bool {
	p := s.peripheral()
	if p == nil {
		return false
	}
	old := p.snapshot.Load()
	p.mu.Lock()
	revision := p.publicationRevision
	stale := maps.Clone(p.schemaStale)
	config := p.config
	diagnostics := append([]error(nil), p.refreshDiagnostics...)
	if p.routeDiagnostic != nil {
		diagnostics = append(diagnostics, p.routeDiagnostic)
	}
	p.mu.Unlock()
	m := &MetadataSnapshot{generation: 1, keyspaces: make(map[string]*KeyspaceMetadata), ranges: make(map[string][]TokenRange), tablets: make(map[metadataTable][]TokenRange)}
	m.diagnostics = diagnostics
	if old != nil {
		m.generation = old.generation + 1
	}
	var hosts []*HostInfo
	var topologyRevision uint64
	if s.hostSource != nil {
		s.hostSource.mu.RLock()
		topologyRevision = s.hostSource.revision
		m.diagnostics = append(m.diagnostics, s.hostSource.diagnostics...)
		for _, h := range s.hostSource.hosts {
			hosts = append(hosts, h)
		}
		s.hostSource.mu.RUnlock()
	}
	var schemaSource *map[string]*KeyspaceMetadata
	if s.metadataDescriber != nil && s.metadataDescriber.metadata != nil {
		schemaSource = s.metadataDescriber.metadata.keyspaceMetadata.keyspaceMap.Load()
	}

	replicaHosts := make([]*HostInfo, 0, len(hosts))
	validTokens := true
	for _, h := range hosts {
		snapshot, err := snapshotV2Host(s, h)
		if err != nil {
			m.diagnostics = append(m.diagnostics, err)
			validTokens = false
		}
		m.hosts = append(m.hosts, snapshot)
		if m.partitioner == "" {
			m.partitioner = h.Partitioner()
		}
		replicaHosts = append(replicaHosts, &HostInfo{hostId: snapshot.ID, dataCenter: snapshot.Datacenter, rack: snapshot.Rack, tokens: append([]string(nil), h.Tokens()...)})
	}
	sort.Slice(m.hosts, func(i, j int) bool { return m.hosts[i].ID.String() < m.hosts[j].ID.String() })
	if len(hosts) > 0 && len(m.hosts) == 0 && old != nil {
		return false
	}
	if s.metadataDescriber != nil && s.metadataDescriber.metadata != nil {
		md := s.metadataDescriber.metadata
		if schemaSource != nil {
			m.schemaSources = *schemaSource
			for name, ks := range *schemaSource {
				if ks != nil {
					if old != nil && old.schemaSources[name] == ks && len(ks.tablesInvalidated) == 0 {
						m.keyspaces[name] = old.keyspaces[name]
					} else {
						m.keyspaces[name] = cloneV2Keyspace(ks)
					}
				}
			}
		}
		if md.tabletsMetadata != nil && (old == nil || old.placements == nil) {
			md.tabletsMetadata.ForEach(func(ks, table string, entries tablets.TabletEntryList) bool {
				r := make([]TokenRange, len(entries))
				for i, e := range entries {
					r[i] = TokenRange{Start: e.FirstToken(), End: e.LastToken()}
					for _, replica := range e.ReplicasUnsafe() {
						r[i].Replicas = append(r[i].Replicas, Target{HostID: UUID(replica.HostUUIDValue()), ShardID: Option[uint32]{Value: uint32(replica.ShardID()), Set: replica.ShardID() >= 0}})
					}
				}
				m.tablets[metadataTable{ks, table}] = r
				return true
			})
		}
	}
	if old != nil {
		for name, prior := range old.keyspaces {
			if m.keyspaces[name] == nil && stale[name] {
				m.keyspaces[name] = cloneV2Keyspace(prior)
			}
			if current := m.keyspaces[name]; current != nil {
				for table := range current.tablesInvalidated {
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
		for table := range old.tablets {
			if ks := m.keyspaces[table.keyspace]; ks != nil {
				if _, ok := ks.Tables[table.table]; ok {
					if _, known := m.tablets[table]; !known {
						m.tablets[table] = nil
					}
				}
			}
		}
	}
	p.feedbackMu.Lock()
	for table := range p.knownTabletTables {
		if _, _, err := m.lookupRanges(table.keyspace, table.table); err != nil {
			delete(p.knownTabletTables, table)
			continue
		}
		if ks := m.keyspaces[table.keyspace]; ks != nil {
			if _, exists := ks.Tables[table.table]; exists {
				if _, known := m.tablets[table]; !known {
					m.tablets[table] = nil
				}
			}
		}
	}
	p.feedbackMu.Unlock()
	// Reuse the established replication-strategy placement on private cold-path
	// descriptors. Invalid tokens were rejected before ring construction.
	if m.partitioner != "" && validTokens {
		partitioner := m.partitioner
		if config.Partitioners[m.partitioner] != nil {
			partitioner = "Murmur3Partitioner"
		}
		ring, err := newTokenRing(partitioner, replicaHosts)
		if err != nil {
			m.diagnostics = append(m.diagnostics, err)
		} else if _, ok := ring.partitioner.(murmur3Partitioner); ok {
			for ks, meta := range m.keyspaces {
				ranges, err := buildV2VnodeRanges(ring, meta)
				if err != nil {
					m.diagnostics = append(m.diagnostics, err)
					continue
				}
				m.ranges[ks] = ranges
			}
		} else {
			m.diagnostics = append(m.diagnostics, typeValidation("partitioner", "no configured int64 partitioner for %q", m.partitioner))
		}
	}
	m.placementEpoch = p.placementEpoch.Load()
	m.placementSignal = &p.placementEpoch
	m.placements = reconcileTabletPlacement(old, m)
	m.hostIndex = buildRoutingHostIndex(m.hosts)
	if config.Metadata.Mode != MetadataFull {
		m.diagnostics = append(m.diagnostics, typeValidation("partitioner", "table partitioners outside loaded metadata disable token routing"))
	}
	for keyspace, ks := range m.keyspaces {
		for table, t := range ks.Tables {
			if t != nil && t.Options.Partitioner != "" && config.Partitioners[t.Options.Partitioner] == nil {
				m.diagnostics = append(m.diagnostics, typeValidation("partitioner", "unknown partitioner %q for %s.%s; token routing disabled", t.Options.Partitioner, keyspace, table))
			}
		}
	}

	if config.DefaultProfile != nil {
		if policy := config.DefaultProfile.Snapshot().Routing; policy != nil {
			if err := policy.Validate(m); err != nil {
				m.diagnostics = append(m.diagnostics, err)
			}
		}
	}
	// All potentially user-provided hooks above run before the publication gate.
	// A concurrent tombstone/source replacement rejects this entire stale build.
	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.publicationRevision != revision || p.snapshot.Load() != old {
		return false
	}
	if s.hostSource != nil {
		s.hostSource.mu.RLock()
		defer s.hostSource.mu.RUnlock()
		if s.hostSource.revision != topologyRevision {
			return false
		}
	}
	if s.metadataDescriber != nil && s.metadataDescriber.metadata != nil && s.metadataDescriber.metadata.keyspaceMetadata.keyspaceMap.Load() != schemaSource {
		return false
	}
	s.updatePreparedSchemaSnapshot(old, m)
	p.snapshot.Store(m)
	return true
}
func snapshotV2Host(s *Session, h *HostInfo) (HostSnapshot, error) {
	if h == nil {
		return HostSnapshot{}, typeValidation("host", "nil host")
	}
	h.mu.RLock()
	address, _ := h.connectAddressLocked()
	addr, _ := netip.AddrFromSlice(address)
	out := HostSnapshot{ID: h.hostId, Address: netip.AddrPortFrom(addr.Unmap(), uint16(h.port)), Datacenter: h.dataCenter, Rack: h.rack, Up: h.state == NodeUp, Enabled: true}
	var tokenErr error
	for _, token := range h.tokens {
		n, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			tokenErr = fmt.Errorf("host %s invalid token %q: %w", out.ID, token, err)
			out.Tokens = nil
			break
		}
		out.Tokens = append(out.Tokens, n)
	}
	h.mu.RUnlock()
	out.Shards = uint32(h.ScyllaShardCount())
	if s != nil {
		if cfg, ok := s.v2PoolConfig(); ok {
			if path, _ := unixContact(*cfg); path != "" {
				out.Address = netip.AddrPort{}
			}
		}
	}
	if s != nil {
		out.Enabled = !s.cfg.filterHost(h)
		if s.pool != nil {
			pool, ok := s.pool.getPool(h)
			out.Connected = ok && pool.Size() > 0
		}
	}
	return out, tokenErr
}
func (s *Session) observeV2Lifecycle(event LifecycleEvent) {
	p := s.peripheral()
	if p == nil {
		return
	}
	p.mu.Lock()
	observer := p.config.LifecycleObserver
	p.mu.Unlock()
	if observer == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil && s.logger != nil {
			s.logger.Printf("gocql: lifecycle observer panicked: %v", recovered)
		}
	}()
	observer.ObserveLifecycle(event)
}

func cloneV2Any(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return cloneV2AnyMap(x)
	case map[string]string:
		return maps.Clone(x)
	case []byte:
		return append([]byte(nil), x...)
	case []string:
		return append([]string(nil), x...)
	case []any:
		r := make([]any, len(x))
		for i, v := range x {
			r[i] = cloneV2Any(v)
		}
		return r
	default:
		return v
	}
}
func cloneV2AnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	r := make(map[string]any, len(m))
	for k, v := range m {
		r[k] = cloneV2Any(v)
	}
	return r
}
func cloneV2Column(c *ColumnMetadata) *ColumnMetadata {
	if c == nil {
		return nil
	}
	r := *c
	r.Index.Options = cloneV2AnyMap(c.Index.Options)
	return &r
}
func cloneV2Columns(c map[string]*ColumnMetadata) map[string]*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make(map[string]*ColumnMetadata, len(c))
	for k, v := range c {
		r[k] = cloneV2Column(v)
	}
	return r
}
func cloneV2ColumnList(c []*ColumnMetadata) []*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make([]*ColumnMetadata, len(c))
	for i, v := range c {
		r[i] = cloneV2Column(v)
	}
	return r
}
func cloneV2TableOptions(o TableMetadataOptions) TableMetadataOptions {
	o.Caching = maps.Clone(o.Caching)
	o.Compaction = maps.Clone(o.Compaction)
	o.Compression = maps.Clone(o.Compression)
	o.CDC = maps.Clone(o.CDC)
	return o
}
func cloneV2Function(f FunctionMetadata) FunctionMetadata {
	f.ArgumentTypes = append([]string(nil), f.ArgumentTypes...)
	f.ArgumentNames = append([]string(nil), f.ArgumentNames...)
	return f
}
func cloneV2Keyspace(k *KeyspaceMetadata) *KeyspaceMetadata {
	if k == nil {
		return nil
	}
	r := *k
	r.StrategyOptions = cloneV2AnyMap(k.StrategyOptions)
	r.tablesInvalidated = maps.Clone(k.tablesInvalidated)
	r.Tables = make(map[string]*TableMetadata, len(k.Tables))
	for n, t := range k.Tables {
		if t == nil {
			r.Tables[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = cloneV2AnyMap(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Flags = append([]string(nil), t.Flags...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Tables[n] = &v
	}
	r.Functions = make(map[string]*FunctionMetadata, len(k.Functions))
	for n, f := range k.Functions {
		if f == nil {
			r.Functions[n] = nil
			continue
		}
		v := cloneV2Function(*f)
		r.Functions[n] = &v
	}
	r.Aggregates = make(map[string]*AggregateMetadata, len(k.Aggregates))
	for n, a := range k.Aggregates {
		if a == nil {
			r.Aggregates[n] = nil
			continue
		}
		v := *a
		v.ArgumentTypes = append([]string(nil), a.ArgumentTypes...)
		v.FinalFunc = cloneV2Function(a.FinalFunc)
		v.StateFunc = cloneV2Function(a.StateFunc)
		r.Aggregates[n] = &v
	}
	r.Types = make(map[string]*TypeMetadata, len(k.Types))
	for n, t := range k.Types {
		if t == nil {
			r.Types[n] = nil
			continue
		}
		v := *t
		v.FieldNames = append([]string(nil), t.FieldNames...)
		v.FieldTypes = append([]string(nil), t.FieldTypes...)
		r.Types[n] = &v
	}
	r.Views = make(map[string]*ViewMetadata, len(k.Views))
	for n, t := range k.Views {
		if t == nil {
			r.Views[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = cloneV2AnyMap(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Views[n] = &v
	}
	r.Indexes = make(map[string]*IndexMetadata, len(k.Indexes))
	for n, t := range k.Indexes {
		if t == nil {
			r.Indexes[n] = nil
			continue
		}
		v := *t
		v.Options = maps.Clone(t.Options)
		v.Columns = cloneV2Columns(t.Columns)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		r.Indexes[n] = &v
	}
	return &r
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
	if s.ringRefresher == nil {
		return errNoControl
	}
	p := s.peripheral()
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	var topologyErr error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err, ok := <-s.ringRefresher.RefreshNow():
		if !ok {
			return ErrClosed
		}
		topologyErr = err
	}
	var routeErr error
	if s.clientRoutesHandler != nil {
		routeErr = s.clientRoutesHandler.refreshContext(ctx)
	}
	schemaErr := s.refreshV2AllSchema(ctx)
	if topologyErr != nil || routeErr != nil {
		return errors.Join(&PartialRefreshError{Generation: s.Metadata().Generation(), Topology: topologyErr, ClientRoutes: routeErr}, schemaErr)
	}
	return schemaErr
}

// routingPartitioner chooses the configured server/table partitioner. Unknown
// names deliberately leave routing tokenless; hashing with a guessed algorithm
// would send requests to the wrong replicas.
func (s *Session) routingPartitioner(keyspace, table string) TokenPartitioner {
	return s.capturedRoutingPartitioner(s.preparedPartitionerName(keyspace, table))
}

type traceSessionRow struct {
	coordinator Nullable[netip.Addr]
	started     Nullable[Timestamp]
	duration    Nullable[int32]
}
type traceActivityRow struct {
	id       TimeUUID
	activity Text
	source   netip.Addr
	elapsed  Nullable[int32]
	thread   Text
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
	profile := defaultProfile()
	if rt := s.typedRuntime(); rt.profile != nil {
		profile = rt.profile.Snapshot()
	}
	if profile.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, profile.Timeout)
		defer cancel()
	}
	uuidType, err := ScalarType(TypeUUID)
	if err != nil {
		return result, err
	}
	params := NewColumns(ColumnSpec{Name: "session_id", Type: uuidType})
	sessionStatement := Statement[UUID, traceSessionRow]{CQL: "SELECT coordinator, started_at, duration FROM system_traces.sessions WHERE session_id = ?", Params: OneParam(UUIDCodec), Rows: Row(
		Column("coordinator", NullableCodec(InetCodec), func(r *traceSessionRow) *Nullable[netip.Addr] { return &r.coordinator }),
		Column("started_at", NullableCodec(TimestampCodec), func(r *traceSessionRow) *Nullable[Timestamp] { return &r.started }),
		Column("duration", NullableCodec(Int32), func(r *traceSessionRow) *Nullable[int32] { return &r.duration }),
	)}
	bound, err := BindUnpreparedOwned(sessionStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { bound.Close(); _ = bound.Wait(ctx) }()
	options := Options{Consistency: Option[Consistency]{Value: One, Set: true}, Idempotent: true, Timeout: Option[time.Duration]{Set: true}}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		rows, err := Run(ctx, s, bound, options)
		if err != nil {
			return result, err
		}
		ready := false
		if rows.Next() {
			row := rows.Row()
			if row.coordinator.Valid && row.started.Valid && row.duration.Valid {
				result.Coordinator = row.coordinator.Value
				result.Started = time.UnixMilli(int64(row.started.Value))
				result.Duration = time.Duration(row.duration.Value) * time.Microsecond
				ready = true
			}
			if rows.Next() {
				_ = rows.Close()
				return result, &ProtocolError{Stage: "trace", Err: fmt.Errorf("duplicate tracing session")}
			}
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return result, err
		}
		if ready {
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	eventsStatement := Statement[UUID, traceActivityRow]{CQL: "SELECT event_id, activity, source, source_elapsed, thread FROM system_traces.events WHERE session_id = ?", Params: OneParam(UUIDCodec), Rows: Row(
		Column("event_id", TimeUUIDCodec, func(r *traceActivityRow) *TimeUUID { return &r.id }),
		Column("activity", TextCodec, func(r *traceActivityRow) *Text { return &r.activity }),
		Column("source", InetCodec, func(r *traceActivityRow) *netip.Addr { return &r.source }),
		Column("source_elapsed", NullableCodec(Int32), func(r *traceActivityRow) *Nullable[int32] { return &r.elapsed }),
		Column("thread", TextCodec, func(r *traceActivityRow) *Text { return &r.thread }),
	)}
	eventsBound, err := BindUnpreparedOwned(eventsStatement, params, id)
	if err != nil {
		return result, err
	}
	defer func() { eventsBound.Close(); _ = eventsBound.Wait(ctx) }()
	rows, err := Run(ctx, s, eventsBound, options)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		r := rows.Row()
		e := TraceEvent{ID: UUID(r.id), Activity: r.activity.String(), Source: r.source, Thread: r.thread.String()}
		if r.elapsed.Valid {
			e.Elapsed = time.Duration(r.elapsed.Value) * time.Microsecond
		}
		result.Events = append(result.Events, e)
	}
	return result, rows.Err()
}

// readV2SystemRows is the context-bound typed control-query seam. It detaches
// metadata values before releasing each page and never invokes legacy Scan.
func readV2SystemRows[P, R any](ctx context.Context, conn *Conn, statement Statement[P, R], columns Columns, value P) ([]R, error) {
	if conn == nil {
		return nil, typeValidation("system query", "nil connection")
	}
	return readV2SystemRowsVia(ctx, conn.version, conn.getSystemRequestState().timeout, conn.executeTyped, statement, columns, value)
}
func readV2SystemRowsVia[P, R any](ctx context.Context, version byte, timeout time.Duration, execute func(context.Context, *attemptRequest) (*responsePage, error), statement Statement[P, R], columns Columns, value P) ([]R, error) {
	if ctx == nil || execute == nil {
		return nil, typeValidation("system query", "nil context or connection")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if err := validateStatement(statement); err != nil {
		return nil, err
	}
	plan, err := statement.Params.Compile(columns)
	if err != nil {
		return nil, err
	}
	values, err := encodeParameters(plan, columns, value, true, version >= 4)
	if err != nil {
		return nil, err
	}
	req := &attemptRequest{statement: statement.CQL, values: values.framed, valueCount: len(values.cells), consistency: One, pageSize: 1000, timeout: timeout}
	var rows []R
	var history pagingHistory
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		response, err := execute(ctx, req)
		if err != nil {
			if response != nil && response.owner != nil {
				response.owner.Release()
			}
			return nil, err
		}
		if response == nil {
			return nil, &ProtocolError{Stage: "system query", Err: fmt.Errorf("missing response")}
		}
		if response.noMetadata {
			response.owner.Release()
			return nil, &ProtocolError{Stage: "system query", Err: fmt.Errorf("unexpected omitted metadata")}
		}
		page, err := newPage(response, statement.Rows, nil)
		if err != nil {
			return nil, err
		}
		for page.Next() {
			row, err := page.Clone()
			if err != nil {
				_ = page.Close()
				return nil, err
			}
			rows = append(rows, row)
		}
		err = page.Err()
		token := append([]byte(nil), response.pagingState...)
		more := response.pagingState != nil
		_ = page.Close()
		if err != nil {
			return nil, err
		}
		if !more {
			return rows, nil
		}
		if err := history.add(PageState{token: token, kind: 1}); err != nil {
			return nil, err
		}
		if token == nil {
			token = []byte{}
		}
		req.pagingState = token
	}
}

type agreementPeer struct {
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
func (s *Session) awaitV2SchemaAgreementOn(ctx context.Context, conn *Conn) error {
	if ctx == nil || conn == nil || conn.host == nil {
		return typeValidation("schema agreement", "nil context or coordinator")
	}
	if s == nil || s.Closed() {
		return ErrClosed
	}
	return s.awaitV2SchemaAgreementUsing(ctx, conn, conn.host.hostUUID())
}
func (s *Session) awaitV2SchemaAgreementUsing(ctx context.Context, coordinator *Conn, required UUID) error {
	p := s.peripheral()
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	p.mu.Lock()
	options := p.config.Metadata
	p.mu.Unlock()
	timeout := options.AgreementTimeout
	if timeout == 0 {
		timeout = s.cfg.MaxWaitSchemaAgreement
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
		for _, h := range s.Metadata().hosts {
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
					v, err := readV2AgreementHost(ctx, job.id, job.conns)
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
				if terminalV2AgreementError(result.err) {
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
func readV2AgreementHost(ctx context.Context, id UUID, conns []*Conn) (UUID, error) {
	stmt := Statement[Unit, agreementPeer]{CQL: "SELECT host_id, schema_version FROM system.local WHERE key = 'local'", Params: NoParams, Rows: Row(Column("host_id", NullableCodec(UUIDCodec), func(r *agreementPeer) *Nullable[UUID] { return &r.id }), Column("schema_version", NullableCodec(UUIDCodec), func(r *agreementPeer) *Nullable[UUID] { return &r.version }))}
	var last error = ErrNoConnections
	for _, conn := range conns {
		if conn == nil || conn.Closed() {
			continue
		}
		rows, err := readV2SystemRowsVia(ctx, conn.version, 0, conn.executeTyped, stmt, NewColumns(), Unit{})
		if err != nil {
			if terminalV2AgreementError(err) {
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
func terminalV2AgreementError(err error) bool {
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
	if s.pool == nil {
		return nil
	}
	s.pool.mu.RLock()
	pool := s.pool.hostConnPools[id]
	s.pool.mu.RUnlock()
	if pool == nil {
		return nil
	}
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	if pool.closed {
		return nil
	}
	var out []*Conn
	switch picker := pool.connPicker.(type) {
	case *typedConnPicker:
		picker.mu.RLock()
		for _, cs := range picker.conns {
			out = append(out, cs...)
		}
		out = append(out, picker.excess...)
		picker.mu.RUnlock()
	case *defaultConnPicker:
		picker.mu.RLock()
		out = append(out, picker.conns...)
		picker.mu.RUnlock()
	case *scyllaConnPicker:
		picker.mu.RLock()
		out = append(out, picker.conns...)
		out = append(out, picker.excessConns...)
		picker.mu.RUnlock()
	}
	return out
}

// PartialRefreshError reports keyspaces whose previous valid schema was kept.
type PartialRefreshError struct {
	Generation   uint64
	Keyspaces    map[string]error
	Topology     error
	ClientRoutes error
}

func (e *PartialRefreshError) Error() string {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	var details []string
	for _, name := range names {
		details = append(details, fmt.Sprintf("%s: %v", name, e.Keyspaces[name]))
	}
	if e.Topology != nil {
		details = append(details, "topology: "+e.Topology.Error())
	}
	if e.ClientRoutes != nil {
		details = append(details, "client routes: "+e.ClientRoutes.Error())
	}
	return fmt.Sprintf("gocql: metadata generation %d retained stale data (%s)", e.Generation, strings.Join(details, "; "))
}
func (e *PartialRefreshError) Unwrap() []error {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]error, 0, len(names))
	for _, name := range names {
		out = append(out, e.Keyspaces[name])
	}
	if e.Topology != nil {
		out = append(out, e.Topology)
	}
	if e.ClientRoutes != nil {
		out = append(out, e.ClientRoutes)
	}
	return out
}

// NoHostsError distinguishes an empty eligible peer scope from agreement.
type NoHostsError struct {
	Operation string
	Causes    []error
}

func (e *NoHostsError) Error() string   { return "gocql: no eligible hosts for " + e.Operation }
func (e *NoHostsError) Unwrap() []error { return append([]error(nil), e.Causes...) }

// Schema rows intentionally materialize small metadata values on the cold path.
// Fields are explicitly registered; no reflection or destination inference runs.
type v2SchemaField[R any] struct {
	name     string
	required bool
	compile  func(DataType) (func(*R, *RowReader) error, error)
}

func v2SchemaColumn[R, V any](name string, decoder Decoder[V], field func(*R) *V) v2SchemaField[R] {
	return v2SchemaField[R]{name: name, compile: func(typ DataType) (func(*R, *RowReader) error, error) {
		nullable := NullableDecoder(decoder)
		if err := nullable.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *RowReader) error {
			v, err := ReadColumn(reader, nullable)
			if err != nil {
				return err
			}
			if v.Valid {
				*field(row) = v.Value
			}
			return nil
		}, nil
	}}
}
func v2RequiredSchemaColumn[R, V any](name string, decoder Decoder[V], field func(*R) *V) v2SchemaField[R] {
	return v2SchemaField[R]{name: name, required: true, compile: func(typ DataType) (func(*R, *RowReader) error, error) {
		if err := decoder.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *RowReader) error {
			value, err := ReadColumn(reader, decoder)
			if err != nil {
				return err
			}
			*field(row) = value
			return nil
		}, nil
	}}
}

type v2SchemaRows[R any] struct {
	fields     []v2SchemaField[R]
	initialize func() *R
}
type v2SchemaRowPlan[R any] struct {
	steps      []func(*R, *RowReader) error
	initialize func() *R
}

func (d v2SchemaRows[R]) Compile(columns Columns) (RowPlan[R], error) {
	fields := make(map[string]v2SchemaField[R], len(d.fields))
	for _, f := range d.fields {
		if _, exists := fields[f.name]; exists {
			return nil, typeValidation("schema row", "duplicate application field %q", f.name)
		}
		fields[f.name] = f
	}
	steps := make([]func(*R, *RowReader) error, columns.Len())
	seen := make(map[string]bool, columns.Len())
	for i := range steps {
		c, _ := columns.At(i)
		if seen[c.Name] {
			return nil, typeValidation("schema row", "duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if f, ok := fields[c.Name]; ok {
			step, err := f.compile(c.Type)
			if err != nil {
				return nil, fmt.Errorf("metadata column %s: %w", c.Name, err)
			}
			steps[i] = step
		} else {
			steps[i] = func(_ *R, r *RowReader) error { return r.SkipColumn() }
		}
	}
	for _, f := range d.fields {
		if f.required && !seen[f.name] {
			return nil, typeValidation("schema row", "missing column %q", f.name)
		}
	}
	return v2SchemaRowPlan[R]{steps: steps, initialize: d.initialize}, nil
}
func (d v2SchemaRows[R]) Clone(v R) R { return v }
func (p v2SchemaRowPlan[R]) Decode(reader *RowReader) (R, error) {
	var result R
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return *new(R), err
	}
	return result, nil
}
func (p v2SchemaRowPlan[R]) DecodeInto(reader *RowReader, dst *R) error {
	if dst == nil {
		return typeValidation("destination", "nil schema row destination")
	}
	var zero R
	*dst = zero
	if p.initialize != nil {
		*dst = *p.initialize()
	}
	for _, step := range p.steps {
		if err := step(dst, reader); err != nil {
			return err
		}
	}
	return nil
}
func (p v2SchemaRowPlan[R]) Clone(v R) R { return v }

type v2MetadataText struct{}

func (v2MetadataText) CheckType(t DataType) error {
	if t.Kind() == TypeAscii {
		return ASCIICodec.CheckType(t)
	}
	return TextCodec.CheckType(t)
}
func (v2MetadataText) Decode(r ValueReader) (string, error) {
	var text Text
	var err error
	if r.Type().Kind() == TypeAscii {
		text, err = ASCIICodec.Decode(r)
	} else {
		text, err = TextCodec.Decode(r)
	}
	if err != nil {
		return "", err
	}
	return text.String(), nil
}
func (v2MetadataText) Clone(v string) string { return v }

type v2MetadataUUIDString struct{}

func (v2MetadataUUIDString) CheckType(t DataType) error { return UUIDCodec.CheckType(t) }
func (v2MetadataUUIDString) Decode(r ValueReader) (string, error) {
	v, e := UUIDCodec.Decode(r)
	if e != nil {
		return "", e
	}
	return v.String(), nil
}
func (v2MetadataUUIDString) Clone(v string) string { return v }

type v2MetadataInt struct{}

func (v2MetadataInt) CheckType(t DataType) error        { return Int32.CheckType(t) }
func (v2MetadataInt) Decode(r ValueReader) (int, error) { v, e := Int32.Decode(r); return int(v), e }
func (v2MetadataInt) Clone(v int) int                   { return v }

type v2MetadataKind struct{}

func (v2MetadataKind) CheckType(t DataType) error { return (v2MetadataText{}).CheckType(t) }
func (v2MetadataKind) Decode(r ValueReader) (ColumnKind, error) {
	v, e := (v2MetadataText{}).Decode(r)
	if e != nil {
		return 0, e
	}
	return columnKindFromSchema(v)
}
func (v2MetadataKind) Clone(v ColumnKind) ColumnKind { return v }

type v2MetadataStrings struct{}

func (v2MetadataStrings) CheckType(t DataType) error {
	if t.Kind() == TypeSet {
		return SetDecoder[Text](TextCodec).CheckType(t)
	}
	return ListDecoder[Text](TextCodec).CheckType(t)
}
func (v2MetadataStrings) Decode(r ValueReader) ([]string, error) {
	decoder := ListDecoder[Text](TextCodec)
	if r.Type().Kind() == TypeSet {
		decoder = SetDecoder[Text](TextCodec)
	}
	v, err := decoder.Decode(r)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, v.Len())
	it := v.Iterator()
	for it.Next() {
		out = append(out, it.Value().String())
	}
	return out, it.Err()
}
func (v2MetadataStrings) Clone(v []string) []string { return append([]string(nil), v...) }

type v2MetadataStringMap struct{}

func (v2MetadataStringMap) CheckType(t DataType) error {
	return MapDecoder[Text, Text](TextCodec, TextCodec).CheckType(t)
}
func (v2MetadataStringMap) Decode(r ValueReader) (map[string]string, error) {
	v, err := MapDecoder[Text, Text](TextCodec, TextCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, v.Len())
	it := v.Iterator()
	for it.Next() {
		entry := it.Value()
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, typeValidation("schema map", "duplicate key %q", key)
		}
		out[key] = entry.Value.String()
	}
	return out, it.Err()
}
func (v2MetadataStringMap) Clone(v map[string]string) map[string]string { return maps.Clone(v) }

type v2MetadataExtensions struct{}

func (v2MetadataExtensions) CheckType(t DataType) error {
	return MapDecoder[Text, Bytes](TextCodec, BlobCodec).CheckType(t)
}
func (v2MetadataExtensions) Decode(r ValueReader) (map[string]any, error) {
	v, err := MapDecoder[Text, Bytes](TextCodec, BlobCodec).Decode(r)
	if err != nil {
		return nil, err
	}
	out := make(map[string]any, v.Len())
	it := v.Iterator()
	for it.Next() {
		entry := it.Value()
		key := entry.Key.String()
		if _, ok := out[key]; ok {
			return nil, typeValidation("schema extensions", "duplicate key %q", key)
		}
		out[key] = entry.Value.OwnedBytes()
	}
	return out, it.Err()
}
func (v2MetadataExtensions) Clone(v map[string]any) map[string]any { return cloneV2AnyMap(v) }

type v2SchemaParams struct{ count int }

func (v2SchemaParams) Compile(columns Columns) (ParamPlan[[]string], error) {
	for i := 0; i < columns.Len(); i++ {
		c, _ := columns.At(i)
		if err := StringEncoder.CheckType(c.Type); err != nil {
			return nil, err
		}
	}
	return v2SchemaParams{count: columns.Len()}, nil
}
func (p v2SchemaParams) Encode(w *ParamWriter, values []string) error {
	if len(values) != p.count {
		return typeValidation("schema parameters", "wrong count")
	}
	for _, value := range values {
		if err := WriteParam(w, StringEncoder, value); err != nil {
			return err
		}
	}
	return nil
}

type v2SystemQueryer interface {
	queryV2System(context.Context, *attemptRequest) (*responsePage, error)
}

func (c *controlConn) queryV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	h := c.getConn()
	if h == nil || h.conn == nil {
		return nil, ErrNoConnections
	}
	conn, ok := h.conn.(*Conn)
	if !ok {
		return nil, typeValidation("system query", "connection does not support typed requests")
	}
	if c.session != nil && conn.isScyllaConn() {
		p := c.session.peripheral()
		p.mu.Lock()
		serverTimeout := p.config.Metadata.ServerTimeout
		p.mu.Unlock()
		if serverTimeout.Set && serverTimeout.Value > 0 && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(request.statement)), "SELECT") {
			copyRequest := *request
			copyRequest.statement = strings.TrimSuffix(strings.TrimSpace(request.statement), ";") + " USING TIMEOUT " + strconv.FormatInt(serverTimeout.Value.Milliseconds(), 10) + "ms"
			request = &copyRequest
		}
	}
	return conn.executeTyped(ctx, request)
}
func queryV2SchemaRows[R any](ctx context.Context, s *Session, stmt string, args []string, decoder RowDecoder[R]) ([]R, error) {
	if !s.useSystemSchema {
		return nil, &UnsupportedSettingError{Setting: "schema metadata", Reason: "server does not provide system_schema tables"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.control == nil {
		return nil, errNoControl
	}
	typ, err := ScalarType(TypeVarchar)
	if err != nil {
		return nil, err
	}
	columns := make([]ColumnSpec, len(args))
	for i := range columns {
		columns[i] = ColumnSpec{Type: typ}
	}
	statement := Statement[[]string, R]{CQL: stmt, Params: v2SchemaParams{}, Rows: decoder}
	if backend, ok := s.control.(v2SystemQueryer); ok {
		rows, err := readV2SystemRowsVia(ctx, 4, s.cfg.MetadataSchemaRequestTimeout, backend.queryV2System, statement, NewColumns(columns...), args)
		return rows, wrapV2SchemaQueryError(stmt, err)
	}
	h := s.control.getConn()
	if h == nil || h.conn == nil {
		return nil, ErrNoConnections
	}
	conn, ok := h.conn.(*Conn)
	if !ok {
		return nil, typeValidation("schema query", "control connection does not support typed requests")
	}
	rows, err := readV2SystemRows(ctx, conn, statement, NewColumns(columns...), args)
	return rows, wrapV2SchemaQueryError(stmt, err)
}

func v2SchemaContext(s *Session) context.Context {
	if s != nil && s.ctx != nil {
		return s.ctx
	}
	return context.Background()
}

func v2DecodeTableMetadata(keyspace string) RowDecoder[TableMetadata] {
	return v2SchemaRows[TableMetadata]{initialize: func() *TableMetadata { return &TableMetadata{Keyspace: keyspace} }, fields: []v2SchemaField[TableMetadata]{
		v2RequiredSchemaColumn("id", v2MetadataUUIDString{}, func(r *TableMetadata) *string { return &r.ID }),
		v2RequiredSchemaColumn("table_name", v2MetadataText{}, func(r *TableMetadata) *string { return &r.Name }),
		v2SchemaColumn("flags", v2MetadataStrings{}, func(r *TableMetadata) *[]string { return &r.Flags }),
		v2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *TableMetadata) *map[string]any { return &r.Extensions }),
		v2SchemaColumn("bloom_filter_fp_chance", Float64, func(r *TableMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		v2SchemaColumn("caching", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Caching }),
		v2SchemaColumn("comment", v2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.Comment }),
		v2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compaction }),
		v2SchemaColumn("compression", v2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compression }),
		v2SchemaColumn("crc_check_chance", Float64, func(r *TableMetadata) *float64 { return &r.Options.CrcCheckChance }),
		v2SchemaColumn("default_time_to_live", v2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.DefaultTimeToLive }),
		v2SchemaColumn("gc_grace_seconds", v2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.GcGraceSeconds }),
		v2SchemaColumn("max_index_interval", v2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MaxIndexInterval }),
		v2SchemaColumn("memtable_flush_period_in_ms", v2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		v2SchemaColumn("min_index_interval", v2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MinIndexInterval }),
		v2SchemaColumn("speculative_retry", v2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

func v2DecodeColumnMetadata(keyspace string) RowDecoder[ColumnMetadata] {
	return v2SchemaRows[ColumnMetadata]{initialize: func() *ColumnMetadata { return &ColumnMetadata{Keyspace: keyspace} }, fields: []v2SchemaField[ColumnMetadata]{
		v2RequiredSchemaColumn("table_name", v2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Table }),
		v2RequiredSchemaColumn("column_name", v2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Name }),
		v2SchemaColumn("clustering_order", v2MetadataText{}, func(r *ColumnMetadata) *string { return &r.ClusteringOrder }),
		v2RequiredSchemaColumn("type", v2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Type }),
		v2RequiredSchemaColumn("kind", v2MetadataKind{}, func(r *ColumnMetadata) *ColumnKind { return &r.Kind }),
		v2RequiredSchemaColumn("position", v2MetadataInt{}, func(r *ColumnMetadata) *int { return &r.ComponentIndex }),
	}}
}

func v2DecodeTypeMetadata(keyspace string) RowDecoder[TypeMetadata] {
	return v2SchemaRows[TypeMetadata]{initialize: func() *TypeMetadata { return &TypeMetadata{Keyspace: keyspace} }, fields: []v2SchemaField[TypeMetadata]{
		v2RequiredSchemaColumn("type_name", v2MetadataText{}, func(r *TypeMetadata) *string { return &r.Name }),
		v2RequiredSchemaColumn("field_names", v2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldNames }),
		v2RequiredSchemaColumn("field_types", v2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldTypes }),
	}}
}

func v2DecodeFunctionMetadata(keyspace string) RowDecoder[FunctionMetadata] {
	return v2SchemaRows[FunctionMetadata]{initialize: func() *FunctionMetadata { return &FunctionMetadata{Keyspace: keyspace} }, fields: []v2SchemaField[FunctionMetadata]{
		v2RequiredSchemaColumn("function_name", v2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Name }),
		v2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentTypes }),
		v2RequiredSchemaColumn("argument_names", v2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentNames }),
		v2RequiredSchemaColumn("body", v2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Body }),
		v2RequiredSchemaColumn("called_on_null_input", Bool, func(r *FunctionMetadata) *bool { return &r.CalledOnNullInput }),
		v2RequiredSchemaColumn("language", v2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Language }),
		v2RequiredSchemaColumn("return_type", v2MetadataText{}, func(r *FunctionMetadata) *string { return &r.ReturnType }),
	}}
}

func v2DecodeAggregateMetadata(keyspace string) RowDecoder[AggregateMetadata] {
	return v2SchemaRows[AggregateMetadata]{initialize: func() *AggregateMetadata { return &AggregateMetadata{Keyspace: keyspace} }, fields: []v2SchemaField[AggregateMetadata]{
		v2RequiredSchemaColumn("aggregate_name", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.Name }),
		v2RequiredSchemaColumn("argument_types", v2MetadataStrings{}, func(r *AggregateMetadata) *[]string { return &r.ArgumentTypes }),
		v2SchemaColumn("final_func", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.finalFunc }),
		v2SchemaColumn("initcond", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.InitCond }),
		v2RequiredSchemaColumn("return_type", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.ReturnType }),
		v2RequiredSchemaColumn("state_func", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.stateFunc }),
		v2RequiredSchemaColumn("state_type", v2MetadataText{}, func(r *AggregateMetadata) *string { return &r.StateType }),
	}}
}

func v2DecodeIndexMetadata(keyspace string) RowDecoder[IndexMetadata] {
	return v2SchemaRows[IndexMetadata]{initialize: func() *IndexMetadata { return &IndexMetadata{KeyspaceName: keyspace} }, fields: []v2SchemaField[IndexMetadata]{
		v2RequiredSchemaColumn("index_name", v2MetadataText{}, func(r *IndexMetadata) *string { return &r.Name }),
		v2RequiredSchemaColumn("keyspace_name", v2MetadataText{}, func(r *IndexMetadata) *string { return &r.KeyspaceName }),
		v2RequiredSchemaColumn("table_name", v2MetadataText{}, func(r *IndexMetadata) *string { return &r.TableName }),
		v2RequiredSchemaColumn("kind", v2MetadataText{}, func(r *IndexMetadata) *string { return &r.Kind }),
		v2RequiredSchemaColumn("options", v2MetadataStringMap{}, func(r *IndexMetadata) *map[string]string { return &r.Options }),
	}}
}

func v2DecodeViewMetadata(keyspace string) RowDecoder[ViewMetadata] {
	return v2SchemaRows[ViewMetadata]{initialize: func() *ViewMetadata { return &ViewMetadata{KeyspaceName: keyspace} }, fields: []v2SchemaField[ViewMetadata]{
		v2RequiredSchemaColumn("id", v2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.ID }),
		v2RequiredSchemaColumn("view_name", v2MetadataText{}, func(r *ViewMetadata) *string { return &r.ViewName }),
		v2RequiredSchemaColumn("base_table_id", v2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.BaseTableID }),
		v2RequiredSchemaColumn("base_table_name", v2MetadataText{}, func(r *ViewMetadata) *string { return &r.BaseTableName }),
		v2RequiredSchemaColumn("include_all_columns", Bool, func(r *ViewMetadata) *bool { return &r.IncludeAllColumns }),
		v2RequiredSchemaColumn("where_clause", v2MetadataText{}, func(r *ViewMetadata) *string { return &r.WhereClause }),
		v2SchemaColumn("extensions", v2MetadataExtensions{}, func(r *ViewMetadata) *map[string]any { return &r.Extensions }),
		v2SchemaColumn("dclocal_read_repair_chance", Float64, func(r *ViewMetadata) *float64 { return &r.DcLocalReadRepairChance }),
		v2SchemaColumn("read_repair_chance", Float64, func(r *ViewMetadata) *float64 { return &r.ReadRepairChance }),
		v2SchemaColumn("bloom_filter_fp_chance", Float64, func(r *ViewMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		v2SchemaColumn("caching", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Caching }),
		v2SchemaColumn("comment", v2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.Comment }),
		v2SchemaColumn("compaction", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compaction }),
		v2SchemaColumn("compression", v2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compression }),
		v2SchemaColumn("crc_check_chance", Float64, func(r *ViewMetadata) *float64 { return &r.Options.CrcCheckChance }),
		v2SchemaColumn("default_time_to_live", v2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.DefaultTimeToLive }),
		v2SchemaColumn("gc_grace_seconds", v2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.GcGraceSeconds }),
		v2SchemaColumn("max_index_interval", v2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MaxIndexInterval }),
		v2SchemaColumn("memtable_flush_period_in_ms", v2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		v2SchemaColumn("min_index_interval", v2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MinIndexInterval }),
		v2SchemaColumn("speculative_retry", v2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

type v2ScyllaTableRow struct {
	Name    string
	Options TableMetadataOptions
}

func v2DecodeScyllaTable() RowDecoder[v2ScyllaTableRow] {
	return v2SchemaRows[v2ScyllaTableRow]{fields: []v2SchemaField[v2ScyllaTableRow]{v2RequiredSchemaColumn("table_name", v2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Name }),
		v2SchemaColumn("cdc", v2MetadataStringMap{}, func(r *v2ScyllaTableRow) *map[string]string { return &r.Options.CDC }),
		v2SchemaColumn("in_memory", Bool, func(r *v2ScyllaTableRow) *bool { return &r.Options.InMemory }),
		v2SchemaColumn("partitioner", v2MetadataText{}, func(r *v2ScyllaTableRow) *string { return &r.Options.Partitioner }),
		v2SchemaColumn("version", v2MetadataUUIDString{}, func(r *v2ScyllaTableRow) *string { return &r.Options.Version }),
	}}
}

type v2KeyspaceRow struct {
	name        string
	durable     bool
	replication map[string]string
}

func v2DecodeKeyspace() RowDecoder[v2KeyspaceRow] {
	return v2SchemaRows[v2KeyspaceRow]{fields: []v2SchemaField[v2KeyspaceRow]{v2SchemaColumn("keyspace_name", v2MetadataText{}, func(r *v2KeyspaceRow) *string { return &r.name }), v2RequiredSchemaColumn("durable_writes", Bool, func(r *v2KeyspaceRow) *bool { return &r.durable }), v2RequiredSchemaColumn("replication", v2MetadataStringMap{}, func(r *v2KeyspaceRow) *map[string]string { return &r.replication })}}
}
func v2KeyspaceFromRow(name string, row v2KeyspaceRow) *KeyspaceMetadata {
	ks := &KeyspaceMetadata{Name: name, DurableWrites: row.durable, StrategyClass: row.replication["class"], StrategyOptions: make(map[string]any, len(row.replication))}
	for k, v := range row.replication {
		if k != "class" {
			ks.StrategyOptions[k] = v
		}
	}
	return ks
}
func getV2KeyspaceMetadata(ctx context.Context, s *Session, name string) (*KeyspaceMetadata, error) {
	rows, err := queryV2SchemaRows(ctx, s, `SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, []string{name}, v2DecodeKeyspace())
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrKeyspaceDoesNotExist
	}
	if len(rows) != 1 {
		return nil, typeValidation("keyspace schema", "duplicate identity %q", name)
	}
	ks := v2KeyspaceFromRow(name, rows[0])
	if err := readV2ScyllaKeyspaces(ctx, s, map[string]*KeyspaceMetadata{name: ks}, name); err != nil {
		return nil, err
	}
	return ks, nil
}
func getV2TableMetadata(ctx context.Context, s *Session, keyspace, table string) ([]TableMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	rows, err := queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.tables`+where, args, v2DecodeTableMetadata(keyspace))
	if err != nil {
		return nil, err
	}
	conn := s.getConn()
	if conn == nil || !conn.isScyllaConn() {
		return rows, nil
	}
	extra, err := queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.scylla_tables`+where, args, v2DecodeScyllaTable())
	if err != nil {
		return nil, err
	}
	options := make(map[string]TableMetadataOptions, len(extra))
	for _, row := range extra {
		if _, ok := options[row.Name]; ok {
			return nil, typeValidation("scylla table schema", "duplicate identity %q", row.Name)
		}
		options[row.Name] = row.Options
	}
	for i := range rows {
		if extra, ok := options[rows[i].Name]; ok {
			rows[i].Options.CDC = extra.CDC
			rows[i].Options.InMemory = extra.InMemory
			rows[i].Options.Partitioner = extra.Partitioner
			rows[i].Options.Version = extra.Version
		}
	}
	return rows, nil
}
func getV2ColumnMetadata(ctx context.Context, s *Session, keyspace, table string) ([]ColumnMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return queryV2SchemaRows(ctx, s, `SELECT `+columnMetadataColumns+` FROM system_schema.columns`+where, args, v2DecodeColumnMetadata(keyspace))
}
func getV2IndexMetadata(ctx context.Context, s *Session, keyspace, table string) ([]IndexMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.indexes`+where, args, v2DecodeIndexMetadata(keyspace))
}
func getV2ViewMetadata(ctx context.Context, s *Session, keyspace, table string) ([]ViewMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND base_table_name = ? ALLOW FILTERING`
		args = append(args, table)
	}
	return queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.views`+where, args, v2DecodeViewMetadata(keyspace))
}
func getV2CreateStatements(ctx context.Context, s *Session, keyspace string) ([]byte, error) {
	decoder := v2SchemaRows[string]{fields: []v2SchemaField[string]{v2RequiredSchemaColumn("create_statement", v2MetadataText{}, func(s *string) *string { return s })}}
	stmt := `DESCRIBE KEYSPACE "` + strings.ReplaceAll(keyspace, `"`, `""`) + `" WITH INTERNALS`
	rows, err := queryV2SchemaRows(ctx, s, stmt, nil, decoder)
	if err != nil {
		var server RequestError
		if errors.As(err, &server) && server.GetCode() == ErrCodeSyntax {
			return nil, nil
		}
		return nil, err
	}
	return []byte(strings.Join(rows, "\n")), nil
}

func (s *Session) refreshV2AllSchema(ctx context.Context) error {
	p := s.peripheral()
	if !p.admitMetadataWork() {
		return ErrClosed
	}
	defer p.workers.Done()
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	config := p.config
	epochs := maps.Clone(p.schemaEpoch)
	revisions := maps.Clone(p.schemaRevision)
	p.mu.Unlock()
	if config.DefaultProfile != nil && config.Metadata.Mode == MetadataDisabled {
		if !s.publishV2Metadata() {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	rows, err := queryV2SchemaRows(ctx, s, `SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces`, nil, v2DecodeKeyspace())
	if err != nil {
		return err
	}
	keyspaces := make(map[string]*KeyspaceMetadata, len(rows))
	allowed := make(map[string]bool, len(config.Metadata.Keyspaces))
	for _, name := range config.Metadata.Keyspaces {
		allowed[name] = true
	}
	for _, row := range rows {
		if row.name == "" {
			return typeValidation("keyspace schema", "missing keyspace identity")
		}
		if _, ok := keyspaces[row.name]; ok {
			return typeValidation("keyspace schema", "duplicate identity %q", row.name)
		}
		if len(allowed) > 0 && !allowed[row.name] {
			continue
		}
		keyspaces[row.name] = v2KeyspaceFromRow(row.name, row)
	}
	if err := readV2ScyllaKeyspaces(ctx, s, keyspaces, ""); err != nil {
		return err
	}

	failures := make(map[string]error)

	for name, base := range keyspaces {
		for attempt := 0; attempt < 8; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			p.mu.Lock()
			epoch := p.schemaEpoch[name]
			revision := p.schemaRevision[name]
			tables := p.captureTableRevisionsLocked(name)
			p.mu.Unlock()
			if attempt > 0 {
				base, err = getV2KeyspaceMetadata(ctx, s, name)
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
				ks, err = fetchV2Keyspace(ctx, s, base)
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
				current, checkErr := getV2KeyspaceMetadata(ctx, s, name)
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
			p.mu.Lock()
			changed := p.schemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables)
			if changed {
				p.mu.Unlock()
				failures[name] = fmt.Errorf("schema changed during refresh")
				continue
			}
			if err != nil {
				p.mu.Unlock()
				failures[name] = err
				break
			}
			if s.metadataDescriber != nil {
				s.invalidateV2ChangedTablets(name, ks)
				s.metadataDescriber.metadata.keyspaceMetadata.set(name, ks)
				p.schemaRevision[name]++
				delete(p.schemaStale, name)
				delete(p.schemaDropped, name)
			}
			p.mu.Unlock()
			delete(failures, name)
			break
		}
	}

	if s.metadataDescriber != nil {
		for name := range s.metadataDescriber.metadata.keyspaceMetadata.get() {
			if len(allowed) > 0 && !allowed[name] {
				continue
			}
			if _, exists := keyspaces[name]; exists {
				continue
			}
			p.mu.Lock()
			if p.schemaEpoch[name] == epochs[name] && p.schemaRevision[name] == revisions[name] {
				p.schemaRevision[name]++
				s.metadataDescriber.metadata.keyspaceMetadata.removeKeyspace(name)
				delete(p.schemaStale, name)
				s.metadataDescriber.RemoveTabletsWithKeyspace(name)
			}
			p.mu.Unlock()
		}
	}
	p.mu.Lock()
	p.refreshDiagnostics = nil
	for name, cause := range failures {
		p.refreshDiagnostics = append(p.refreshDiagnostics, fmt.Errorf("keyspace %s: %w", name, cause))
	}
	p.mu.Unlock()
	p.schemaGeneration.Add(1)
	if !s.publishV2Metadata() {
		return s.v2MetadataPublicationError()
	}
	if len(failures) > 0 {
		s.scheduleV2SchemaRetry()
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: failures}
	}
	return nil
}
func (s *Session) refreshV2Keyspace(ctx context.Context, name string) error {
	p := s.peripheral()
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

	p := s.peripheral()
	p.mu.Lock()
	epoch := p.schemaEpoch[name]
	revision := p.schemaRevision[name]
	tables := p.captureTableRevisionsLocked(name)
	config := p.config
	p.mu.Unlock()
	if config.DefaultProfile != nil && (config.Metadata.Mode == MetadataDisabled || len(config.Metadata.Keyspaces) > 0 && !slices.Contains(config.Metadata.Keyspaces, name)) {
		if !s.publishV2Metadata() {
			return s.v2MetadataPublicationError()
		}
		return nil
	}
	base, err := getV2KeyspaceMetadata(ctx, s, name)
	if errors.Is(err, ErrKeyspaceDoesNotExist) {
		p.mu.Lock()
		if p.schemaEpoch[name] == epoch && p.schemaRevision[name] == revision {
			p.schemaRevision[name]++
			s.metadataDescriber.metadata.keyspaceMetadata.removeKeyspace(name)
			delete(p.schemaStale, name)
			s.metadataDescriber.RemoveTabletsWithKeyspace(name)
		}
		p.mu.Unlock()
		if !s.publishV2Metadata() {
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
		ks, err = fetchV2Keyspace(ctx, s, base)
	}
	if err != nil {
		return s.v2SchemaRefreshFailure(name, err)
	}
	p.mu.Lock()
	if p.schemaEpoch[name] != epoch || p.schemaRevision[name] != revision || !p.tableRevisionsCurrentLocked(name, tables) {
		p.mu.Unlock()
		if attempt < 7 {
			return s.refreshV2KeyspaceAttempt(ctx, name, attempt+1)
		}
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: fmt.Errorf("schema changed during refresh")}}
	}
	s.invalidateV2ChangedTablets(name, ks)
	s.metadataDescriber.metadata.keyspaceMetadata.set(name, ks)
	p.schemaRevision[name]++
	delete(p.schemaStale, name)
	delete(p.schemaDropped, name)
	p.schemaGeneration.Add(1)
	p.mu.Unlock()
	if !s.publishV2Metadata() {
		return s.v2MetadataPublicationError()
	}
	return nil
}
func (s *Session) scheduleV2SchemaRetry() {
	s.peripheral().schemaRetryNeeded.Store(true)
	s.startV2SchemaRetry()
}
func (s *Session) startV2SchemaRetry() {
	p := s.peripheral()
	if !p.retryScheduled.CompareAndSwap(false, true) {
		return
	}
	if !p.startWorker(func() {
		ctx := v2SchemaContext(s)
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-p.done:
			p.retryScheduled.Store(false)
			return
		case <-ctx.Done():
			p.retryScheduled.Store(false)
			return
		case <-timer.C:
		}
		p.mu.Lock()
		closed := p.closed
		timeout := p.config.Metadata.RequestTimeout
		scopes := maps.Clone(p.preparedRefreshScopes)
		p.mu.Unlock()
		if closed {
			p.retryScheduled.Store(false)
			return
		}
		if timeout <= 0 {
			timeout = time.Minute
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		if p.schemaRetryNeeded.Swap(false) {
			if err := s.refreshV2AllSchema(ctx); err != nil {
				p.schemaRetryNeeded.Store(true)
			}
		}
		for keyspace, version := range scopes {
			if err := s.reconcileV2PreparedScope(ctx, keyspace); err == nil {
				p.mu.Lock()
				if p.preparedRefreshScopes[keyspace] == version {
					delete(p.preparedRefreshScopes, keyspace)
				}
				p.mu.Unlock()
				p.schemaReconciled.Add(1)
			}
		}
		cancel()
		p.retryScheduled.Store(false)
		p.mu.Lock()
		pending := len(p.preparedRefreshScopes) > 0
		p.mu.Unlock()
		if (pending || p.schemaRetryNeeded.Load()) && !s.Closed() {
			s.startV2SchemaRetry()
		}
	}) {
		p.retryScheduled.Store(false)
	}
}

func fetchV2Keyspace(ctx context.Context, s *Session, base *KeyspaceMetadata) (*KeyspaceMetadata, error) {
	var tables []TableMetadata
	var columns []ColumnMetadata
	var types []TypeMetadata
	var functions []FunctionMetadata
	var aggregates []AggregateMetadata
	var indexes []IndexMetadata
	var views []ViewMetadata
	var create []byte
	group, ctx := errgroup.WithContext(ctx)
	name := base.Name
	group.Go(func() error { var err error; tables, err = getV2TableMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; columns, err = getV2ColumnMetadata(ctx, s, name, ""); return err })
	group.Go(func() error {
		var err error
		types, err = queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.types WHERE keyspace_name = ?`, []string{name}, v2DecodeTypeMetadata(name))
		return err
	})
	if s.hasAggregatesAndFunctions {
		group.Go(func() error {
			var err error
			functions, err = queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`, []string{name}, v2DecodeFunctionMetadata(name))
			return err
		})
		group.Go(func() error {
			var err error
			aggregates, err = queryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`, []string{name}, v2DecodeAggregateMetadata(name))
			return err
		})
	}
	group.Go(func() error { var err error; indexes, err = getV2IndexMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; views, err = getV2ViewMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; create, err = getV2CreateStatements(ctx, s, name); return err })
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := validateV2SchemaParts(base, tables, columns, types, functions, aggregates, indexes, views, s.v2MetadataDepthLimit()); err != nil {
		return nil, err
	}
	compileMetadata(base, tables, columns, functions, aggregates, types, indexes, views, create)
	return base, nil
}

func validateV2SchemaParts(ks *KeyspaceMetadata, tables []TableMetadata, columns []ColumnMetadata, types []TypeMetadata, functions []FunctionMetadata, aggregates []AggregateMetadata, indexes []IndexMetadata, views []ViewMetadata, depthLimit ...int) error {
	maxDepth := 64
	if len(depthLimit) > 0 && depthLimit[0] > 0 {
		maxDepth = depthLimit[0]
	}
	if ks == nil || ks.Name == "" {
		return typeValidation("schema", "missing keyspace identity")
	}
	unique := func(kind string, names []string) error {
		seen := make(map[string]bool, len(names))
		for _, name := range names {
			if name == "" || seen[name] {
				return typeValidation("schema", "empty or duplicate %s identity %q", kind, name)
			}
			seen[name] = true
		}
		return nil
	}
	names := make([]string, 0, len(tables))
	knownTables := make(map[string]bool)
	for _, v := range tables {
		names = append(names, v.Name)
		knownTables[v.Name] = true
	}
	if err := unique("table", names); err != nil {
		return err
	}
	names = nil
	for _, v := range views {
		names = append(names, v.ViewName)
		if knownTables[v.ViewName] {
			return typeValidation("schema", "table/view identity collision %q", v.ViewName)
		}
		knownTables[v.ViewName] = true
	}
	if err := unique("view", names); err != nil {
		return err
	}
	names = nil
	for _, v := range indexes {
		names = append(names, v.Name)
		knownTables[v.Name+"_index"] = true
	}
	if err := unique("index", names); err != nil {
		return err
	}
	udts := make(map[string]bool, len(types))
	names = nil
	for _, v := range types {
		names = append(names, v.Name)
		udts[v.Name] = true
	}
	if err := unique("UDT", names); err != nil {
		return err
	}
	dependencies := make(map[string][]string, len(types))
	for _, v := range types {
		if len(v.FieldNames) != len(v.FieldTypes) {
			return typeValidation("schema UDT", "field count mismatch for %q", v.Name)
		}
		if err := unique("UDT field", v.FieldNames); err != nil {
			return err
		}
		for _, typ := range v.FieldTypes {
			refs, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth)
			if err != nil {
				return err
			}
			dependencies[v.Name] = append(dependencies[v.Name], refs...)
		}
	}
	visited := make(map[string]uint8)
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] == 1 {
			return typeValidation("schema UDT", "cycle through %q", name)
		}
		if visited[name] == 2 {
			return nil
		}
		visited[name] = 1
		for _, dep := range dependencies[name] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[name] = 2
		return nil
	}
	for name := range udts {
		if err := visit(name); err != nil {
			return err
		}
	}
	identities := make(map[metadataTable]bool, len(columns))
	positions := make(map[string]map[ColumnKind]map[int]bool)
	for _, v := range columns {
		if !knownTables[v.Table] {
			return typeValidation("schema column", "unknown table %q", v.Table)
		}
		identity := metadataTable{v.Table, v.Name}
		if identities[identity] || v.Name == "" {
			return typeValidation("schema column", "duplicate or empty identity %q.%q", v.Table, v.Name)
		}
		identities[identity] = true
		if _, err := validateV2CQLTypeLimit(ks.Name, v.Type, udts, maxDepth); err != nil {
			return err
		}
		if v.Kind == ColumnPartitionKey || v.Kind == ColumnClusteringKey {
			if v.ComponentIndex < 0 || v.ComponentIndex >= len(columns) {
				return typeValidation("schema key order", "invalid position %d", v.ComponentIndex)
			}
			if positions[v.Table] == nil {
				positions[v.Table] = make(map[ColumnKind]map[int]bool)
			}
			if positions[v.Table][v.Kind] == nil {
				positions[v.Table][v.Kind] = make(map[int]bool)
			}
			if positions[v.Table][v.Kind][v.ComponentIndex] {
				return typeValidation("schema key order", "duplicate position %d", v.ComponentIndex)
			}
			positions[v.Table][v.Kind][v.ComponentIndex] = true
		}
	}
	for _, table := range tables {
		if len(positions[table.Name][ColumnPartitionKey]) == 0 {
			return typeValidation("schema table", "missing partition key for %q", table.Name)
		}
	}
	for table, kinds := range positions {
		for _, pos := range kinds {
			for i := 0; i < len(pos); i++ {
				if !pos[i] {
					return typeValidation("schema key order", "noncontiguous positions for %q", table)
				}
			}
		}
	}
	names = nil
	for _, v := range functions {
		names = append(names, v2FunctionIdentity(v.Name, v.ArgumentTypes))
		if len(v.ArgumentNames) != len(v.ArgumentTypes) {
			return typeValidation("schema function", "argument count mismatch for %q", v.Name)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.ReturnType) {
			if _, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	if err := unique("function", names); err != nil {
		return err
	}
	names = nil
	for _, v := range aggregates {
		names = append(names, v2FunctionIdentity(v.Name, v.ArgumentTypes))
		if resolveV2Function(functions, v.stateFunc, append([]string{v.StateType}, v.ArgumentTypes...)) == nil {
			return typeValidation("schema aggregate", "unknown state function %q", v.stateFunc)
		}
		if v.finalFunc != "" && resolveV2Function(functions, v.finalFunc, []string{v.StateType}) == nil {
			return typeValidation("schema aggregate", "unknown final function %q", v.finalFunc)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.StateType, v.ReturnType) {
			if _, err := validateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	return unique("aggregate", names)
}
func v2FunctionIdentity(name string, args []string) string {
	return name + "(" + strings.Join(args, ",") + ")"
}
func resolveV2Function(functions []FunctionMetadata, name string, args []string) *FunctionMetadata {
	for i := range functions {
		f := &functions[i]
		if f.Name != name || len(f.ArgumentTypes) != len(args) {
			continue
		}
		same := true
		for i := range args {
			if f.ArgumentTypes[i] != args[i] {
				same = false
				break
			}
		}
		if same {
			return f
		}
	}
	return nil
}

type v2CQLTypeParser struct {
	source, keyspace string
	offset           int
	maxDepth         int
	udts             map[string]bool
	refs             []string
}

func validateV2CQLType(keyspace, source string, udts map[string]bool) ([]string, error) {
	return validateV2CQLTypeLimit(keyspace, source, udts, 64)
}
func validateV2CQLTypeLimit(keyspace, source string, udts map[string]bool, maxDepth int) ([]string, error) {

	p := v2CQLTypeParser{source: source, keyspace: keyspace, udts: udts, maxDepth: maxDepth}
	if err := p.parse(0, false); err != nil {
		return nil, err
	}
	p.space()
	if p.offset != len(source) {
		return nil, typeValidation("schema type", "trailing syntax in %q", source)
	}
	return p.refs, nil
}
func (p *v2CQLTypeParser) space() {
	for p.offset < len(p.source) {
		switch p.source[p.offset] {
		case ' ', '\t', '\r', '\n':
			p.offset++
		default:
			return
		}
	}
}
func (p *v2CQLTypeParser) identifier() (string, bool, error) {
	p.space()
	start := p.offset
	if start >= len(p.source) {
		return "", false, typeValidation("schema type", "truncated type %q", p.source)
	}
	if p.source[start] == '"' {
		p.offset++
		var out strings.Builder
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '"' {
				if p.offset < len(p.source) && p.source[p.offset] == '"' {
					p.offset++
					out.WriteByte('"')
					continue
				}
				if out.Len() == 0 {
					return "", true, typeValidation("schema type", "empty identifier")
				}
				return out.String(), true, nil
			}
			out.WriteByte(c)
		}
		return "", true, typeValidation("schema type", "unterminated identifier")
	}
	for p.offset < len(p.source) {
		c := p.source[p.offset]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || p.offset > start && c >= '0' && c <= '9' {
			p.offset++
		} else {
			break
		}
	}
	if p.offset == start {
		return "", false, typeValidation("schema type", "invalid identifier in %q", p.source)
	}
	return strings.ToLower(p.source[start:p.offset]), false, nil
}
func (p *v2CQLTypeParser) parse(depth int, frozen bool) error {
	if depth >= p.maxDepth {
		return typeValidation("schema type", "maximum nesting depth exceeded")
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '\'' {
		p.offset++
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '\'' {
				if p.offset < len(p.source) && p.source[p.offset] == '\'' {
					p.offset++
					continue
				}
				return nil
			}
		}
		return typeValidation("schema type", "unterminated custom type")
	}
	name, quoted, err := p.identifier()
	if err != nil {
		return err
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '.' {
		if name != p.keyspace {
			return typeValidation("schema type", "foreign keyspace %q", name)
		}
		p.offset++
		name, quoted, err = p.identifier()
		if err != nil {
			return err
		}
		quoted = true
		p.space()
	}
	if p.offset < len(p.source) && p.source[p.offset] == '<' {
		if quoted {
			return typeValidation("schema type", "UDT cannot have parameters")
		}
		switch name {
		case "list", "set", "map", "tuple", "frozen", "vector":
		default:
			return typeValidation("schema type", "unknown constructor %q", name)
		}
		p.offset++
		count := 0
		for {
			if name == "vector" && count == 1 {
				p.space()
				start := p.offset
				for p.offset < len(p.source) && p.source[p.offset] >= '0' && p.source[p.offset] <= '9' {
					p.offset++
				}
				dimension, err := strconv.ParseUint(p.source[start:p.offset], 10, 16)
				if err != nil || dimension == 0 {
					return typeValidation("schema type", "invalid vector dimension")
				}
			} else {
				if err := p.parse(depth+1, name == "frozen"); err != nil {
					return err
				}
			}
			count++
			p.space()
			if p.offset >= len(p.source) {
				return typeValidation("schema type", "unterminated parameters")
			}
			c := p.source[p.offset]
			p.offset++
			if c == '>' {
				break
			}
			if c != ',' {
				return typeValidation("schema type", "invalid parameter separator")
			}
		}
		expected := 1
		if name == "map" || name == "vector" {
			expected = 2
		}
		if name != "tuple" && count != expected {
			return typeValidation("schema type", "wrong arity for %q", name)
		}
		return nil
	}
	if !quoted && !frozen {
		if _, ok := cqlNativeTypes[name]; ok || name == "empty" {
			return nil
		}
	}
	if !p.udts[name] {
		return typeValidation("schema type", "unknown UDT %q", name)
	}
	p.refs = append(p.refs, name)
	return nil
}

// typedSchemaGeneration deliberately excludes topology and tablet-cache updates.
// Those changes must not invalidate every prepared statement.
func (s *Session) typedSchemaGeneration() uint64 {
	p := s.peripheral()
	if p == nil {
		return 0
	}
	return p.schemaGeneration.Load() + 1
}

func (s *Session) recordV2Latency(latency time.Duration) {
	if s == nil || latency < 0 {
		return
	}
	p := s.peripheral()
	p.latencyMu.Lock()
	p.latency[p.latencyCount%uint64(len(p.latency))] = latency
	p.latencyCount++
	p.latencyMu.Unlock()
}
func (s *Session) recordV2Orphan() {
	if s != nil {
		s.peripheral().orphans.Add(1)
	}
}

func wrapV2SchemaQueryError(statement string, err error) error {
	if err == nil {
		return nil
	}
	kind := "keyspace"
	switch {
	case strings.Contains(statement, "system_schema.scylla_tables"):
		kind = "scylla table"
	case strings.Contains(statement, "system_schema.tables"):
		kind = "table"
	case strings.Contains(statement, "system_schema.columns"):
		kind = "column"
	case strings.Contains(statement, "system_schema.views"):
		kind = "view"
	case strings.Contains(statement, "system_schema.types"):
		kind = "type"
	case strings.Contains(statement, "system_schema.indexes"):
		kind = "index"
	case strings.Contains(statement, "system_schema.functions"):
		kind = "function"
	case strings.Contains(statement, "system_schema.aggregates"):
		kind = "aggregate"
	}
	return fmt.Errorf("error querying %s schema: %w", kind, err)
}

func (s *Session) markV2SchemaStale(keyspace string, dropped bool) {
	p := s.peripheral()
	p.mu.Lock()
	if dropped {
		delete(p.schemaStale, keyspace)
	} else {
		p.schemaStale[keyspace] = true
	}
	p.mu.Unlock()
}
func (s *Session) invalidateV2ChangedTablets(keyspace string, current *KeyspaceMetadata) {
	if s.metadataDescriber == nil {
		return
	}
	previous, _ := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(keyspace)
	if previous == nil {
		if m := s.peripheral().snapshot.Load(); m != nil {
			previous = m.keyspaces[keyspace]
		}
	}
	if previous == nil {
		return
	}
	if !compareInterfaceMaps(previous.StrategyOptions, current.StrategyOptions) || previous.StrategyClass != current.StrategyClass {
		s.metadataDescriber.RemoveTabletsWithKeyspace(keyspace)
		return
	}
	for name, table := range previous.Tables {
		next := current.Tables[name]
		if next == nil || !table.Equals(next) {
			s.metadataDescriber.RemoveTabletsWithTable(keyspace, name)
		}
	}
}

func (s *Session) dropV2KeyspaceIfCurrent(name string, epoch uint64, revision ...uint64) bool {
	p := s.peripheral()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.schemaEpoch[name] != epoch || len(revision) > 0 && p.schemaRevision[name] != revision[0] {
		return false
	}
	p.schemaRevision[name]++
	if s.metadataDescriber != nil {
		s.metadataDescriber.metadata.keyspaceMetadata.removeKeyspace(name)
		s.metadataDescriber.RemoveTabletsWithKeyspace(name)
	}
	delete(p.schemaStale, name)
	p.schemaDropped[name] = true
	return true
}
func (s *Session) confirmV2DroppedKeyspace(ctx context.Context, name string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p := s.peripheral()
	p.mu.Lock()
	epoch := p.schemaEpoch[name]
	revision := p.schemaRevision[name]
	p.mu.Unlock()
	_, err := getV2KeyspaceMetadata(ctx, s, name)
	if errors.Is(err, ErrKeyspaceDoesNotExist) {
		return s.dropV2KeyspaceIfCurrent(name, epoch, revision), nil
	}
	return false, err
}

func (s *Session) runV2PeriodicRefresh(options MetadataOptions) {
	ticker := time.NewTicker(options.PeriodicRefresh)
	defer ticker.Stop()
	p := s.peripheral()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
		}
		ctx := v2SchemaContext(s)
		timeout := options.RequestTimeout
		if timeout <= 0 {
			timeout = time.Minute
		}
		ctx, cancel := context.WithTimeout(ctx, timeout)
		err := s.RefreshMetadata(ctx)
		cancel()
		if err != nil && s.logger != nil {
			s.logger.Printf("gocql: periodic metadata refresh: %v", err)
		}
	}
}

func (s *Session) v2MetadataDepthLimit() int {
	p := s.peripheral()
	p.mu.Lock()
	limit := p.config.Limits.MaxMetadataDepth
	p.mu.Unlock()
	if limit <= 0 {
		return 64
	}
	return limit
}

// startWorker serializes admission with shutdown; Add can never race Wait.
func (p *v2PeripheralState) startWorker(fn func()) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	go func() { defer p.workers.Done(); fn() }()
	return true
}

func (p *v2PeripheralState) admitMetadataWork() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.workers.Add(1)
	return true
}

func (p *v2PeripheralState) captureTableRevisionsLocked(keyspace string) map[metadataTable]uint64 {
	out := make(map[metadataTable]uint64)
	for key, revision := range p.tableRevision {
		if key.keyspace == keyspace {
			out[key] = revision
		}
	}
	return out
}
func (p *v2PeripheralState) tableRevisionsCurrentLocked(keyspace string, before map[metadataTable]uint64) bool {
	for key, revision := range p.tableRevision {
		if key.keyspace == keyspace && before[key] != revision {
			return false
		}
	}
	return true
}

func (s *Session) v2SchemaRefreshFailure(name string, cause error) error {
	p := s.peripheral()
	p.mu.Lock()
	if !p.schemaDropped[name] {
		p.schemaStale[name] = true
	}
	p.refreshDiagnostics = []error{fmt.Errorf("keyspace %s: %w", name, cause)}
	p.mu.Unlock()
	s.publishV2Metadata()
	s.scheduleV2SchemaRetry()
	return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{name: cause}}
}

func (s *Session) v2MetadataPublicationError() error {
	return &PartialRefreshError{Generation: s.Metadata().Generation(), Keyspaces: map[string]error{"<publication>": fmt.Errorf("metadata sources changed throughout publication; refresh again")}}
}
