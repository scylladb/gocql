package gocql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type prepareCacheKey struct {
	cql, keyspace string
}
type prepareFlight struct {
	done  chan struct{}
	state *preparedState
	err   error
}
type sessionRuntime struct {
	schema                                                       *preparedSchemaTracker
	prepareWorkersMu                                             sync.Mutex
	prepareWorkers                                               sync.WaitGroup
	prepareWorkersClosed                                         bool
	preparing                                                    map[prepareCacheKey]*prepareFlight
	prepareOrder                                                 []prepareCacheKey
	mu                                                           sync.Mutex
	profile                                                      *ProfileHandle
	config                                                       Config
	prepared                                                     map[prepareCacheKey]*preparedState
	requestID                                                    atomic.Uint64
	requests, attempts, errors, retries, speculations, cancelled atomic.Uint64
	inflight                                                     atomic.Int64
	nextHost                                                     atomic.Uint64
}

func (s *Session) typedRuntime() *sessionRuntime {
	s.typedMu.Lock()
	defer s.typedMu.Unlock()
	if s.typedState == nil {
		s.typedState = &sessionRuntime{prepared: make(map[prepareCacheKey]*preparedState)}
	}
	return s.typedState
}
func (rt *sessionRuntime) metricsSnapshot() MetricsSnapshot {
	return MetricsSnapshot{Requests: rt.requests.Load(), Attempts: rt.attempts.Load(), Errors: rt.errors.Load(), Retries: rt.retries.Load(), Speculations: rt.speculations.Load(), Cancelled: rt.cancelled.Load(), InFlight: rt.inflight.Load()}
}
func (s *Session) typedGeneration() uint64 { return s.typedSchemaGeneration() }

func (s *Session) selectTypedConn(ctx context.Context, routing RoutingInfo, plan QueryPlan) (*Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, ErrClosed
	}
	if s.pool == nil || s.hostSource == nil {
		return nil, ErrNoConnections
	}
	hosts := s.hostSource.getHostsList()
	if len(hosts) == 0 {
		return nil, ErrNoConnections
	}
	pick := func(host *HostInfo, target Target) *Conn {
		if host == nil || !host.IsUp() || s.cfg.filterHost(host) {
			return nil
		}
		pool, ok := s.pool.getPool(host)
		if !ok {
			return nil
		}
		return s.pickTypedPool(pool, target, routing)
	}
	if plan != nil {
		for {
			target, ok := plan.Next()
			if !ok {
				break
			}
			for _, host := range hosts {
				if host.hostUUID() == target.HostID {
					if conn := pick(host, target); conn != nil {
						return conn, nil
					}
					break
				}
			}
		}
		return nil, ErrNoConnections
	}
	if routing.HostID.Set {
		for _, host := range hosts {
			if host.hostUUID() == routing.HostID.Value {
				if conn := pick(host, Target{HostID: routing.HostID.Value, ShardID: routing.ShardID}); conn != nil {
					return conn, nil
				}
				break
			}
		}
		return nil, ErrNoConnections
	}
	start := int(s.typedRuntime().nextHost.Add(1) % uint64(len(hosts)))
	for i := 0; i < len(hosts); i++ {
		if conn := pick(hosts[(start+i)%len(hosts)], Target{}); conn != nil {
			return conn, nil
		}
	}
	return nil, ErrNoConnections
}
func (s *Session) prepareTyped(ctx context.Context, statement, keyspace string, options PrepareOptions) (state *preparedState, err error) {
	rt := s.typedRuntime()
	generation := s.typedGeneration()
	tracker := rt.schemaTracker()
	started := tracker.start()
	key := prepareCacheKey{cql: statement, keyspace: keyspace}
	rt.mu.Lock()
	cached := rt.prepared[key]
	if cached != nil && cached.cacheCurrent() {
		rt.touchPrepare(key)
		rt.mu.Unlock()
		return cached, nil
	}
	if cached != nil {
		delete(rt.prepared, key)
		rt.forgetPrepareOrder(key)
	}
	if flight := rt.preparing[key]; flight != nil {
		rt.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			return flight.state, flight.err
		}
	}
	flight := &prepareFlight{done: make(chan struct{})}
	if rt.preparing == nil {
		rt.preparing = make(map[prepareCacheKey]*prepareFlight)
	}
	rt.preparing[key] = flight
	rt.mu.Unlock()
	defer func() {
		rt.mu.Lock()
		flight.state = state
		flight.err = err
		delete(rt.preparing, key)
		close(flight.done)
		rt.mu.Unlock()
	}()
	conn := options.connection
	if conn == nil {
		conn, err = s.selectTypedConn(ctx, RoutingInfo{HostID: options.HostID}, nil)
	}
	if err != nil {
		return nil, err
	}
	wireKeyspace := keyspace
	if conn.version < 5 {
		if keyspace != s.cfg.Keyspace {
			return nil, typeValidation("prepare keyspace", "protocol v3/v4 requires the session keyspace")
		}
		wireKeyspace = ""
	}
	page, err := conn.executeTyped(ctx, &attemptRequest{statement: statement, keyspace: wireKeyspace, prepare: true, trace: options.Trace})
	if page != nil {
		defer page.owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	info, err := s.hostPreparedInfo(conn, page, &preparedState{statement: statement, keyspace: keyspace})
	if err != nil {
		return nil, err
	}
	info.Generation = generation
	dependencies := tracker.dependencies(info.Keyspace, page.bindColumns, page.columns)
	epoch := dependencies.since(started)
	result := &typedResultMetadata{columns: page.columns, epoch: epoch, tracker: dependencies.tracker}
	state = &preparedState{session: s, protocol: conn.version, statement: statement, keyspace: keyspace, parameters: page.bindColumns, columns: page.columns, info: info, dependencies: dependencies, initialEpoch: epoch, result: result, resultHost: conn.host.hostUUID(), partitioner: s.preparedPartitionerName(info.Keyspace, info.Table), hosts: map[UUID]*hostPrepared{conn.host.hostUUID(): {id: page.preparedID, metadataID: page.metadataID, columns: page.columns, generation: generation, revision: 1, epoch: epoch, result: result}}}

	tracker.mu.RLock()
	defer tracker.mu.RUnlock()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if cached = rt.prepared[key]; cached != nil && cached.cacheCurrent() {
		rt.touchPrepare(key)
		return cached, nil
	}
	limit := rt.config.PreparedCacheSize
	if rt.profile == nil && limit == 0 {
		limit = 1000
	}
	if limit == 0 || !epoch.current() {
		return state, nil
	}
	for len(rt.prepared) >= limit && len(rt.prepareOrder) > 0 {
		old := rt.prepareOrder[0]
		rt.prepareOrder[0] = prepareCacheKey{}
		rt.prepareOrder = rt.prepareOrder[1:]
		delete(rt.prepared, old)
	}
	rt.prepared[key] = state
	rt.touchPrepare(key)
	return state, nil
}
func (s *Session) prepareOnConn(ctx context.Context, conn *Conn, state *preparedState, values encodedValues, force bool) (*hostPrepared, error) {
	return s.prepareOnConnExpected(ctx, conn, state, values, force, nil)
}
func (s *Session) prepareOnConnExpected(ctx context.Context, conn *Conn, state *preparedState, values encodedValues, force bool, failed *hostPrepared) (*hostPrepared, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Closed() {
		return nil, ErrClosed
	}
	if err := state.reconcilePendingConflict(); err != nil {
		return nil, err
	}
	id := conn.host.hostUUID()
	state.mu.RLock()
	cached := state.hosts[id]
	state.mu.RUnlock()
	if cached != nil && cached.epoch.current() && (!force || failed != nil && cached != failed) {
		return cached, nil
	}
	epoch := state.dependencies.snapshot()
	state.mu.Lock()
	cached = state.hosts[id]
	if cached != nil && cached.epoch.current() && (!force || failed != nil && cached != failed) {
		state.mu.Unlock()
		return cached, nil
	}
	flight := state.preparing[id]
	if flight == nil || flight.previous != cached || !flight.epoch.equal(epoch) {
		flight = &hostPrepareFlight{done: make(chan struct{}), previous: cached, epoch: epoch, started: state.dependencies.tracker.start()}
		if state.preparing == nil {
			state.preparing = make(map[UUID]*hostPrepareFlight)
		}
		rt := s.typedRuntime()
		if !rt.beginPreparedWorker(s) {
			state.mu.Unlock()
			return nil, ErrClosed
		}
		state.preparing[id] = flight
		parameters := values.columns
		go func() {
			defer rt.prepareWorkers.Done()
			timeout := s.typedRuntime().config.Metadata.RequestTimeout
			if timeout <= 0 {
				timeout = 60 * time.Second
			}
			sharedCtx, cancel := context.WithTimeout(conn.ctx, timeout)
			defer cancel()
			result, err := s.prepareHost(sharedCtx, conn, state, parameters, flight)
			state.mu.Lock()
			flight.result = result
			flight.err = err
			if state.preparing[id] == flight {
				delete(state.preparing, id)
			}
			close(flight.done)
			state.mu.Unlock()
		}()
	}
	flight.waiters.Add(1)
	state.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		return flight.result, flight.err
	}
}
func (s *Session) prepareHost(ctx context.Context, conn *Conn, state *preparedState, parameters Columns, flight *hostPrepareFlight) (entry *hostPrepared, err error) {
	conflict := false
	defer func() {
		if conflict {
			if refreshErr := state.noteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	wireKeyspace := state.keyspace
	if conn.version < 5 {
		wireKeyspace = ""
	}
	page, err := conn.executeTyped(ctx, &attemptRequest{statement: state.statement, keyspace: wireKeyspace, prepare: true})
	if page != nil {
		defer page.owner.Release()
	}
	if err != nil {
		return nil, err
	}
	if page == nil {
		return nil, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	if !state.parameters.Equal(page.bindColumns) || !parameters.Equal(page.bindColumns) {
		return nil, &SchemaChangedError{Statement: state.statement, Previous: parameters, Current: page.bindColumns}
	}
	info, err := s.hostPreparedInfo(conn, page, state)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(info.PartitionKeyIndexes, state.info.PartitionKeyIndexes) || info.Keyspace != state.info.Keyspace || info.Table != state.info.Table || info.Conditional != state.info.Conditional || s.preparedPartitionerName(info.Keyspace, info.Table) != state.partitioner {
		return nil, &SchemaChangedError{Statement: state.statement, Previous: state.parameters, Current: page.bindColumns}
	}
	revision := uint64(1)
	if flight.previous != nil {
		revision = flight.previous.revision + 1
	}
	state.dependencies.extend(state.info.Keyspace, state.parameters, page.columns)
	epoch := state.dependencies.since(flight.started)
	result := &typedResultMetadata{columns: page.columns, epoch: epoch, tracker: state.dependencies.tracker}
	state.mu.RLock()
	if state.result != nil && state.result.columns.Equal(page.columns) {
		result.columns = state.result.columns
		if state.result.epoch.equal(epoch) {
			result = state.result
		}
	}
	state.mu.RUnlock()
	candidate := &hostPrepared{id: page.preparedID, metadataID: page.metadataID, columns: result.columns, generation: s.typedGeneration(), revision: revision, epoch: epoch, result: result}

	// Exact-entry/epoch CAS. A losing response keeps its own immutable candidate;
	// it is never republished against a newly loaded host revision.
	state.dependencies.tracker.mu.RLock()
	defer state.dependencies.tracker.mu.RUnlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.hosts[conn.host.hostUUID()] == flight.previous && flight.epoch.current() && candidate.epoch.current() {
		state.hosts[conn.host.hostUUID()] = candidate
		conflict = state.resultHost != conn.host.hostUUID() && state.result != nil && !state.result.columns.Equal(result.columns)
		if !conflict {
			state.resultHost = conn.host.hostUUID()
			state.result = result
			state.initialEpoch = candidate.epoch
			state.columns = result.columns
		}
	}
	return candidate, nil
}
func (s *Session) hostPreparedInfo(conn *Conn, page *responsePage, state *preparedState) (PreparedInfo, error) {
	info := PreparedInfo{Keyspace: state.keyspace, PartitionKeyIndexes: page.pkIndexes, Conditional: page.lwt || conditionalCQL(state.statement)}
	for _, columns := range []Columns{page.bindColumns, page.columns} {
		if col, ok := columns.At(0); ok && info.Table == "" {
			if col.Keyspace != "" {
				info.Keyspace = col.Keyspace
			}
			info.Table = col.Table
		}
	}
	if conn.version < 4 && len(info.PartitionKeyIndexes) == 0 && info.Table != "" {
		if ks := s.Metadata().keyspaces[info.Keyspace]; ks != nil {
			if table := ks.Tables[info.Table]; table != nil {
				for _, key := range table.PartitionKey {
					index := -1
					for i, c := range page.bindColumns.columns {
						if c.Name == key.Name && c.Table == info.Table && c.Keyspace == info.Keyspace {
							index = i
							break
						}
					}
					if index < 0 {
						info.PartitionKeyIndexes = nil
						break
					}
					info.PartitionKeyIndexes = append(info.PartitionKeyIndexes, index)
				}
			}
		}
	}
	for _, index := range info.PartitionKeyIndexes {
		if index < 0 || index >= page.bindColumns.Len() {
			return PreparedInfo{}, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("invalid partition key index %d", index)}
		}
	}
	return info, nil
}

// selectPreparedResponse resolves a response using only the attempt's retained
// immutable entry. Later schema events do not invalidate bytes already received.
func selectPreparedResponse(state *preparedState, host UUID, prepared *hostPrepared, sentID []byte, tracksID bool, response *responsePage, observed ...uint64) (err error) {
	if response.noMetadata {
		if prepared == nil || prepared.result == nil || prepared.columns.Len() != response.columnCount || !bytes.Equal(prepared.metadataID, sentID) || len(response.metadataID) > 0 && !bytes.Equal(response.metadataID, prepared.metadataID) || tracksID && len(prepared.metadataID) == 0 {
			return &ProtocolError{Stage: "result metadata", Err: fmt.Errorf("missing matching metadata")}
		}
		response.columns = prepared.columns
		response.metadata = prepared.result
		return nil
	}
	if state == nil || prepared == nil || response.kind != 2 {
		return nil
	}
	conflict := false
	defer func() {
		if conflict {
			if refreshErr := state.noteMetadataConflict(); err == nil {
				err = refreshErr
			}
		}
	}()
	state.mu.RLock()
	conflict = state.hosts[host] == prepared && state.resultHost != host && state.result != nil && !state.result.columns.Equal(response.columns)
	state.mu.RUnlock()
	changed := !prepared.columns.Equal(response.columns) || len(response.metadataID) > 0 && !bytes.Equal(prepared.metadataID, response.metadataID)
	if !changed {
		response.columns = prepared.columns
		response.metadata = prepared.result
		return nil
	}
	result := prepared.result
	if !prepared.columns.Equal(response.columns) {
		result = &typedResultMetadata{columns: response.columns, tracker: state.dependencies.tracker}
	}
	response.metadata = result
	started := state.dependencies.tracker.start()
	if len(observed) > 0 {
		started = observed[0]
	}
	state.dependencies.extend(state.info.Keyspace, state.parameters, response.columns)
	candidate := *prepared
	candidate.epoch = state.dependencies.since(started)
	if result != prepared.result {
		result.epoch = candidate.epoch
	}
	candidate.revision++
	candidate.columns = result.columns
	candidate.result = result
	candidate.metadataChanged = response.metadataChanged
	if len(response.metadataID) > 0 {
		candidate.metadataID = response.metadataID
	}
	state.dependencies.tracker.mu.RLock()
	defer state.dependencies.tracker.mu.RUnlock()
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.hosts[host] == prepared && prepared.epoch.current() && candidate.epoch.current() {
		conflict = conflict || state.resultHost != host && state.result != nil && !state.result.columns.Equal(response.columns)
		state.hosts[host] = &candidate
		if state.result == prepared.result && !conflict {
			state.resultHost = host
			state.result = result
			state.initialEpoch = candidate.epoch
			state.columns = result.columns
		}
	}
	return nil
}

type logicalExecution struct {
	id       uint64
	started  time.Time
	once     sync.Once
	observer Observer
	session  *Session
	request  *executionRequest
}

func (l *logicalExecution) finish(result Result, err error) {
	if l == nil {
		return
	}
	l.once.Do(func() {
		rt := l.session.typedRuntime()
		rt.inflight.Add(-1)
		l.session.recordV2Latency(time.Since(l.started))
		if err != nil {
			rt.errors.Add(1)
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			rt.cancelled.Add(1)
		}
		if l.observer != nil {
			event := RequestEvent{ID: l.id, Statement: l.request.statement, Parameters: l.request.values.columns, Routing: l.request.routing, Result: cloneResult(result), Err: err}
			if l.request.prepared != nil {
				event.Parameters = l.request.prepared.parameters
			}
			if l.request.batch != nil {
				for _, entry := range l.request.batch.items {
					event.BatchStatements = append(event.BatchStatements, entry.statement)
				}
			}
			l.session.observeSafely(func() { l.observer.ObserveRequest(event) })
		}
	})
}
func (s *Session) observeSafely(fn func()) {
	defer func() {
		if v := recover(); v != nil && s.logger != nil {
			s.logger.Printf("gocql: observer panic: %v", v)
		}
	}()
	fn()
}
func admitExecution[R any](ctx context.Context, s *Session, b *Bound[R], options Options) (context.Context, context.CancelFunc, *executionRequest, error) {
	if ctx == nil || s == nil || b == nil || b.request.owner == nil {
		return nil, nil, nil, typeValidation("execution", "nil context, session or binding")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if s.Closed() {
		return nil, nil, nil, ErrClosed
	}
	resolved, err := resolveOptions(s, options)
	if err != nil {
		return nil, nil, nil, err
	}
	request := b.request
	request.options = resolved
	if options.connection != nil {
		request.connection = options.connection
	}
	if request.prepared != nil && request.prepared.session != s {
		return nil, nil, nil, typeValidation("session", "prepared statement belongs to another session")
	}
	if request.connection != nil && request.connection.session != s {
		return nil, nil, nil, typeValidation("connection", "connection belongs to another session")
	}
	request.keyspace = options.Keyspace
	if request.keyspace == "" {
		request.keyspace = b.request.keyspace
		if request.keyspace == "" {
			request.keyspace = s.cfg.Keyspace
		}
	}
	if options.Keyspace != "" && request.routing.Keyspace != "" && options.Keyspace != request.routing.Keyspace {
		return nil, nil, nil, typeValidation("keyspace", "override differs from bound statement")
	}
	if request.routing.Keyspace == "" {
		request.routing.Keyspace = options.Keyspace
		if request.routing.Keyspace == "" {
			request.routing.Keyspace = s.cfg.Keyspace
		}
	}
	request.routing.Consistency = resolved.profile.Consistency
	request.routing.HostID = options.HostID
	request.routing.ShardID = options.ShardID
	request.routing.Conditional = request.routing.Conditional || resolved.profile.Consistency == Serial || resolved.profile.Consistency == LocalSerial
	if err := request.owner.admit(); err != nil {
		return nil, nil, nil, err
	}
	runctx, cancel := context.WithCancel(ctx)
	if resolved.profile.Timeout > 0 {
		cancel()
		runctx, cancel = context.WithTimeout(ctx, resolved.profile.Timeout)
	}
	rt := s.typedRuntime()
	observer := options.Observer
	if observer == nil {
		observer = rt.config.Observer
	}
	request.logical = &logicalExecution{id: rt.requestID.Add(1), started: time.Now(), observer: observer, session: s, request: &request}
	rt.requests.Add(1)
	rt.inflight.Add(1)
	if !request.routing.Conditional && !requestHasCounter(request) && !request.options.options.Timestamp.Set && rt.config.TimestampGenerator != nil {
		request.options.options.Timestamp = Option[int64]{Value: rt.config.TimestampGenerator(), Set: true}
	}
	return runctx, cancel, &request, nil
}
func Run[R any](ctx context.Context, s *Session, b *Bound[R], options Options) (*Rows[R], error) {
	runctx, cancel, request, err := admitExecution(ctx, s, b, options)
	if err != nil {
		return nil, err
	}
	response, err := s.executePage(runctx, request, StartPage())
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(executionErrorResult(err), err)
		return nil, err
	}
	plans := b.plans
	if plans == nil {
		plans = &resultPlanCache[R]{}
	}
	page, err := newPageWithPlans(response, b.rows, plans, nil)
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(response.result, err)
		return nil, err
	}
	rows := &Rows[R]{ctx: runctx, cancel: cancel, session: s, request: request, decoder: b.rows, page: page, info: response.result, plans: plans, history: pagingHistory{maxCount: s.typedRuntime().config.Limits.MaxPagingStates, maxBytes: s.typedRuntime().config.Limits.MaxPagingStateBytes}}
	rows.startPrefetch()
	rows.release = func() { request.owner.release(); request.logical.finish(rows.info, rows.err) }
	return rows, nil
}
func RunPage[R any](ctx context.Context, s *Session, b *Bound[R], state PageState, options Options) (*Page[R], error) {
	return runSinglePage(ctx, s, b, state, options, false)
}
func RunUnpaged[R any](ctx context.Context, s *Session, b *Bound[R], options Options) (*Page[R], error) {
	return runSinglePage(ctx, s, b, StartPage(), options, true)
}
func runSinglePage[R any](ctx context.Context, s *Session, b *Bound[R], state PageState, options Options, unpaged bool) (*Page[R], error) {
	if state.Complete() {
		return nil, ErrPagingComplete
	}
	runctx, cancel, request, err := admitExecution(ctx, s, b, options)
	if err != nil {
		return nil, err
	}
	if unpaged {
		request.options.profile.PageSize = 0
	}
	response, err := s.executePage(runctx, request, state)
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(executionErrorResult(err), err)
		return nil, err
	}
	if unpaged && response.state.More() {
		response.owner.Release()
		err = typeValidation("unpaged response", "server returned continuation")
		cancel()
		request.owner.release()
		request.logical.finish(response.result, err)
		return nil, err
	}
	plans := b.plans
	if plans == nil {
		plans = &resultPlanCache[R]{}
	}
	page, err := newPageWithPlans(response, b.rows, plans, nil)
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(response.result, err)
		return nil, err
	}
	page.ctx = runctx
	page.cancel = cancel
	page.session = s
	page.release = func() { cancel(); request.owner.release(); request.logical.finish(response.result, page.err) }
	return page, nil
}
func Exec[R any](ctx context.Context, s *Session, b *Bound[R], options Options) (Result, error) {
	if b == nil {
		return Result{}, typeValidation("binding", "nil binding")
	}
	execution := &Bound[Unit]{request: b.request, rows: execRowsDecoder{}}
	rows, err := Run(ctx, s, execution, options)
	if err != nil {
		return executionErrorResult(err), err
	}
	defer rows.Close()
	rows.rejectColumns = true
	for rows.Next() {
	}
	return rows.Info(), rows.Err()
}

type typedAttemptOutcome struct {
	response    *responsePage
	err         error
	conn        *Conn
	event       AttemptEvent
	consistency Consistency
}

func (s *Session) executePage(ctx context.Context, request *executionRequest, state PageState) (*responsePage, error) {
	if request.options.profile.PageTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.options.profile.PageTimeout)
		defer cancel()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var plan QueryPlan
	if policy := request.options.profile.Routing; policy != nil {
		metadata := s.Metadata()
		if err := policy.Validate(metadata); err != nil {
			return nil, err
		}
		plan = policy.Plan(request.routing, metadata)
	}
	outcomes := make(chan typedAttemptOutcome)
	type delayedAttempt struct {
		conn        *Conn
		consistency Consistency
	}
	delayed := make(chan delayedAttempt)
	scheduled := 0
	aggregate := Result{AchievedConsistency: request.options.profile.Consistency}
	startedCount, active := 0, 0
	launch := func(conn *Conn, consistency Consistency, speculative bool) error {
		var err error
		if conn == nil && request.connection != nil {
			conn = request.connection
		}
		if conn == nil {
			conn, err = s.selectTypedConn(ctx, request.routing, plan)
			if err != nil {
				return err
			}
		}
		startedCount++
		active++
		index := startedCount
		request.owner.retain()
		s.typedRuntime().attempts.Add(1)
		if speculative {
			s.typedRuntime().speculations.Add(1)
		}
		go func() {
			defer request.owner.release()
			started := time.Now()
			response, err := s.executeTypedAttempt(ctx, conn, request, state, consistency)
			event := AttemptEvent{Attempt: index, Target: Target{HostID: conn.host.hostUUID(), ShardID: request.routing.ShardID}, Started: started, Latency: time.Since(started), Err: err, Cancelled: errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded), Speculative: speculative}
			if conn.scyllaSupported.nrShards > 0 {
				event.Target.ShardID = Option[uint32]{Value: uint32(conn.scyllaSupported.shard), Set: true}
			}
			if request.logical != nil {
				event.RequestID = request.logical.id
				if request.logical.observer != nil {
					s.observeSafely(func() { request.logical.observer.ObserveAttempt(event) })
				}
			}
			if policy := request.options.profile.Routing; policy != nil {
				s.observeSafely(func() { policy.Observe(event) })
			}
			select {
			case outcomes <- typedAttemptOutcome{response: response, err: err, conn: conn, event: event, consistency: consistency}:
			case <-ctx.Done():
				if response != nil {
					response.owner.Release()
				}
			}
		}()
		return nil
	}
	if err := launch(nil, request.options.profile.Consistency, false); err != nil {
		return nil, &ExecutionError{Err: err, Result: aggregate}
	}
	var timer *time.Timer
	var speculate <-chan time.Time
	arm := func() {
		if timer != nil {
			timer.Stop()
			timer = nil
		}
		speculate = nil
		if !request.options.options.Idempotent || request.routing.Conditional || request.options.profile.Speculation == nil {
			return
		}
		delay, ok := request.options.profile.Speculation.Delay(startedCount, s.Metrics())
		if !ok || delay < 0 {
			return
		}
		timer = time.NewTimer(delay)
		speculate = timer.C
	}
	arm()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			aggregate.Attempts = startedCount
			return nil, &ExecutionError{Err: ctx.Err(), Result: aggregate}
		case pending := <-delayed:
			scheduled--
			if err := launch(pending.conn, pending.consistency, false); err != nil {
				lastErr = err
				if active == 0 && scheduled == 0 {
					return nil, &ExecutionError{Err: err, Result: aggregate}
				}
			} else {
				s.typedRuntime().retries.Add(1)
				arm()
			}
		case <-speculate:
			if err := launch(nil, request.options.profile.Consistency, true); err == nil {
				arm()
			} else {
				speculate = nil
			}
		case outcome := <-outcomes:
			active--
			response, attemptErr := outcome.response, outcome.err
			aggregate.Attempts = startedCount
			aggregate.Latency += outcome.event.Latency
			aggregate.HostID = outcome.event.Target.HostID
			aggregate.AchievedConsistency = outcome.consistency
			if response != nil {
				aggregate.Warnings = append(aggregate.Warnings, response.warnings...)
				if len(response.traceID) == 16 {
					var id UUID
					copy(id[:], response.traceID)
					aggregate.TraceIDs = append(aggregate.TraceIDs, id)
				}
				for _, entry := range response.payload {
					if entry.Value.Valid {
						entry.Value.Value = entry.Value.Value.Clone()
					}
					aggregate.CustomPayload = append(aggregate.CustomPayload, entry)
				}
			}
			if attemptErr == nil {
				response.result = aggregate
				response.result.Rows = int64(response.rowCount)
				if response.pagingState != nil {
					response.state = PageState{token: response.pagingState, kind: 1}
				} else {
					response.state = completePageState()
				}
				return response, nil
			}
			lastErr = attemptErr
			if response != nil {
				response.owner.Release()
			}
			decision := RetryDecision{}
			if retry := request.options.profile.Retry; retry != nil {
				decision = retry.Decide(AttemptError{Err: attemptErr, Attempt: outcome.event.Attempt, Idempotent: request.options.options.Idempotent, Consistency: outcome.consistency, Conditional: request.routing.Conditional, BytesWritten: attemptBytesWritten(attemptErr)})
			}
			if decision.Action == IgnoreWriteError {
				return &responsePage{state: completePageState(), result: aggregate}, nil
			}
			if decision.Action == StopRetry {
				if active == 0 && scheduled == 0 {
					return nil, &ExecutionError{Err: lastErr, Result: aggregate}
				}
				continue
			}
			if decision.Action != RetrySame && decision.Action != RetryNext {
				return nil, &ExecutionError{Err: typeValidation("retry", "unknown retry action"), Result: aggregate}
			}
			consistency := outcome.consistency
			if decision.Consistency.Set {
				if !validConsistency(decision.Consistency.Value) {
					return nil, typeValidation("retry consistency", "invalid consistency")
				}
				consistency = decision.Consistency.Value
			}
			if decision.Delay < 0 {
				return nil, typeValidation("retry delay", "negative delay")
			}
			conn := outcome.conn
			if decision.Action == RetryNext {
				conn = nil
			}
			if decision.Delay > 0 {
				scheduled++
				request.owner.retain()
				go func(wait time.Duration, next delayedAttempt) {
					defer request.owner.release()
					timer := time.NewTimer(wait)
					defer timer.Stop()
					select {
					case <-ctx.Done():
						return
					case <-timer.C:
					}
					select {
					case delayed <- next:
					case <-ctx.Done():
					}
				}(decision.Delay, delayedAttempt{conn: conn, consistency: consistency})
				continue
			}

			if err := launch(conn, consistency, false); err != nil {
				lastErr = err
				if active == 0 {
					return nil, &ExecutionError{Err: lastErr, Result: aggregate}
				}
			} else {
				s.typedRuntime().retries.Add(1)
				arm()
			}
		}
	}
}
func (s *Session) executeTypedAttempt(ctx context.Context, conn *Conn, request *executionRequest, state PageState, consistency Consistency) (*responsePage, error) {
	opts := request.options.options
	if conn.version < 5 && request.keyspace != "" && request.keyspace != s.cfg.Keyspace {
		return nil, typeValidation("keyspace", "protocol v3/v4 requires the session keyspace")
	}
	if conn.version < 4 {
		if len(opts.CustomPayload) > 0 {
			return nil, typeValidation("payload", "requires protocol v4")
		}
		for _, cell := range request.values.cells {
			if cell.state == Unset {
				return nil, typeValidation("unset", "requires protocol v4")
			}
		}
	}
	if opts.NowInSeconds.Set && conn.version < 5 {
		return nil, typeValidation("now in seconds", "requires protocol v5")
	}
	wireRequest := &attemptRequest{statement: request.statement, values: request.values.framed, valueCount: len(request.values.cells), consistency: consistency, pageSize: int(request.options.profile.PageSize), keyspace: request.keyspace, trace: opts.Trace, payload: opts.CustomPayload}
	if conn.version < 5 {
		wireRequest.keyspace = ""
	}
	if state.More() {
		wireRequest.pagingState = state.token
	}
	if request.options.profile.SerialConsistency.Set {
		wireRequest.serialConsistency = request.options.profile.SerialConsistency.Value
	}
	if opts.Timestamp.Set {
		wireRequest.timestamp = opts.Timestamp.Value
		wireRequest.defaultTimestamp = true
	}
	if opts.NowInSeconds.Set {
		now := opts.NowInSeconds.Value
		wireRequest.nowInSeconds = &now
	}
	var prepared *hostPrepared
	var err error
	if request.prepared != nil {
		prepared, err = s.prepareOnConn(ctx, conn, request.prepared, request.values, false)
		if err != nil {
			return nil, err
		}
		wireRequest.preparedID = prepared.id
		wireRequest.resultMetadataID = prepared.metadataID
		wireRequest.skipMetadata = !opts.IncludeMetadata && metadataIDTracked(conn.tracksResultMetadataID(), prepared.metadataID)
	}
	if request.batch != nil {
		wireRequest.batch, err = s.prepareBatchAttempt(ctx, conn, request.batch)
		if err != nil {
			return nil, err
		}
	}
	lease := request.owner.writerLease(request.values.framed)
	defer lease.Release()
	wireRequest.lease = lease
	for repair := 0; repair < 2; repair++ {
		observed := uint64(0)
		if request.prepared != nil {
			observed = request.prepared.dependencies.tracker.start()
		}
		response, executeErr := conn.executeTyped(ctx, wireRequest)
		s.applyTypedFeedback(response, request.routing)
		var unprepared *RequestErrUnprepared
		if executeErr != nil && errors.As(executeErr, &unprepared) && repair == 0 {
			if response != nil {
				response.owner.Release()
			}
			if request.prepared != nil && bytes.Equal(unprepared.StatementId, wireRequest.preparedID) {
				prepared, err = s.prepareOnConnExpected(ctx, conn, request.prepared, request.values, true, prepared)
				if err != nil {
					return nil, err
				}
				wireRequest.preparedID = prepared.id
				wireRequest.resultMetadataID = prepared.metadataID
				wireRequest.skipMetadata = !opts.IncludeMetadata && metadataIDTracked(conn.tracksResultMetadataID(), prepared.metadataID)
				continue
			}
			if request.batch != nil {
				if err = s.repairBatch(ctx, conn, request.batch, unprepared.StatementId); err != nil {
					return nil, err
				}
				wireRequest.batch, err = s.prepareBatchAttempt(ctx, conn, request.batch)
				if err != nil {
					return nil, err
				}
				continue
			}
			return nil, executeErr
		}
		if executeErr != nil {
			return response, executeErr
		}
		if response == nil {
			return nil, &ProtocolError{Stage: "execute", Err: fmt.Errorf("missing response")}
		}
		if err := selectPreparedResponse(request.prepared, conn.host.hostUUID(), prepared, wireRequest.resultMetadataID, conn.tracksResultMetadataID(), response, observed); err != nil {
			response.owner.Release()
			return nil, err
		}

		if response.schemaChange {
			config := s.typedRuntime().config.Metadata
			if config.AutomaticAgreement {
				if err := s.awaitV2SchemaAgreementOn(ctx, conn); err != nil {
					return response, err
				}
			}
			if config.RefreshAfterAgreement {
				var refreshErr error
				if response.keyspace != "" {
					refreshErr = s.refreshV2Keyspace(ctx, response.keyspace)
				} else {
					refreshErr = s.RefreshMetadata(ctx)
				}
				if refreshErr != nil && !errors.Is(refreshErr, ErrKeyspaceDoesNotExist) {
					return response, refreshErr
				}
			}
		}
		return response, nil
	}
	return nil, fmt.Errorf("gocql: failed unprepared repair")
}

// pickTypedPool carries native shard targets through connection selection without
// manufacturing an old query object or discarding table-specific tablet routing.
func (s *Session) pickTypedPool(pool *hostConnPool, target Target, routing RoutingInfo) *Conn {
	defer pool.requestTypedRepair()
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	if !pool.pickable() {
		return nil
	}
	if picker, ok := pool.connPicker.(*typedConnPicker); ok {
		return picker.pickTarget(target, routing)
	}
	picker, ok := pool.connPicker.(*scyllaConnPicker)
	if !ok {
		if routing.Token.Set {
			if p, ok := pool.connPicker.(int64ConnPicker); ok {
				return p.PickInt64(routing.Token.Value, nil)
			}
		}
		return pool.connPicker.Pick(nil, nil)
	}
	shard := target.ShardID
	if !shard.Set && routing.Token.Set && routing.Keyspace != "" && routing.Table != "" {
		if targets, err := s.Metadata().Replicas(routing.Keyspace, routing.Table, routing.Token.Value); err == nil {
			for _, candidate := range targets {
				if candidate.HostID == pool.host.hostUUID() && candidate.ShardID.Set {
					shard = candidate.ShardID
					break
				}
			}
		}
	}
	picker.mu.RLock()
	defer picker.mu.RUnlock()
	if len(picker.conns) == 0 {
		return nil
	}
	index := -1
	if shard.Set {
		index = int(shard.Value)
	} else if routing.Token.Set {
		index = picker.shardOf(int64Token(routing.Token.Value))
	}
	if index >= 0 && index < len(picker.conns) && picker.conns[index] != nil {
		conn := picker.conns[index]
		if shard.Set || routing.Conditional {
			return conn
		}
		return picker.maybeReplaceWithLessBusyConnection(conn)
	}
	return picker.leastBusyConn()
}

func (rt *sessionRuntime) forgetPrepareOrder(key prepareCacheKey) {
	order := rt.prepareOrder[:0]
	for _, old := range rt.prepareOrder {
		if old != key {
			order = append(order, old)
		}
	}
	clear(rt.prepareOrder[len(order):])
	rt.prepareOrder = order
}
func (rt *sessionRuntime) touchPrepare(key prepareCacheKey) {
	rt.forgetPrepareOrder(key)
	rt.prepareOrder = append(rt.prepareOrder, key)
}

func executionErrorResult(err error) Result {
	var failure *ExecutionError
	if errors.As(err, &failure) {
		return failure.Result
	}
	return Result{}
}
func attemptBytesWritten(err error) bool {
	var query *QueryError
	if errors.As(err, &query) {
		return query.PotentiallyExecuted()
	}
	return false
}

func (s *Session) applyTypedFeedback(response *responsePage, routing RoutingInfo) {
	if response == nil || routing.Keyspace == "" || routing.Table == "" || s.metadataDescriber == nil {
		return
	}
	for _, entry := range response.payload {
		if entry.Key != "tablets-routing-v1" || !entry.Value.Valid {
			continue
		}
		hint, err := parseTypedTabletHint(entry.Value.Value, routing.Keyspace, routing.Table)
		if err != nil {
			if s.logger != nil {
				s.logger.Printf("gocql: tablet hint rejected: %v", err)
			}
			continue
		}
		s.applyV2TabletHint(hint)
	}
}

func requestHasCounter(request executionRequest) bool {
	if request.batch != nil && request.batch.kind == CounterBatch {
		return true
	}
	for _, cell := range request.values.cells {
		if cell.typ.Kind() == TypeCounter {
			return true
		}
	}
	return false
}
