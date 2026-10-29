package gocql

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type prepareCacheKey struct {
	cql, keyspace string
	generation    uint64
}
type prepareFlight struct {
	done  chan struct{}
	state *preparedState
	err   error
}
type sessionRuntime struct {
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
	key := prepareCacheKey{cql: statement, keyspace: keyspace, generation: generation}
	rt.mu.Lock()
	cached := rt.prepared[key]
	if cached != nil {
		rt.touchPrepare(key)
		rt.mu.Unlock()
		return cached, nil
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
	info := PreparedInfo{Keyspace: keyspace, PartitionKeyIndexes: append([]int(nil), page.pkIndexes...), Conditional: page.lwt || conditionalCQL(statement), Generation: generation}
	if col, ok := page.bindColumns.At(0); ok {
		if info.Keyspace == "" {
			info.Keyspace = col.Keyspace
		}
		info.Table = col.Table
	}
	if info.Table == "" {
		if col, ok := page.columns.At(0); ok {
			info.Table = col.Table
			if info.Keyspace == "" {
				info.Keyspace = col.Keyspace
			}
		}
	}
	if conn.version < 4 && len(info.PartitionKeyIndexes) == 0 && info.Keyspace != "" && info.Table != "" {
		if table := s.Metadata().keyspaces[info.Keyspace]; table != nil {
			if metadata := table.Tables[info.Table]; metadata != nil {
				for _, key := range metadata.PartitionKey {
					index := -1
					for i := 0; i < page.bindColumns.Len(); i++ {
						column, _ := page.bindColumns.At(i)
						if column.Keyspace == info.Keyspace && column.Table == info.Table && column.Name == key.Name {
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
			return nil, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("invalid partition key index %d", index)}
		}
	}
	state = &preparedState{session: s, protocol: conn.version, statement: statement, keyspace: keyspace, parameters: page.bindColumns, columns: page.columns, info: info, hosts: map[UUID]hostPrepared{conn.host.hostUUID(): {id: append([]byte(nil), page.preparedID...), metadataID: append([]byte(nil), page.metadataID...), columns: page.columns, generation: generation}}}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if cached = rt.prepared[key]; cached != nil {
		rt.touchPrepare(key)
		return cached, nil
	}
	limit := rt.config.PreparedCacheSize
	if rt.profile == nil && limit == 0 {
		limit = 1000
	}
	if limit == 0 {
		return state, nil
	}
	if len(rt.prepared) >= limit && len(rt.prepareOrder) > 0 {
		old := rt.prepareOrder[0]
		rt.prepareOrder = rt.prepareOrder[1:]
		delete(rt.prepared, old)
	}
	rt.prepared[key] = state
	rt.touchPrepare(key)
	return state, nil
}
func (s *Session) prepareOnConn(ctx context.Context, conn *Conn, state *preparedState, values encodedValues, force bool) (hostPrepared, error) {
	generation := s.typedGeneration()
	id := conn.host.hostUUID()
	state.mu.RLock()
	cached, ok := state.hosts[id]
	expected := state.parameters
	state.mu.RUnlock()
	if ok && !force && cached.generation == generation {
		return cached, nil
	}
	wireKeyspace := state.keyspace
	if conn.version < 5 {
		wireKeyspace = ""
	}
	page, err := conn.executeTyped(ctx, &attemptRequest{statement: state.statement, keyspace: wireKeyspace, prepare: true})
	if page != nil {
		defer page.owner.Release()
	}
	if err != nil {
		return hostPrepared{}, err
	}
	if page == nil {
		return hostPrepared{}, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("missing response")}
	}
	if !expected.Equal(page.bindColumns) {
		return hostPrepared{}, &SchemaChangedError{Statement: state.statement, Previous: expected, Current: page.bindColumns}
	}
	for _, index := range page.pkIndexes {
		if index < 0 || index >= len(values.cells) {
			return hostPrepared{}, &ProtocolError{Stage: "prepare", Err: fmt.Errorf("invalid partition key index %d", index)}
		}
	}
	previous := cached
	cached = hostPrepared{id: append([]byte(nil), page.preparedID...), metadataID: append([]byte(nil), page.metadataID...), columns: page.columns, generation: generation, revision: previous.revision}
	if !previous.columns.Equal(page.columns) || !bytes.Equal(previous.metadataID, page.metadataID) {
		cached.revision++
	}
	state.mu.Lock()
	current, exists := state.hosts[id]
	if exists && current.generation >= generation && (current.generation > generation || current.revision > previous.revision) {
		state.mu.Unlock()
		return current, nil
	}
	if generation >= state.info.Generation {
		state.hosts[id] = cached
		state.columns = page.columns
		state.info.Generation = generation
		state.info.PartitionKeyIndexes = append([]int(nil), page.pkIndexes...)
		state.info.Conditional = page.lwt || conditionalCQL(state.statement)
	}
	state.mu.Unlock()
	return cached, nil
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
	page, err := newPage(response, b.rows, nil)
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(response.result, err)
		return nil, err
	}
	rows := &Rows[R]{ctx: runctx, cancel: cancel, session: s, request: request, decoder: b.rows, page: page, info: response.result, seen: make(map[string]struct{})}
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
		return nil, typeValidation("page state", "completed page cannot be resumed")
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
	page, err := newPage(response, b.rows, nil)
	if err != nil {
		cancel()
		request.owner.release()
		request.logical.finish(response.result, err)
		return nil, err
	}
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
					response.state = RestorePageState(response.pagingState)
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
		wireRequest.pagingState = state.Bytes()
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
	var prepared hostPrepared
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
		response, executeErr := conn.executeTyped(ctx, wireRequest)
		s.applyTypedFeedback(response, request.routing)
		var unprepared *RequestErrUnprepared
		if executeErr != nil && errors.As(executeErr, &unprepared) && repair == 0 {
			if response != nil {
				response.owner.Release()
			}
			if request.prepared != nil && bytes.Equal(unprepared.StatementId, wireRequest.preparedID) {
				prepared, err = s.prepareOnConn(ctx, conn, request.prepared, request.values, true)
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
		if response.noMetadata {
			if request.prepared == nil || prepared.generation != s.typedGeneration() || prepared.columns.Len() != response.columnCount || !bytes.Equal(prepared.metadataID, wireRequest.resultMetadataID) || (len(response.metadataID) > 0 && !bytes.Equal(response.metadataID, prepared.metadataID)) || (conn.tracksResultMetadataID() && len(prepared.metadataID) == 0) {
				response.owner.Release()
				return nil, &ProtocolError{Stage: "result metadata", Err: fmt.Errorf("missing matching metadata")}
			}
			response.columns = prepared.columns
		}
		if request.prepared != nil && !response.noMetadata && response.kind == 2 {
			state := request.prepared
			state.mu.Lock()
			hp := state.hosts[conn.host.hostUUID()]
			if prepared.generation >= state.info.Generation && hp.generation == prepared.generation && hp.revision == prepared.revision && bytes.Equal(hp.metadataID, prepared.metadataID) {
				changed := !hp.columns.Equal(response.columns) || (len(response.metadataID) > 0 && !bytes.Equal(hp.metadataID, response.metadataID))
				if state.columns.Equal(prepared.columns) {
					state.columns = response.columns
				}
				hp.columns = response.columns
				if len(response.metadataID) > 0 {
					hp.metadataID = append([]byte(nil), response.metadataID...)
				}
				if changed {
					hp.revision++
				}
				hp.metadataChanged = response.metadataChanged
				state.hosts[conn.host.hostUUID()] = hp
			}
			state.mu.Unlock()
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
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	if !pool.pickable() {
		return nil
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

func (rt *sessionRuntime) touchPrepare(key prepareCacheKey) {
	for i, old := range rt.prepareOrder {
		if old == key {
			copy(rt.prepareOrder[i:], rt.prepareOrder[i+1:])
			rt.prepareOrder = rt.prepareOrder[:len(rt.prepareOrder)-1]
			break
		}
	}
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
