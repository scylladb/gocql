/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	mrand "math/rand/v2"
	"net"

	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/debounce"
	"github.com/scylladb/gocql/v2/events"
	"github.com/scylladb/gocql/v2/internal/debug"
	"github.com/scylladb/gocql/v2/internal/eventbus"

	"github.com/scylladb/gocql/v2/tablets"
)

// Session owns cluster metadata, pooled connections and typed executions.
// It is safe for concurrent use. Connect constructs validated sessions.
type Session struct {
	typedMu              sync.Mutex
	typedState           *sessionRuntime
	v2Peripheral         atomic.Pointer[v2PeripheralState]
	control              controlConnection
	ctx                  context.Context
	logger               StdLogger
	policy               HostSelectionPolicy
	connectObserver      ConnectObserver
	frameObserver        FrameHeaderObserver
	streamObserver       StreamObserver
	initErr              error
	nodeEvents           *eventDebouncer
	hostSource           *ringDescriber
	pool                 *policyConnPool
	ringRefresher        *debounce.RefreshDebouncer
	readyCh              chan struct{}
	cancel               context.CancelFunc
	schemaEvents         *eventDebouncer
	metadataDescriber    *metadataDescriber
	eventBus             *eventbus.EventBus[events.Event]
	connCfg              *ConnConfig
	clientRoutesHandler  *ClientRoutesHandler
	driverConfigReporter *driverConfigReporter
	// id is a globally unique identifier for this session, reported to
	// the cluster via the SESSION_ID STARTUP option so that all connections
	// belonging to the same session can be correlated in system.clients.
	id                        string
	addressTranslator         AddressTranslator
	cfg                       clusterSettings
	mu                        sync.RWMutex
	sessionStateMu            sync.RWMutex
	isClosing                 bool
	closeDone                 chan struct{}
	hasAggregatesAndFunctions bool
	useSystemSchema           bool
	tabletsRoutingV1          bool
	isInitialized             bool
	isClosed                  bool
}

func resolveInitialEndpoints(resolver DNSResolver, addrs []string, defaultPort int, logger StdLogger) ([]*HostInfo, error) {
	var hosts []*HostInfo
	var errs []error
	for _, hostaddr := range addrs {
		resolvedHosts, err := resolveInitialEndpoint(resolver, hostaddr, defaultPort)
		if err != nil {
			err = fmt.Errorf("failed to resolve endpoint %q: %w", hostaddr, err)
			errs = append(errs, err)
			logger.Println(err.Error())
			continue
		}
		hosts = append(hosts, resolvedHosts...)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("failed to resolve any of the provided hostnames: %w", errors.Join(errs...))
	}
	return hosts, nil
}

func newSessionCommon(cfg clusterSettings) (*Session, error) {
	cfg.applyClientRoutesConfig()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("gocql: unable to create session: cluster config validation failed: %v", err)
	}

	// TODO: we should take a context in here at some point
	ctx, cancel := context.WithCancel(context.TODO())

	s := &Session{
		cfg:               cfg,
		connectObserver:   cfg.ConnectObserver,
		ctx:               ctx,
		cancel:            cancel,
		logger:            cfg.logger(),
		addressTranslator: cfg.AddressTranslator,
		readyCh:           make(chan struct{}, 1),
	}

	if cfg.ClientRoutesConfig != nil {
		s.clientRoutesHandler = NewClientRoutesAddressTranslator(*cfg.ClientRoutesConfig, s.cfg.DNSResolver, s.cfg.SslOpts != nil, s.logger)
		s.addressTranslator = s.clientRoutesHandler
	}

	if !cfg.DisableDriverConfigReporting {
		s.driverConfigReporter = newDriverConfigReporter(s)
	}
	s.id = newSessionID(s.logger)

	// Close created resources on error otherwise they'll leak
	var err error
	defer func() {
		if err != nil {
			s.Close()
		}
	}()

	s.metadataDescriber = newMetadataDescriber(s)

	s.eventBus = eventbus.New[events.Event](cfg.EventBusConfig, cfg.Logger)
	if err = s.eventBus.Start(); err != nil {
		return nil, fmt.Errorf("gocql: unable to create session: %v", err)
	}

	s.nodeEvents = newEventDebouncer("NodeEvents", s.handleNodeEvent, s.logger)
	s.schemaEvents = newEventDebouncer("SchemaEvents", s.handleSchemaEventAndPublish, s.logger)
	resync := func() {
		s.debounceRingRefresh()
		s.scheduleV2SchemaRetry()
		s.publishV2LocalEvent(Event{Kind: OverflowEvent, Generation: s.Metadata().Generation(), Change: "CONTROL_RESYNC"})
	}
	s.schemaEvents.resync = resync
	s.nodeEvents.resync = resync

	s.hostSource = &ringDescriber{cfg: &s.cfg, logger: s.logger}
	s.ringRefresher = debounce.NewRefreshDebouncer(debounce.RingRefreshDebounceTime, func() error {
		return s.refreshRing()
	})

	if cfg.PoolConfig.HostSelectionPolicy == nil {
		cfg.PoolConfig.HostSelectionPolicy = TokenAwareHostPolicy(RoundRobinHostPolicy())
	}
	s.pool = cfg.PoolConfig.buildPool(s)

	s.policy = cfg.PoolConfig.HostSelectionPolicy
	s.policy.Init(s)
	s.connectObserver = cfg.ConnectObserver
	s.frameObserver = cfg.FrameHeaderObserver
	s.streamObserver = cfg.StreamObserver

	//Check the TLS Config before trying to connect to anything external
	connCfg, err := connConfig(&s.cfg)
	if err != nil {
		//TODO: Return a typed error
		return nil, fmt.Errorf("gocql: unable to create session: %v", err)
	}
	s.connCfg = connCfg
	return s, nil
}

// sessionIDStartupKey is the STARTUP option correlating the connections that
// belong to one session in system.clients. Unlike driverConfigStartupKey it is
// sent on every connection, since correlating them is the whole point.
const sessionIDStartupKey = "SESSION_ID"

// newSessionID generates a globally unique identifier for a session.
//
// Every connection reports one, so this always returns an id rather than
// propagating a failure to generate it.
//
// The fallback is defensive rather than reachable: RandomUUID reads from
// crypto/rand, which since Go 1.24 terminates the process instead of
// returning an error when the system entropy source fails
// (golang/go#66821). Since RandomUUID's signature still admits an error,
// handling it costs little and keeps the id unconditional whatever a future
// implementation does.
func newSessionID(logger StdLogger) string {
	id, err := RandomUUID()
	if err != nil {
		logger.Printf("gocql: unable to generate a random session id, falling back to a pseudo-random one: %v", err)
		id = pseudoRandomUUID()
	}
	return id.String()
}

// pseudoRandomUUID builds a version 4 UUID from the runtime's pseudo-random
// source, for the case where crypto/rand reports a failure instead of
// aborting the process. A session id merely correlates the connections of one
// session in system.clients and is never a security token, so a weaker source
// of randomness is preferable to no id at all.
func pseudoRandomUUID() UUID {
	var u UUID
	binary.BigEndian.PutUint64(u[:8], mrand.Uint64())
	binary.BigEndian.PutUint64(u[8:], mrand.Uint64())
	u[6] = u[6]&0x0F | 0x40 // set version to 4 (random uuid)
	u[8] = u[8]&0x3F | 0x80 // set to IETF variant
	return u
}

// ID returns the globally unique identifier of this session.
//
// Every connection the session opens reports this value to the cluster as the
// SESSION_ID startup option, where it appears in the
// system.clients.client_options column. Logging it, recording it in a support
// bundle or attaching it as a metric label is what allows client-side
// observations to be matched against those rows, instead of correlating by
// address and port.
//
// The id is assigned when the session is created and never changes, so it is
// safe to call concurrently and safe to read after the session is closed.
func (s *Session) ID() string {
	return s.id
}

// connectSettings wraps an existing Node.
func connectSettings(cfg clusterSettings) (*Session, error) {
	s, err := newSessionCommon(cfg)
	if err != nil {
		return nil, err
	}

	if err = s.init(); err != nil {
		if err == ErrNoConnectionsStarted {
			//This error used to be generated inside connectSettings & returned directly
			//Forward it on up to be backwards compatible
			return nil, ErrNoConnectionsStarted
		} else {
			// TODO(zariel): dont wrap this error in fmt.Errorf, return a typed error
			return nil, fmt.Errorf("gocql: unable to create session: %v", err)
		}
	}

	s.readyCh <- struct{}{}
	close(s.readyCh)

	return s, nil
}

func connectSettingsAsync(cfg clusterSettings) (*Session, error) {
	s, err := newSessionCommon(cfg)
	if err != nil {
		return nil, err
	}

	go func() {
		if initErr := s.init(); initErr != nil {
			s.sessionStateMu.Lock()
			s.initErr = fmt.Errorf("gocql: unable to create session: %v", initErr)
			s.sessionStateMu.Unlock()
		}

		s.readyCh <- struct{}{}
		close(s.readyCh)
	}()

	return s, nil
}

// SubscribeToEvents adds a new subscriber to the event bus.
// name: subscriber name
// queueSize: buffer size for the subscriber events, when buffer is overflowed events are dropped
// filter: optional filter function (can be nil to receive all events)
//
// Returns a Subscriber instance that provides access to events and a Stop method.
func (s *Session) SubscribeToEvents(name string, queueSize int, filter eventbus.FilterFunc[events.Event]) *eventbus.Subscriber[events.Event] {
	return s.eventBus.Subscribe(name, queueSize, filter)
}

func (s *Session) init() error {
	if s.cfg.disableInit {
		return nil
	}

	hosts, err := resolveInitialEndpoints(s.cfg.DNSResolver, s.cfg.Hosts, s.cfg.Port, s.logger)
	if err != nil {
		return err
	}

	var partitioner string

	if !s.cfg.disableControlConn {
		s.control = createControlConn(s)
		reconnectionPolicy := s.cfg.InitialReconnectionPolicy
		// Validate rejects a non-positive limit, but a custom policy can still
		// return one here; read it once so the loop runs at least once.
		maxRetries := reconnectionPolicy.GetMaxRetries()
		if maxRetries <= 0 {
			return fmt.Errorf("no connection attempted, %T.GetMaxRetries returned %d", reconnectionPolicy, maxRetries)
		}
		for i := 0; i < maxRetries; i++ {
			if i != 0 {
				time.Sleep(reconnectionPolicy.GetInterval(i))
			}

			if s.cfg.ProtoVersion == 0 {
				var proto int
				proto, err = s.control.discoverProtocol(hosts)
				if err != nil {
					err = fmt.Errorf("unable to discover protocol version: %w\n", err)
					if debug.Enabled {
						s.logger.Println(err.Error())
					}
					continue
				} else if proto == 0 {
					return errors.New("unable to discovery protocol version")
				}

				// TODO(zariel): we really only need this in 1 place
				s.cfg.ProtoVersion = proto
				s.connCfg.ProtoVersion = proto
			}

			if err = s.control.connect(hosts); err != nil {
				err = fmt.Errorf("unable to create control connection: %w\n", err)
				if debug.Enabled {
					s.logger.Println(err.Error())
				}
				continue
			}
			break
		}
		if err != nil {
			return fmt.Errorf("unable to connect to the cluster, last error: %w", err)
		}

		conn := s.control.getConn().conn.(*Conn)
		conn.mu.Lock()
		s.tabletsRoutingV1 = conn.isTabletSupported()
		conn.mu.Unlock()

		s.hostSource.setControlConn(s.control)

		if s.clientRoutesHandler != nil {
			if err = s.clientRoutesHandler.Initialize(s); err != nil {
				return fmt.Errorf("unable to initialize client routes handler: %w", err)
			}
		}

		if !s.cfg.DisableInitialHostLookup {
			var newHosts []*HostInfo
			newHosts, partitioner, err = s.hostSource.GetHostsFromSystem()
			if err != nil {
				return err
			}

			hosts = newHosts // filtered nodes remain visible without pools
		} else {
			hosts, err = s.v2ContactIdentities(v2SchemaContext(s), hosts)
			if err != nil {
				return err
			}
		}

		newer, _ := checkSystemSchema(s.control)
		s.useSystemSchema = newer
		defer conn.finalizeConnection()
	} else {
		// For testing purposes we populate host ids
		for _, host := range hosts {
			if host.hostId.IsEmpty() {
				host.hostId = MustRandomUUID()
			}
		}
	}

	if partitioner != "" {
		s.policy.SetPartitioner(partitioner)
	}

	hostMap := make(map[string]*HostInfo, len(hosts))
	for _, host := range hosts {
		hostMap[host.HostID()] = host
	}

	hosts = hosts[:0]
	// each host will increment left and decrement it after connecting and once
	// there's none left, we'll close hostCh
	var left int64
	// we will receive up to len(hostMap) of messages so create a buffer so we
	// don't end up stuck in a goroutine if we stopped listening
	connectedCh := make(chan struct{}, len(hostMap))
	// we add one here because we don't want to end up closing hostCh until we're
	// done looping and the decerement code might be reached before we've looped
	// again
	atomic.AddInt64(&left, 1)
	for _, host := range hostMap {
		host := s.hostSource.addOrUpdate(host)
		if s.cfg.filterHost(host) {
			continue
		}

		atomic.AddInt64(&left, 1)
		go func() {
			s.pool.addHost(host)
			connectedCh <- struct{}{}

			// if there are no hosts left, then close the hostCh to unblock the loop
			// below if its still waiting
			if atomic.AddInt64(&left, -1) == 0 {
				close(connectedCh)
			}
		}()

		hosts = append(hosts, host)
	}
	// once we're done looping we subtract the one we initially added and check
	// to see if we should close
	if atomic.AddInt64(&left, -1) == 0 {
		close(connectedCh)
	}

	if s.cfg.disableControlConn {
		version := s.hostSource.getHostsList()[0].Version()
		s.useSystemSchema = version.AtLeast(3, 0, 0)
		s.hasAggregatesAndFunctions = version.AtLeast(2, 2, 0)
	}

	// before waiting for them to connect, add them all to the policy so we can
	// utilize efficiencies by calling AddHosts if the policy supports it
	type bulkAddHosts interface {
		AddHosts([]*HostInfo)
	}
	if v, ok := s.policy.(bulkAddHosts); ok {
		v.AddHosts(hosts)
	} else {
		for _, host := range hosts {
			s.policy.AddHost(host)
		}
	}

	readyPolicy, _ := s.policy.(ReadyPolicy)
	// now loop over connectedCh until it's closed (meaning we've connected to all)
	// or until the policy says we're ready
	for range connectedCh {
		if readyPolicy != nil && readyPolicy.Ready() {
			break
		}
	}

	// TODO(zariel): we probably dont need this any more as we verify that we
	// can connect to one of the endpoints supplied by using the control conn.
	// See if there are any connections in the pool
	if s.cfg.ReconnectInterval > 0 {
		go s.reconnectDownedHosts(s.cfg.ReconnectInterval)
	}

	if s.pool.Size() == 0 {
		return ErrNoConnectionsStarted
	}

	// Invoke KeyspaceChanged to let the policy cache the session keyspace
	// parameters. This is used by tokenAwareHostPolicy to discover replicas.
	if !s.cfg.disableControlConn && s.cfg.Keyspace != "" {
		s.policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: s.cfg.Keyspace})
	}

	if err = s.policy.IsOperational(s); err != nil {
		return fmt.Errorf("gocql: unable to create session: %v", err)
	}

	s.sessionStateMu.Lock()
	s.isInitialized = true
	s.sessionStateMu.Unlock()

	return nil
}

// AwaitSchemaAgreement will wait until schema versions across all nodes in the
// cluster are the same (as seen from the point of view of the control connection).
// The maximum amount of time this takes is governed
// by the MaxWaitSchemaAgreement setting in the configuration (default: 60s).
// AwaitSchemaAgreement returns an error in case schema versions are not the same
// after the timeout specified in MaxWaitSchemaAgreement elapses.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	return s.awaitV2SchemaAgreement(ctx)
}

func (s *Session) reconnectDownedHosts(intv time.Duration) {
	if policy := s.typedRuntime().config.Reconnect; policy != nil {
		s.reconnectTypedDownedHosts(policy, intv)
		return
	}
	reconnectTicker := time.NewTicker(intv)
	defer reconnectTicker.Stop()

	for {
		select {
		case <-reconnectTicker.C:
			hosts := s.hostSource.getHostsList()

			// Print session.hostSource for debug.
			if debug.Enabled {
				buf := bytes.NewBufferString("Session.hostSource:")
				for _, h := range hosts {
					buf.WriteString("[" + h.ConnectAddress().String() + ":" + h.State().String() + "]")
				}
				s.logger.Println(buf.String())
			}

			for _, h := range hosts {
				if h.IsUp() {
					continue
				}
				// we let the pool call handleNodeConnected to change the host state
				s.pool.addHost(h)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// SetConsistency sets the default consistency level for this session. This
// setting can also be changed on a per-query basis and the default value
// is Quorum.
func (s *Session) Close() error {

	s.sessionStateMu.Lock()
	if s.isClosing {
		done := s.closeDone
		s.sessionStateMu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	s.isClosing = true
	s.closeDone = make(chan struct{})
	done := s.closeDone
	s.sessionStateMu.Unlock()
	defer close(done)
	preparedRuntime := s.stopPreparedWorkers()
	executions := s.stopExecutions()
	if s.cancel != nil {
		s.cancel()
	}
	s.closeV2Peripheral()

	if s.pool != nil {
		s.pool.Close()
	}

	if s.control != nil {
		s.control.close()
	}

	if s.nodeEvents != nil {
		s.nodeEvents.stop()
	}

	if s.schemaEvents != nil {
		s.schemaEvents.stop()
	}

	if s.eventBus != nil {
		_ = s.eventBus.Stop()
	}

	if s.ringRefresher != nil {
		s.ringRefresher.Stop()
	}

	if s.clientRoutesHandler != nil {
		s.clientRoutesHandler.Stop()
	}

	s.sessionStateMu.Lock()
	s.isClosed = true
	s.sessionStateMu.Unlock()

	s.peripheral().workers.Wait()
	if s.metadataDescriber != nil && s.metadataDescriber.metadata != nil {
		s.metadataDescriber.metadata.tabletsMetadata.Close()
	}
	if preparedRuntime != nil {
		preparedRuntime.prepareWorkers.Wait()
	}
	for _, execution := range executions {
		<-execution.lifetime.done
	}
	return nil
}

func (s *Session) Closed() bool {
	s.sessionStateMu.RLock()
	closed := s.isClosed || s.isClosing
	s.sessionStateMu.RUnlock()
	return closed
}

func (s *Session) initialized() bool {
	s.sessionStateMu.RLock()
	initialized := s.isInitialized
	s.sessionStateMu.RUnlock()
	return initialized
}

func (s *Session) Ready() error {
	s.sessionStateMu.RLock()
	err := ErrSessionNotReady
	if s.isInitialized || s.initErr != nil {
		err = s.initErr
	}
	s.sessionStateMu.RUnlock()
	return err
}

func (s *Session) WaitUntilReady() error {
	<-s.readyCh
	return s.initErr
}

func (s *Session) removeHost(h *HostInfo) {
	s.policy.RemoveHost(h)
	s.pool.removeHost(h.hostUUID())
	s.hostSource.removeHost(h.HostID())
}

// KeyspaceMetadata returns the schema metadata for the keyspace specified. Returns an error if the keyspace does not exist.
func (s *Session) KeyspaceMetadata(keyspace string) (*KeyspaceMetadata, error) {
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if err := s.Ready(); err != nil {
		return nil, err
	} else if keyspace == "" {
		return nil, ErrNoKeyspace
	}

	return s.metadataDescriber.GetKeyspace(keyspace)
}

// TableMetadata returns the schema metadata for the specified table. Returns an error if the keyspace or table does not exist.
func (s *Session) TableMetadata(keyspace, table string) (*TableMetadata, error) {
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if err := s.Ready(); err != nil {
		return nil, err
	} else if keyspace == "" {
		return nil, ErrNoKeyspace
	} else if table == "" {
		return nil, ErrNoTable
	}

	return s.metadataDescriber.GetTable(keyspace, table)
}

// TabletsMetadata returns the metadata about all tablets across all keyspaces and tables.
//
// Deprecated: Use [Session.TableTabletsMetadata] for per-table lookups or
// [Session.ForEachTablet] to iterate without aggregating into a flat list.
func (s *Session) TabletsMetadata() (tablets.TabletInfoList, error) {
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if err := s.Ready(); err != nil {
		return nil, err
	} else if !s.tabletsRoutingV1 {
		return nil, ErrTabletsNotUsed
	}

	return s.metadataDescriber.getTablets(), nil
}

// TableTabletsMetadata returns the tablet metadata for the specified keyspace and table.
// Returns (nil, nil) when no tablets exist for the given keyspace/table.
func (s *Session) TableTabletsMetadata(keyspace, table string) (tablets.TabletEntryList, error) {
	// fail fast
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if err := s.Ready(); err != nil {
		return nil, err
	} else if !s.tabletsRoutingV1 {
		return nil, ErrTabletsNotUsed
	} else if keyspace == "" {
		return nil, ErrNoKeyspace
	} else if table == "" {
		return nil, ErrNoTable
	}

	return s.metadataDescriber.getTableTablets(keyspace, table), nil
}

// ForEachTablet iterates over all keyspace/table pairs and their tablet entries,
// calling fn for each one. If fn returns false, iteration stops early.
// The entries slice is a shallow copy; do not mutate individual entries.
func (s *Session) ForEachTablet(fn func(keyspace, table string, entries tablets.TabletEntryList) bool) error {
	// fail fast
	if s.Closed() {
		return ErrSessionClosed
	} else if err := s.Ready(); err != nil {
		return err
	} else if !s.tabletsRoutingV1 {
		return ErrTabletsNotUsed
	}
	if fn == nil {
		return nil
	}

	s.metadataDescriber.forEachTablet(fn)
	return nil
}

func (s *Session) getConn() *Conn {
	hosts := s.hostSource.getHostsList()
	for _, host := range hosts {
		if !host.IsUp() {
			continue
		}

		pool, ok := s.pool.getPool(host)
		if !ok {
			continue
		} else if conn := pool.Pick(nil); conn != nil {
			return conn
		}
	}

	return nil
}

// findTabletReplicasUnsafeForToken returns the raw replica slice for the tablet
// owning the given token. The returned slice must not be modified by callers.
func translateAddressPort(addressTranslator AddressTranslator, host *HostInfo, addr AddressPort, logger StdLogger) (AddressPort, error) {
	if addressTranslator == nil || !addr.IsValid() {
		return addr, nil
	}
	translatorV2, ok := addressTranslator.(AddressTranslatorV2)
	if !ok {
		newAddr, newPort := addressTranslator.Translate(addr.Address, int(addr.Port))
		if debug.Enabled {
			logger.Printf("gocql: translated address %q to '%v:%d'", addr, newAddr, newPort)
		}
		if newPort < 1 || newPort > 65535 {
			return addr, fmt.Errorf("invalid translated port %d: port must be between 1 and 65535", newPort)
		}
		return AddressPort{
			Address: newAddr,
			Port:    uint16(newPort),
		}, nil
	}
	newAddr, err := translatorV2.TranslateHost(host, addr)
	if err != nil {
		if debug.Enabled {
			logger.Printf("gocql: failed to translate address %q: %s", addr, err.Error())
		}
		return addr, err
	}
	if debug.Enabled {
		logger.Printf("gocql: translated address %q to %q", addr, newAddr)
	}
	return newAddr, nil
}

// routing key indexes LRU cache
// routingKeyInfoCacheKey avoids concatenating keyspace+stmt into a string on
// every cache lookup; lru.Cache already takes a comparable key type for
// exactly this reason.
func (s *Session) GetHosts() []*HostInfo {
	return s.hostSource.getHostsList()
}

type HostInformation interface {
	Peer() net.IP
	ConnectAddress() net.IP
	UntranslatedConnectAddress() net.IP
	BroadcastAddress() net.IP
	ListenAddress() net.IP
	RPCAddress() net.IP
	PreferredIP() net.IP
	DataCenter() string
	Rack() string
	HostID() string
	WorkLoad() string
	Partitioner() string
	ClusterName() string
	Tokens() []string
	Port() int
	IsUp() bool
	ScyllaShardAwarePort() uint16
	ScyllaShardAwarePortTLS() uint16
	ScyllaShardCount() int
}

type HostPoolInfo interface {
	GetConnectionCount() int
	GetExcessConnectionCount() int
	GetShardCount() int
	String() string
	InFlight() int
	Host() HostInformation
	IsClosed() bool
}

// GetHostPoolByID returns the connection pool for hostID, or nil if the session
// holds no pool for it -- including when hostID is not a well-formed UUID.
//
// The explicit nil matters: returning the *hostConnPool unconditionally would
// box a nil pointer into a non-nil HostPoolInfo, so a caller's `if pool != nil`
// would pass and the first method call would panic.
func (s *Session) GetHostPoolByID(hostID string) HostPoolInfo {
	hostPool, ok := s.pool.getPoolByHostID(hostID)
	if !ok {
		return nil
	}
	return hostPool
}

// IterateHostPools calls iter once for each host's connection pool, stopping
// early if iter returns false. Iteration order is unspecified.
//
// iter runs while an internal lock on the pool is held, so it must not call
// back into the Session -- anything that adds or removes a host, or closes the
// session, will deadlock -- and it should not block.
func (s *Session) IterateHostPools(iter func(info HostPoolInfo) bool) {
	s.pool.iteratePool(iter)
}

type ObservedConnect struct {
	// Host is the information about the host about to connect
	Host *HostInfo

	Start time.Time // time immediately before the dial is called
	End   time.Time // time immediately after the dial returned

	// Err is the connection error (if any)
	Err error
}

// ConnectObserver is the interface implemented by connect observers / stat collectors.
type ConnectObserver interface {
	// ObserveConnect gets called when a new connection to cassandra is made.
	ObserveConnect(ObservedConnect)
}

type Error struct {
	Message string
	Code    int
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound             = errors.New("not found")
	ErrUnavailable          = errors.New("unavailable")
	ErrUnsupported          = errors.New("feature not supported")
	ErrTooManyStmts         = errors.New("too many statements")
	ErrUseStmt              = errors.New("use statements aren't supported. Please see https://github.com/apache/cassandra-gocql-driver for explanation.")
	ErrSessionClosed        = errors.New("session has been closed")
	ErrNoConnections        = errors.New("gocql: no hosts available in the pool")
	ErrNoKeyspace           = errors.New("no keyspace provided")
	ErrNoTable              = errors.New("no table name provided")
	ErrKeyspaceDoesNotExist = errors.New("keyspace does not exist")
	ErrNoMetadata           = errors.New("no metadata available")
	ErrTabletsNotUsed       = errors.New("tablets not used")
	ErrSessionNotReady      = errors.New("session is not ready yet")
)

// BatchSizeMaximum is the maximum number of statements a batch operation can have.
// This limit is set by cassandra and could change in the future.
const BatchSizeMaximum = 65535
