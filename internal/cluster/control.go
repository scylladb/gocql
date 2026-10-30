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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/rand"

	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gocql/v2/events"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

var (
	InternalRandr    *rand.Rand
	InternalMutRandr sync.Mutex
)

func init() {
	b := make([]byte, 4)
	if _, err := crand.Read(b); err != nil {
		panic(fmt.Sprintf("unable to seed random number generator: %v", err))
	}

	InternalRandr = rand.New(rand.NewSource(int64(readInt(b))))
}

const (
	InternalControlConnStarting = 0
	InternalControlConnStarted  = 1
	InternalControlConnClosing  = -1
)

type InternalControlConnection interface {
	InternalGetConn() *InternalConnHost
	InternalAwaitSchemaAgreement() error
	InternalDiscoverProtocol(hosts []*HostInfo) (int, error)
	InternalConnect(hosts []*HostInfo) error
	InternalClose()
	InternalGetSession() *Session
	InternalReconnect() error
}

type InternalControlConn struct {
	workerMu        sync.Mutex
	workers         sync.WaitGroup
	connections     map[*Conn]struct{}
	closeOnce       sync.Once
	InternalConn    atomic.Pointer[InternalConnHost]
	InternalSession *Session
	InternalQuit    chan struct{}
	state           int32
	reconnecting    int32
}

func (c *InternalControlConn) InternalGetSession() *Session {
	return c.InternalSession
}

func InternalCreateControlConn(session *Session) *InternalControlConn {

	control := &InternalControlConn{
		InternalSession: session,
		InternalQuit:    make(chan struct{}),
	}

	return control
}

func (c *InternalControlConn) InternalHeartBeat() {
	if !atomic.CompareAndSwapInt32(&c.state, InternalControlConnStarting, InternalControlConnStarted) {
		return
	}

	sleepTime := 1 * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()

	for {
		timer.Reset(sleepTime)

		select {
		case <-c.InternalQuit:
			return
		case <-timer.C:
		}

		resp, err := c.InternalWriteFrame(&writeOptionsFrame{})
		if err != nil {
			goto reconn
		}

		switch resp.(type) {
		case *frm.SupportedFrame:
			// Everything ok
			sleepTime = 30 * time.Second
			continue
		case error:
			goto reconn
		default:

			// Reachable from the wire: parseFrame builds a frame for every opcode it
			// knows, and this goroutine has no recover above it. There is no failure
			// counter here as there is in Conn.heartBeat -- every non-SUPPORTED
			// outcome already reconnects, so this arm joins them rather than adding
			// a bound only it would respect.
			c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unexpected frame in response to options: %T\n", resp)
			goto reconn
		}

	reconn:
		// try to connect a bit faster
		sleepTime = 1 * time.Second
		c.InternalReconnect()
		continue
	}
}

func InternalShuffleHosts(hosts []*HostInfo) []*HostInfo {
	shuffled := make([]*HostInfo, len(hosts))
	copy(shuffled, hosts)

	InternalMutRandr.Lock()
	InternalRandr.Shuffle(len(hosts), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	InternalMutRandr.Unlock()

	return shuffled
}

// this is going to be version dependant and a nightmare to maintain :(
var InternalProtocolSupportRe = regexp.MustCompile(`the lowest supported version is \d+ and the greatest is (\d+)$`)

func InternalParseProtocolFromError(err error) int {
	// I really wish this had the actual info in the error frame...
	matches := InternalProtocolSupportRe.FindAllStringSubmatch(err.Error(), -1)
	if len(matches) != 1 || len(matches[0]) != 2 {
		if verr, ok := err.(*protocolError); ok {
			return int(verr.InternalFrame.Header().Version.Version())
		}
		return 0
	}

	max, err := strconv.Atoi(matches[0][1])
	if err != nil {
		return 0
	}

	return max
}

func (c *InternalControlConn) InternalDiscoverProtocol(hosts []*HostInfo) (int, error) {
	hosts = InternalShuffleHosts(hosts)

	connCfg := *c.InternalSession.privateStorage.InternalConnCfg
	connCfg.ProtoVersion = protoVersion4 // TODO: define maxProtocol

	handler := connErrorHandlerFn(func(c *Conn, err error, closed bool) {
		// we should never get here, but if we do it means we connected to a
		// host successfully which means our attempted protocol version worked
		if !closed {
			c.Close()
		}
	})

	var err error
	for _, host := range hosts {
		var conn *Conn
		conn, err = InternalSessionDial(c.InternalSession, c.InternalSession.privateStorage.InternalCtx, host, &connCfg, handler)
		// not need to call conn.finalizeConnection since this connection to be terminated right away
		if conn != nil {
			conn.Close()
		}

		if err == nil {
			return connCfg.ProtoVersion, nil
		}

		if proto := InternalParseProtocolFromError(err); proto > 0 {
			return proto, nil
		}
	}

	return 0, err
}

// controlConnConfig returns the connection config used for control connections.
// It is the only place where control-connection specific settings are applied,
// so that every path which (re)establishes the control connection agrees on them.
//
// It is declared here, rather than alongside the rest of Session, so it sits
// next to the paths whose agreement it exists to guarantee.
//
// controlConn.InternalDiscoverProtocol is a deliberate exception: it hand-copies
// *s.connCfg instead of going through here, because its throwaway probe
// connections are discarded immediately and must not be marked as the control
// connection - doing so would race the real control connection and report
// DRIVER_CONFIG more than once.
func InternalSessionControlConnConfig(s *Session) *ConnConfig {
	cfg := *s.privateStorage.InternalConnCfg
	cfg.InternalDisableCoalesce = true
	cfg.InternalIsControlConn = true
	return &cfg
}

func (c *InternalControlConn) InternalConnect(hosts []*HostInfo) error {
	if len(hosts) == 0 {
		return errors.New("control: no endpoints specified")
	}

	// shuffle endpoints so not all drivers will connect to the same initial
	// node.
	hosts = InternalShuffleHosts(hosts)

	cfg := InternalSessionControlConnConfig(c.InternalSession)

	var conn *Conn
	var err error
	for _, host := range hosts {
		conn, err = InternalSessionDial(c.InternalSession, c.InternalSession.privateStorage.InternalCtx, host, cfg, c)
		// conn.finalizeConnection() to be called outside of this function, since initialization process is not completed yet
		if err != nil {
			c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable to dial control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
			continue
		}
		err = c.setupConn(conn)
		if err == nil {
			break
		}
		c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable setup control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
		conn.Close()
		conn = nil
	}
	if conn == nil {
		return fmt.Errorf("unable to connect to initial hosts: %w", err)
	}

	// we could fetch the initial ring here and update initial host data. So that
	// when we return from here we have a ring topology ready to go.

	c.startWorker(c.InternalHeartBeat)

	return nil
}

type InternalConnHost struct {
	InternalConn ConnInterface
	InternalHost *HostInfo
}

func (c *InternalControlConn) setupConn(conn *Conn) error {
	setupCtx, cancel := conn.InternalOpeningContext(conn.InternalCtx)
	defer cancel()
	stopAndJoin := conn.InternalSetupCancellation(setupCtx)
	defer stopAndJoin()
	// we need up-to-date host info for the filterHost call below
	hosts, err := InternalReadV2Topology(setupCtx, conn, qrySystemLocal, c.InternalSession.privateStorage.InternalCfg.Port)
	var host *HostInfo
	if err == nil {
		if len(hosts) == 0 {
			err = typeValidation("control topology", "query returned 0 rows")
		} else if len(hosts) != 1 || implEndpoint.InternalHostInfoHostUUID(hosts[0]).IsEmpty() {
			err = typeValidation("control topology", "expected one local identity")
		} else {
			host = hosts[0]
		}
	}
	if err != nil {
		return err
	}

	if InternalClusterSettingsFilterHost(&c.InternalSession.privateStorage.InternalCfg, host) {
		return fmt.Errorf("host was filtered: %v", host.ConnectAddress())
	}

	if err := c.registerEventsContext(setupCtx, conn); err != nil {
		return fmt.Errorf("register events: %v", err)
	}

	if err := conn.InternalValidateOpeningTarget(setupCtx); err != nil {
		return err
	}
	stopAndJoin()
	if err := setupCtx.Err(); err != nil {
		return err
	}
	implEndpoint.InternalHostInfoUpdate(conn.InternalHost, host)
	host = conn.InternalHost
	ch := &InternalConnHost{InternalConn: conn, InternalHost: host}
	c.workerMu.Lock()
	if atomic.LoadInt32(&c.state) == InternalControlConnClosing {
		c.workerMu.Unlock()
		return ErrClosed
	}
	if c.connections == nil {
		c.connections = make(map[*Conn]struct{})
	}
	c.connections[conn] = struct{}{}
	old := c.InternalConn.Swap(ch)
	c.workerMu.Unlock()
	var oldHost events.HostInfo
	if old != nil && old.InternalHost != nil {
		oldHost.HostID = old.InternalHost.HostID()
		oldHost.Host = old.InternalHost.ConnectAddress()
		oldHost.Port = old.InternalHost.Port()
	}
	InternalSessionPublishV2Metadata(c.InternalSession)
	InternalSessionPublishEvent(c.InternalSession, &events.ControlConnectionRecreatedEvent{
		OldHost: oldHost,
		NewHost: events.HostInfo{
			HostID: host.HostID(),
			Host:   host.ConnectAddress(),
			Port:   host.Port(),
		},
	})
	if c.InternalSession.initialized() {
		InternalSessionPeripheral( // We connected to control conn, so add the connect the host in pool as well.
			// Notify session we can start trying to connect to the node.
			// We can't start the fill before the session is initialized, otherwise the fill would interfere
			// with the fill called by Session.init. Session.init needs to wait for its fill to finish and that
			// would return immediately if we started the fill here.
			// TODO(martin-sucha): Trigger pool refill for all hosts, like in reconnectDownedHosts?
			c.InternalSession).InternalStartWorker(func() { c.InternalSession.startPoolFill(host) })
	}
	return nil
}

func (c *InternalControlConn) registerEvents(conn *Conn) error {
	ctx, cancel := conn.InternalOpeningContext(conn.InternalCtx)
	defer cancel()
	return c.registerEventsContext(ctx, conn)
}
func (c *InternalControlConn) registerEventsContext(ctx context.Context, conn *Conn) error {
	var events []string

	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableTopologyEvents {
		events = append(events, "TOPOLOGY_CHANGE")
	}
	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableNodeStatusEvents {
		events = append(events, "STATUS_CHANGE")
	}
	if !c.InternalSession.privateStorage.InternalCfg.Events.DisableSchemaEvents {
		events = append(events, "SCHEMA_CHANGE")
	}
	if c.InternalSession.privateStorage.InternalCfg.ClientRoutesConfig != nil && !c.InternalSession.privateStorage.InternalCfg.privateStorage.InternalDisableClientRouteEvents {
		events = append(events, "CLIENT_ROUTES_CHANGE")
	}

	if len(events) == 0 {
		return nil
	}

	framer, err := conn.InternalExec(ctx,
		&writeRegisterFrame{
			Events: events,
		}, nil, conn.InternalCfg.ConnectTimeout)
	if err != nil {
		return err
	}
	defer framer.Release()

	frame, err := framer.ParseFrame()
	if err != nil {
		return err
	} else if _, ok := frame.(*frm.ReadyFrame); !ok {
		return fmt.Errorf("unexpected frame in response to register: got %T: %v\n", frame, frame)
	}

	return nil
}

func (c *InternalControlConn) InternalReconnect() error {
	if atomic.LoadInt32(&c.state) == InternalControlConnClosing {
		return fmt.Errorf("control connection is closing")
	}
	if !atomic.CompareAndSwapInt32(&c.reconnecting, 0, 1) {
		return fmt.Errorf("control connection is reconnecting")
	}
	defer atomic.StoreInt32(&c.reconnecting, 0)

	err := c.attemptReconnect()
	if err != nil {
		err = fmt.Errorf("gocql: unable to reconnect control connection: %w\n", err)
		c.InternalSession.privateStorage.InternalLogger.Printf(err.Error())
		return err
	}

	err = InternalSessionRefreshRingNow(c.InternalSession)
	if err != nil {
		c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable to refresh ring: %v\n", err)
	}

	if c.InternalSession.privateStorage.InternalMetadataDescriber == nil {
		return err
	}
	err = c.InternalSession.privateStorage.InternalMetadataDescriber.InternalRefreshAllSchema()
	if err != nil {
		c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable to refresh the schema: %v\n", err)
	}
	return nil
}

func (c *InternalControlConn) attemptReconnect() error {
	hosts := c.InternalSession.privateStorage.InternalHostSource.InternalGetHostsList()
	hosts = InternalShuffleHosts(hosts)

	// keep the old behavior of connecting to the old host first by moving it to
	// the front of the slice
	ch := c.InternalGetConn()
	if ch != nil {
		for i := range hosts {
			if hosts[i].Equal(ch.InternalHost) {
				hosts[0], hosts[i] = hosts[i], hosts[0]
				break
			}
		}
		ch.InternalConn.Close()
	}

	err := c.attemptReconnectToAnyOfHosts(hosts)
	if err == nil {
		return nil
	}
	c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable to connect to any ring node: %v\n", err)
	c.InternalSession.privateStorage.InternalLogger.Printf("gocql: control falling back to initial contact points.\n")
	// Fallback to initial contact points, as it may be the case that all known initialHosts
	// changed their IPs while keeping the same hostname(s).
	initialHosts, resolvErr := resolveInitialEndpoints(c.InternalSession.privateStorage.InternalCfg.DNSResolver, c.InternalSession.privateStorage.InternalCfg.Hosts, c.InternalSession.privateStorage.InternalCfg.Port, c.InternalSession.privateStorage.InternalLogger)
	if resolvErr != nil {
		return fmt.Errorf("resolve contact points' hostnames: %v", resolvErr)
	}

	return c.attemptReconnectToAnyOfHosts(initialHosts)
}

func (c *InternalControlConn) attemptReconnectToAnyOfHosts(hosts []*HostInfo) error {
	cfg := InternalSessionControlConnConfig(c.InternalSession)
	for _, host := range hosts {
		conn, err := InternalSessionDial(c.InternalSession, c.InternalSession.privateStorage.InternalCtx, host, cfg, c)
		if err != nil {
			if c.InternalSession.privateStorage.InternalCfg.ConvictionPolicy.AddFailure(err, host) {
				InternalSessionHandleNodeDown(c.InternalSession, host.ConnectAddress(), host.Port())
			}
			c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable to dial control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
			continue
		}
		err = c.setupConn(conn)
		if err != nil {
			c.InternalSession.privateStorage.InternalLogger.Printf("gocql: unable setup control conn %v:%v: %v\n", host.ConnectAddress(), host.Port(), err)
			conn.Close()
			continue
		}
		conn.InternalFinalizeConnection()
		InternalSessionPublishEvent(c.InternalSession, &events.ControlConnectionRecreatedEvent{
			NewHost: events.HostInfo{
				Host:   host.ConnectAddress(),
				Port:   host.Port(),
				HostID: host.HostID(),
			},
		})
		return nil
	}
	return fmt.Errorf("unable to connect to any known node: %v", hosts)
}

func (c *InternalControlConn) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		return
	}

	oldConn := c.InternalGetConn()

	// If connection has long gone, and not been attempted for awhile,
	// it's possible to have oldConn as nil here (#1297).
	if oldConn != nil && oldConn.InternalConn != conn {
		return
	}

	c.startWorker(func() { _ = c.InternalReconnect() })
}

func (c *InternalControlConn) InternalGetConn() *InternalConnHost {
	return c.InternalConn.Load()
}

// writeFrame sends frame w on the control connection and returns the parsed
// response frame.
//
// NOTE: The returned frame must not retain any byte-slice references to the
// framer's read buffer, because the framer is released back to the pool
// immediately after parseFrame returns (via defer). Frame types that use
// readBytesCopy (e.g. SupportedFrame, AuthChallengeFrame, AuthSuccessFrame)
// are safe; frame types that use readBytes and expose []byte fields would not
// be safe and must not be returned from this function.
func (c *InternalControlConn) InternalWriteFrame(w frameBuilder) (frame, error) {
	ch := c.InternalGetConn()
	if ch == nil {
		return nil, errNoControl
	}

	framer, err := ch.InternalConn.InternalExec(v2SchemaContext(InternalNewSchemaSource(c.InternalSession)), w, nil, c.InternalSession.privateStorage.InternalCfg.MetadataSchemaRequestTimeout)
	if err != nil {
		return nil, err
	}
	defer framer.Release()

	return framer.ParseFrame()
}

func (c *InternalControlConn) InternalAwaitSchemaAgreement() error {
	return c.InternalSession.awaitV2SchemaAgreement(v2SchemaContext(InternalNewSchemaSource(c.InternalSession)))
}

func (c *InternalControlConn) startWorker(fn func()) bool {
	c.workerMu.Lock()
	defer c.workerMu.Unlock()
	if atomic.LoadInt32(&c.state) == InternalControlConnClosing {
		return false
	}
	c.workers.Add(1)
	go func() { defer c.workers.Done(); fn() }()
	return true
}
func (c *InternalControlConn) InternalClose() {
	c.closeOnce.Do(func() {
		c.workerMu.Lock()
		atomic.StoreInt32(&c.state, InternalControlConnClosing)
		if c.InternalQuit != nil {
			close(c.InternalQuit)
		}
		c.workerMu.Unlock()
	})
	if current := c.InternalGetConn(); current != nil {
		current.InternalConn.Close()
	}
	c.workers.Wait()
	c.workerMu.Lock()
	connections := make([]*Conn, 0, len(c.connections))
	for conn := range c.connections {
		connections = append(connections, conn)
	}
	c.workerMu.Unlock()
	for _, conn := range connections {
		conn.Close()
	}
	for _, conn := range connections {
		conn.InternalWaitEventDispatch()
	}
}
