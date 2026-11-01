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

package transport

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/tablets"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func InternalApprove(authenticator string, approvedAuthenticators []string) bool {
	if len(approvedAuthenticators) == 0 {
		return true
	}
	for _, s := range approvedAuthenticators {
		if authenticator == s {
			return true
		}
	}
	return false
}

type Authenticator interface {
	Challenge(req []byte) (resp []byte, auth Authenticator, err error)
	Success(data []byte) error
}

// PasswordAuthenticator specifies credentials to be used when authenticating.
// It can be configured with an "allow list" of authenticator class names to avoid
// attempting to authenticate with Cassandra if it doesn't provide an expected authenticator.
type PasswordAuthenticator struct {
	Username string
	Password string
	// Setting this to nil or empty will allow authenticating with any authenticator
	// provided by the server.  This is the default behavior of most other driver
	// implementations.
	AllowedAuthenticators []string
}

func (p PasswordAuthenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if !InternalApprove(string(req), p.AllowedAuthenticators) {
		return nil, nil, fmt.Errorf("unexpected authenticator %q", req)
	}
	resp := make([]byte, 2+len(p.Username)+len(p.Password))
	resp[0] = 0
	copy(resp[1:], p.Username)
	resp[len(p.Username)+1] = 0
	copy(resp[2+len(p.Username):], p.Password)
	return resp, nil, nil
}

func (p PasswordAuthenticator) Success(data []byte) error {
	return nil
}

// SslOptions configures TLS use.
//
// Warning: Due to historical reasons, the SslOptions is insecure by default, so you need to set EnableHostVerification
// to true if no Config is set. Most users should set SslOptions.Config to a *tls.Config.
// SslOptions and Config.InsecureSkipVerify interact as follows:
//
//	Config.InsecureSkipVerify | EnableHostVerification | Result
//	Config is nil             | false                  | do not verify host
//	Config is nil             | true                   | verify host
//	false                     | false                  | verify host
//	true                      | false                  | do not verify host
//	false                     | true                   | verify host
//	true                      | true                   | verify host
type SslOptions struct {
	*tls.Config

	// CertPath and KeyPath are optional depending on server
	// config, but both fields must be omitted to avoid using a
	// client certificate
	CertPath string
	KeyPath  string
	CaPath   string //optional depending on server config
	// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this
	// on.
	// This option is basically the inverse of tls.Config.InsecureSkipVerify.
	// See InsecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info.
	//
	// See SslOptions documentation to see how EnableHostVerification interacts with the provided tls.Config.
	EnableHostVerification bool
	// DisableStrictCertificateValidation disables strict chain validation.
	// Strict validation requires TLS verification (InsecureSkipVerify=false).
	// When false (default) with verification enabled, the driver validates the
	// entire chain up to a self-signed root. When true, Go's default applies.
	//
	// Deprecated: This option is provided for backward compatibility and will be removed
	// in a future version. You should ensure your certificate chains are properly configured
	// and avoid using this option.
	DisableStrictCertificateValidation bool
}

type ConnConfig struct {
	InternalReceiveCache    *wire.ReceiveCache
	TypedConfig             *Config
	TypedAuth               AuthProvider
	TypedLimits             ProtocolLimits
	Dialer                  legacyDialer
	Logger                  StdLogger
	Authenticator           Authenticator
	Compressor              Compressor
	HostDialer              HostDialer
	AuthProvider            func(h *HostInfo) (Authenticator, error)
	InternalTlsConfig       *tls.Config
	CQLVersion              string
	ConnectTimeout          time.Duration
	ReadTimeout             time.Duration
	WriteTimeout            time.Duration
	ProtoVersion            int
	Keepalive               time.Duration
	InternalDisableCoalesce bool
	// isControlConn marks the connection used by the control connection, which is
	// the only one reporting the driver configuration on startup.
	InternalIsControlConn bool
}

func (c *ConnConfig) InternalLogger() StdLogger {
	if c.Logger == nil {
		return &defaultLogger{}
	}
	return c.Logger
}

type ConnErrorHandler interface {
	HandleError(conn *Conn, err error, closed bool)
}

type InternalConnErrorHandlerFn func(conn *Conn, err error, closed bool)

func (fn InternalConnErrorHandlerFn) HandleError(conn *Conn, err error, closed bool) {
	fn(conn, err, closed)
}

type ConnInterface interface {
	Close()
	InternalExec(ctx context.Context, req frameBuilder, tracer Tracer, requestTimeout time.Duration) (*framer, error)
	InternalGetIsSchemaV2() bool
	InternalSetSchemaV2(s bool)
	InternalIsScyllaConn() bool
	InternalGetScyllaSupported() ScyllaConnectionFeatures
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
type Conn struct {
	InternalOpeningSharderEpoch               uint64
	InternalOpeningDeadline                   time.Time
	eventMu                                   sync.Mutex
	eventsClosed                              bool
	eventDone                                 chan struct{}
	InternalTargetEpoch                       uint64
	InternalValidateTarget                    func(context.Context) error
	InternalStreamBudget                      *streamMetadataBudget
	eventOnce                                 sync.Once
	eventQueue                                chan InternalConnEvent
	InternalExclusiveFlag                     atomic.Bool
	InternalSharedOps, orphans                int
	sharedChanged                             chan struct{}
	pauseWanted                               bool
	readerPaused, readerResume, readerResumed chan struct{}
	auth                                      Authenticator
	InternalCfg                               *ConnConfig
	InternalFrameObserver                     FrameHeaderObserver
	InternalStreamObserver                    StreamObserver
	InternalW                                 contextWriter
	InternalLogger                            StdLogger
	InternalCtx                               context.Context
	InternalErrorHandler                      ConnErrorHandler
	InternalCompressor                        Compressor
	// segCompressor is compressor narrowed to the two Append methods the v5 segment
	// codec takes, resolved once by resolveSegmentCompressor during the handshake. The
	// receive path would otherwise repeat that assertion for every segment it reads,
	// for a result that cannot change: compressor is written at dial and once more by
	// startupCoordinator.startup, and never again.
	//
	// Nil is the uncompressed segment layout, and is also what a pre-v5 connection
	// leaves it as -- it never reaches the segment codec, and its compressor need not
	// support segments at all.
	InternalSegCompressor segment.Compressor
	InternalSupported     map[string][]string
	InternalStreams       *streams.IDGenerator
	InternalHost          *HostInfo
	// calls stores a map from stream ID to callReq.
	// This map is protected by mu.
	// calls should not be used when closed is true, calls is set to nil when closed=true.
	InternalCalls map[int]*InternalCallReq
	// segScratch holds the reusable buffers inbound v5 segments are read into.
	// Only touched by the receive path, which runs on the serve() goroutine.
	segScratch segment.Scratch
	// headerReader is the reader the current frame or segment header is read
	// through (see readFrameHeader, readFirstSegmentHeader). Reused rather than
	// allocated per header, and like segScratch only touched by whichever
	// goroutine is currently receiving. The two header reads never nest, and the
	// startup coordinator's reader never overlaps serve(): a frameTicker tick is
	// only sent while the previous response is still outstanding, and
	// processFrameSource touches neither field after handing that response to its
	// caller.
	//
	// Note that last clause is what makes it safe, not an ordering — setupConn
	// returns on the startupErr send, which the options goroutine performs before
	// close(frameTicker), so the startup reader can still be unwinding when
	// serve() starts. Work added to processFrameSource after the response is
	// delivered would need a real barrier here.
	headerReader            headerReader
	InternalR               connReadSource
	InternalHooks           InternalConnectionHooks
	InternalStartupIdentity InternalStartupIdentity
	InternalStartupReporter InternalStartupReporter
	InternalMetadataTimeout time.Duration
	InternalCoalesceWait    time.Duration
	InternalPoolPolicy      InternalConnectionPoolPolicy
	InternalFramers         InternalConnFramers
	InternalCancel          context.CancelFunc
	// currentKeyspace is the keyspace this connection was switched to by
	// Conn.UseKeyspace, and the default the prepared-statement cache is keyed by
	// (see executeQuery/executeBatch). It deliberately tracks only driver-issued
	// USE: a `USE` statement a caller executes as an ordinary query switches the
	// server side of whichever single pooled connection it landed on, and the
	// driver does not follow it — so cache keys keep using the configured keyspace.
	//
	// Atomic because UseKeyspace is exported: a caller invoking it on a live
	// connection would otherwise race the request goroutines reading it.
	currentKeyspace atomic.Pointer[string]
	InternalAddr    string
	// systemRequest carries the timeouts driver-issued system queries are sent
	// with. See systemRequestState for why the two travel together.
	//
	// Atomic because finalizeConnection switches the timeout from
	// cfg.ConnectTimeout to cfg.MetadataSchemaRequestTimeout at the very end of
	// Session.init, by which point the control connection is already registered
	// for server push events: a SCHEMA_CHANGE arriving inside that window makes
	// the event-debouncer goroutine run querySystem on this connection
	// concurrently with the write.
	systemRequest           atomic.Pointer[InternalSystemRequestState]
	InternalCqlProtoExts    []cqlProtocolExtension
	InternalScyllaSupported ScyllaConnectionFeatures
	InternalWriteTimeout    atomic.Int64
	InternalMu              sync.Mutex
	tabletsRoutingV1        int32
	headerBuf               [headSize]byte
	InternalIsShardAware    bool
	InternalRequestedShard  Option[uint32]
	// true if connection close process for the connection started.
	// closed is protected by mu.
	InternalClosed     bool
	InternalIsSchemaV2 bool
	InternalVersion    uint8
}

func (c *Conn) InternalGetIsSchemaV2() bool {
	return c.InternalIsSchemaV2
}

func (c *Conn) InternalSetSchemaV2(s bool) {
	c.InternalIsSchemaV2 = s
}

// systemRequestState is the immutable pair of timeouts a driver-issued system
// query is sent with: the client-side deadline, and the ScyllaDB-only
// " USING TIMEOUT ...ms" clause pre-rendered from it (empty when the clause does
// not apply). The two are published as one snapshot so a reader can never pair
// one with a stale version of the other - which would ask the server to abort a
// system query on a deadline the client is not waiting on, or vice versa.
//
// Field order keeps the string first so the GC scans 8 pointer bytes, not 16
// (govet's fieldalignment).
type InternalSystemRequestState struct {
	InternalUsingClause string
	InternalTimeout     time.Duration
}

// systemRequestStatement returns stmt with the USING TIMEOUT clause appended and
// the client-side timeout to send it with, both taken from one snapshot so the
// two can never disagree. It is the only way the query paths should reach them.
func (c *Conn) InternalSystemRequestStatement(stmt string) (string, time.Duration) {
	state := c.InternalGetSystemRequestState()
	return stmt + state.InternalUsingClause, state.InternalTimeout
}

// getSystemRequestState returns the current snapshot.
func (c *Conn) InternalGetSystemRequestState() InternalSystemRequestState {
	if state := c.systemRequest.Load(); state != nil {
		return *state
	}
	return InternalSystemRequestState{}
}

// setSystemRequestTimeout publishes t together with the clause derived from it.
// The clause is ScyllaDB-only and needs a positive timeout, so it is empty
// otherwise - a timeout the caller disabled must not leave an older clause in
// force.
func (c *Conn) InternalSetSystemRequestTimeout(t time.Duration) {
	next := InternalSystemRequestState{InternalTimeout: t}
	if t > time.Duration(0) && c.InternalIsScyllaConn() {
		next.InternalUsingClause = " USING TIMEOUT " + strconv.FormatInt(t.Milliseconds(), 10) + "ms"
	}
	c.systemRequest.Store(&next)
}

// recalculateSystemRequestTimeout re-renders the clause for the timeout already
// in effect. It is called once the connection knows whether it talks to
// ScyllaDB, which the clause depends on.
//
// It re-reads the timeout rather than naming the value its only caller knows is
// in force (cfg.ConnectTimeout, from dialWithoutObserver): republishing whatever
// is current cannot overwrite a timeout some later change publishes earlier in
// startup, which would put every system query back under an unrelated setting.
func (c *Conn) recalculateSystemRequestTimeout() {
	c.InternalSetSystemRequestTimeout(c.InternalGetSystemRequestState().InternalTimeout)
}

func (c *Conn) InternalFinalizeConnection() {

	c.InternalWriteTimeout.Store(int64(c.InternalCfg.WriteTimeout))
	c.InternalSetSystemRequestTimeout(c.InternalMetadataTimeout)
	c.InternalW.InternalSetWriteTimeout(c.InternalCfg.WriteTimeout)
	c.InternalR.SetTimeout(c.InternalCfg.ReadTimeout)
}

func (c *Conn) InternalGetScyllaSupported() ScyllaConnectionFeatures {
	return c.InternalScyllaSupported
}

func InternalTranslateHostAddresses(addressTranslator AddressTranslator, host *HostInfo, logger StdLogger) (translatedAddresses, error) {
	port := host.Port()
	if port < 1 || port > 65535 {
		return translatedAddresses{}, fmt.Errorf("invalid CQL port %d for host %s: port must be between 1 and 65535", port, host.UntranslatedConnectAddress())
	}
	addr, err := implEndpoint.InternalTranslateAddressPort(addressTranslator, host, AddressPort{
		Address: host.UntranslatedConnectAddress(),
		Port:    uint16(port),
	}, logger)
	if err != nil {
		return translatedAddresses{}, fmt.Errorf("unable to translate regular cql address: %w", err)
	}
	resultedInfo := translatedAddresses{
		CQL: addr,
	}

	scyllaFeatures := host.ScyllaFeatures()
	if port := scyllaFeatures.ShardAwarePort(); port != 0 {
		addr, err = implEndpoint.InternalTranslateAddressPort(addressTranslator, host,
			AddressPort{
				Address: host.UntranslatedConnectAddress(),
				Port:    port,
			}, logger)
		if err != nil {
			return translatedAddresses{}, fmt.Errorf("unable to translate shard aware address: %w", err)
		}
		resultedInfo.ShardAware = addr
	}
	if port := scyllaFeatures.ShardAwarePortTLS(); port != 0 {
		addr, err = implEndpoint.InternalTranslateAddressPort(addressTranslator, host,
			AddressPort{
				Address: host.UntranslatedConnectAddress(),
				Port:    port,
			}, logger)
		if err != nil {
			return translatedAddresses{}, fmt.Errorf("unable to translate shard aware tls address: %w", err)
		}
		resultedInfo.ShardAwareTLS = addr
	}
	return resultedInfo, nil
}

// dialShard establishes a connection to a host/shard and notifies the session's connectObserver.
// If nrShards is zero, shard-aware dialing is disabled.
// note: every connection needs to get `conn.finalizeConnection` called on it when initialization process is done
type InternalConnectionLifetimeKey struct{}

func (c *Conn) InternalInit(ctx context.Context, dialedHost *DialedHost) error {
	c.InternalR.SetTimeout(c.InternalCfg.ConnectTimeout)
	c.InternalWriteTimeout.Store(int64(c.InternalCfg.ConnectTimeout))
	c.InternalW.InternalSetWriteTimeout(c.InternalCfg.ConnectTimeout)

	if c.InternalCfg.AuthProvider != nil {
		var err error
		c.auth, err = c.InternalCfg.AuthProvider(c.InternalHost)
		if err != nil {
			return err
		}
	} else {
		c.auth = c.InternalCfg.Authenticator
	}

	startup := &InternalStartupCoordinator{
		frameTicker: make(chan struct{}),
		conn:        c,
	}

	if c.InternalCfg.InternalIsControlConn {
		startup.driverConfigReporter = c.InternalStartupReporter
	}

	if err := startup.setupConn(ctx); err != nil {
		return err
	}

	if c.InternalCoalesceWait > 0 && !c.InternalCfg.InternalDisableCoalesce && (!dialedHost.DisableCoalesce || c.InternalCfg.TypedConfig != nil) {
		c.InternalW = newWriteCoalescer(dialedHost.Conn, c.InternalCfg.ConnectTimeout, c.InternalCoalesceWait, c.InternalCtx.Done())
	}

	if c.InternalIsScyllaConn() {
		c.InternalSetSchemaV2(false)
	}

	go c.InternalServe(c.InternalCtx)
	go c.InternalHeartBeat(c.InternalCtx)

	return nil
}

func (c *Conn) Write(p []byte) (n int, err error) {
	if err = c.beginShared(c.InternalCtx); err != nil {
		return 0, err
	}
	defer c.endShared()
	return c.InternalW.InternalWriteContext(context.Background(), p)
}

// Read reads data from the connection.
//
// The driver itself reads through the connection's connReader (which owns the
// read-deadline handling); Read is retained so that *Conn keeps satisfying
// io.Reader for external callers, as it does io.Writer via Write.
func (c *Conn) Read(p []byte) (n int, err error) {
	if c.InternalExclusiveFlag.Load() {
		return 0, &UnsupportedSettingError{Setting: "raw connection read", Reason: "reader is reserved for streaming"}
	}
	return c.InternalR.Read(p)
}

type InternalStartupCoordinator struct {
	conn                 *Conn
	frameTicker          chan struct{}
	driverConfigReporter InternalStartupReporter
}

func (s *InternalStartupCoordinator) setupConn(ctx context.Context) error {
	var cancel context.CancelFunc
	if s.conn.InternalCfg.ConnectTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.conn.InternalCfg.ConnectTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	startupCompleted := &atomic.Bool{}
	startupCompleted.Store(false)

	startupErr := make(chan error)
	go func() {
		for range s.frameTicker {
			err := s.conn.InternalRecv(ctx, startupCompleted.Load())
			if err != nil {
				select {
				case startupErr <- err:
				case <-ctx.Done():
				}

				return
			}
		}
	}()

	go func() {
		defer close(s.frameTicker)
		err := s.options(ctx, startupCompleted)
		select {
		case startupErr <- err:
		case <-ctx.Done():
		}
	}()

	select {
	case err := <-startupErr:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return errors.New("gocql: no response to connection startup within timeout")
	}

	return nil
}

// write sends the given frame on the connection during startup and returns
// the parsed response frame.
//
// NOTE: The returned frame must not retain any byte-slice references to the
// framer's read buffer, because the framer is released back to the pool
// immediately after parseFrame returns (via defer). Frame types that use
// readBytesCopy (e.g. SupportedFrame, AuthChallengeFrame, AuthSuccessFrame)
// are safe; frame types that use readBytes and expose []byte fields would not
// be safe and must not be returned from this function.
func (s *InternalStartupCoordinator) write(ctx context.Context, frame frameBuilder, startupCompleted *atomic.Bool) (frame, error) {
	select {
	case s.frameTicker <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	framer, err := s.conn.execInternal(ctx, frame, nil, s.conn.InternalCfg.ConnectTimeout, startupCompleted.Load())
	if err != nil {
		return nil, err
	}
	defer framer.Release()

	return framer.ParseFrame()
}

func (s *InternalStartupCoordinator) options(ctx context.Context, startupCompleted *atomic.Bool) error {
	frame, err := s.write(ctx, &writeOptionsFrame{}, startupCompleted)
	if err != nil {
		return err
	}

	v, ok := frame.(*frm.SupportedFrame)
	if !ok {
		return NewErrProtocol("Unknown type of response to startup frame: %T", frame)
	}

	if config := s.conn.InternalCfg.TypedConfig; config != nil {
		if err := InternalValidateV2Supported(v.Supported, config.Pool); err != nil {
			return err
		}
	}
	s.conn.InternalSupported = v.Supported
	s.conn.InternalScyllaSupported = parseSupported(s.conn.InternalSupported, s.conn.InternalLogger)
	s.conn.recalculateSystemRequestTimeout()
	if _, poolOwned := s.conn.InternalErrorHandler.(InternalPoolOwner); !poolOwned || s.conn.InternalCfg.TypedConfig == nil {
		if current := s.conn.InternalHost.ScyllaFeatures(); current != s.conn.InternalScyllaSupported.ScyllaHostFeatures {
			implEndpoint.InternalHostInfoSetScyllaFeatures(s.conn.InternalHost, s.conn.InternalScyllaSupported.ScyllaHostFeatures)
		}
	}
	s.conn.InternalCqlProtoExts = parseCQLProtocolExtensions(s.conn.InternalSupported, s.conn.InternalVersion, s.conn.InternalLogger)

	if s.conn.InternalCfg.TypedConfig != nil {
		s.conn.InternalCqlProtoExts = implEndpoint.InternalTypedTabletExtensions(s.conn.InternalCqlProtoExts, s.conn.InternalSupported)
	}

	err = s.startup(ctx, startupCompleted)
	if err != nil {
		return err
	}
	s.conn.InternalInitFramerCache()
	return nil
}

// startupOptions builds the STARTUP options the driver always sends.
//
// The application's options go in first so the driver-owned keys below can
// overwrite them, never the other way round. They describe the driver, the
// protocol it is speaking and the session the connection belongs to, and are
// not the application's to change: a callback that set CQL_VERSION could make
// every connection in the cluster fail the handshake, one that set DRIVER_NAME
// or DRIVER_VERSION would misreport the driver to the server for the life of
// the connection, and one that set SESSION_ID would break correlating a
// session's connections in system.clients.
//
// The ordering is easy to lose, which is how it was lost: upstream has no
// ApplicationInfo hook and writes these three as a map literal, so merging the
// two put the callback last.
func InternalStartupOptions(cqlVersion, driverName, driverVersion string, info ApplicationInfo, driverConfig InternalStartupReporter, sessionID string, isScyllaConn bool) map[string]string {
	m := map[string]string{}

	if info != nil {
		info.UpdateStartupOptions(m)
	}

	if driverConfig != nil {
		driverConfig.InternalUpdateStartupOptions(m, isScyllaConn)
	}

	m[InternalSessionIDStartupKey] = sessionID
	m["CQL_VERSION"] = cqlVersion
	m["DRIVER_NAME"] = driverName
	m["DRIVER_VERSION"] = driverVersion

	return m
}

func (s *InternalStartupCoordinator) startup(ctx context.Context, startupCompleted *atomic.Bool) error {

	m := InternalStartupOptions(
		s.conn.InternalCfg.CQLVersion,
		s.conn.InternalStartupIdentity.DriverName,
		s.conn.InternalStartupIdentity.DriverVersion,
		s.conn.InternalStartupIdentity.ApplicationInfo,
		s.driverConfigReporter,
		s.conn.InternalStartupIdentity.SessionID,
		s.conn.InternalIsScyllaConn(),
	)

	delete(m, "COMPRESSION")

	if s.conn.InternalCompressor != nil {
		comp := s.conn.InternalSupported["COMPRESSION"]
		name := s.conn.InternalCompressor.Name()
		for _, compressor := range comp {
			if compressor == name {
				m["COMPRESSION"] = compressor
				break
			}
		}

		if _, ok := m["COMPRESSION"]; !ok {
			if s.conn.InternalCfg.TypedConfig != nil {
				return &UnsupportedSettingError{Setting: "compression", Reason: "server does not advertise requested " + name}
			}
			s.conn.InternalLogger.Printf("gocql: %s does not support the requested %q compression, continuing uncompressed (server offers: %v)\n",
				s.conn.InternalAddr, name, comp)
			s.conn.InternalCompressor = nil
		}
	}

	if err := s.conn.InternalResolveSegmentCompressor(); err != nil {
		return err
	}

	for _, ext := range s.conn.InternalCqlProtoExts {
		serialized := ext.InternalSerialize()
		for k, v := range serialized {
			m[k] = v
		}
	}

	frame, err := s.write(ctx, &writeStartupFrame{Opts: m}, startupCompleted)
	if err != nil {
		return err
	}

	switch v := frame.(type) {
	case error:
		return v
	case *frm.ReadyFrame:

		startupCompleted.Store(true)
		return nil
	case *frm.AuthenticateFrame:

		startupCompleted.Store(true)
		return s.authenticateHandshake(ctx, v, startupCompleted)
	default:
		return NewErrProtocol("Unknown type of response to startup frame: %s", v)
	}
}

func (s *InternalStartupCoordinator) authenticateHandshake(ctx context.Context, authFrame *frm.AuthenticateFrame, startupCompleted *atomic.Bool) error {
	if s.conn.InternalCfg != nil && s.conn.InternalCfg.TypedAuth != nil {
		host, _ := implEndpoint.SnapshotHost(s.conn.InternalHost)
		auth := InternalNewV2Authenticator(ctx, host, s.conn.InternalCfg.TypedAuth)
		s.conn.auth = auth
		defer auth.Close()
	}

	if s.conn.auth == nil {
		return fmt.Errorf("authentication required (using %q)", authFrame.Class)
	}

	resp, challenger, err := s.conn.auth.Challenge([]byte(authFrame.Class))
	if err != nil {
		return err
	}

	req := &writeAuthResponseFrame{Data: resp}
	for {
		frame, err := s.write(ctx, req, startupCompleted)
		if err != nil {
			return err
		}

		switch v := frame.(type) {
		case error:
			return v
		case *frm.AuthSuccessFrame:
			if challenger != nil {
				return challenger.Success(v.Data)
			}
			return nil
		case *frm.AuthChallengeFrame:

			if challenger == nil {
				return fmt.Errorf("unexpected authentication challenge from %s: the authenticator for %q does not support multi-step authentication", s.conn.InternalAddr, authFrame.Class)
			}
			resp, challenger, err = challenger.Challenge(v.Data)
			if err != nil {
				return err
			}

			req = &writeAuthResponseFrame{
				Data: resp,
			}
		default:
			return fmt.Errorf("unknown frame response during authentication: %v", v)
		}
	}
}

func (c *Conn) InternalCloseWithError(err error) {
	if c == nil {
		return
	}

	c.InternalMu.Lock()
	if c.InternalClosed {
		c.InternalMu.Unlock()
		return
	}
	c.InternalClosed = true
	c.changeLocked()
	callsToClose := c.InternalCalls

	c.InternalCalls = nil
	c.InternalMu.Unlock()

	// Close the socket before joining calls: an accepted span write may be
	// blocked in the kernel (or TLS). Waiting for that call before interrupting
	// the socket would deadlock shutdown and retain its input indefinitely.
	var cerr error
	if err == nil {

		c.InternalCancel()
	}
	cerr = c.close()

	seen := make(map[*InternalCallReq]bool, len(callsToClose))
	for _, req := range callsToClose {
		if seen[req] {
			continue
		}
		seen[req] = true

		if err != nil {

			select {
			case req.InternalResp <- InternalCallResp{InternalErr: err}:

			case <-req.InternalTimeout:

			}
		}
		req.waitExecDone("closeWithError")
		if req.streamObserverContext != nil {
			req.streamObserverEndOnce.Do(func() {
				req.streamObserverContext.StreamAbandoned(ObservedStream{
					Host: c.InternalHost,
				})
			})
		}
		InternalPutCallReq(req)
	}

	c.InternalFramers.close()

	if err != nil {
		c.InternalCancel()
	}

	if err != nil {
		c.InternalErrorHandler.HandleError(c, err, true)
	} else if cerr != nil {

		c.InternalErrorHandler.HandleError(c, cerr, true)
	}
}

func (c *Conn) InternalIsTabletSupported() bool {
	return atomic.LoadInt32(&c.tabletsRoutingV1) == 1
}

func (c *Conn) setTabletSupported(val bool) {
	intVal := int32(0)
	if val {
		intVal = 1
	}
	atomic.StoreInt32(&c.tabletsRoutingV1, intVal)
}

// usesMetadataID reports whether SCYLLA_USE_METADATA_ID was negotiated on this
// connection. This is the extension alone; see tracksResultMetadataID for the
// question the request path actually asks.
//
// It reads the framer config rather than a separate field on Conn, so the request
// path and the pooled framers that encode and decode the result metadata ID cannot
// disagree: a Conn that believed the extension was on while its framers did not
// would ask the server to skip metadata while writing no ID to compare against.
// initFramerCache populates the config during connection setup, before any query
// can run.
//
// One framer is not built from that config: framerPool.get falls back to newFramer
// when the pool is disabled, which yields scyllaUseMetadataID false regardless of
// what was negotiated. That is correct during the handshake, and unreachable from
// the request path afterwards — execInternal acquires its framer before addCall
// rejects a closed connection, so a framer taken after the pool closed belongs to a
// call that never writes a frame. It is a property of call ordering rather than of
// construction, so see #982, which also covers flagLWT and tabletsRoutingV1.
func (c *Conn) InternalUsesMetadataID() bool {
	return c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID
}

// tracksResultMetadataID reports whether an EXECUTE on this connection carries a
// result metadata ID for the server to compare its own against — either because
// native protocol v5 makes the field mandatory, or because
// SCYLLA_USE_METADATA_ID backported it to v4. Either way the server answers a
// stale ID with METADATA_CHANGED, which is what makes skipping result metadata
// recoverable. See shouldSkipResultMetadata.
//
// Read from the same framer config as usesMetadataID, for the same reason, and
// because initCache has already masked the protocol version there.
func (c *Conn) InternalTracksResultMetadataID() bool {
	return c.InternalFramers.InternalDefaults.InternalProto > protoVersion4 || c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID
}

func (c *Conn) close() error {
	return c.InternalR.Close()
}

func (c *Conn) Close() {
	c.InternalCloseWithError(nil)
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) InternalServe(ctx context.Context) {
	var err error
	for {
		if c.pauseSharedReader(ctx) {
			if ctx.Err() != nil {
				break
			}
			continue
		}
		err = c.InternalRecv(ctx, true)
		if err == nil {
			continue
		}

		if errors.Is(err, ErrReadHeaderTimeout) {
			c.InternalLogger.Print("gocql: read header timeout")
			err = nil
			continue
		}
		break
	}

	c.InternalCloseWithError(err)
}

func (c *Conn) discardFrame(r io.Reader, head frm.FrameHeader) error {

	_, err := io.CopyN(io.Discard, r, int64(head.Length))
	if err != nil {
		return err
	}
	return nil
}

type InternalProtocolError struct {
	InternalFrame frame
}

func (p *InternalProtocolError) Error() string {
	if err, ok := p.InternalFrame.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("gocql: received unexpected frame on stream %d: %v", p.InternalFrame.Header().Stream, p.InternalFrame)
}

func (c *Conn) InternalHeartBeat(ctx context.Context) {
	if c.InternalCfg != nil && c.InternalCfg.TypedConfig != nil {
		c.typedHeartbeat(ctx)
		return
	}
	sleepTime := 1 * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()

	var failures int

	for {
		if failures > 5 {
			c.InternalCloseWithError(fmt.Errorf("gocql: heartbeat failed"))
			return
		}

		timer.Reset(sleepTime)

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		framer, err := c.InternalExec(context.Background(), &writeOptionsFrame{}, nil, c.InternalCfg.ConnectTimeout)
		if err != nil {
			failures++
			continue
		}

		resp, err := framer.ParseFrame()
		framer.Release()
		if err != nil {

			failures++
			continue
		}

		switch resp.(type) {
		case *frm.SupportedFrame:

			sleepTime = 30 * time.Second
			failures = 0
		case error:

			failures++
		default:

			c.InternalLogger.Printf("gocql: unexpected frame in response to options: %T\n", resp)
			failures++

			sleepTime = 1 * time.Second
		}
	}
}

func (c *Conn) InternalRecv(ctx context.Context, startupCompleted bool) error {

	if startupCompleted && c.InternalVersion > protoVersion4 {
		return c.InternalRecvSegment(ctx)
	}

	return c.InternalProcessFrame(ctx, c.InternalR)
}

func (c *Conn) InternalProcessFrame(ctx context.Context, r io.Reader) error {
	return c.InternalProcessFrameSource(ctx, frameSource{InternalR: r})
}

// readFrameHeader reads one CQL frame header from r with the read deadline
// disarmed for its first byte: the serve() loop waits indefinitely for the next
// inbound frame, so a short ReadTimeout must not fire on an idle connection. Once
// the peer has started sending, headerReader re-arms the deadline, so the rest of
// the header — and the body read that follows — are bounded by the operational
// timeout. The disarm is also cleared via defer, which covers the paths that
// deliver no byte at all, and means a panic cannot leave the connection
// deadline-free.
//
// Disarming through a dedicated flag (rather than zeroing and restoring the
// connReader timeout) keeps the operational timeout intact, so a concurrent
// finalizeConnection switching the reader from ConnectTimeout to ReadTimeout is
// never lost. The header read itself clears any stale deadline (connReader.Read).
//
// Unlike readFirstSegmentHeader, the disarm here is a dynamic check: on proto v5 r
// is a reader over an already-received segment payload, which has no deadline to
// disarm. Missing it in that case is correct, not a bug.
func (c *Conn) InternalReadFrameHeader(r io.Reader) (frm.FrameHeader, error) {
	d, _ := r.(deadlineDisarmer)
	if d != nil {
		d.InternalSetDisarm(true)
		defer d.InternalSetDisarm(false)
	}

	c.headerReader.InternalReset(r, d)
	return readHeader(&c.headerReader, c.headerBuf[:])
}

func (c *Conn) InternalProcessFrameSource(ctx context.Context, src frameSource) error {

	r := src.InternalR

	headStartTime, headEndTime := src.InternalNetStart, src.InternalNetEnd
	measureHere := headStartTime.IsZero() && c.InternalFrameObserver != nil

	if measureHere {
		headStartTime = time.Now()
	}

	head, err := c.InternalReadFrameHeader(r)

	if measureHere {
		headEndTime = time.Now()
	}
	if err != nil {
		return err
	}

	if c.InternalFrameObserver != nil {
		c.InternalFrameObserver.ObserveFrameHeader(context.Background(), ObservedFrameHeader{
			Version: head.Version,
			Flags:   head.Flags,
			Stream:  int16(head.Stream),
			Opcode:  head.Op,
			Length:  int32(head.Length),
			Start:   headStartTime,
			End:     headEndTime,
			Host:    c.InternalHost,
		})
	}

	if src.InternalSegment != nil && head.Length > src.InternalSegment.Len() {
		return fmt.Errorf("gocql: frame header declares a %d byte body but only %d bytes remain in the self-contained segment", head.Length, src.InternalSegment.Len())
	}

	if head.Stream >= c.InternalStreams.NumStreams {
		return fmt.Errorf("gocql: frame header stream is beyond call expected bounds: %d", head.Stream)
	} else if head.Stream < 0 {

		framer, err := c.readFrameIntoFramer(src, head)
		if err != nil {
			return err
		}

		frame, err := framer.ParseFrame()

		c.InternalReleaseReadFramer(framer)
		if err != nil {
			if head.Stream == -1 {

				c.InternalLogger.Printf("gocql: unable to parse event frame: %v\n", err)
				return nil
			}
			return err
		}

		if head.Stream == -1 {
			if c.InternalHooks != nil {
				if err := c.dispatchEvent(frame); err != nil {
					return err
				}
			}
			return nil
		}

		return &InternalProtocolError{
			InternalFrame: frame,
		}
	}

	c.InternalMu.Lock()
	if c.InternalClosed {
		c.InternalMu.Unlock()
		return ErrConnectionClosed
	}
	call, ok := c.InternalCalls[head.Stream]

	if call != nil && ok {
		if streamID := call.InternalStreamID; head.Stream != streamID {
			c.InternalMu.Unlock()

			return fmt.Errorf("gocql: response for stream %d dispatched to a call on stream %d", head.Stream, streamID)
		}
	}
	delete(c.InternalCalls, head.Stream)
	c.InternalMu.Unlock()
	if call == nil || !ok {
		c.InternalLogger.Printf("gocql: received response for stream which has no handler: header=%v\n", head)
		return c.discardFrame(r, head)
	}

	if call.InternalTyped {
		return c.receiveTypedCall(ctx, call, src, head)
	}

	framer := c.InternalGetReadFramer()

	err = src.InternalReadBody(framer, &head)

	desynced := InternalBodyReadDesyncedConn(err)

	select {
	case call.InternalResp <- InternalCallResp{InternalFramer: framer, InternalErr: err, removedFromCalls: true}:

	case <-call.InternalTimeout:
		c.InternalAbandonRecvCall(call, framer)
	case <-ctx.Done():
		c.InternalAbandonRecvCall(call, framer)
	}

	if desynced {
		return err
	}

	return nil
}

// bodyReadDesyncedConn reports whether a frame-body read failure left the
// connection at an unknown stream offset, in which case it is fatal: the
// unconsumed remainder of the body is still on the wire and every subsequent read
// would be mis-framed. A decode failure, or an over-long frame whose body was
// successfully discarded, leaves the stream aligned and stays a per-request error.
//
// Two kinds of failure qualify.
//
// A network error means the body was read partially or not at all. errors.As
// rather than a type assertion: readFrame wraps the read error, so an assertion on
// the wrapper never matches (which is precisely how this used to leave desynced
// connections in the pool).
//
// A failed read deadline arm means connReader.Read never reached the socket, so
// the whole body is still queued on a connection that is otherwise healthy — the
// worst case, because nothing else will surface the problem and serve() reads the
// body as its next frame header. It needs naming rather than being left to the
// net.Error test: net.Conn.SetReadDeadline is only conventionally a *net.OpError,
// and a connection from a user-supplied Dialer or HostDialer may return a plain
// error, which would classify this as per-request.
func InternalBodyReadDesyncedConn(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errArmReadDeadline) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

func (c *Conn) readFrameIntoFramer(src frameSource, head frm.FrameHeader) (*framer, error) {
	framer := c.InternalGetReadFramer()

	if err := src.InternalReadBody(framer, &head); err != nil {
		c.InternalReleaseReadFramer(framer)
		return nil, err
	}
	return framer, nil
}

func (c *Conn) InternalAbandonRecvCall(call *InternalCallReq, framer *framer) {
	c.InternalReleaseReadFramer(framer)
	c.releaseStream(call)
	call.waitExecDone("abandonRecvCall")
	InternalPutCallReq(call)
}

func (c *Conn) releaseStream(call *InternalCallReq) {
	c.InternalMu.Lock()
	if call.orphaned {
		c.orphans--
		call.orphaned = false
	}
	c.InternalStreams.Clear(call.InternalStreamID)
	c.changeLocked()
	c.InternalMu.Unlock()

	if call.streamObserverContext != nil {
		call.streamObserverEndOnce.Do(func() {
			call.streamObserverContext.StreamFinished(ObservedStream{
				Host: c.InternalHost,
			})
		})
	}
}

func (c *Conn) InternalRecvSegment(ctx context.Context) error {
	return c.InternalRecvLeasedSegment(ctx)
}

// observedNow returns the current time when a frame header observer is
// installed, and the zero time otherwise. Callers thread the result into
// frameSource, where a zero value means "not measured" and processFrameSource
// falls back to timing the header read itself.
func (c *Conn) observedNow() time.Time {
	if c.InternalFrameObserver == nil {
		return time.Time{}
	}
	return time.Now()
}

// readFirstSegmentHeader reads the header of the next segment, with the read
// deadline disarmed for its first byte so an idle serve() loop can block
// indefinitely waiting for the next frame to begin. The deadline is disarmed via a
// dedicated flag (so connReader.Read does not re-arm it), which leaves the
// operational timeout value intact, so a concurrent finalizeConnection switching
// the reader from ConnectTimeout to ReadTimeout is never clobbered.
//
// Only the idle wait is unbounded. headerReader re-arms the deadline as soon as the
// peer delivers a byte, so the rest of the header is bounded by ReadTimeout and a
// peer that sends a header prefix and then stalls cannot hold the serve goroutine;
// the deferred clear covers the paths that deliver no byte at all, and a panic.
//
// A read timeout during the idle wait is normalised to ErrReadHeaderTimeout so
// serve() treats it as a benign idle timeout instead of closing the connection —
// but only if the read consumed nothing. A timeout partway through a header leaves
// the stream at an unknown offset, so it stays a plain error and takes the
// connection down rather than mis-framing everything that follows. That is the
// timeout the re-arm above makes reachable.
func (c *Conn) InternalReadFirstSegmentHeader() (segment.Header, error) {

	c.InternalR.InternalSetDisarm(true)
	defer c.InternalR.InternalSetDisarm(false)

	c.headerReader.InternalReset(c.InternalR, c.InternalR)

	hdr, err := segment.ReadHeader(&c.headerReader, c.InternalSegCompressor != nil)
	if err != nil {
		var netErr net.Error
		if c.headerReader.InternalN == 0 && errors.As(err, &netErr) && netErr.Timeout() {
			return segment.Header{}, fmt.Errorf("%w: %w", ErrReadHeaderTimeout, err)
		}
		return segment.Header{}, err
	}
	return hdr, nil
}

// readSegmentPayload reads the payload of a segment whose header has already been
// read, in the layout c.segCompressor selects — nil being the uncompressed one. The
// narrowing behind that field happened once during the handshake; see
// Conn.resolveSegmentCompressor.
//
// The header readers above take their layout from the same field. They branch on a
// bool where this branches on the compressor being non-nil, and a connection whose two
// answers disagreed would read an 8-byte compressed header as a 6-byte uncompressed
// one and mis-frame everything after it.
func (c *Conn) InternalReadSegmentPayload(hdr segment.Header) ([]byte, error) {
	return segment.ReadPayload(c.InternalR, hdr, c.InternalSegCompressor, &c.segScratch)
}

// recvSplitFrame reassembles a single CQL frame that the peer split across
// multiple non-self-contained segments and processes it. first is the payload of
// the segment already consumed by recvSegment.
//
// Every read here runs with the deadline armed (the peer is mid-transfer), so a
// peer that stops sending fails within one ReadTimeout — see connReader.Read for
// how a read that stalls but keeps progressing is resumed instead. The
// reassembly buffer is allocated exactly once, sized to the frame length the peer
// declared in the CQL frame header, and appending the arriving payloads is bounded
// by that length. So neither a lying header nor incremental growth can inflate it:
// growing a buffer to a frm.MaxFrameSize frame would end up holding ~512 MiB for a
// valid 256 MiB response. Ownership of the buffer is then handed to the read
// framer rather than copied into it, so the frame is never resident twice.
//
// The declared length itself is the peer's to choose, up to frm.MaxFrameSize, so a
// small hostile prologue still buys this one allocation before any body byte has
// arrived. Accepted deliberately, because it is bounded: at most once per
// connection — the continuation reads run under ReadTimeout, so a peer that
// stalls after declaring takes the connection down with it. Growing the buffer
// as payloads arrive would blunt that, at the cost of roughly doubling peak
// memory for every valid large frame, which is the common case. Contrast the
// self-contained path, where the same lie is repeatable per-request and is
// therefore rejected before the allocation instead (see frameSource).
//
// netStart/netEnd are the network-read window of the first segment, for
// FrameHeaderObserver. netEnd is extended below if the CQL header itself needed
// more segments to arrive — but only that far: the observer's End is when the
// header finished arriving, not the rest of the frame.
func (c *Conn) InternalRecvSplitFrame(ctx context.Context, first []byte, netStart, netEnd time.Time) error {

	if len(first) < headSize {
		accumulated := append([]byte(nil), first...)
		for len(accumulated) < headSize {
			payload, err := c.InternalReadContinuationSegment()
			if err != nil {
				return err
			}
			accumulated = append(accumulated, payload...)
		}
		first = accumulated
		netEnd = c.observedNow()
	}

	head, err := readHeader(bytes.NewReader(first[:headSize]), c.headerBuf[:])
	if err != nil {
		return err
	}
	if head.Length < 0 || head.Length > frm.MaxFrameSize {
		return fmt.Errorf("gocql: invalid frame body length in segmented frame: %d", head.Length)
	}
	total := headSize + head.Length
	if len(first) > total {
		return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", total)
	}

	frame := make([]byte, 0, total)
	frame = append(frame, first...)
	for len(frame) < total {
		payload, err := c.InternalReadContinuationSegment()
		if err != nil {
			return err
		}
		if len(frame)+len(payload) > total {
			return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", total)
		}
		frame = append(frame, payload...)
	}

	return c.InternalProcessFrameSource(ctx, frameSource{
		InternalR:        bytes.NewReader(frame),
		InternalBody:     frame[headSize:],
		InternalNetStart: netStart,
		InternalNetEnd:   netEnd,
	})
}

// readContinuationSegment reads the next segment of a frame split across several
// segments and returns its payload, which aliases c.segScratch and is therefore
// only valid until the next segment is read. The reads run with the deadline armed,
// so a peer that stops sending fails within one ReadTimeout (connReader.Read).
// A self-contained segment, or one that makes no forward progress (empty
// payload), is rejected so a hostile peer cannot drive an infinite reassembly
// loop.
func (c *Conn) InternalReadContinuationSegment() ([]byte, error) {
	hdr, err := segment.ReadHeader(c.InternalR, c.InternalSegCompressor != nil)
	if err != nil {
		return nil, fmt.Errorf("gocql: failed to read continuation segment header: %w", err)
	}
	if hdr.IsSelfContained {
		return nil, fmt.Errorf("gocql: received self-contained segment, but expected a continuation")
	}
	payload, err := c.InternalReadSegmentPayload(hdr)
	if err != nil {
		return nil, fmt.Errorf("gocql: failed to read continuation segment payload: %w", err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("gocql: continuation segment made no progress (empty payload)")
	}
	return payload, nil
}

func (c *Conn) processAllFramesInSegment(ctx context.Context, r *bytes.Reader, netStart, netEnd time.Time) error {
	// A self-contained segment carries one or more complete CQL frames, so we
	// drain them all. This is safe to iterate: the segment payload has already
	// been CRC32-verified, and processFrameSource consumes exactly one frame
	// (header + body) per call, keeping r aligned for the next iteration. It
	// returns a non-nil error only for connection-fatal conditions (which stops
	// the loop); a per-request decode error is delivered to that request's
	// waiting caller and it returns nil, so sibling frames in the same segment are
	// still processed.
	//
	// r is passed as the segment as well as the reader, which is what bounds each
	// frame's declared body length by the bytes actually left in the segment (see
	// frameSource).
	//
	// Every frame packed into this segment reports the same observer window: they
	// did all arrive in the one network read, and attributing a slice of it to
	// each would be an invention.
	var err error
	for r.Len() > 0 && err == nil {
		err = c.InternalProcessFrameSource(ctx, frameSource{
			InternalR:        r,
			InternalSegment:  r,
			InternalNetStart: netStart,
			InternalNetEnd:   netEnd,
		})
	}

	return err
}

var _ connReadSource = (*connReader)(nil)

type InternalCallReq struct {
	orphaned      bool
	InternalTyped bool
	// streamObserverContext is notified about events regarding this stream
	streamObserverContext StreamObserverContext
	// resp will receive the frame that was sent as a response to this stream.
	InternalResp     chan InternalCallResp
	InternalTimeout  chan struct{} // indicates to recv() that a call has timed out
	timer            *time.Timer
	InternalStreamID int // current stream in use
	// streamObserverEndOnce ensures that either StreamAbandoned or StreamFinished is called,
	// but not both.
	streamObserverEndOnce sync.Once
	InternalDone          sync.WaitGroup
}

var InternalCallReqPool = sync.Pool{
	New: func() any {
		return &InternalCallReq{
			InternalResp: make(chan InternalCallResp),
		}
	},
}

func InternalGetCallReq(streamID int) *InternalCallReq {
	call := InternalCallReqPool.Get().(*InternalCallReq)
	call.InternalTimeout = make(chan struct{})
	call.InternalTyped = false
	call.orphaned = false
	call.InternalStreamID = streamID
	call.streamObserverContext = nil
	call.streamObserverEndOnce = sync.Once{}
	call.InternalDone = sync.WaitGroup{}
	call.InternalDone.Add(1)
	return call
}

func InternalPutCallReq(call *InternalCallReq) {
	if call.timer != nil {
		if !call.timer.Stop() {
			select {
			case <-call.timer.C:
			default:
			}
		}
	}
	call.streamObserverContext = nil
	call.streamObserverEndOnce = sync.Once{}
	call.InternalStreamID = 0
	call.InternalTimeout = nil
	InternalCallReqPool.Put(call)
}

func (call *InternalCallReq) finishExec() {
	call.InternalDone.Done()
}

func (call *InternalCallReq) waitExecDone(where string) {
	InternalWaitCallReqDone(call, where)
}

// removeCallIfOpen removes a call from c.calls only if exec() still owns its
// cleanup. Once the connection has started closing, closeWithError() becomes
// responsible for draining and recycling detached callReqs.
func (c *Conn) InternalRemoveCallIfOpen(streamID int) bool {
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()

	if c.InternalClosed || c.InternalCalls == nil {
		return false
	}

	if _, present := c.InternalCalls[streamID]; !present {

		return false
	}
	delete(c.InternalCalls, streamID)
	return true
}

type InternalCallResp struct {
	InternalPage *InternalResponsePage
	// framer is the response frame.
	// May be nil if err is not nil.
	InternalFramer *framer
	// err is error encountered, if any.
	InternalErr error
	// removedFromCalls means the sender already deleted this call from c.calls, so
	// closeWithError's drain will never see it and the receiver owns releasing the
	// stream and recycling the callReq. Set by processFrameSource on every outcome
	// it delivers directly; Conn.closed races this delivery and cannot answer it.
	removedFromCalls bool
}

// addCall attempts to add a call to c.calls.
// It fails with error if the connection already started closing or if a call for the given stream
// already exists.
func (c *Conn) addCall(call *InternalCallReq) error {
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()
	if c.InternalClosed {
		return ErrConnectionClosed
	}
	existingCall := c.InternalCalls[call.InternalStreamID]
	if existingCall != nil {
		return fmt.Errorf("attempting to use stream already in use: %d -> %d", call.InternalStreamID,
			existingCall.InternalStreamID)
	}
	c.InternalCalls[call.InternalStreamID] = call
	return nil
}

// exec executes a frame on the connection and returns the response framer.
//
// IMPORTANT: The caller takes ownership of the returned framer and MUST call
// framer.Release() when done reading the response. Failure to release the framer
// will leak memory and prevent buffer reuse.
//
// The framer should be released as soon as the response data is no longer needed,
// typically via defer immediately after parsing or after transferring ownership
// to an Iter.
func (c *Conn) InternalExec(ctx context.Context, req frameBuilder, tracer Tracer, requestTimeout time.Duration) (*framer, error) {
	return c.execInternal(ctx, req, tracer, requestTimeout, true)
}

func (c *Conn) execInternal(ctx context.Context, req frameBuilder, tracer Tracer, requestTimeout time.Duration, startupCompleted bool) (*framer, error) {
	if err := c.beginShared(ctx); err != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err})
	}
	defer c.endShared()
	return c.execAdmitted(ctx, req, tracer, requestTimeout, startupCompleted)
}

func (c *Conn) execAdmitted(ctx context.Context, req frameBuilder, tracer Tracer, requestTimeout time.Duration, startupCompleted bool) (*framer, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctxErr, InternalPotentiallyExecuted: false})
	}

	stream, ok := c.InternalStreams.GetStream()
	if !ok {
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrNoStreams, InternalPotentiallyExecuted: false})
	}

	framer := c.InternalGetWriteFramer()

	call := InternalGetCallReq(stream)

	if c.InternalStreamObserver != nil {
		call.streamObserverContext = c.InternalStreamObserver.StreamContext(ctx)
	}

	if err := c.addCall(call); err != nil {
		call.finishExec()
		InternalPutCallReq(call)
		c.InternalReleaseWriteFramer(framer)
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
	}

	var (
		stopWaiting   bool
		releaseStream bool
		recycleCall   bool
		closeErr      error
	)

	defer func() {
		if closeErr != nil {
			c.InternalCloseWithError(closeErr)
		}
	}()

	defer func() {
		if stopWaiting {
			close(call.InternalTimeout)
		}
		call.finishExec()
		if releaseStream {
			c.releaseStream(call)
		}
		if recycleCall {
			InternalPutCallReq(call)
		}
	}()

	if tracer != nil {
		framer.Trace()
	}

	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{
			Host: c.InternalHost,
		})
	}

	err := req.BuildFrame(framer, stream)
	if err != nil {
		c.InternalReleaseWriteFramer(framer)

		stopWaiting = true
		if c.InternalRemoveCallIfOpen(call.InternalStreamID) {

			releaseStream = true
			recycleCall = true
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
	}

	if c.InternalVersion > protoVersion4 && startupCompleted {
		if err = framer.PrepareModernLayout(); err != nil {
			c.InternalReleaseWriteFramer(framer)

			stopWaiting = true
			if c.InternalRemoveCallIfOpen(call.InternalStreamID) {
				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: false})
		}
	}

	n, err := c.InternalW.InternalWriteContext(ctx, framer.Buf)
	c.InternalReleaseWriteFramer(framer)
	if err != nil {

		stopWaiting = true
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && n == 0 {

			if c.InternalRemoveCallIfOpen(call.InternalStreamID) {

				releaseStream = true
				recycleCall = true
			}
		} else {

			closeErr = err
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: err, InternalPotentiallyExecuted: true})
	}

	var timeoutCh <-chan time.Time
	if requestTimeout > 0 {
		if call.timer == nil {
			call.timer = time.NewTimer(requestTimeout)
		} else {
			if !call.timer.Stop() {
				select {
				case <-call.timer.C:
				default:
				}
			}
			call.timer.Reset(requestTimeout)
		}
		timeoutCh = call.timer.C
	}

	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	select {
	case resp := <-call.InternalResp:
		stopWaiting = true
		if resp.InternalErr != nil {
			c.InternalReleaseReadFramer(resp.InternalFramer)
			if resp.removedFromCalls {

				releaseStream = true
				recycleCall = true
			}
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: resp.InternalErr, InternalPotentiallyExecuted: true})
		}

		releaseStream = true
		recycleCall = true

		if v := resp.InternalFramer.Header.Version.Version(); v != c.InternalVersion {
			c.InternalReleaseReadFramer(resp.InternalFramer)
			return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: NewErrProtocol("unexpected protocol version in response: got %d expected %d", v, c.InternalVersion), InternalPotentiallyExecuted: true})
		}

		return resp.InternalFramer, nil
	case <-timeoutCh:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrTimeoutNoResponse, InternalPotentiallyExecuted: true, InternalTimeout: requestTimeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-ctxDone:
		stopWaiting = true
		if c.markOrphan(call) {
			closeErr = InternalErrOrphanBudget
		}
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ctx.Err(), InternalPotentiallyExecuted: true, InternalTimeout: requestTimeout, InternalInFlight: c.InternalStreams.InUse()})
	case <-c.InternalCtx.Done():
		stopWaiting = true
		return nil, runtimeerrors.NewQueryError(runtimeerrors.QueryErrorState{InternalErr: ErrConnectionClosed, InternalPotentiallyExecuted: true})
	}
}

// ObservedStream observes a single request/response stream.
type ObservedStream struct {
	// Host of the connection used to send the stream.
	Host *HostInfo
}

// StreamObserver is notified about request/response pairs.
// Streams are created for executing queries/batches or
// internal requests to the database and might live longer than
// execution of the query - the stream is still tracked until
// response arrives so that stream IDs are not reused.
type StreamObserver interface {
	// StreamContext is called before creating a new stream.
	// ctx is context passed to Session.Query / Session.Batch,
	// but might also be an internal context (for example
	// for internal requests that use control connection).
	// StreamContext might return nil if it is not interested
	// in the details of this stream.
	// StreamContext is called before the stream is created
	// and the returned StreamObserverContext might be discarded
	// without any methods called on the StreamObserverContext if
	// creation of the stream fails.
	// Note that if you don't need to track per-stream data,
	// you can always return the same StreamObserverContext.
	StreamContext(ctx context.Context) StreamObserverContext
}

// StreamObserverContext is notified about state of a stream.
// A stream is started every time a request is written to the server
// and is finished when a response is received.
// It is abandoned when the underlying network connection is closed
// before receiving a response.
type StreamObserverContext interface {
	// StreamStarted is called when the stream is started.
	// This happens just before a request is written to the wire.
	StreamStarted(observedStream ObservedStream)

	// StreamAbandoned is called when we stop waiting for response.
	// This happens when the underlying network connection is closed.
	// StreamFinished won't be called if StreamAbandoned is.
	StreamAbandoned(observedStream ObservedStream)

	// StreamFinished is called when we receive a response for the stream.
	StreamFinished(observedStream ObservedStream)
}

func InternalMetadataIDTracked(idExchangeActive bool, resultMetadataID []byte) bool {
	return idExchangeActive && len(resultMetadataID) > 0
}

func (c *Conn) Closed() bool {
	c.InternalMu.Lock()
	defer c.InternalMu.Unlock()
	return c.InternalClosed
}

func (c *Conn) Address() string {
	return c.InternalAddr
}

func (c *Conn) AvailableStreams() int {
	if c.InternalExclusiveFlag.Load() {
		return 0
	}
	return c.InternalStreams.Available()
}

func InternalUseKeyspaceStmt(keyspace string) string {
	return `USE "` + strings.ReplaceAll(keyspace, `"`, `""`) + `"`
}

func (c *Conn) UseKeyspace(keyspace string) error {
	return c.InternalUseKeyspaceContext(c.InternalCtx, keyspace)
}

func (c *Conn) InternalUseKeyspaceContext(ctx context.Context, keyspace string) error {
	q := &useKeyspaceFrame{Statement: InternalUseKeyspaceStmt(keyspace)}

	framer, err := c.InternalExec(ctx, q, nil, c.InternalCfg.ConnectTimeout)
	if err != nil {
		return err
	}
	defer framer.Release()

	resp, err := framer.ParseFrame()
	if err != nil {
		return err
	}

	switch x := resp.(type) {
	case *resultKeyspaceFrame:
	case error:
		return x
	default:
		return NewErrProtocol("unknown frame in response to USE: %v", x)
	}

	c.setCurrentKeyspace(keyspace)

	return nil
}

// setCurrentKeyspace records the keyspace this connection was switched to. See the
// currentKeyspace field for why it is atomic.
func (c *Conn) setCurrentKeyspace(keyspace string) {
	c.currentKeyspace.Store(&keyspace)
}

// getCurrentKeyspace returns the keyspace this connection was switched to, or ""
// if it was never switched (Cluster.Keyspace empty, or a control connection).
func (c *Conn) getCurrentKeyspace() string {
	if ks := c.currentKeyspace.Load(); ks != nil {
		return *ks
	}
	return ""
}

func InternalGetSchemaAgreement(localSchemaVersion string, querySystemPeersRows []InternalSchemaAgreementHost, logger StdLogger) error {
	versions := make(map[string]struct{})

	for _, row := range querySystemPeersRows {
		if !row.IsValid() {
			logger.Printf("invalid peer or peer with empty schema_version: peer=%q", row)
			continue
		}
		versions[row.SchemaVersion.String()] = struct{}{}
	}

	if localSchemaVersion != "" {
		versions[localSchemaVersion] = struct{}{}
	}

	if len(versions) > 1 {
		schemas := make([]string, 0, len(versions))
		for schema := range versions {
			schemas = append(schemas, schema)
		}

		return func() *ErrSchemaMismatch {
			opaqueStorageValue := new(ErrSchemaMismatch)
			opaqueStorageValue.privateStorage.schemas = schemas
			return opaqueStorageValue
		}()
	}

	return nil
}

type InternalSchemaAgreementHost struct {
	DataCenter    string
	Rack          string
	RPCAddress    string
	HostID        UUID
	SchemaVersion UUID
}

func (h *InternalSchemaAgreementHost) IsValid() bool {
	return h.DataCenter != "" && h.Rack != "" && h.HostID.String() != "" && h.SchemaVersion.String() != ""
}

type ErrSchemaMismatch struct {
	privateStorage ErrSchemaMismatchStorage
}

func (e *ErrSchemaMismatch) Error() string {
	return fmt.Sprintf("gocql: cluster schema versions not consistent: %+v", e.privateStorage.schemas)
}

func InternalUnmarshalTabletHint(hint []byte, _ uint8, keyspace, table string) (tablets.TabletInfo, error) {
	return tablets.ParseHint(hint, keyspace, table)
}

const InternalQrySystemPeers = "SELECT peer, data_center, host_id, rack, release_version, rpc_address, schema_version, tokens FROM system.peers"

const InternalQrySystemPeersCassandra = "SELECT peer, data_center, host_id, preferred_ip, rack, release_version, rpc_address, schema_version, tokens FROM system.peers"

const InternalQrySystemPeersV2 = "SELECT peer, data_center, host_id, native_address, native_port, preferred_ip, rack, release_version, schema_version, tokens FROM system.peers_v2"

const InternalQrySystemLocal = "SELECT broadcast_address, cluster_name, data_center, host_id, listen_address, partitioner, rack, release_version, rpc_address, schema_version, tokens FROM system.local WHERE key='local'"

// connectionHooks is the session-owned event/agreement boundary. Transport owns
// socket readers and dispatch queues; the coordinator owns publication workers.
type InternalConnectionHooks interface {
	InternalStartConnectionWorker(func()) bool
	InternalConnectionWorkersDone() <-chan struct{}
	InternalHandleConnectionEvent(context.Context, frame, func()) bool
}

type InternalStartupIdentity struct {
	DriverName, DriverVersion string
	ApplicationInfo           ApplicationInfo
	SessionID                 string
}

type InternalStartupReporter interface{ InternalUpdateStartupOptions(map[string]string, bool) }

type InternalPoolOwner interface{ InternalNotifyReservationChanged() }

type InternalConnectionPoolPolicy struct {
	InternalDisableShardAware bool
	InternalMaxExcessRate     float32
}

// frameSource supplies one CQL frame to processFrame. The header always comes
// from r — the socket, or a buffer holding an already-received segment payload.
//
// Field order is dictated by govet's fieldalignment rather than by how the fields
// relate: body's length and capacity are the only trailing words here that hold no
// pointer, so it goes last to keep the pointer-scanned prefix short. It belongs
// with r.
type InternalFrameSource struct {
	InternalOwner *wire.Lease
	InternalChain wire.Chain
	InternalR     io.Reader
	// segment, when non-nil, is the self-contained v5 segment payload r reads
	// from. Such a segment carries only whole frames, so a header declaring a body
	// longer than what is left in the segment is a framing violation, and
	// processFrameSource rejects it before readFrame acts on the declared length.
	//
	// That check is what keeps the length honest here. Off the socket, a lying
	// header costs the peer the bytes it claimed or stalls into a net.Error that
	// takes the connection down; out of a segment the short read is immediate and
	// yields io.ErrUnexpectedEOF, which is not a net.Error, so processFrameSource
	// keeps it per-request and leaves the connection up. A ~20-byte segment could
	// otherwise buy a frm.MaxFrameSize allocation, repeatable for as long as the peer
	// cares to send them.
	//
	// Nil on the pre-v5 socket path, and nil for a reassembled frame, where
	// framer.adoptFrameBody already matches the declared length exactly.
	InternalSegment *bytes.Reader
	// netStart/netEnd are the window of the network read that delivered these
	// bytes, for FrameHeaderObserver. On v5 the CQL header is parsed out of a
	// segment that has already arrived, so processFrameSource cannot measure the
	// network wait itself — the reader that could is recvSegment, and it passes
	// the window down here.
	//
	// Zero means "not measured", which is the pre-v5 socket path: there
	// processFrameSource reads the header off the wire and times it directly.
	InternalNetStart, InternalNetEnd time.Time
	// body is set only when the whole frame body is already in memory and the
	// buffer holding it can be given away: the read framer then adopts it instead
	// of reading and copying the body a second time (see framer.adoptFrameBody).
	InternalBody []byte
}

// readBody fills f with the frame body described by head, either by reading it
// from s.r or by adopting the buffer s already holds.
func (s InternalFrameSource) InternalReadBody(f *framer, head *frm.FrameHeader) error {
	if s.InternalOwner != nil {
		return f.ReadFrame(InternalNewChainReader(s.InternalChain), head)
	}
	if s.InternalBody != nil {
		return f.AdoptFrameBody(s.InternalBody, head)
	}
	return f.ReadFrame(s.InternalR, head)
}

// headerReader reads one frame or segment header, bounding the read-deadline
// disarm to the wait for its first byte.
//
// The idle wait has to be unbounded: serve() blocks on the next header for as long
// as the peer has nothing to send, and a short ReadTimeout must not drop a healthy
// connection. But once the first byte has arrived the peer is mid-header, and the
// rest belongs under ReadTimeout like any other transfer — otherwise a peer that
// sends one byte and then stops holds the serve goroutine for as long as it keeps
// the socket open, and the connection is never dropped.
//
// The disarm is per-read (connReader.armDeadline runs once per read attempt), so
// bounding it means capping the first read to a single byte and clearing the disarm
// before the next one; the remainder is then read with the deadline armed. Capping
// performs a separate socket read. The production reader intentionally does not
// prefetch payload bytes: bodies are read directly into their final leased storage.
//
// n counts the bytes delivered. That is the caller's benign-vs-fatal signal for a
// header-read timeout: nothing consumed is the idle peer, part of a header consumed
// leaves the stream at an unknown offset (readFirstSegmentHeader, readHeader).
type InternalHeaderReader struct {
	r io.Reader
	// disarm is the reader whose deadline is disarmed for the first byte, and is
	// cleared once that byte arrives so the cap and the re-arm happen exactly once.
	// Nil when there is no deadline to bound: on proto v5 the CQL header is parsed
	// out of a segment payload that has already been received.
	disarm    InternalDeadlineDisarmer
	InternalN int
}

func (h *InternalHeaderReader) InternalReset(r io.Reader, disarm InternalDeadlineDisarmer) {
	h.r = r
	h.disarm = disarm
	h.InternalN = 0
}

func (h *InternalHeaderReader) Read(p []byte) (int, error) {
	if h.disarm != nil && len(p) > 1 {

		p = p[:1]
	}
	n, err := h.r.Read(p)
	h.InternalN += n
	if n > 0 && h.disarm != nil {

		h.disarm.InternalSetDisarm(false)
		h.disarm = nil
	}
	return n, err
}

// deadlineDisarmer is a reader whose read deadline can be disarmed for a read
// that is expected to wait indefinitely (the idle wait for the next frame or
// segment header). See connReader.armDeadline.
type InternalDeadlineDisarmer interface {
	InternalSetDisarm(bool)
}

// connReadSource is the reader Conn reads through: the part of net.Conn the
// receive path actually uses, plus read-timeout control and the deadline disarm.
//
// Unexported, deliberately. The disarm is not optional for the connection's own
// reader, so requiring it here turns "this reader silently loses the disarm"
// from a runtime surprise into a compile error, and an unexported method means
// only this package can implement it. That costs nothing: Conn.r is unexported
// and no exported API accepts or returns one.
type InternalConnReadSource interface {
	// Read reads data from the connection.
	Read(p []byte) (n int, err error)

	// Close closes the connection.
	Close() error

	// RemoteAddr returns the remote network address, if known.
	RemoteAddr() net.Addr

	// SetTimeout sets the timeout duration for reading data from the conn.
	SetTimeout(timeout time.Duration)

	// GetTimeout returns the timeout duration.
	GetTimeout() time.Duration

	InternalDeadlineDisarmer
}

// connReader implements connReadSource.
type InternalConnReader struct {
	deadlineMu   sync.Mutex
	pause        atomic.Bool
	InternalConn net.Conn
	// r is an explicit compatibility source used by legacy fixtures. Production
	// connections leave it nil so header reads cannot prefetch body bytes.
	InternalR      *bufio.Reader
	timeout        atomic.Int64
	InternalDisarm atomic.Bool
}

// maxReadAttempts bounds how many times Read re-arms the deadline and resumes a
// read that timed out while still making progress. See Read.
const InternalMaxReadAttempts = 5

// errArmReadDeadline marks a read that failed before it reached the socket,
// because the read deadline could not be armed. Nothing was consumed, which makes
// it fatal to a frame body read: the body is still queued on a connection that is
// otherwise healthy (see bodyReadDesyncedConn). It is a named error rather than a
// bare wrap because that classification cannot be inferred from the underlying
// error, which need not be a net.Error.
var InternalErrArmReadDeadline = errors.New("gocql: unable to arm the read deadline")

// Read fills p, resuming across read-deadline expiries for as long as the peer
// keeps delivering bytes.
//
// The retry exists because ReadTimeout is a per-read budget, not a transfer budget:
// a large frame body arriving over a slow link can need more than one budget while
// being perfectly healthy. Each attempt arms a fresh deadline and resumes at p[n:],
// so no bytes are dropped.
//
// It is gated on forward progress, which is what keeps ReadTimeout meaningful as
// "identify faulty connections early and drop it" (see clusterSettings.ReadTimeout):
// an attempt that delivered nothing means the peer has stopped, and that fails
// immediately in a single ReadTimeout rather than after maxReadAttempts of them.
// Only a timeout is resumable — for any other network error the stream's position
// is no longer trustworthy, so resuming could silently mis-frame.
//
// Note the caller can still observe a partial read: on failure n bytes of p were
// consumed off the connection and cannot be put back, so a frame reader that gets
// an error here has to treat the connection as desynced (see processFrameSource).
func (c *InternalConnReader) Read(p []byte) (n int, err error) {
	for attempt := 0; attempt < InternalMaxReadAttempts; attempt++ {
		if aerr := c.armDeadline(); aerr != nil {

			return n, fmt.Errorf("%w: %w", InternalErrArmReadDeadline, aerr)
		}

		var source io.Reader = c.InternalConn
		if c.InternalR != nil {
			source = c.InternalR
		}
		nn, rerr := io.ReadFull(source, p[n:])
		n += nn
		if rerr == nil {
			return n, nil
		}
		err = rerr

		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {

			break
		}
		if nn == 0 {

			break
		}
	}

	return n, err
}

// armDeadline arms (or clears) the underlying read deadline for the read attempt
// that is about to start — once per attempt, so a Read that resumes after a timeout
// gets a fresh deadline, and so a concurrent finalizeConnection switching the
// reader from ConnectTimeout to ReadTimeout is picked up by the next attempt.
//
// The setter error is returned rather than dropped: a net.Conn
// whose SetReadDeadline fails would otherwise be read with no deadline at all,
// or with a stale one from an earlier read. This mirrors the write path, where
// deadlineContextWriter.writeContext and writeCoalescer.flush both report
// SetWriteDeadline failures instead of writing anyway.
func (c *InternalConnReader) armDeadline() error {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	if c.InternalConn == nil {
		return nil
	}
	if c.InternalDisarm.Load() {
		if c.pause.Load() {
			return c.InternalConn.SetReadDeadline(time.Now())
		}

		return c.InternalConn.SetReadDeadline(time.Time{})
	}
	if timeout := c.GetTimeout(); timeout > 0 {
		return c.InternalConn.SetReadDeadline(time.Now().Add(timeout))
	}

	return c.InternalConn.SetReadDeadline(time.Time{})
}

// setDisarm enables or disables the read-deadline disarm used around the
// frame/segment header read (see Read). It deliberately leaves the operational
// timeout value untouched so it can be toggled without racing a concurrent
// finalizeConnection.
func (c *InternalConnReader) InternalSetDisarm(v bool) {
	c.deadlineMu.Lock()
	c.InternalDisarm.Store(v)
	c.deadlineMu.Unlock()
}

func (c *InternalConnReader) Close() error {
	return c.InternalConn.Close()
}

func (c *InternalConnReader) RemoteAddr() net.Addr {
	return c.InternalConn.RemoteAddr()
}

func (c *InternalConnReader) SetTimeout(timeout time.Duration) {
	c.timeout.Store(int64(timeout))
}

func (c *InternalConnReader) GetTimeout() time.Duration {
	return time.Duration(c.timeout.Load())
}

// contextWriter is like io.Writer, but takes context as well.
type InternalContextWriter interface {
	// writeContext writes p to the connection.
	//
	// If ctx is canceled before we start writing p (e.g. during waiting while another write is currently in progress),
	// p is not written and ctx.Err() is returned. Context is ignored after we start writing p (i.e. we don't interrupt
	// blocked writes that are in progress) so that we always either write the full frame or not write it at all.
	//
	// It returns the number of bytes written from p (0 <= n <= len(p)) and any error that caused the write to stop
	// early. writeContext must return a non-nil error if it returns n < len(p). writeContext must not modify the
	// data in p, even temporarily.
	InternalWriteContext(ctx context.Context, p []byte) (n int, err error)

	InternalSetWriteTimeout(timeout time.Duration)
}

type InternalDeadlineWriter interface {
	SetWriteDeadline(time.Time) error
	io.Writer
}

type InternalDeadlineContextWriter struct {
	InternalW InternalDeadlineWriter
	// semaphore protects critical section for SetWriteDeadline/Write.
	// It is a channel with capacity 1.
	InternalSemaphore chan struct{}
	// quit closed once the connection is closed.
	InternalQuit    chan struct{}
	InternalTimeout atomic.Int64
	// spanScratch is protected by InternalSemaphore. Only cleared descriptors
	// are retained between writes, never request payloads.
	spanScratch net.Buffers
}

func (c *InternalDeadlineContextWriter) InternalSetWriteTimeout(timeout time.Duration) {
	c.InternalTimeout.Store(int64(timeout))
}

// writeContext implements contextWriter.
func (c *InternalDeadlineContextWriter) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.InternalQuit:
		return 0, ErrConnectionClosed
	case c.InternalSemaphore <- struct{}{}:

	}

	defer func() {

		<-c.InternalSemaphore
	}()

	if err := c.InternalW.SetWriteDeadline(InternalEffectiveWriteDeadline(c.InternalTimeout.Load())); err != nil {
		return 0, err
	}

	return c.InternalW.Write(p)
}

func InternalNewWriteCoalescer(conn InternalDeadlineWriter, writeTimeout, coalesceDuration time.Duration,
	quit <-chan struct{}) *InternalWriteCoalescer {
	wc := &InternalWriteCoalescer{
		InternalWriteCh: make(chan InternalWriteRequest),
		InternalC:       conn,
		InternalQuit:    quit,
	}
	wc.InternalSetWriteTimeout(writeTimeout)
	go wc.writeFlusher(coalesceDuration)
	return wc
}

type InternalWriteCoalescer struct {
	InternalC                InternalDeadlineWriter
	InternalQuit             <-chan struct{}
	InternalWriteCh          chan InternalWriteRequest
	InternalTestEnqueuedHook func()
	InternalTestFlushedHook  func()
	timeout                  atomic.Int64
}

func (w *InternalWriteCoalescer) InternalSetWriteTimeout(timeout time.Duration) {
	w.timeout.Store(int64(timeout))
}

type InternalWriteRequest struct {
	// resultChan is a channel (with buffer size 1) where to send results of the write.
	resultChan chan<- InternalWriteResult
	// data to write.
	data  []byte
	chain wire.Chain
}

type InternalWriteResult struct {
	InternalErr error
	InternalN   int
	// A span request can be MaxInt32 body bytes plus its header on 32-bit.
	// Keep its completion independent of the io.Writer-sized legacy result.
	InternalWritten int64
}

// writeResultChanPool pools buffered channels used for write coalescer results.
// Each channel is used in a strict produce-once/consume-once pattern:
// the flusher goroutine sends exactly one writeResult, and writeContext
// reads exactly one. After reading, the channel is empty and safe to reuse.
var InternalWriteResultChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan InternalWriteResult, 1)
	},
}

// writeContext implements contextWriter.
func (w *InternalWriteCoalescer) InternalWriteContext(ctx context.Context, p []byte) (int, error) {
	resultChan := InternalWriteResultChanPool.Get().(chan InternalWriteResult)
	wr := InternalWriteRequest{
		resultChan: resultChan,
		data:       p,
	}

	select {
	case <-ctx.Done():
		InternalWriteResultChanPool.Put(resultChan)
		return 0, ctx.Err()
	case <-w.InternalQuit:
		InternalWriteResultChanPool.Put(resultChan)
		return 0, io.EOF
	case w.InternalWriteCh <- wr:

	}

	if w.InternalTestEnqueuedHook != nil {
		w.InternalTestEnqueuedHook()
	}

	result := <-resultChan
	InternalWriteResultChanPool.Put(resultChan)
	return result.InternalN, result.InternalErr
}

func (w *InternalWriteCoalescer) writeFlusher(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	if !timer.Stop() {
		<-timer.C
	}

	w.InternalWriteFlusherImpl(timer.C, func() { timer.Reset(interval) })
}

func (w *InternalWriteCoalescer) InternalWriteFlusherImpl(timerC <-chan time.Time, resetTimer func()) {
	running := false

	var buffers net.Buffers
	var resultChans []chan<- InternalWriteResult
	var sizes []int64

	for {
		select {
		case req := <-w.InternalWriteCh:
			if req.data != nil {
				buffers = append(buffers, req.data)
				sizes = append(sizes, int64(len(req.data)))
			} else {
				_ = req.chain.Each(func(s wire.Span) error { buffers = append(buffers, s.Bytes()); return nil })
				sizes = append(sizes, req.chain.Len())
			}
			resultChans = append(resultChans, req.resultChan)
			if !running {

				resetTimer()
				running = true
			}
		case <-w.InternalQuit:
			result := InternalWriteResult{
				InternalN:   0,
				InternalErr: io.EOF,
			}

			for _, resultChan := range resultChans {

				resultChan <- result
			}
			return
		case <-timerC:
			running = false
			w.InternalFlushSizes(resultChans, buffers, sizes)
			buffers = nil
			resultChans = nil
			sizes = nil
			if w.InternalTestFlushedHook != nil {
				w.InternalTestFlushedHook()
			}
		}
	}
}

func (w *InternalWriteCoalescer) flush(resultChans []chan<- InternalWriteResult, buffers net.Buffers) {
	sizes := make([]int64, len(buffers))
	for i, b := range buffers {
		sizes[i] = int64(len(b))
	}
	w.InternalFlushSizes(resultChans, buffers, sizes)
}

func (w *InternalWriteCoalescer) InternalFlushSizes(resultChans []chan<- InternalWriteResult, buffers net.Buffers, sizes []int64) {
	if err := w.InternalC.SetWriteDeadline(InternalEffectiveWriteDeadline(w.timeout.Load())); err != nil {
		for _, ch := range resultChans {
			ch <- InternalWriteResult{InternalErr: err}
		}
		return
	}

	scratch := append(net.Buffers(nil), buffers...)
	n, err := InternalWriteSpanBuffers(w.InternalC, scratch)
	var total int64
	for _, size := range sizes {
		total += size
	}
	if err == nil && n != total {
		err = io.ErrShortWrite
	}
	for i, size := range sizes {
		completed := min(n, size)
		if completed < 0 {
			completed = 0
		}
		resultErr := err
		if completed == size {
			resultErr = nil
		}
		resultChans[i] <- InternalWriteResult{InternalN: int(min(completed, int64(^uint(0)>>1))), InternalWritten: completed, InternalErr: resultErr}
		n -= completed
	}
}

// Socket deadlines are absolute. Disabling the operational timeout must clear
// the startup deadline, which may otherwise expire during a long-lived session.
func InternalEffectiveWriteDeadline(timeout int64) time.Time {
	if timeout > 0 {
		return time.Now().Add(time.Duration(timeout))
	}
	return time.Time{}
}
