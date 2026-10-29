/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2TransportDefaultsAndValidation(t *testing.T) {
	c := DefaultConfig("localhost")
	if c.ConnectTimeout != 5*time.Second || c.DNSTimeout != 5*time.Second || c.ReadTimeout != 0 || c.WriteTimeout != 0 || c.SocketKeepalive != 0 || c.CoalesceWait != 0 || c.KeepaliveInterval != 30*time.Second || c.KeepaliveTimeout != 30*time.Second || c.Pool.SourcePortStart != 49152 || c.Pool.SourcePortEnd != 65535 {
		t.Fatal("transport defaults", c)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Pool.ConnectionsPerShard = 0 }, func(c *Config) { c.Pool.MaxRequestsPerConnection = 0 }, func(c *Config) { c.Pool.MaxOrphanedStreams = -1 }, func(c *Config) { c.Pool.SourcePortStart = 0 }, func(c *Config) { c.Pool.SourcePortStart = 65535; c.Pool.SourcePortEnd = 49152 }, func(c *Config) { c.Linger = Option[time.Duration]{Value: time.Millisecond, Set: true} }} {
		invalid := DefaultConfig("localhost")
		change(&invalid)
		if err := validateConfig(invalid); err == nil {
			t.Fatal("invalid transport setting accepted")
		}
	}
	for _, capacity := range []int{1, 2, 63, 64, 65, 32768} {
		gen := streams.NewLimited(capacity)
		seen := make(map[int]bool)
		for i := 0; i < capacity; i++ {
			id, ok := gen.GetStream()
			if !ok || id < 0 || id >= capacity || seen[id] {
				t.Fatal("inexact capacity", capacity, id, ok)
			}
			seen[id] = true
		}
		if _, ok := gen.GetStream(); ok || gen.Available() != 0 || !seen[0] {
			t.Fatal("capacity/stream zero", capacity)
		}
	}
}
func step04TLS(t testing.TB) (tls.Certificate, *tls.Config) {
	t.Helper()
	key := codecTestMust(ecdsa.GenerateKey(elliptic.P256(), rand.Reader))
	template := &x509.Certificate{SerialNumber: big.NewInt(42), DNSNames: []string{"transport.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der := codecTestMust(x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key))
	leaf := codecTestMust(x509.ParseCertificate(der))
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf, OCSPStaple: []byte("ocsp"), SignedCertificateTimestamps: [][]byte{[]byte("sct")}}
	return cert, &tls.Config{RootCAs: pool, ServerName: "transport.test", MinVersion: tls.VersionTLS13}
}
func TestV2TLSConfigurationDetachAndHookContext(t *testing.T) {
	cert, tlsConfig := step04TLS(t)
	tlsConfig.Certificates = []tls.Certificate{cert}
	tlsConfig.NextProtos = []string{"cql"}
	tlsConfig.ClientCAs = tlsConfig.RootCAs
	cloned := cloneV2TLSConfig(tlsConfig)
	originalDER := append([]byte(nil), cloned.Certificates[0].Certificate[0]...)
	tlsConfig.Certificates[0].Certificate[0][0] ^= 1
	tlsConfig.Certificates[0].OCSPStaple[0] = 'X'
	tlsConfig.Certificates[0].SignedCertificateTimestamps[0][0] = 'X'
	tlsConfig.NextProtos[0] = "mutated"
	if !bytes.Equal(cloned.Certificates[0].Certificate[0], originalDER) || string(cloned.Certificates[0].OCSPStaple) != "ocsp" || string(cloned.Certificates[0].SignedCertificateTimestamps[0]) != "sct" || cloned.NextProtos[0] != "cql" || cloned.RootCAs == tlsConfig.RootCAs || cloned.ClientCAs == tlsConfig.ClientCAs {
		t.Fatal("TLS mutable containers alias")
	}
	config := DefaultConfig("localhost")
	config.TLS = cloned
	config.TLSForHost = func(ctx context.Context, host HostSnapshot) (*tls.Config, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(host.Tokens) > 0 {
			host.Tokens[0] = 999
		}
		return nil, nil
	}
	d := v2TLSHostDialer{config: config}
	snapshot := HostSnapshot{Tokens: []int64{7}}
	inherited, err := d.selectedTLS(context.Background(), snapshot)
	if err != nil || inherited.ServerName != "transport.test" || snapshot.Tokens[0] != 7 {
		t.Fatal("inheritance/host isolation", err)
	}
	config.TLS = nil
	d.config = config
	if _, err = d.selectedTLS(context.Background(), snapshot); err == nil {
		t.Fatal("TLS hook silently disabled TLS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = d.selectedTLS(ctx, snapshot); !errors.Is(err, context.Canceled) {
		t.Fatal("hook context", err)
	}
}
func step04Host(t *testing.T, address net.Addr) *HostInfo {
	t.Helper()
	host, port := codecTestMust2(net.SplitHostPort(address.String()))
	_ = port
	addr := address.(*net.TCPAddr)
	h := &HostInfo{connectAddress: net.ParseIP(host), port: addr.Port, hostId: UUID{15: 1}}
	h.setTranslatedConnectionInfo(translatedAddresses{CQL: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAware: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAwareTLS: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}})
	return h
}
func codecTestMust2(a, b string, err error) (string, string) {
	if err != nil {
		panic(err)
	}
	return a, b
}
func TestV2TLSShardDialUsesSelectedSourceAndHook(t *testing.T) {
	cert, tlsConfig := step04TLS(t)
	listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
	defer listener.Close()
	host := step04Host(t, listener.Addr())
	accepted := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			accepted <- err
			return
		}
		defer raw.Close()
		accepted <- tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}}).Handshake()
	}()
	config := DefaultConfig("localhost")
	config.TLSForHost = func(ctx context.Context, s HostSnapshot) (*tls.Config, error) { return tlsConfig, nil }
	var selected DialTarget
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		selected = target
		return (DefaultDialer{}).DialContext(ctx, target)
	}
	d := v2TLSHostDialer{config: config}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := d.DialShard(ctx, host, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Conn.Close()
	if err = <-accepted; err != nil {
		t.Fatal(err)
	}
	if !selected.ShardID.Set || selected.ShardID.Value != 1 || !selected.LocalAddress.IsValid() || selected.LocalAddress.Port()%2 != 1 || selected.LocalAddress.Port() < config.Pool.SourcePortStart || !conn.shardTarget.Set {
		t.Fatal("shard target/source lost", selected)
	}
	// A changed TLS target is detected before publication, without holding locks.
	tlsConfig.ServerName = "changed.test"
	if err = conn.validateTarget(ctx); err == nil {
		t.Fatal("late TLS target accepted")
	}
}
func TestV2SourcePortExhaustionAndNATFallback(t *testing.T) {
	host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042, hostId: UUID{15: 1}}
	host.setTranslatedConnectionInfo(translatedAddresses{CQL: AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 9042}, ShardAware: AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 19042}})
	config := DefaultConfig("localhost")
	config.Pool.SourcePortStart = 49152
	config.Pool.SourcePortEnd = 49155
	var targeted, ordinary int
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		if target.ShardID.Set {
			targeted++
			return nil, ErrScyllaSourcePortAlreadyInUse
		}
		ordinary++
		a, b := net.Pipe()
		b.Close()
		return a, nil
	}
	result, err := (&v2TLSHostDialer{config: config}).DialShard(context.Background(), host, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	result.Conn.Close()
	if targeted != 2 || ordinary != 1 || result.shardTarget.Set {
		t.Fatal("range exhaustion fallback", targeted, ordinary)
	}
	c := &Conn{scyllaSupported: ScyllaConnectionFeatures{ScyllaHostFeatures: ScyllaHostFeatures{nrShards: 2}, shard: 0}, requestedShard: Option[uint32]{Value: 1, Set: true}, streams: streams.New()}
	p := codecTestMust(newTypedConnPicker(c, config.Pool))
	if p.Put(c) == nil {
		t.Fatal("NAT wrong shard accepted")
	}
	if _, shards := p.NextShard(); shards != 0 {
		t.Fatal("NAT ordinary-port cooldown absent")
	}
}
func TestV2PoolExcessAndLateSharderFence(t *testing.T) {
	makeConn := func(shard, shards int) *Conn {
		return &Conn{scyllaSupported: ScyllaConnectionFeatures{ScyllaHostFeatures: ScyllaHostFeatures{nrShards: shards}, shard: shard}, streams: streams.New()}
	}
	opts := DefaultConfig("localhost").Pool
	opts.MaxExcessPerShard = 1
	c := makeConn(0, 3)
	p := codecTestMust(newTypedConnPicker(c, opts))
	if p.Put(c) != nil || p.Put(makeConn(0, 3)) != nil {
		t.Fatal("initial/exact excess rejected")
	}
	if p.Put(makeConn(0, 3)) == nil {
		t.Fatal("one shard exceeded excess cap")
	}
	opts.MaxExcessPerShard = 0
	p = codecTestMust(newTypedConnPicker(c, opts))
	p.Put(c)
	if p.Put(makeConn(0, 3)) == nil || p.GetExcessConnectionCount() != 0 {
		t.Fatal("zero excess ignored")
	}
	generation := p.openingGeneration()
	if err := p.put(makeConn(0, 2), generation, true); err != nil {
		t.Fatal(err)
	}
	if err := p.put(makeConn(1, 3), generation, true); err == nil || p.GetShardCount() != 2 {
		t.Fatal("late old sharder reinstalled")
	}
}
func TestV2UnixTargetAndUnsupportedSettings(t *testing.T) {
	config := DefaultConfig("unix:" + t.TempDir() + "/maintenance")
	config.Protocol = 3
	config.Metadata.InitialDiscovery = false
	targetSeen := false
	config.Dialer = DialerFunc(func(ctx context.Context, target DialTarget) (net.Conn, error) {
		targetSeen = true
		if target.Network != "unix" || target.Address != config.ContactPoints[0][5:] || target.Host.Address.IsValid() || target.LocalAddress.IsValid() || target.ShardID.Set {
			t.Fatal("Unix target leaked TCP placeholder", target)
		}
		a, b := net.Pipe()
		b.Close()
		return a, nil
	})
	h := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
	c, err := (&v2TLSHostDialer{config: config}).DialHost(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	c.Conn.Close()
	if !targetSeen {
		t.Fatal("dialer bypassed")
	}
	for _, change := range []func(*Config){func(c *Config) { c.Protocol = 4 }, func(c *Config) { c.TLS = &tls.Config{} }, func(c *Config) { c.LocalAddress = netip.MustParseAddr("127.0.0.1") }} {
		invalid := config
		change(&invalid)
		var unsupported *UnsupportedSettingError
		if err := validateConfig(invalid); !errors.As(err, &unsupported) {
			t.Fatal("Unix unsupported", err)
		}
	}
}
func TestV2TypedKeepaliveDisabledAndBlockedWriteTimeout(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	config := DefaultConfig("localhost")
	config.KeepaliveInterval = 0
	c.cfg = &ConnConfig{TypedConfig: &config}
	done := make(chan struct{})
	go func() { c.heartBeat(c.ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled heartbeat active")
	}
	server.Close()
	c, _ = typedTestConn(t, 4, nil)
	config.KeepaliveInterval = time.Millisecond
	config.KeepaliveTimeout = 10 * time.Millisecond
	c.cfg = &ConnConfig{TypedConfig: &config}
	done = make(chan struct{})
	go func() { c.heartBeat(c.ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat timeout did not cover blocked write")
	}
	if !c.Closed() {
		t.Fatal("timed out writer left connection alive")
	}
}
func TestV2EventMetadataBudgetAndTerminalAdmission(t *testing.T) {
	c, _ := typedTestConn(t, 4, nil)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	consumer := &eventDebouncer{timer: timer, logger: nopLogger{}}
	consumer.mu.Lock()
	session := &Session{ctx: c.ctx, logger: nopLogger{}, nodeEvents: consumer}
	c.session = session
	budget := &streamMetadataBudget{limit: 800}
	if err := budget.reserve(400); err != nil {
		t.Fatal(err)
	}
	e := newTypedEnvelope()
	e.str("STATUS_CHANGE", false)
	e.str("DOWN", false)
	e.u8(4)
	e.chain(typedTestChain(t, []byte{127, 0, 0, 2}))
	e.u32(9042)
	body := typedTestBytes(t, codecTestMust(e.freeze()))
	queued := 0
	for i := 0; i < 20; i++ {
		scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(body), int64(len(body)), wire.StreamLimits{}))
		cursor := &streamMetadataCursor{scope: scope, budget: budget}
		p := &typedParser{cursor: cursor, proto: 4}
		event := p.streamEvent(frm.FrameHeader{Version: 0x84, Stream: -1, Op: frm.OpEvent})
		if p.err != nil {
			cursor.release()
			break
		}
		if err := scope.RequireEnd(); err != nil {
			t.Fatal(err)
		}
		if err := c.dispatchEventOwned(event, cursor.release); err != nil {
			t.Fatal(err)
		}
		queued++
	}
	if queued == 0 || queued > 3 || budget.bytes() > 800 {
		t.Fatal("queued events escaped simultaneous budget", queued, budget.bytes())
	}
	c.cancel()
	consumer.mu.Unlock()
	c.waitEventDispatch()
	budget.release(400)
	if budget.bytes() != 0 {
		t.Fatal("close did not release queued charges", budget.bytes())
	}
	var released atomic.Int32
	if err := c.dispatchEventOwned(&frm.ClientRoutesChanged{}, func() { released.Add(1) }); !errors.Is(err, ErrClosed) || released.Load() != 1 {
		t.Fatal("terminal enqueue retained ownership", err, released.Load())
	}
	// Event semantic consumption rejects bytes after a fully parsed event.
	trailing := append(append([]byte(nil), body...), 99)
	scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(trailing), int64(len(trailing)), wire.StreamLimits{}))
	cursor := &streamMetadataCursor{scope: scope, budget: &streamMetadataBudget{limit: 4096}}
	p := &typedParser{cursor: cursor, proto: 4}
	p.streamEvent(frm.FrameHeader{})
	if p.err != nil || scope.RequireEnd() == nil {
		t.Fatal("trailing event bytes accepted", p.err)
	}
	cursor.release()
}

type step04DNS func(string) ([]net.IP, error)

func (f step04DNS) LookupIP(name string) ([]net.IP, error) { return f(name) }
func TestV2ConnectionSetupDeadlineIncludesDNSAndTranslation(t *testing.T) {
	config := DefaultConfig("setup.test")
	config.ConnectTimeout = 10 * time.Second
	config.DNSTimeout = 20 * time.Second
	resolver := &v2DNSResolver{base: step04DNS(func(string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	}), timeout: config.DNSTimeout, setupTimeout: config.ConnectTimeout}
	resolver.startup.Store(&contextBox{ctx: context.Background()})
	defer resolver.clearStartup()
	hosts := codecTestMust(resolveInitialEndpoint(resolver, "setup.test:9042", 9042))
	host := hosts[0]
	expected := host.initialSetupDeadline
	var observed time.Time
	entered := make(chan struct{})
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		observed, _ = ctx.Deadline()
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	session := &Session{logger: nopLogger{}, addressTranslator: AddressTranslatorFunc(func(ip net.IP, port int) (net.IP, int) { time.Sleep(20 * time.Millisecond); return ip, port })}
	session.cfg.DNSResolver = resolver
	cfg := &ConnConfig{ProtoVersion: 4, ConnectTimeout: config.ConnectTimeout, HostDialer: &v2TLSHostDialer{config: config}, TypedConfig: &config}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := session.dialShard(ctx, host, cfg, connErrorHandlerFn(func(*Conn, error, bool) {}), 0, 0)
		done <- err
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatal("setup ended before staged hook", err)
	case <-time.After(time.Second):
		t.Fatal("staged hook not entered")
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) || !observed.Equal(expected) {
		t.Fatal("setup deadline restarted after DNS/translation", observed, expected, err)
	}

	if !host.claimSetupDeadline().IsZero() {
		t.Fatal("warm connection inherited consumed setup deadline")
	}

}
func TestV2OpeningValidationKeepsOriginalDeadlineAndZeroMeaning(t *testing.T) {
	expected := time.Now().Add(10 * time.Second)
	entered := make(chan struct{})
	var observed time.Time
	c := &Conn{openingDeadline: expected, validateTarget: func(ctx context.Context) error {
		observed, _ = ctx.Deadline()
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.validateOpeningTarget(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !observed.Equal(expected) {
		t.Fatal("validation got new setup budget", observed, expected, err)
	}
	c.openingDeadline = time.Now().Add(-time.Second)
	c.validateTarget = func(context.Context) error { t.Fatal("expired setup reached publication hook"); return nil }
	if err := c.validateOpeningTarget(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired original deadline ignored", err)
	}
	entered = make(chan struct{})
	c.openingDeadline = time.Time{}
	c.validateTarget = func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); ok {
			t.Error("zero local timeout added deadline")
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel = context.WithCancel(context.Background())
	go func() { done <- c.validateOpeningTarget(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("caller cancellation lost", err)
	}
}

func TestV2UnsupportedSocketSettingsKeepTypedError(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	config := DefaultConfig("localhost")
	config.ReceiveBuffer = Option[int]{Value: 4096, Set: true}
	var unsupported *UnsupportedSettingError
	if err := applyV2SocketOptions(a, config, "tcp"); !errors.As(err, &unsupported) {
		t.Fatal("unsupported backend setting", err)
	}
}

type step04Reconnect struct {
	calls chan int
	again bool
	pool  *hostConnPool
}

func (p *step04Reconnect) Delay(attempt int) (time.Duration, bool) {
	if p.pool != nil {
		p.pool.transportState()
	}
	p.calls <- attempt
	return 5 * time.Millisecond, p.again
}
func TestV2PoolRepairPolicyAndClosedState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := DefaultConfig("localhost")
	policy := &step04Reconnect{calls: make(chan int, 4), again: true}
	config.Reconnect = policy
	s := &Session{ctx: ctx, logger: nopLogger{}, connCfg: &ConnConfig{TypedConfig: &config}}
	pool := newHostConnPool(s, &HostInfo{}, 1, "")
	policy.pool = pool
	// A full empty test picker avoids network work but leaves the actual repair
	// scheduling/backoff state machine active.
	pool.connPicker = &defaultConnPicker{size: 0}
	pool.scheduleTypedRepair()
	if attempt := <-policy.calls; attempt != 0 {
		t.Fatal(attempt)
	}
	deadline := time.After(time.Second)
	for {
		pool.mu.RLock()
		pending := pool.repairScheduled
		pool.mu.RUnlock()
		if !pending {
			break
		}
		select {
		case <-deadline:
			t.Fatal("repair timer did not finish")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	policy.again = false
	pool.scheduleTypedRepair()
	if attempt := <-policy.calls; attempt != 1 {
		t.Fatal("repair attempt not advanced", attempt)
	}
	pool.Close()
	if pool.transportState() != poolClosed {
		t.Fatal("pool not terminal")
	}
	pool.scheduleTypedRepair()
	select {
	case <-policy.calls:
		t.Fatal("closed pool scheduled repair")
	default:
	}
}

func TestV2ResolvedContactsOwnIndependentSetupDeadlines(t *testing.T) {
	resolver := &v2DNSResolver{base: step04DNS(func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("127.0.0.1")}, nil }), setupTimeout: time.Minute}
	first := codecTestMust(resolveInitialEndpoint(resolver, "first.test:9042", 9042))[0]
	second := codecTestMust(resolveInitialEndpoint(resolver, "second.test:9142", 9042))[0]
	// Distinct sentinel deadlines make identity isolation deterministic even when
	// both lookups complete in one clock tick and resolve to the same IP.
	first.initialSetupDeadline = time.Unix(100, 1)
	second.initialSetupDeadline = time.Unix(200, 2)
	if d := first.claimSetupDeadline(); !d.Equal(time.Unix(100, 1)) {
		t.Fatal("first opening stole second deadline", d)
	}
	if d := first.claimSetupDeadline(); !d.IsZero() {
		t.Fatal("opening reused spent deadline", d)
	}
	if d := second.claimSetupDeadline(); !d.Equal(time.Unix(200, 2)) {
		t.Fatal("second contact deadline lost", d)
	}
	if first.hostname == second.hostname || first.Port() == second.Port() {
		t.Fatal("contact identity merged prematurely")
	}
}

type step04LongRepairPolicy struct {
	calls atomic.Int32
	again bool
}

func (p *step04LongRepairPolicy) Delay(int) (time.Duration, bool) {
	p.calls.Add(1)
	return time.Hour, p.again
}

type step04CountingHostDialer struct{ calls *atomic.Int32 }

func (d step04CountingHostDialer) DialHost(context.Context, *HostInfo) (*DialedHost, error) {
	d.calls.Add(1)
	return nil, io.EOF
}
func TestV2TypedRepairTriggersHonorPolicyDelayAndStop(t *testing.T) {
	for _, again := range []bool{true, false} {
		t.Run(fmt.Sprint(again), func(t *testing.T) {
			c, _ := typedTestConn(t, 4, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			config := DefaultConfig("localhost")
			config.Pool.ConnectionsPerHost = 1
			policy := &step04LongRepairPolicy{again: again}
			config.Reconnect = policy
			var dials atomic.Int32
			s := &Session{ctx: ctx, logger: nopLogger{}, connCfg: &ConnConfig{TypedConfig: &config, HostDialer: step04CountingHostDialer{&dials}}}
			s.cfg.ReconnectionPolicy = &NoReconnectionPolicy{}
			h := &HostInfo{hostId: UUID{15: 1}, state: NodeUp}
			c.host = h
			c.session = s
			pool := newHostConnPool(s, h, 1, "")
			defer pool.Close()
			pool.started = true
			pool.state = poolReady
			pool.connPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
			pool.connPicker.Put(c)
			s.hostSource = &ringDescriber{hosts: map[string]*HostInfo{h.HostID(): h}}
			s.pool = &policyConnPool{hostConnPools: map[UUID]*hostConnPool{h.hostUUID(): pool}}
			pool.HandleError(c, io.EOF, true)
			for i := 0; i < 10; i++ {
				pool.Pick(nil, nil)
				s.pickTypedPool(pool, Target{}, RoutingInfo{})
				pool.fill()
			}
			wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancelWait()
			if r, err := pool.reserveStream(wait, Target{HostID: h.hostUUID()}, RoutingInfo{}); err == nil {
				r.close(false)
				t.Fatal("broken pool manufactured reservation")
			}
			if dials.Load() != 0 || policy.calls.Load() != 1 {
				t.Fatal("refill trigger bypassed active delay/stop", again, dials.Load(), policy.calls.Load())
			}
			pool.Close()
			pool.HandleError(c, io.EOF, true)
			pool.fill()
			if dials.Load() != 0 || policy.calls.Load() != 1 {
				t.Fatal("closed pool restarted repair")
			}
		})
	}
}
func TestV2TypedSelectionWaitsForReservedCapacity(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	c := conns[0]
	pool := s.pool.hostConnPools[c.host.hostUUID()]
	config := DefaultConfig("localhost")
	config.Pool.ConnectionsPerHost = 1
	s.connCfg = &ConnConfig{TypedConfig: &config}
	c.cfg = &ConnConfig{TypedConfig: &config}
	pool.session = s
	pool.repairCtx, pool.repairCancel = context.WithCancel(c.ctx)
	defer pool.repairCancel()
	pool.started = true
	pool.state = poolReady
	pool.connPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
	pool.connPicker.Put(c)
	c.errorHandler = pool
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := codecTestMust(pool.reserveStream(ctx, Target{HostID: c.host.hostUUID()}, RoutingInfo{}))
	bound := runtimeQuery(t)
	defer bound.Close()
	done := make(chan error, 1)
	go func() {
		rows, err := Run(ctx, s, bound, Options{HostID: Option[UUID]{Value: c.host.hostUUID(), Set: true}})
		if rows != nil {
			rows.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatal("normal typed selection failed instead of waiting for reserved slot", err)
	case <-time.After(10 * time.Millisecond):
	}
	if n := pool.Size(); n != 1 {
		t.Fatal("waiting created extra pool slots", n)
	}
	go func() {
		stream := typedTestReadRequest(t, c, peers[0])
		step04WriteResponse(t, c, peers[0], stream, typedTestRowsBody([]byte("resumed")))
	}()
	if err := r.close(true); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("waiter failed after release", err)
		}
	case <-ctx.Done():
		t.Fatal("waiter did not resume")
	}
	r = codecTestMust(pool.reserveStream(ctx, Target{HostID: c.host.hostUUID()}, RoutingInfo{}))
	defer r.close(true)
	wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWait()
	_, err := Run(wait, s, bound, Options{HostID: Option[UUID]{Value: c.host.hostUUID(), Set: true}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting ignored caller cancellation", err)
	}
}

func TestV2OpeningEndpointGenerationAndClosedPoolFence(t *testing.T) {
	for _, mode := range []string{"endpoint", "closed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client, server := net.Pipe()
			defer server.Close()
			selected, release := make(chan struct{}), make(chan struct{})
			config := DefaultConfig("localhost")
			config.Pool.ConnectionsPerHost = 1
			config.KeepaliveInterval = 0
			config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
				close(selected)
				select {
				case <-release:
					return client, nil
				case <-ctx.Done():
					client.Close()
					return nil, ctx.Err()
				}
			}
			host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042, hostId: UUID{15: 1}}
			s := &Session{ctx: ctx, logger: nopLogger{}}
			s.cfg.ReconnectionPolicy = &NoReconnectionPolicy{}
			s.connCfg = &ConnConfig{TypedConfig: &config, ProtoVersion: 4, CQLVersion: "3.0.0", ConnectTimeout: time.Second, HostDialer: &v2TLSHostDialer{config: config}, Logger: nopLogger{}}
			pool := newHostConnPool(s, host, 1, "")
			defer pool.Close()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				for {
					var h [9]byte
					if _, err := io.ReadFull(server, h[:]); err != nil {
						return
					}
					if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(h[5:]))); err != nil {
						return
					}
					body := []byte{}
					op := frm.OpReady
					if h[4] == byte(frm.OpOptions) {
						op = frm.OpSupported
						body = []byte{0, 0}
					}
					response := []byte{0x84, 0, h[2], h[3], byte(op)}
					response = binary.BigEndian.AppendUint32(response, uint32(len(body)))
					response = append(response, body...)
					if _, err := server.Write(response); err != nil {
						return
					}
				}
			}()
			done := make(chan error, 1)
			go func() { done <- pool.connect() }()
			<-selected
			if mode == "endpoint" {
				host.setTranslatedConnectionInfo(translatedAddresses{CQL: AddressPort{Address: net.ParseIP("127.0.0.2"), Port: 9042}})
				close(release)
			} else {
				pool.Close()
			}
			select {
			case err := <-done:
				if err == nil && mode == "endpoint" {
					t.Fatal("late endpoint opening accepted")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("opening did not stop")
			}
			if pool.Size() != 0 {
				t.Fatal("late opening entered pool")
			}
			if mode == "closed" && pool.transportState() != poolClosed {
				t.Fatal("closed pool resurrected")
			}
			client.Close()
			<-serverDone
		})
	}
}

func TestV2ReservedPreferredShardUsesAllowedFreeAlternative(t *testing.T) {
	makeConn := func(shard int) *Conn {
		return &Conn{scyllaSupported: ScyllaConnectionFeatures{ScyllaHostFeatures: ScyllaHostFeatures{nrShards: 2}, shard: shard}, streams: streams.New()}
	}
	first, second := makeConn(0), makeConn(1)
	p := codecTestMust(newTypedConnPicker(first, DefaultConfig("localhost").Pool))
	p.Put(first)
	p.Put(second)
	first.exclusiveFlag.Store(true)
	routing := RoutingInfo{Token: Option[int64]{Value: math.MinInt64, Set: true}}
	if got := p.pickTarget(Target{}, routing); got != second {
		t.Fatal("non-strict selection waited despite idle eligible connection")
	}
	if got := p.pickTarget(Target{ShardID: Option[uint32]{Value: 0, Set: true}}, routing); got != first {
		t.Fatal("strict shard was not preserved")
	}
}

func TestV2TLSIdentityUsesLogicalHostAcrossTranslatedEndpoint(t *testing.T) {
	cert, configTLS := step04TLS(t)
	configTLS.ServerName = ""
	var name string
	configTLS.VerifyConnection = func(state tls.ConnectionState) error { name = state.ServerName; return nil }
	listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
	defer listener.Close()
	host := step04Host(t, listener.Addr())
	host.hostname = "transport.test"
	host.connectAddress = net.ParseIP("192.0.2.12")
	done := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer raw.Close()
		done <- tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}}).Handshake()
	}()
	config := DefaultConfig("transport.test")
	config.TLS = configTLS
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, err := (&v2TLSHostDialer{config: config}).DialHost(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Conn.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if name != "transport.test" {
		t.Fatal("proxy/translated address replaced logical TLS identity", name)
	}
}
