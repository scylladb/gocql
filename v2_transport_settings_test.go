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
	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
)

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
	d := v2TLSHostDialer{InternalConfig: connectivityConfig(config)}
	snapshot := HostSnapshot{Tokens: []int64{7}}
	inherited, err := d.InternalSelectedTLS(context.Background(), snapshot)
	if err != nil || inherited.ServerName != "transport.test" || snapshot.Tokens[0] != 7 {
		t.Fatal("inheritance/host isolation", err)
	}
	config.TLS = nil
	d.InternalConfig = connectivityConfig(config)
	if _, err = d.InternalSelectedTLS(context.Background(), snapshot); err == nil {
		t.Fatal("TLS hook silently disabled TLS")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = d.InternalSelectedTLS(ctx, snapshot); !errors.Is(err, context.Canceled) {
		t.Fatal("hook context", err)
	}
}
func step04Host(t *testing.T, address net.Addr) *HostInfo {
	t.Helper()
	host, port := codecTestMust2(net.SplitHostPort(address.String()))
	_ = port
	addr := address.(*net.TCPAddr)
	h := func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalConnectAddress = net.ParseIP(host)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalPort = addr.Port
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = UUID{15: 1}
		return opaqueStorageValue
	}()
	implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(h, translatedAddresses{CQL: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAware: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}, ShardAwareTLS: AddressPort{Address: net.ParseIP(host), Port: uint16(addr.Port)}})
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
	d := v2TLSHostDialer{InternalConfig: connectivityConfig(config)}
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
	if !selected.ShardID.Set || selected.ShardID.Value != 1 || !selected.LocalAddress.IsValid() || selected.LocalAddress.Port()%2 != 1 || selected.LocalAddress.Port() < config.Pool.SourcePortStart || !implEndpoint.DialedHostData(conn).InternalShardTarget.Set {
		t.Fatal("shard target/source lost", selected)
	}
	// A changed TLS target is detected before publication, without holding locks.
	tlsConfig.ServerName = "changed.test"
	if err = implEndpoint.DialedHostData(conn).InternalValidateTarget(ctx); err == nil {
		t.Fatal("late TLS target accepted")
	}
}
func TestV2SourcePortExhaustionAndNATFallback(t *testing.T) {
	host := func() *HostInfo {
		opaqueStorageValuex := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = net.ParseIP("127.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalPort = 9042
		implEndpoint.HostInfoData(opaqueStorageValuex).InternalHostId = UUID{15: 1}
		return opaqueStorageValuex
	}()
	implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, translatedAddresses{CQL: AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 9042}, ShardAware: AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 19042}})
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
	result, err := (&v2TLSHostDialer{InternalConfig: connectivityConfig(config)}).DialShard(context.Background(), host, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	result.Conn.Close()
	if targeted != 2 || ordinary != 1 || implEndpoint.DialedHostData(result).InternalShardTarget.Set {
		t.Fatal("range exhaustion fallback", targeted, ordinary)
	}
	c := &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
		opaqueStorageValuexxx := new(ScyllaConnectionFeatures)
		opaqueStorageValuexxx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
			opaqueStorageValuexx := new(ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalNrShards = 2
			return opaqueStorageValuexx
		}()
		implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxx).InternalShard = 0
		return opaqueStorageValuexxx
	}(), InternalRequestedShard: Option[uint32]{Value: 1, Set: true}, InternalStreams: streams.New()}
	p := codecTestMust(newTypedConnPicker(c, config.Pool))
	if p.Put(c) == nil {
		t.Fatal("NAT wrong shard accepted")
	}
	if _, shards := p.NextShard(); shards != 0 {
		t.Fatal("NAT ordinary-port cooldown absent")
	}
}
func TestV2PoolExcessAndLateSharderFence(t *testing.T) {
	makeConn := func(shard, shards int) *driverConn {
		return &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
			opaqueStorageValuexxxxx := new(ScyllaConnectionFeatures)
			opaqueStorageValuexxxxx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
				opaqueStorageValuexxxx := new(ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxx).InternalNrShards = shards
				return opaqueStorageValuexxxx
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxx).InternalShard = shard
			return opaqueStorageValuexxxxx
		}(), InternalStreams: streams.New()}
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
	generation := p.InternalOpeningGeneration()
	if err := p.InternalPut(makeConn(0, 2), generation, true); err != nil {
		t.Fatal(err)
	}
	if err := p.InternalPut(makeConn(1, 3), generation, true); err == nil || p.GetShardCount() != 2 {
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
	h := func() *HostInfo {
		opaqueStorageValuexxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalPort = 9042
		return opaqueStorageValuexxxxxx
	}()
	c, err := (&v2TLSHostDialer{InternalConfig: connectivityConfig(config)}).DialHost(context.Background(), h)
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
	c.InternalCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
	done := make(chan struct{})
	go func() { c.InternalHeartBeat(c.InternalCtx); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disabled heartbeat active")
	}
	server.Close()
	c, _ = typedTestConn(t, 4, nil)
	config.KeepaliveInterval = time.Millisecond
	config.KeepaliveTimeout = 10 * time.Millisecond
	c.InternalCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
	done = make(chan struct{})
	go func() { c.InternalHeartBeat(c.InternalCtx); close(done) }()
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
	consumer := &eventDebouncer{InternalTimer: timer, InternalLogger: nopLogger{}}
	consumer.InternalMu.Lock()
	session := func() *Session {
		opaqueStorageValuexxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalCtx = c.InternalCtx
		implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalLogger = nopLogger{}
		implCluster.SessionData(opaqueStorageValuexxxxxxx).InternalNodeEvents = consumer
		return opaqueStorageValuexxxxxxx
	}()
	c.InternalHooks = newConnectionHooks(session)
	budget := &streamMetadataBudget{Limit: 800}
	if err := budget.Reserve(400); err != nil {
		t.Fatal(err)
	}
	e := newTypedEnvelope()
	e.Str("STATUS_CHANGE", false)
	e.Str("DOWN", false)
	e.U8(4)
	e.Chain(typedTestChain(t, []byte{127, 0, 0, 2}))
	e.U32(9042)
	body := typedTestBytes(t, codecTestMust(e.Freeze()))
	queued := 0
	for i := 0; i < 20; i++ {
		scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(body), int64(len(body)), wire.StreamLimits{}))
		cursor := &streamMetadataCursor{Scope: scope, Budget: budget}
		p := &typedParser{Cursor: cursor, Proto: 4}
		event := p.StreamEvent(frm.FrameHeader{Version: 0x84, Stream: -1, Op: frm.OpEvent})
		if p.Err != nil {
			cursor.Release()
			break
		}
		if err := scope.RequireEnd(); err != nil {
			t.Fatal(err)
		}
		if err := c.InternalDispatchEventOwned(event, cursor.Release); err != nil {
			t.Fatal(err)
		}
		queued++
	}
	if queued == 0 || queued > 3 || budget.Bytes() > 800 {
		t.Fatal("queued events escaped simultaneous budget", queued, budget.Bytes())
	}
	c.InternalCancel()
	consumer.InternalMu.Unlock()
	c.InternalWaitEventDispatch()
	budget.Release(400)
	if budget.Bytes() != 0 {
		t.Fatal("close did not release queued charges", budget.Bytes())
	}
	var released atomic.Int32
	if err := c.InternalDispatchEventOwned(&frm.ClientRoutesChanged{}, func() { released.Add(1) }); !errors.Is(err, ErrClosed) || released.Load() != 1 {
		t.Fatal("terminal enqueue retained ownership", err, released.Load())
	}
	// Event semantic consumption rejects bytes after a fully parsed event.
	trailing := append(append([]byte(nil), body...), 99)
	scope := codecTestMust(wire.NewStream(context.Background(), bytes.NewReader(trailing), int64(len(trailing)), wire.StreamLimits{}))
	cursor := &streamMetadataCursor{Scope: scope, Budget: &streamMetadataBudget{Limit: 4096}}
	p := &typedParser{Cursor: cursor, Proto: 4}
	p.StreamEvent(frm.FrameHeader{})
	if p.Err != nil || scope.RequireEnd() == nil {
		t.Fatal("trailing event bytes accepted", p.Err)
	}
	cursor.Release()
}

type step04DNS func(string) ([]net.IP, error)

func (f step04DNS) LookupIP(name string) ([]net.IP, error) { return f(name) }
func TestV2ConnectionSetupDeadlineIncludesDNSAndTranslation(t *testing.T) {
	config := DefaultConfig("setup.test")
	config.ConnectTimeout = 10 * time.Second
	config.DNSTimeout = 20 * time.Second
	resolver := &v2DNSResolver{InternalBase: step04DNS(func(string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("127.0.0.1")}, nil
	}), InternalTimeout: config.DNSTimeout, InternalSetupTimeout: config.ConnectTimeout}
	resolver.InternalStartup.Store(&contextBox{InternalCtx: context.Background()})
	defer resolver.InternalClearStartup()
	hosts := codecTestMust(resolveInitialEndpoint(resolver, "setup.test:9042", 9042))
	host := hosts[0]
	expected := implEndpoint.HostInfoData(host).InternalInitialSetupDeadline
	var observed time.Time
	entered := make(chan struct{})
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		observed, _ = ctx.Deadline()
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	session := func() *Session {
		opaqueStorageValuexxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalLogger = nopLogger{}
		implCluster.SessionData(opaqueStorageValuexxxxxxxx).InternalAddressTranslator = AddressTranslatorFunc(func(ip net.IP, port int) (net.IP, int) { time.Sleep(20 * time.Millisecond); return ip, port })
		return opaqueStorageValuexxxxxxxx
	}()
	implCluster.SessionData(session).InternalCfg.DNSResolver = resolver
	cfg := &driverConnConfig{ProtoVersion: 4, ConnectTimeout: config.ConnectTimeout, HostDialer: &v2TLSHostDialer{InternalConfig: connectivityConfig(config)}, TypedConfig: connectivityConfigPointer(config)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := implCluster.InternalSessionDialShard(session, ctx, host, cfg, connErrorHandlerFn(func(*driverConn, error, bool) {}), 0, 0)
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

	if !implEndpoint.InternalHostInfoClaimSetupDeadline(host).IsZero() {
		t.Fatal("warm connection inherited consumed setup deadline")
	}

}
func TestV2OpeningValidationKeepsOriginalDeadlineAndZeroMeaning(t *testing.T) {
	expected := time.Now().Add(10 * time.Second)
	entered := make(chan struct{})
	var observed time.Time
	c := &driverConn{InternalOpeningDeadline: expected, InternalValidateTarget: func(ctx context.Context) error {
		observed, _ = ctx.Deadline()
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.InternalValidateOpeningTarget(ctx) }()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) || !observed.Equal(expected) {
		t.Fatal("validation got new setup budget", observed, expected, err)
	}
	c.InternalOpeningDeadline = time.Now().Add(-time.Second)
	c.InternalValidateTarget = func(context.Context) error { t.Fatal("expired setup reached publication hook"); return nil }
	if err := c.InternalValidateOpeningTarget(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("expired original deadline ignored", err)
	}
	entered = make(chan struct{})
	c.InternalOpeningDeadline = time.Time{}
	c.InternalValidateTarget = func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); ok {
			t.Error("zero local timeout added deadline")
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	ctx, cancel = context.WithCancel(context.Background())
	go func() { done <- c.InternalValidateOpeningTarget(ctx) }()
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
		p.pool.InternalTransportState()
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
	s := func() *Session {
		opaqueStorageValuexxxxxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalCtx = ctx
		implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalLogger = nopLogger{}
		implCluster.SessionData(opaqueStorageValuexxxxxxxxx).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
		return opaqueStorageValuexxxxxxxxx
	}()
	pool := newHostConnPool(s, func() *HostInfo {
		opaqueStorageValuexxxxxxxxxx := new(HostInfo)
		return opaqueStorageValuexxxxxxxxxx
	}(), 1, "")
	policy.pool = pool
	// A full empty test picker avoids network work but leaves the actual repair
	// scheduling/backoff state machine active.
	pool.InternalConnPicker = &defaultConnPicker{InternalSize: 0}
	pool.InternalScheduleTypedRepair()
	if attempt := <-policy.calls; attempt != 0 {
		t.Fatal(attempt)
	}
	deadline := time.After(time.Second)
	for {
		pool.InternalMu.RLock()
		pending := pool.InternalRepairScheduled
		pool.InternalMu.RUnlock()
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
	pool.InternalScheduleTypedRepair()
	if attempt := <-policy.calls; attempt != 1 {
		t.Fatal("repair attempt not advanced", attempt)
	}
	pool.Close()
	if pool.InternalTransportState() != poolClosed {
		t.Fatal("pool not terminal")
	}
	pool.InternalScheduleTypedRepair()
	select {
	case <-policy.calls:
		t.Fatal("closed pool scheduled repair")
	default:
	}
}

func TestV2ResolvedContactsOwnIndependentSetupDeadlines(t *testing.T) {
	resolver := &v2DNSResolver{InternalBase: step04DNS(func(string) ([]net.IP, error) { return []net.IP{net.ParseIP("127.0.0.1")}, nil }), InternalSetupTimeout: time.Minute}
	first := codecTestMust(resolveInitialEndpoint(resolver, "first.test:9042", 9042))[0]
	second := codecTestMust(resolveInitialEndpoint(resolver, "second.test:9142", 9042))[0]
	implEndpoint.
		// Distinct sentinel deadlines make identity isolation deterministic even when
		// both lookups complete in one clock tick and resolve to the same IP.
		HostInfoData(first).InternalInitialSetupDeadline = time.Unix(100, 1)
	implEndpoint.HostInfoData(second).InternalInitialSetupDeadline = time.Unix(200, 2)
	if d := implEndpoint.InternalHostInfoClaimSetupDeadline(first); !d.Equal(time.Unix(100, 1)) {
		t.Fatal("first opening stole second deadline", d)
	}
	if d := implEndpoint.InternalHostInfoClaimSetupDeadline(first); !d.IsZero() {
		t.Fatal("opening reused spent deadline", d)
	}
	if d := implEndpoint.InternalHostInfoClaimSetupDeadline(second); !d.Equal(time.Unix(200, 2)) {
		t.Fatal("second contact deadline lost", d)
	}
	if implEndpoint.HostInfoData(first).InternalHostname == implEndpoint.HostInfoData(second).InternalHostname || first.Port() == second.Port() {
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
			s := func() *Session {
				opaqueStorageValuexxxxxxxxxxx := new(Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalCtx = ctx
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalLogger = nopLogger{}
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxx).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config), HostDialer: step04CountingHostDialer{&dials}}
				return opaqueStorageValuexxxxxxxxxxx
			}()
			implCluster.SessionData(s).InternalCfg.ReconnectionPolicy = &NoReconnectionPolicy{}
			h := func() *HostInfo {
				opaqueStorageValuexxxxxxxxxxxx := new(HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalHostId = UUID{15: 1}
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalState = NodeUp
				return opaqueStorageValuexxxxxxxxxxxx
			}()
			c.InternalHost = h
			c.InternalHooks = newConnectionHooks(s)
			pool := newHostConnPool(s, h, 1, "")
			defer pool.Close()
			pool.InternalStarted = true
			pool.InternalState = poolReady
			pool.InternalConnPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
			pool.InternalConnPicker.Put(c)
			implCluster.SessionData(s).InternalHostSource = &ringDescriber{InternalHosts: map[string]*HostInfo{h.HostID(): h}}
			implCluster.SessionData(s).InternalPool = &policyConnPool{InternalHostConnPools: map[UUID]*hostConnPool{implEndpoint.InternalHostInfoHostUUID(h): pool}}
			pool.HandleError(c, io.EOF, true)
			for i := 0; i < 10; i++ {
				pool.Pick(nil)
				implCluster.InternalSessionPickTypedPool(s, pool, Target{}, RoutingInfo{})
				pool.InternalFill()
			}
			wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancelWait()
			if r, err := pool.InternalReserveStream(wait, Target{HostID: implEndpoint.InternalHostInfoHostUUID(h)}, RoutingInfo{}); err == nil {
				r.InternalClose(false)
				t.Fatal("broken pool manufactured reservation")
			}
			if dials.Load() != 0 || policy.calls.Load() != 1 {
				t.Fatal("refill trigger bypassed active delay/stop", again, dials.Load(), policy.calls.Load())
			}
			pool.Close()
			pool.HandleError(c, io.EOF, true)
			pool.InternalFill()
			if dials.Load() != 0 || policy.calls.Load() != 1 {
				t.Fatal("closed pool restarted repair")
			}
		})
	}
}
func TestV2TypedSelectionWaitsForReservedCapacity(t *testing.T) {
	s, conns, peers := runtimeTestSession(t, 1)
	c := conns[0]
	pool := implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)]
	config := DefaultConfig("localhost")
	config.Pool.ConnectionsPerHost = 1
	implCluster.SessionData(s).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
	c.InternalCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config)}
	pool.InternalServices = newPoolServices(s)
	pool.InternalRepairCtx, pool.InternalRepairCancel = context.WithCancel(c.InternalCtx)
	defer pool.InternalRepairCancel()
	pool.InternalStarted = true
	pool.InternalState = poolReady
	pool.InternalConnPicker = codecTestMust(newTypedConnPicker(c, config.Pool))
	pool.InternalConnPicker.Put(c)
	c.InternalErrorHandler = pool
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := codecTestMust(pool.InternalReserveStream(ctx, Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost)}, RoutingInfo{}))
	bound := runtimeQuery(t)
	defer bound.Close()
	done := make(chan error, 1)
	go func() {
		rows, err := Run(ctx, s, bound, Options{HostID: Option[UUID]{Value: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), Set: true}})
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
	if err := r.InternalClose(true); err != nil {
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
	r = codecTestMust(pool.InternalReserveStream(ctx, Target{HostID: implEndpoint.InternalHostInfoHostUUID(c.InternalHost)}, RoutingInfo{}))
	defer r.InternalClose(true)
	wait, cancelWait := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancelWait()
	_, err := Run(wait, s, bound, Options{HostID: Option[UUID]{Value: implEndpoint.InternalHostInfoHostUUID(c.InternalHost), Set: true}})
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
			host := func() *HostInfo {
				opaqueStorageValuexxxxxxxxxxxxx := new(HostInfo)
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalPort = 9042
				implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalHostId = UUID{15: 1}
				return opaqueStorageValuexxxxxxxxxxxxx
			}()
			s := func() *Session {
				opaqueStorageValuexxxxxxxxxxxxxx := new(Session)
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalCtx = ctx
				implCluster.SessionData(opaqueStorageValuexxxxxxxxxxxxxx).InternalLogger = nopLogger{}
				return opaqueStorageValuexxxxxxxxxxxxxx
			}()
			implCluster.SessionData(s).InternalCfg.ReconnectionPolicy = &NoReconnectionPolicy{}
			implCluster.SessionData(s).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config), ProtoVersion: 4, CQLVersion: "3.0.0", ConnectTimeout: time.Second, HostDialer: &v2TLSHostDialer{InternalConfig: connectivityConfig(config)}, Logger: nopLogger{}}
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
			go func() { done <- pool.InternalConnect() }()
			<-selected
			if mode == "endpoint" {
				implEndpoint.InternalHostInfoSetTranslatedConnectionInfo(host, translatedAddresses{CQL: AddressPort{Address: net.ParseIP("127.0.0.2"), Port: 9042}})
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
			if mode == "closed" && pool.InternalTransportState() != poolClosed {
				t.Fatal("closed pool resurrected")
			}
			client.Close()
			<-serverDone
		})
	}
}

func TestV2ReservedPreferredShardUsesAllowedFreeAlternative(t *testing.T) {
	makeConn := func(shard int) *driverConn {
		return &driverConn{InternalScyllaSupported: *func() *ScyllaConnectionFeatures {
			opaqueStorageValuexxxxxxxxxxxxxxxx := new(ScyllaConnectionFeatures)
			opaqueStorageValuexxxxxxxxxxxxxxxx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
				opaqueStorageValuexxxxxxxxxxxxxxx := new(ScyllaHostFeatures)
				implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxxxxxxxxxxxx).InternalNrShards = 2
				return opaqueStorageValuexxxxxxxxxxxxxxx
			}()
			implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxxxxxxxxxxxxxx).InternalShard = shard
			return opaqueStorageValuexxxxxxxxxxxxxxxx
		}(), InternalStreams: streams.New()}
	}
	first, second := makeConn(0), makeConn(1)
	p := codecTestMust(newTypedConnPicker(first, DefaultConfig("localhost").Pool))
	p.Put(first)
	p.Put(second)
	first.InternalExclusiveFlag.Store(true)
	routing := RoutingInfo{Token: Option[int64]{Value: math.MinInt64, Set: true}}
	if got := p.InternalPickTarget(Target{}, routing); got != second {
		t.Fatal("non-strict selection waited despite idle eligible connection")
	}
	if got := p.InternalPickTarget(Target{StrictShard: true, ShardID: Option[uint32]{Value: 0, Set: true}}, routing); got != first {
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
	implEndpoint.HostInfoData(host).InternalHostname = "transport.test"
	implEndpoint.HostInfoData(host).InternalConnectAddress = net.ParseIP("192.0.2.12")
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
	conn, err := (&v2TLSHostDialer{InternalConfig: connectivityConfig(config)}).DialHost(ctx, host)
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
