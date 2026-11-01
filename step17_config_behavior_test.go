//go:build unit

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	implCluster "github.com/scylladb/gocql/v2/internal/cluster"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	implExecution "github.com/scylladb/gocql/v2/internal/execution"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	routing "github.com/scylladb/gocql/v2/internal/routing"
)

func TestStep17TimestampHookOnceAcrossRetryAndPages(t *testing.T) {
	s, _, peers := runtimeTestSession(t, 1)
	defer s.Close()
	var calls atomic.Int32
	runtimeFor(s).InternalConfig.TimestampGenerator = func() int64 { calls.Add(1); return 987654321 }
	done := make(chan error, 1)
	go func() {
		for step := 0; step < 5; step++ {
			_, stream, body, err := step03ReadRequest(peers[0])
			if err != nil {
				done <- err
				return
			}
			at := 4 + int(binary.BigEndian.Uint32(body[:4])) + 2
			hasTimestamp := body[at]&0x20 != 0
			if step < 3 {
				if !hasTimestamp || int64(binary.BigEndian.Uint64(body[len(body)-8:])) != 987654321 {
					done <- errors.New("generated timestamp changed across attempts/pages")
					return
				}
			}
			if step == 3 && (!hasTimestamp || binary.BigEndian.Uint64(body[len(body)-8:]) != 0) {
				done <- errors.New("explicit zero timestamp lost")
				return
			}
			if step == 4 && hasTimestamp {
				done <- errors.New("conditional operation generated timestamp")
				return
			}
			var response []byte
			if step == 0 {
				response = step03ErrorResponse(t, stream, ErrCodeServer, nil)
			} else if step == 1 {
				response = typedTestResponse(t, 4, stream, runtimeRowsBody([]byte("page"), "a"))
			} else {
				response = typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "b"))
			}
			if _, err = peers[0].Write(response); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	profile := defaultProfile()
	profile.Prefetch = 0
	profile.Retry = step03RetryOnce{}
	options := Options{Profile: codecTestMust(NewProfile(profile))}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	b := runtimeQuery(t)
	rows := codecTestMust(Run(ctx, s, b, options))
	var count int
	for rows.Next() {
		count++
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := b.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if count != 2 || calls.Load() != 1 {
		t.Fatal("generator not one per logical execution", count, calls.Load())
	}
	explicit := runtimeQuery(t)
	opts := options
	opts.Timestamp = Option[int64]{Set: true, Value: 0}
	page := codecTestMust(RunUnpaged(ctx, s, explicit, opts))
	if err := page.Close(); err != nil {
		t.Fatal(err)
	}
	explicit.Close()
	if err := explicit.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	conditional := codecTestMust(BindUnpreparedBorrowed(Statement[Unit, Bytes]{Kind: ConditionalStatement, CQL: "SELECT value FROM ks.tbl", Params: NoParams, Rows: OneColumn(BlobCodec)}, NewColumns(), Unit{}))
	page = codecTestMust(RunUnpaged(ctx, s, conditional, options))
	if err := page.Close(); err != nil {
		t.Fatal(err)
	}
	conditional.Close()
	if err := conditional.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("explicit/conditional execution invoked generator", calls.Load())
	}
}

type step17SocketOptions struct {
	*net.TCPConn
	read, write, linger                int
	readCalls, writeCalls, lingerCalls int
	period                             time.Duration
}

func (c *step17SocketOptions) SetReadBuffer(n int) error {
	c.read = n
	c.readCalls++
	return c.TCPConn.SetReadBuffer(n)
}
func (c *step17SocketOptions) SetWriteBuffer(n int) error {
	c.write = n
	c.writeCalls++
	return c.TCPConn.SetWriteBuffer(n)
}
func (c *step17SocketOptions) SetLinger(n int) error {
	c.linger = n
	c.lingerCalls++
	return c.TCPConn.SetLinger(n)
}
func (c *step17SocketOptions) SetKeepAlivePeriod(n time.Duration) error {
	c.period = n
	return c.TCPConn.SetKeepAlivePeriod(n)
}
func TestStep17SocketBufferSettingsReachActualSocket(t *testing.T) {
	listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, e := listener.Accept()
		if e == nil {
			accepted <- c
		}
	}()
	raw := codecTestMust(net.Dial("tcp", listener.Addr().String()))
	defer raw.Close()
	peer := <-accepted
	defer peer.Close()
	wrapped := &step17SocketOptions{TCPConn: raw.(*net.TCPConn)}
	config := DefaultConfig("localhost")
	config.ReceiveBuffer = Option[int]{Set: true, Value: 4096}
	config.SendBuffer = Option[int]{Set: true, Value: 8192}
	config.Linger = Option[time.Duration]{Set: true, Value: 2 * time.Second}
	config.SocketKeepalive = 7 * time.Second
	if err := applyV2SocketOptions(wrapped, config, "tcp"); err != nil {
		t.Fatal(err)
	}
	if wrapped.read != 4096 || wrapped.write != 8192 || wrapped.linger != 2 || wrapped.period != 7*time.Second {
		t.Fatal("socket settings were not applied", wrapped)
	}
	zero := config
	zero.ReceiveBuffer = Option[int]{Set: true}
	zero.SendBuffer = Option[int]{Set: true}
	zero.Linger = Option[time.Duration]{Set: true}
	if err := applyV2SocketOptions(wrapped, zero, "tcp"); err != nil {
		t.Fatal(err)
	}
	if wrapped.read != 0 || wrapped.write != 0 || wrapped.linger != 0 || wrapped.readCalls != 2 || wrapped.writeCalls != 2 || wrapped.lingerCalls != 2 {
		t.Fatal("explicit zero socket settings were omitted", wrapped)
	}
	// Pipe has no TCP preflight controls, so each requested option reaches its
	// own unsupported-setting branch rather than failing earlier at NODELAY.
	for _, field := range []string{"receive", "send", "linger"} {
		a, b := net.Pipe()
		c := DefaultConfig("localhost")
		switch field {
		case "receive":
			c.ReceiveBuffer = config.ReceiveBuffer
		case "send":
			c.SendBuffer = config.SendBuffer
		case "linger":
			c.Linger = config.Linger
		}
		var unsupported *UnsupportedSettingError
		e := applyV2SocketOptions(a, c, "tcp")
		a.Close()
		b.Close()
		if !errors.As(e, &unsupported) || unsupported.Setting != map[string]string{"receive": "receive buffer", "send": "send buffer", "linger": "linger"}[field] {
			t.Fatal("wrong unsupported setting", field, e)
		}
	}
}

func TestStep17EventFlagsAndIdentityReachNativeStartup(t *testing.T) {
	for _, enabled := range []string{"none", "topology", "status", "schema", "routes"} {
		t.Run(enabled, func(t *testing.T) {
			srv := newControlTestServer(t, func(srv *controlTestServer) {
				srv.queries = func(query string, stream int, response *framer) bool {
					if !strings.Contains(query, "system.client_routes") {
						return false
					}
					columns := []rowsCol{{"connection_id", optNative(TypeVarchar)}, {"host_id", optNative(TypeUUID)}, {"address", optNative(TypeVarchar)}, {"port", optNative(TypeInt)}, {"tls_port", optNative(TypeInt)}}
					writeRows(response, stream, columns, [][][]byte{{[]byte("route"), testLocalHostID[:], []byte("127.0.0.1"), binary.BigEndian.AppendUint32(nil, uint32(srv.port())), nil}})
					return true
				}
			})
			var starts atomic.Int32
			srv.setAuthHook(func(request *framer, stream int, response *framer) {
				actual := readStartupOptions(t, request)
				for key, want := range map[string]string{"DRIVER_NAME": "custom-driver", "DRIVER_VERSION": "17.2", "APPLICATION_NAME": "release-probe", "APPLICATION_VERSION": "17.3", "CLIENT_ID": "caller-id"} {
					if actual[key] != want {
						t.Errorf("STARTUP %s=%q want%q", key, actual[key], want)
					}
				}
				starts.Add(1)
				response.WriteHeader(0, frm.OpReady, stream)
			})
			config := DefaultConfig(srv.Address)
			config.Protocol = 4
			config.KeepaliveInterval = 0
			config.Metadata.InitialDiscovery = false
			config.Metadata.Mode = MetadataDisabled
			config.Metadata.PeriodicRefresh = 0
			config.ReportConfiguration = false
			config.Pool.ConnectionsPerHost = 1
			config.Pool.ShardAwarePort = false
			config.Metadata.TopologyEvents = enabled == "topology"
			config.Metadata.StatusEvents = enabled == "status"
			config.Metadata.SchemaEvents = enabled == "schema"
			config.Metadata.ClientRouteEvents = enabled == "routes"
			config.ClientRoutes = &ClientRoutesOptions{ConnectionIDs: []string{"route"}, HostnameOverrides: map[string]string{"route": "127.0.0.1"}}
			config.Identity = IdentityOptions{DriverName: "custom-driver", DriverVersion: "17.2", ApplicationName: "release-probe", ApplicationVersion: "17.3", ClientID: "caller-id"}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			session, err := Connect(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			if err = session.Close(); err != nil {
				t.Fatal(err)
			}
			if starts.Load() == 0 {
				t.Fatal("no actual STARTUP observed")
			}
			registrations := srv.registered()
			if enabled == "none" {
				if len(registrations) != 0 {
					t.Fatal("disabled event flags sent REGISTER", registrations)
				}
				return
			}
			want := map[string]string{"topology": "TOPOLOGY_CHANGE", "status": "STATUS_CHANGE", "schema": "SCHEMA_CHANGE", "routes": "CLIENT_ROUTES_CHANGE"}[enabled]
			if len(registrations) == 0 {
				t.Fatal("enabled event omitted REGISTER")
			}
			for _, events := range registrations {
				if !slices.Equal(events, []string{want}) {
					t.Fatal("wrong native REGISTER events", events, want)
				}
			}
		})
	}
}

func TestStep17SNIProxyAddressSelectsActualTLSProxy(t *testing.T) {
	cert, clientTLS := step04TLS(t)
	listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		raw, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer raw.Close()
		server := tls.Server(raw, &tls.Config{Certificates: []tls.Certificate{cert}})
		err = server.Handshake()
		if err == nil && server.ConnectionState().ServerName != "transport.test" {
			err = fmt.Errorf("logical SNI lost: %q", server.ConnectionState().ServerName)
		}
		done <- err
	}()
	config := DefaultConfig("localhost")
	config.TLS = clientTLS
	config.Pool.ShardAwarePort = false
	config.ClientRoutes = &ClientRoutesOptions{TLS: true, ConnectionIDs: []string{"proxy"}, HostnameOverrides: map[string]string{"proxy": "transport.test"}, SNIProxyAddress: listener.Addr().String()}
	var endpoint string
	config.DialHost = func(ctx context.Context, target DialTarget) (net.Conn, error) {
		endpoint = target.Address
		return (DefaultDialer{}).DialContext(ctx, target)
	}
	host := step04Host(t, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	connection, err := (&v2TLSHostDialer{InternalConfig: connectivityConfig(config)}).DialHost(ctx, host)
	if err != nil {
		t.Fatal(err)
	}
	connection.Conn.Close()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if endpoint != listener.Addr().String() {
		t.Fatal("SNI proxy endpoint ignored", endpoint, listener.Addr())
	}
}

func TestStep17ExplicitLatencyThresholdChangesDecision(t *testing.T) {
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{Latency: LatencyOptions{Enabled: true, ExclusionThreshold: 3, MinimumSamples: 1}})).(*routingPolicy)
	now := time.Now()
	for id, value := range map[byte]float64{1: float64(time.Millisecond), 2: float64(2500 * time.Microsecond), 3: float64(3500 * time.Microsecond)} {
		policy.InternalLatencies.Store(UUID{15: id}, &routingLatency{InternalValue: value, InternalSamples: 1, InternalUpdated: now})
	}
	policy.Observe(AttemptEvent{Target: Target{HostID: UUID{15: 4}}, Started: now, Latency: time.Millisecond})
	decisions := policy.InternalDecisions.Load()
	if decisions == nil {
		t.Fatal("no latency decision")
	}
	if _, slow := decisions.InternalSlow[UUID{15: 2}]; slow {
		t.Fatal("custom threshold3 was ignored")
	}
	if _, slow := decisions.InternalSlow[UUID{15: 3}]; !slow {
		t.Fatal("custom threshold3 did not exclude slower host")
	}
}

func TestStep17ReadAheadUpperBoundKeepsRawPayloadForCaller(t *testing.T) {
	s, c, peer := step07StreamSession(t, 4, nil)
	defer s.Close()
	spy := c.InternalR.(*connReader).InternalConn.(*step07ReadTargets)
	payload := bytes.Repeat([]byte{17}, 64<<10)
	done := make(chan error, 1)
	go func() {
		stream := typedTestReadRequest(t, c, peer)
		_, err := peer.Write(step07Response(t, c, stream, runtimeRowsBody(nil, string(payload))))
		done <- err
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	bound := runtimeQuery(t)
	defer bound.Close()
	cursor := codecTestMust(RunStream(ctx, s, bound, StreamOneColumn(StreamBlob), StreamOptions{Limits: StreamLimits{ReadAheadBytes: 1}}))
	spy.mu.Lock()
	for _, target := range spy.targets {
		if len(target) > 16 {
			t.Error("raw payload read ahead before destination", len(target))
		}
	}
	spy.mu.Unlock()
	dst := make([]byte, 0, len(payload))
	if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) {
		t.Fatal("positive read-ahead cap changed output", cursor.Err())
	}
	if cursor.NextInto(&dst) || cursor.Err() != nil {
		t.Fatal(cursor.Err())
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStep17IncludeMetadataOverrideChangesExecuteWireFlag(t *testing.T) {
	s, _, _ := runtimeTestSession(t, 0)
	defer s.Close()
	c, peer := typedTestConn(t, 4, nil)
	id := UUID{15: 1}
	host := new(HostInfo)
	implEndpoint.HostInfoData(host).InternalHostId = id
	implEndpoint.HostInfoData(host).InternalState = NodeUp
	c.InternalHost = host
	c.InternalHooks = newConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[id.String()] = host
	picker := newDefaultConnPicker(1)
	picker.Put(c)
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[id] = &hostConnPool{InternalHost: host, InternalConnPicker: picker}
	c.InternalFramers.InternalDefaults.InternalScyllaUseMetadataID = true
	go c.InternalServe(c.InternalCtx)
	columns := NewColumns(ColumnSpec{Keyspace: "ks", Table: "tbl", Name: "value", Type: codecTestMust(ScalarType(TypeBlob))})
	state, _ := step03PreparedState(t, s, columns)
	handle := new(Prepared[Unit, Bytes])
	storage := implExecution.PreparedData(handle)
	storage.InternalState = state
	storage.InternalParams = codecTestMust(NoParams.Compile(NewColumns()))
	storage.InternalRows = OneColumn(BlobCodec)
	storage.InternalPlans = &resultPlanCache[Bytes]{}
	bound := codecTestMust(BindBorrowed(handle, Unit{}))
	defer bound.Close()
	done := make(chan error, 1)
	go func() {
		for _, include := range []bool{false, true, false} {
			opcode, stream, body, err := step03ReadRequest(peer)
			if err != nil {
				done <- err
				return
			}
			if opcode != 10 {
				done <- fmt.Errorf("wanted EXECUTE got%d", opcode)
				return
			}
			at := 0
			at += 2 + int(binary.BigEndian.Uint16(body[at:]))
			at += 2 + int(binary.BigEndian.Uint16(body[at:]))
			at += 2
			skip := body[at]&2 != 0
			if skip == include {
				done <- fmt.Errorf("IncludeMetadata=%t produced skip=%t", include, skip)
				return
			}
			if _, err = peer.Write(typedTestResponse(t, 4, stream, runtimeRowsBody(nil, "value"))); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, include := range []bool{false, true, false} {
		page := codecTestMust(RunUnpaged(ctx, s, bound, Options{IncludeMetadata: include}))
		if !page.Next() || string(page.Row().OwnedBytes()) != "value" {
			t.Fatal(page.Err())
		}
		if err := page.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	bound.Close()
	if err := bound.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestStep17LatencyLargeRetryWholePlan(t *testing.T) {
	period := time.Duration(1<<63 - 1)
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{TokenAware: true, Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, RetryPeriod: period, UpdateInterval: time.Hour}})).(*routingPolicy)
	metadata := v2TestMetadata()
	slow := UUID{15: 1}
	healthy := UUID{15: 2}
	routing.SnapshotData(metadata).InternalRanges["ks"][0].Replicas = []Target{{HostID: slow}}
	now := time.Now()
	old := now.Add(-period).Add(-time.Minute)
	policy.Observe(AttemptEvent{Target: Target{HostID: healthy}, Started: old, Latency: time.Millisecond})
	policy.Observe(AttemptEvent{Target: Target{HostID: slow}, Started: old, Latency: 10 * time.Millisecond})
	policy.InternalDecisions.Store(&routing.InternalRoutingDecisions{InternalSlow: map[UUID]time.Time{slow: now.Add(-time.Minute)}})
	info := RoutingInfo{Keyspace: "ks", Table: "table", Token: Option[int64]{Set: true}, Kind: OrdinaryStatement}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var probes atomic.Int32
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			target, ok := policy.Plan(info, metadata).Next()
			if !ok {
				t.Error("missing target")
			}
			if target.HostID == slow {
				probes.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if probes.Load() != 1 {
		t.Fatalf("valid MaxDuration retry admitted %d probes, want1", probes.Load())
	}
}
func TestStep17LatencyLargePublicationInterval(t *testing.T) {
	policy := codecTestMust(NewRoutingPolicy(RoutingOptions{Latency: LatencyOptions{Enabled: true, MinimumSamples: 1, UpdateInterval: time.Duration(1<<63 - 1)}})).(*routingPolicy)
	now := time.Now()
	policy.Observe(AttemptEvent{Target: Target{HostID: UUID{15: 1}}, Started: now, Latency: time.Millisecond})
	first := policy.InternalDecisions.Load()
	policy.Observe(AttemptEvent{Target: Target{HostID: UUID{15: 2}}, Started: now, Latency: 3 * time.Millisecond})
	if policy.InternalDecisions.Load() != first {
		t.Fatal("valid MaxDuration UpdateInterval republished before deadline without a new minimum")
	}
}
