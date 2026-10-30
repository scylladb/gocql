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
	"context"
	"crypto/tls"
	"errors"
	"github.com/scylladb/gocql/v2/internal/streams"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func step04ValidSupported() map[string][]string {
	return map[string][]string{"SCYLLA_SHARD": {"0"}, "SCYLLA_NR_SHARDS": {"12"}, "SCYLLA_PARTITIONER": {"org.apache.cassandra.dht.Murmur3Partitioner"}, "SCYLLA_SHARDING_ALGORITHM": {"biased-token-round-robin"}, "SCYLLA_SHARDING_IGNORE_MSB": {"0"}}
}
func TestV2SupportedShardingValidationBeforeAllocation(t *testing.T) {
	options := DefaultConfig("localhost").Pool
	for _, change := range []func(map[string][]string){
		func(v map[string][]string) { v["SCYLLA_SHARD"] = nil }, func(v map[string][]string) { delete(v, "SCYLLA_SHARD") }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"0"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"65536"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"9223372036854775807"} }, func(v map[string][]string) { v["SCYLLA_NR_SHARDS"] = []string{"12", "13"} }, func(v map[string][]string) { v["SCYLLA_SHARD"] = []string{"12"} }, func(v map[string][]string) { v["SCYLLA_SHARD"] = []string{"-1"} }, func(v map[string][]string) { v["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"64"} }, func(v map[string][]string) { v["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"-1"} }, func(v map[string][]string) { v["SCYLLA_PARTITIONER"] = nil }, func(v map[string][]string) { v["SCYLLA_SHARD_AWARE_PORT"] = nil }, func(v map[string][]string) { v[rateLimitError] = []string{"ERROR_CODE"} }, func(v map[string][]string) { v[lwtAddMetadataMarkKey] = []string{"LWT_OPTIMIZATION_META_BIT_MASK"} },
	} {
		values := step04ValidSupported()
		change(values)
		var protocol *ProtocolError
		if err := validateV2Supported(values, options); !errors.As(err, &protocol) {
			t.Fatal("malformed SUPPORTED not rejected before parse", values, err)
		}
		// Legacy callers are also panic-free, even if they do not request typed setup.
		parseSupported(values, nopLogger{})
	}
	for _, values := range []map[string][]string{nil, {}, {scyllaUseMetadataID: nil}, {"SCYLLA_SHARD_AWARE_PORT": {"19042"}, lwtAddMetadataMarkKey: {"LWT_OPTIMIZATION_META_BIT_MASK=536870912"}}, step04ValidSupported()} {
		if err := validateV2Supported(values, options); err != nil {
			t.Fatal("legitimate absent/optional capability rejected", values, err)
		}
	}
	values := step04ValidSupported()
	if got := parseSupported(values, nopLogger{}); implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalNrShards != 12 || implEndpoint.ScyllaHostFeaturesData(&got.ScyllaHostFeatures).InternalMsbIgnore != 0 {
		t.Fatal("explicit zero MSB disabled valid sharding", got)
	}
	values["SCYLLA_NR_SHARDS"] = []string{"65535"}
	values["SCYLLA_SHARD"] = []string{"65534"}
	values["SCYLLA_SHARDING_IGNORE_MSB"] = []string{"63"}
	if err := validateV2Supported(values, options); err != nil {
		t.Fatal("valid16-bit maximum rejected", err)
	}
	maximum := parseSupported(values, nopLogger{})
	if implEndpoint.ScyllaHostFeaturesData(&maximum.ScyllaHostFeatures).InternalNrShards != 65535 || implEndpoint.ScyllaConnectionFeaturesData(&maximum).InternalShard != 65534 || implEndpoint.ScyllaHostFeaturesData(&maximum.ScyllaHostFeatures).InternalMsbIgnore != 63 {
		t.Fatal(maximum)
	}
}
func TestV2PoolTargetArithmeticAndMaximumValidShardCount(t *testing.T) {
	options := DefaultConfig("localhost").Pool
	for _, bad := range []ScyllaConnectionFeatures{{ScyllaHostFeatures: *func() *ScyllaHostFeatures {
		opaqueStorageValue := new(ScyllaHostFeatures)
		implEndpoint.ScyllaHostFeaturesData(opaqueStorageValue).InternalNrShards = 65536
		return opaqueStorageValue
	}()}, {ScyllaHostFeatures: *func() *ScyllaHostFeatures {
		opaqueStorageValuex := new(ScyllaHostFeatures)
		implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuex).InternalNrShards = -1
		return opaqueStorageValuex
	}()}, {ScyllaHostFeatures: *func() *ScyllaHostFeatures {
		opaqueStorageValuexx := new(ScyllaHostFeatures)
		implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalNrShards = 2
		implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexx).InternalMsbIgnore = 64
		return opaqueStorageValuexx
	}()}, *func() *ScyllaConnectionFeatures {
		opaqueStorageValuexxxx := new(ScyllaConnectionFeatures)
		opaqueStorageValuexxxx.ScyllaHostFeatures = *func() *ScyllaHostFeatures {
			opaqueStorageValuexxx := new(ScyllaHostFeatures)
			implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxx).InternalNrShards = 2
			return opaqueStorageValuexxx
		}()
		implEndpoint.ScyllaConnectionFeaturesData(opaqueStorageValuexxxx).InternalShard = 2
		return opaqueStorageValuexxxx
	}()} {
		if _, err := newTypedConnPicker(&driverConn{InternalScyllaSupported: bad}, options); err == nil {
			t.Fatal("invalid sharder reached allocation", bad)
		}
	}
	options.ConnectionsPerShard = math.MaxInt
	if _, err := checkedV2PoolBudget(options, 2); err == nil {
		t.Fatal("target multiplication overflow")
	}
	options.ConnectionsPerShard = 1
	options.MaxExcessPerShard = math.MaxInt
	if _, err := checkedV2PoolBudget(options, 2); err == nil {
		t.Fatal("excess multiplication overflow")
	}
	if _, err := checkedV2PoolBudget(options, 1); err == nil {
		t.Fatal("target plus excess overflow")
	}
	options.MaxExcessPerShard = 0
	options.ConnectionsPerShard = math.MaxInt
	if target, err := checkedV2PoolBudget(options, 1); err != nil || target != math.MaxInt {
		t.Fatal("valid representable large target capped", target, err)
	}
	options.ConnectionsPerHost = math.MaxInt
	options.MaxExcessPerShard = math.MaxInt
	if target, err := checkedV2PoolBudget(options, 65535); err != nil || target != math.MaxInt {
		t.Fatal("inactive shard sizing overrode host target", target, err)
	}
	options = DefaultConfig("localhost").Pool
	options.MaxExcessPerShard = 0
	c := &driverConn{InternalScyllaSupported: ScyllaConnectionFeatures{ScyllaHostFeatures: *func() *ScyllaHostFeatures {
		opaqueStorageValuexxxxx := new(ScyllaHostFeatures)
		implEndpoint.ScyllaHostFeaturesData(opaqueStorageValuexxxxx).InternalNrShards = 65535
		return opaqueStorageValuexxxxx
	}()}}
	p, err := newTypedConnPicker(c, options)
	if err != nil || len(p.InternalConns) != 65535 || p.InternalTarget() != 65535 {
		t.Fatal("valid largest sharder rejected", err)
	}
}

type step04BoundedOpeningDialer struct {
	active, max, calls atomic.Int32
	entered            chan struct{}
	release            chan struct{}
}

func (d *step04BoundedOpeningDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	active := d.active.Add(1)
	defer d.active.Add(-1)
	d.calls.Add(1)
	for {
		old := d.max.Load()
		if old >= active || d.max.CompareAndSwap(old, active) {
			break
		}
	}
	d.entered <- struct{}{}
	select {
	case <-d.release:
		return nil, errors.New("injected opening failure")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func TestV2HugePoolTargetUsesBoundedDialWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := DefaultConfig("localhost")
	config.Pool.ConnectionsPerHost = math.MaxInt
	dialer := &step04BoundedOpeningDialer{entered: make(chan struct{}, 64), release: make(chan struct{})}
	s := func() *Session {
		opaqueStorageValuexxxxxx := new(Session)
		implCluster.SessionData(opaqueStorageValuexxxxxx).InternalCtx = ctx
		implCluster.SessionData(opaqueStorageValuexxxxxx).InternalLogger = nopLogger{}
		implCluster.SessionData(opaqueStorageValuexxxxxx).InternalConnCfg = &driverConnConfig{TypedConfig: connectivityConfigPointer(config), ProtoVersion: 4, HostDialer: dialer, Logger: nopLogger{}}
		return opaqueStorageValuexxxxxx
	}()
	implCluster.SessionData(s).InternalCfg.ReconnectionPolicy = &NoReconnectionPolicy{}
	pool := newHostConnPool(s, func() *HostInfo {
		opaqueStorageValuexxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalConnectAddress = net.ParseIP("127.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalPort = 9042
		return opaqueStorageValuexxxxxxx
	}(), math.MaxInt, "")
	defer pool.Close()
	done := make(chan error, 1)
	go func() { done <- pool.InternalConnectMany(math.MaxInt) }()
	for i := 0; i < 32; i++ {
		select {
		case <-dialer.entered:
		case <-time.After(3 * time.Second):
			close(dialer.release)
			t.Fatal("bounded workers not admitted", i)
		}
	}
	close(dialer.release)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("failed batch reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("huge target batch did not stop after failure")
	}
	if dialer.max.Load() > 32 || dialer.calls.Load() != 32 {
		t.Fatal("unbounded dials/goroutines for huge target", dialer.max.Load(), dialer.calls.Load())
	}
}
func TestV2ShardingZeroMSBMapsEntireTokenRange(t *testing.T) {
	features := parseSupported(step04ValidSupported(), nopLogger{})
	options := DefaultConfig("localhost").Pool
	p := codecTestMust(newTypedConnPicker(&driverConn{InternalScyllaSupported: features}, options))
	if p.InternalShardOf(math.MinInt64) != 0 || p.InternalShardOf(math.MaxInt64) != 11 {
		t.Fatal("zero MSB semantics incorrect")
	}
	// The full32-bit LWT mask remains a wire bitset on both native architectures.
	values := step04ValidSupported()
	values[lwtAddMetadataMarkKey] = []string{"LWT_OPTIMIZATION_META_BIT_MASK=2147483648"}
	if err := validateV2Supported(values, options); err != nil {
		t.Fatal(err)
	}
	ext := newLwtAddMetaMarkExt(values, nopLogger{})
	if ext == nil || ext.InternalSerialize()[lwtAddMetadataMarkKey] != "LWT_OPTIMIZATION_META_BIT_MASK="+strconv.FormatUint(1<<31, 10) {
		t.Fatal("high LWT bit did not roundtrip")
	}
}

func TestV2LogicalTLSNamesHandleIPv6AndZones(t *testing.T) {
	for address, expected := range map[string]string{"[::1]:9042": "::1", "[fe80::1%lo]:9042": "fe80::1", "::1": "::1", "transport.test:9042": "transport.test", "127.0.0.1:9042": "127.0.0.1"} {
		original := &tls.Config{}
		selected := tlsConfigForAddr(original, address)
		if selected.ServerName != expected || original.ServerName != "" {
			t.Fatal("logical TLS identity", address, selected.ServerName, expected)
		}
	}
}

type step04OpaqueTCP struct{ net.Conn }
type step04ForwardingTCP struct {
	*net.TCPConn
	mu        sync.Mutex
	noDelay   []bool
	keepalive []bool
}

func (c *step04ForwardingTCP) SetNoDelay(value bool) error {
	c.mu.Lock()
	c.noDelay = append(c.noDelay, value)
	c.mu.Unlock()
	return c.TCPConn.SetNoDelay(value)
}
func (c *step04ForwardingTCP) SetKeepAlive(value bool) error {
	c.mu.Lock()
	c.keepalive = append(c.keepalive, value)
	c.mu.Unlock()
	return c.TCPConn.SetKeepAlive(value)
}
func TestV2TCPWrapperSocketControlsApplyOrFailExplicitly(t *testing.T) {
	listener := codecTestMust(net.Listen("tcp", "127.0.0.1:0"))
	defer listener.Close()
	peer := make(chan net.Conn, 1)
	go func() {
		c, err := listener.Accept()
		if err == nil {
			peer <- c
		}
	}()
	raw := codecTestMust(net.Dial("tcp", listener.Addr().String()))
	defer raw.Close()
	server := <-peer
	defer server.Close()
	tcp := raw.(*net.TCPConn)
	config := DefaultConfig("localhost")
	opaque := &step04OpaqueTCP{Conn: tcp}
	var unsupported *UnsupportedSettingError
	if err := applyV2SocketOptions(opaque, config, "tcp"); !errors.As(err, &unsupported) {
		t.Fatal("TCP wrapper silently ignored NODELAY", err)
	}
	forwarding := &step04ForwardingTCP{TCPConn: tcp}
	for _, value := range []bool{true, false} {
		config.NoDelay = value
		if err := applyV2SocketOptions(forwarding, config, "tcp"); err != nil {
			t.Fatal(err)
		}
	}
	if len(forwarding.noDelay) != 2 || !forwarding.noDelay[0] || forwarding.noDelay[1] || len(forwarding.keepalive) != 2 || forwarding.keepalive[0] || forwarding.keepalive[1] {
		t.Fatal("wrapped socket controls not applied", forwarding.noDelay, forwarding.keepalive)
	}
}
func TestV2ConnectionPickerPositionWrapIsBounded(t *testing.T) {
	options := DefaultConfig("localhost").Pool
	options.ConnectionsPerShard = 3
	c := &driverConn{}
	p := codecTestMust(newTypedConnPicker(c, options))
	p.InternalConns[0] = []*driverConn{{InternalStreams: streams.New()}, {InternalStreams: streams.New()}, {InternalStreams: streams.New()}}
	p.InternalPos.Store(math.MaxInt64)
	if p.InternalPickTarget(Target{}, RoutingInfo{}) == nil {
		t.Fatal("wrapped position lost eligible connection")
	}
}
