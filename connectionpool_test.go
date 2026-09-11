//go:build unit
// +build unit

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

package gocql

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql/debounce"
)

func TestSetupTLSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                       string
		opts                       *SslOptions
		expectedInsecureSkipVerify bool
	}{
		{
			name: "Config nil, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config nil, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tlsConfig, err := setupTLSConfig(test.opts, &defaultLogger{})
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}
			if tlsConfig.InsecureSkipVerify != test.expectedInsecureSkipVerify {
				t.Fatalf("got %v, but expected %v", tlsConfig.InsecureSkipVerify,
					test.expectedInsecureSkipVerify)
			}

			// Verify that VerifyPeerCertificate is set when InsecureSkipVerify is false
			// and DisableStrictCertificateValidation is false (default)
			if !tlsConfig.InsecureSkipVerify && tlsConfig.VerifyPeerCertificate == nil {
				t.Fatal("VerifyPeerCertificate should be set when InsecureSkipVerify is false")
			}
			// Verify that VerifyPeerCertificate is not set when InsecureSkipVerify is true
			if tlsConfig.InsecureSkipVerify && tlsConfig.VerifyPeerCertificate != nil {
				t.Fatal("VerifyPeerCertificate should not be set when InsecureSkipVerify is true")
			}
		})
	}
}

// errorConn is a mock net.Conn whose Close returns an error,
// triggering HandleError via Conn.closeWithError.
type errorConn struct {
	net.Conn
}

func (e errorConn) Close() error {
	return errors.New("mock close error")
}

func (e errorConn) SetTimeout(_ time.Duration) {}
func (e errorConn) GetTimeout() time.Duration  { return 0 }

// setDisarm is required of any reader installed as Conn.r (connReadSource). This
// mock is never read from, so there is nothing to disarm.
func (e errorConn) setDisarm(_ bool) {}

// TestHostConnPoolCloseDeadlock verifies that hostConnPool.Close() does not
// self-deadlock when defaultConnPicker closes connections that trigger
// HandleError callbacks.
//
// Deadlock chain (before fix):
//
//	Close() -> connPicker.Close() -> conn.Close() -> HandleError() -> pool.mu.Lock() (DEADLOCK)
func TestHostConnPoolCloseDeadlock(t *testing.T) {
	t.Parallel()

	host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
	session := &Session{
		cfg: ClusterConfig{
			NumConns:         2,
			ConvictionPolicy: &SimpleConvictionPolicy{},
		},
		logger: nopLogger{},
	}

	pool := &hostConnPool{
		session:    session,
		host:       host,
		size:       2,
		keyspace:   "test",
		connPicker: nopConnPicker{},
		logger:     nopLogger{},
		debouncer:  debounce.NewSimpleDebouncer(),
	}

	// Build a defaultConnPicker with Conns that trigger HandleError on Close.
	picker := newDefaultConnPicker(2)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		conn := &Conn{
			r:            errorConn{},
			errorHandler: pool,
			cancel:       cancel,
			ctx:          ctx,
			logger:       nopLogger{},
		}
		_ = picker.Put(conn)
	}
	pool.connPicker = picker

	// Close must return before timeout; otherwise the pool is deadlocked.
	done := make(chan struct{})
	go func() {
		pool.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success — Close returned without deadlocking.
	case <-time.After(5 * time.Second):
		t.Fatal("hostConnPool.Close() deadlocked: timed out after 5 seconds")
	}
}

// TestHostConnPoolConnectClosedPoolDoesNotDeadlock verifies that connect's
// already-closed-pool path does not close a connection while holding pool.mu.
func TestHostConnPoolConnectClosedPoolDoesNotDeadlock(t *testing.T) {
	t.Parallel()

	host := &HostInfo{connectAddress: net.ParseIP("127.0.0.1"), port: 9042}
	session := &Session{
		cfg: ClusterConfig{
			NumConns:         1,
			ConvictionPolicy: &SimpleConvictionPolicy{},
		},
		logger: nopLogger{},
	}

	pool := &hostConnPool{
		session:    session,
		host:       host,
		size:       1,
		keyspace:   "test",
		connPicker: nopConnPicker{},
		logger:     nopLogger{},
		debouncer:  debounce.NewSimpleDebouncer(),
		closed:     true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	conn := &Conn{
		r:            errorConn{},
		errorHandler: pool,
		cancel:       cancel,
		ctx:          ctx,
		logger:       nopLogger{},
	}

	done := make(chan struct{})
	go func() {
		pool.mu.Lock()
		if pool.closed {
			pool.mu.Unlock()
			conn.Close()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("closed-pool connect cleanup deadlocked: timed out after 5 seconds")
	}
}

// newUndialedHostPool builds a hostConnPool for host without connecting it.
// newHostConnPool is a pure constructor -- it installs a nopConnPicker and
// leaves the pool unfilled -- so this needs no server, and every HostPoolInfo
// accessor the iteration exposes is safe on the result.
func newUndialedHostPool(t *testing.T, session *Session, ip string) *hostConnPool {
	t.Helper()

	addr := net.ParseIP(ip)
	if addr == nil {
		t.Fatalf("newUndialedHostPool: %q is not an IP", ip)
	}
	host := &HostInfo{
		hostId:           MustRandomUUID(),
		connectAddress:   addr,
		broadcastAddress: addr,
		port:             9042,
	}
	return newHostConnPool(session, host, 1, "")
}

// TestPolicyConnPoolIteratePool covers policyConnPool.iteratePool, the engine
// behind the public Session.IterateHostPools. It had no coverage in either
// lane.
//
// The behaviour worth pinning is the early exit: iteratePool holds a read lock
// across the whole walk and breaks when the callback returns false, so a caller
// can stop after finding what it wants. A refactor that dropped the break would
// still pass a "visits everything" test.
func TestPolicyConnPoolIteratePool(t *testing.T) {
	t.Parallel()

	newPool := func(t *testing.T, ips ...string) *policyConnPool {
		t.Helper()
		session := &Session{logger: &testLogger{}}
		p := &policyConnPool{hostConnPools: map[UUID]*hostConnPool{}}
		for _, ip := range ips {
			hp := newUndialedHostPool(t, session, ip)
			p.hostConnPools[hp.host.hostUUID()] = hp
		}
		return p
	}

	t.Run("visits every pool", func(t *testing.T) {
		t.Parallel()

		p := newPool(t, "127.0.20.1", "127.0.20.2", "127.0.20.3")

		seen := map[string]bool{}
		p.iteratePool(func(info HostPoolInfo) bool {
			seen[info.Host().ConnectAddress().String()] = true
			return true
		})

		for _, want := range []string{"127.0.20.1", "127.0.20.2", "127.0.20.3"} {
			if !seen[want] {
				t.Errorf("pool for %s was never visited (saw %v)", want, seen)
			}
		}
	})

	t.Run("stops when the callback returns false", func(t *testing.T) {
		t.Parallel()

		p := newPool(t, "127.0.20.1", "127.0.20.2", "127.0.20.3")

		visits := 0
		p.iteratePool(func(HostPoolInfo) bool {
			visits++
			return false
		})

		if visits != 1 {
			t.Errorf("callback ran %d times after returning false, want 1", visits)
		}
	})

	t.Run("empty pool never invokes the callback", func(t *testing.T) {
		t.Parallel()

		p := newPool(t)

		p.iteratePool(func(HostPoolInfo) bool {
			t.Error("callback ran for a pool with no hosts")
			return true
		})
	})
}

// closeSignalPicker is a ConnPicker that reports when it has been closed.
//
// Closing a retired pool is the half of policyConnPool.removeHost that dropping
// the map entry does not prove: hostConnPool.Close is what reaches
// connPicker.Close and hangs up the host's connections, and it runs on a new
// goroutine, so there is nothing synchronous to assert on. Signalling from the
// picker makes the wait deterministic rather than a poll, and embedding
// nopConnPicker keeps this to the one method that matters.
type closeSignalPicker struct {
	nopConnPicker

	once   sync.Once
	closed chan struct{}
}

func newCloseSignalPicker() *closeSignalPicker {
	return &closeSignalPicker{closed: make(chan struct{})}
}

// Close is idempotent. hostConnPool.Close already guards against closing twice,
// but a double close here would panic rather than fail, which is a bad way for
// a regression in that guard to surface.
func (p *closeSignalPicker) Close() {
	p.once.Do(func() { close(p.closed) })
}

// newSignallingHostPool builds an undialed pool for host whose Close is
// observable. The connPicker is replaced before the pool is published to
// hostConnPools, so nothing else can be reading it yet.
func newSignallingHostPool(session *Session, host *HostInfo) (*hostConnPool, *closeSignalPicker) {
	pool := newHostConnPool(session, host, 1, "")
	picker := newCloseSignalPicker()
	pool.connPicker = picker
	return pool, picker
}

func awaitPoolClosed(t *testing.T, picker *closeSignalPicker, what string) {
	t.Helper()
	select {
	case <-picker.closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s: connection pool was never closed", what)
	}
}

func assertPoolNotClosed(t *testing.T, picker *closeSignalPicker, what string) {
	t.Helper()
	select {
	case <-picker.closed:
		t.Errorf("%s: connection pool was closed", what)
	default:
	}
}

// TestSessionRemoveHost covers Session.removeHost, which the ring refresher
// calls when a host leaves the ring (host_source.go). It had no coverage in
// either lane.
//
// It has to retire the host from all three places that track it -- the
// selection policy, the connection pool and the host source. Dropping any one
// leaves the driver routing to a node that is gone, so each is asserted
// separately.
func TestSessionRemoveHost(t *testing.T) {
	t.Parallel()

	// newNodeEventFixture (events_node_test.go) already assembles exactly the
	// trio removeHost touches: a recording policy, a connection pool and a
	// host source.
	f := newNodeEventFixture(t)
	host := f.addHost(t, "127.0.20.1")
	other := f.addHost(t, "127.0.20.2")

	// The fixture's pool is empty by default, and policyConnPool.removeHost
	// short-circuits on a missing host -- so with an empty pool the
	// s.pool.removeHost leg of removeHost is unobservable and dropping it
	// entirely would still pass. Populate it so that leg is actually pinned.
	// newHostConnPool is a pure constructor, so neither pool dials.
	hostPool, hostPicker := newSignallingHostPool(f.session, host)
	otherPool, otherPicker := newSignallingHostPool(f.session, other)
	f.session.pool.hostConnPools = map[UUID]*hostConnPool{
		host.hostUUID():  hostPool,
		other.hostUUID(): otherPool,
	}

	if _, ok := f.session.hostSource.getHostByIP("127.0.20.1"); !ok {
		t.Fatal("precondition: host is not in the host source")
	}
	if _, ok := f.session.pool.getPoolByHostID(host.HostID()); !ok {
		t.Fatal("precondition: host has no connection pool")
	}

	f.session.removeHost(host)

	assertStrings(t, "RemoveHost", f.policy.removeHostCalls(), []string{"127.0.20.1"})

	// policyConnPool.removeHost does two things, and the map entry only proves
	// the first. Dropping the entry without closing the pool leaves the retired
	// host's connections open -- a leak on every topology change, and a
	// regression the entry check alone cannot see, since reducing removeHost to
	// a bare delete() still satisfies it.
	if _, ok := f.session.pool.getPoolByHostID(host.HostID()); ok {
		t.Error("removed host still has a connection pool")
	}
	awaitPoolClosed(t, hostPicker, "removed host")

	if _, ok := f.session.pool.getPoolByHostID(other.HostID()); !ok {
		t.Error("removeHost also dropped an unrelated host's connection pool")
	}
	// Safe as a non-blocking check: the wait above already established that the
	// goroutine removeHost started has run, and it is the only one that could
	// have closed anything.
	assertPoolNotClosed(t, otherPicker, "unrelated host")

	if _, ok := f.session.hostSource.getHostByIP("127.0.20.1"); ok {
		t.Error("removed host is still resolvable in the host source")
	}
	// ringDescriber keeps two indexes, and getHostByIP only proves one of them.
	// Its ok comes from hostIPToUUID alone, so a host left behind in the hosts
	// map is invisible to the check above -- and hosts is what getHostsList
	// reads, which is what controlConn.attemptReconnect dials. Assert the ID
	// index separately.
	if got := f.session.hostSource.getHost(host.HostID()); got != nil {
		t.Error("removed host is still in the host source by ID")
	}

	if _, ok := f.session.hostSource.getHostByIP("127.0.20.2"); !ok {
		t.Error("removeHost also dropped an unrelated host")
	}
	if got := f.session.hostSource.getHost(other.HostID()); got == nil {
		t.Error("unrelated host disappeared from the host source by ID")
	}
}
