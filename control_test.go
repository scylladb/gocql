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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

type cancelledReconnectDialer struct {
	started chan struct{}
	calls   int
}

func (d *cancelledReconnectDialer) DialHost(ctx context.Context, _ *HostInfo) (*DialedHost, error) {
	d.calls++
	if d.calls == 1 {
		close(d.started)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type reconnectConvictionPolicy struct {
	SimpleConvictionPolicy
	failures int
}

func (p *reconnectConvictionPolicy) AddFailure(err error, host *HostInfo) bool {
	p.failures++
	return p.SimpleConvictionPolicy.AddFailure(err, host)
}

func TestControlConnReconnectStopsOnSessionCancellation(t *testing.T) {
	for _, cancelBeforeDial := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelBeforeDial=%t", cancelBeforeDial), func(t *testing.T) {
			const timeout = 5 * time.Second
			dialer := &cancelledReconnectDialer{started: make(chan struct{})}
			conviction := &reconnectConvictionPolicy{}
			logger := &recordingLogger{}
			cfg := NewCluster("fallback.invalid")
			cfg.ProtoVersion = 4
			cfg.HostDialer = dialer
			cfg.ConvictionPolicy = conviction
			cfg.DNSResolver = brokenDNSResolver{}
			cfg.Logger = logger
			s, err := newSessionCommon(*cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.Close)
			s.hostSource.hosts = map[string]*HostInfo{
				"first":  {connectAddress: net.ParseIP("127.0.0.1"), port: 9042},
				"second": {connectAddress: net.ParseIP("127.0.0.2"), port: 9042},
			}
			c := createControlConn(s)
			s.control = c
			if cancelBeforeDial {
				s.cancel()
			}
			done := make(chan error, 1)
			go func() { done <- c.attemptReconnect() }()
			if !cancelBeforeDial {
				select {
				case <-dialer.started:
				case <-time.After(timeout):
					t.Fatal("reconnect did not start dialing")
				}
				s.cancel()
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Errorf("reconnect error = %v, want context.Canceled", err)
				}
			case <-time.After(timeout):
				t.Fatal("cancelled reconnect did not return")
			}
			wantCalls := 1
			if cancelBeforeDial {
				wantCalls = 0
			}
			if dialer.calls != wantCalls || conviction.failures != 0 {
				t.Errorf("dial calls = %d, want %d; host failures = %d, want 0", dialer.calls, wantCalls, conviction.failures)
			}
			if logs := logger.String(); strings.Contains(logs, "unable to dial control conn") || strings.Contains(logs, "falling back") {
				t.Errorf("shutdown reported node failures or attempted fallback: %s", logs)
			}
		})
	}
}

func TestControlConnCloseBeforeHeartbeat(t *testing.T) {
	t.Parallel()

	c := createControlConn(nil)
	c.close()
	if state := atomic.LoadInt32(&c.state); state != controlConnClosing {
		t.Fatalf("control connection state = %d, want closing", state)
	}
	select {
	case <-c.quit:
	default:
		t.Fatal("control connection shutdown signal is still open")
	}

	// A heartbeat scheduled by connect must not start after shutdown.
	c.heartBeat()
	c.close()
}

func TestControlConnCloseUnblocksHeartbeat(t *testing.T) {
	t.Parallel()

	const timeout = 5 * time.Second
	conn, peer := newTestExecConn(t, testContextWriter{})
	t.Cleanup(func() { _ = peer.Close() })
	s := &Session{ctx: context.Background(), logger: nopLogger{}, connCfg: &ConnConfig{}}
	s.hostSource = &ringDescriber{cfg: &s.cfg, logger: s.logger}
	c := createControlConn(s)
	c.conn.Store(&connHost{conn: conn})
	heartbeatDone := make(chan struct{})
	closeDone := make(chan struct{})
	closeStarted := false
	go func() {
		c.heartBeat()
		close(heartbeatDone)
	}()
	t.Cleanup(func() {
		if !closeStarted {
			go func() {
				c.close()
				close(closeDone)
			}()
		}
		// Rescue a failed shutdown without leaving a pending exec or worker.
		conn.Close()
		for _, done := range []<-chan struct{}{closeDone, heartbeatDone} {
			select {
			case <-done:
			case <-time.After(timeout):
				t.Error("control shutdown worker did not exit")
			}
		}
	})

	// The zero metadata timeout leaves OPTIONS pending until the socket closes.
	waitForSingleCallWithin(t, conn, timeout)
	closeStarted = true
	go func() {
		c.close()
		close(closeDone)
	}()
	select {
	case <-closeDone:
	case <-time.After(timeout):
		t.Fatal("control close blocked on an unanswered heartbeat")
	}
}

func TestControlConnRejectsPublicationAfterClose(t *testing.T) {
	const timeout = 5 * time.Second
	registering := make(chan struct{})
	release := make(chan struct{})
	srv := newControlTestServer(t, func(srv *controlTestServer) {
		srv.registerReply = func(stream int, resp *framer) {
			close(registering)
			<-release
			resp.writeHeader(0, frm.OpReady, stream)
		}
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	s, err := newSessionCommon(*srv.cluster())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	c := createControlConn(s)
	s.control = c
	hosts, err := resolveInitialEndpoints(s.cfg.DNSResolver, s.cfg.Hosts, s.cfg.Port, s.logger)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := s.dial(s.ctx, hosts[0], s.controlConnConfig(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	setupDone := make(chan error, 1)
	go func() { setupDone <- c.setupConn(conn) }()
	select {
	case <-registering:
	case <-time.After(timeout):
		t.Fatal("control setup did not reach registration")
	}
	c.close()
	close(release)
	select {
	case err := <-setupDone:
		if err == nil {
			t.Error("control setup published a connection after shutdown")
		}
	case <-time.After(timeout):
		t.Fatal("control setup did not exit")
	}
	if !conn.Closed() {
		t.Error("late control connection was left open")
	}
}

func TestHostInfo_Lookup(t *testing.T) {
	t.Parallel()

	resolver := NewSimpleDNSResolver(true)

	tests := [...]struct {
		addr string
		ip   net.IP
	}{
		{"127.0.0.1", net.IPv4(127, 0, 0, 1)},
		{"localhost", net.IPv4(127, 0, 0, 1)}, // TODO: this may be host dependant
	}

	for i, test := range tests {
		hosts, err := resolveInitialEndpoint(resolver, test.addr, 1)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		host := hosts[0]
		if !host.ConnectAddress().Equal(test.ip) {
			t.Errorf("expected ip %v got %v for addr %q", test.ip, host.ConnectAddress(), test.addr)
		}
	}
}

func TestParseProtocol(t *testing.T) {
	t.Parallel()

	tests := [...]struct {
		err   error
		proto int
	}{
		{
			err: &protocolError{
				frame: frm.ErrorFrame{
					Code:    0x10,
					Message: "Invalid or unsupported protocol version (5); the lowest supported version is 3 and the greatest is 4",
				},
			},
			proto: protoVersion4,
		},
		{
			err: &protocolError{
				frame: frm.ErrorFrame{
					FrameHeader: frm.FrameHeader{
						Version: 0x83,
					},
					Code:    0x10,
					Message: "Invalid or unsupported protocol version: 5",
				},
			},
			proto: protoVersion3,
		},
	}

	for i, test := range tests {
		if proto := parseProtocolFromError(test.err); proto != test.proto {
			t.Errorf("%d: exepcted proto %d got %d", i, test.proto, proto)
		}
	}
}
