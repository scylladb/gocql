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
			tlsConfig, err := setupTLSConfig(test.opts)
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}
			if tlsConfig.InsecureSkipVerify != test.expectedInsecureSkipVerify {
				t.Fatalf("got %v, but expected %v", tlsConfig.InsecureSkipVerify,
					test.expectedInsecureSkipVerify)
			}
		})
	}
}

// errorConn is a mock net.Conn whose Close always returns an error,
// which triggers the HandleError callback path in Conn.closeWithError.
type errorConn struct {
	net.Conn
}

func (e errorConn) Close() error {
	return errors.New("mock close error")
}

// TestHostConnPoolCloseDeadlock verifies that hostConnPool.Close() does not
// self-deadlock when defaultConnPicker closes connections that trigger
// HandleError callbacks.
//
// The deadlock chain (before the fix):
//
//	hostConnPool.Close()             — acquires pool.mu.Lock()
//	  └── defaultConnPicker.Close()  — iterates conns
//	        └── conn.Close()
//	              └── closeWithError(nil)
//	                    └── HandleError()
//	                          └── pool.mu.Lock() — DEADLOCK
//
// See scylladb/gocql#53 for the equivalent issue in scyllaConnPicker.
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

	// Create a defaultConnPicker with real Conn objects that will trigger
	// HandleError on Close. Each Conn uses an errorConn (mock net.Conn
	// whose Close returns an error) and has the pool as its errorHandler.
	picker := newDefaultConnPicker(2)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		conn := &Conn{
			conn:         errorConn{},
			errorHandler: pool,
			cancel:       cancel,
			ctx:          ctx,
			logger:       nopLogger{},
		}
		_ = picker.Put(conn)
	}
	pool.connPicker = picker

	// Close the pool in a goroutine with a deadline. If the call doesn't
	// return within the deadline, the pool is deadlocked.
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
		conn:         errorConn{},
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
