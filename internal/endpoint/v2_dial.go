// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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

package endpoint

import (
	"context"

	"net"
	"net/netip"

	"time"
)

// DialTarget is a resolved raw connection target. Hooks must honor the selected
// endpoint and source, or return an error. TLS and protocol setup follow dialing.
type DialTarget struct {
	Host             HostSnapshot
	Network, Address string
	LocalAddress     netip.AddrPort
	ShardID          Option[uint32]
}

type Dialer interface {
	DialContext(context.Context, DialTarget) (net.Conn, error)
}

// DialerFunc adapts a context-aware raw connection hook to Dialer.
type DialerFunc func(context.Context, DialTarget) (net.Conn, error)

func (f DialerFunc) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	return f(ctx, target)
}

// DefaultDialer opens the selected target with Go's network dialer. It also
// supports Unix targets; TCP source selection is never applied to Unix sockets.
type DefaultDialer struct{ ConnectTimeout, SocketKeepalive time.Duration }

func (d DefaultDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	keepalive := d.SocketKeepalive
	if keepalive == 0 {
		keepalive = -1
	}
	n := net.Dialer{Timeout: d.ConnectTimeout, KeepAlive: keepalive}
	if target.LocalAddress.IsValid() {
		if target.Network != "tcp" && target.Network != "tcp4" && target.Network != "tcp6" {
			return nil, typeValidation("local address", "source selection requires TCP")
		}
		n.LocalAddr = net.TCPAddrFromAddrPort(target.LocalAddress)
	}
	return n.DialContext(ctx, target.Network, target.Address)
}
