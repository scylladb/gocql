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
	"net"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

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
