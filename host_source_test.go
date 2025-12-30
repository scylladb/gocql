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
)

func TestUnmarshalCassVersion(t *testing.T) {
	t.Parallel()

	tests := [...]struct {
		data    string
		version cassVersion
	}{
		{"3.2", cassVersion{Major: 3, Minor: 2, Patch: 0, Qualifier: ""}},
		{"2.10.1-SNAPSHOT", cassVersion{Major: 2, Minor: 10, Patch: 1, Qualifier: ""}},
		{"1.2.3", cassVersion{Major: 1, Minor: 2, Patch: 3, Qualifier: ""}},
		{"4.0-rc2", cassVersion{Major: 4, Minor: 0, Patch: 0, Qualifier: "rc2"}},
		{"4.3.2-rc1", cassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1"}},
		{"4.3.2-rc1-qualifier1", cassVersion{Major: 4, Minor: 3, Patch: 2, Qualifier: "rc1-qualifier1"}},
		{"4.3-rc1-qualifier1", cassVersion{Major: 4, Minor: 3, Patch: 0, Qualifier: "rc1-qualifier1"}},
	}

	for i, test := range tests {
		v := &cassVersion{}
		if err := v.UnmarshalCQL(nil, []byte(test.data)); err != nil {
			t.Errorf("%d: %v", i, err)
		} else if *v != test.version {
			t.Errorf("%d: expected %#+v got %#+v", i, test.version, *v)
		}
	}
}

func TestCassVersionBefore(t *testing.T) {
	t.Parallel()

	tests := [...]struct {
		version             cassVersion
		major, minor, patch int
		Qualifier           string
	}{
		{cassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
		{cassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 0, ""},
		{cassVersion{Major: 0, Minor: 0, Patch: 1, Qualifier: ""}, 0, 0, 0, ""},

		{cassVersion{Major: 1, Minor: 0, Patch: 0, Qualifier: ""}, 0, 1, 0, ""},
		{cassVersion{Major: 0, Minor: 1, Patch: 0, Qualifier: ""}, 0, 0, 1, ""},
		{cassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},

		{cassVersion{Major: 4, Minor: 1, Patch: 0, Qualifier: ""}, 3, 1, 2, ""},
	}

	for i, test := range tests {
		if test.version.Before(test.major, test.minor, test.patch) {
			t.Errorf("%d: expected v%d.%d.%d to be before %v", i, test.major, test.minor, test.patch, test.version)
		}
	}

}

func TestIsValidPeer(t *testing.T) {
	t.Parallel()

	host := &HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		rack:       "myRack",
		hostId:     "0",
		dataCenter: "datacenter",
		tokens:     []string{"0", "1"},
	}

	if !isValidPeer(host) {
		t.Errorf("expected %+v to be a valid peer", host)
	}

	host.rack = ""
	if isValidPeer(host) {
		t.Errorf("expected %+v to NOT be a valid peer", host)
	}
}

func TestIsZeroToken(t *testing.T) {
	t.Parallel()

	host := &HostInfo{
		rpcAddress: net.ParseIP("0.0.0.0"),
		rack:       "myRack",
		hostId:     "0",
		dataCenter: "datacenter",
		tokens:     []string{"0", "1"},
	}

	if isZeroToken(host) {
		t.Errorf("expected %+v to NOT be a zero-token host", host)
	}

	host.tokens = []string{}
	if !isZeroToken(host) {
		t.Errorf("expected %+v to be a zero-token host", host)
	}
}

func TestHostInfo_ConnectAddress(t *testing.T) {
	t.Parallel()

	var localhost = net.IPv4(127, 0, 0, 1)
	tests := []struct {
		name          string
		connectAddr   net.IP
		rpcAddr       net.IP
		broadcastAddr net.IP
		peer          net.IP
	}{
		{name: "rpc_address", rpcAddr: localhost},
		{name: "connect_address", connectAddr: localhost},
		{name: "broadcast_address", broadcastAddr: localhost},
		{name: "peer", peer: localhost},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &HostInfo{
				connectAddress:   test.connectAddr,
				rpcAddress:       test.rpcAddr,
				broadcastAddress: test.broadcastAddr,
				peer:             test.peer,
			}

			if addr := host.ConnectAddress(); !addr.Equal(localhost) {
				t.Fatalf("expected ConnectAddress to be %s got %s", localhost, addr)
			}
		})
	}
}

func TestAddressPort(t *testing.T) {
	t.Parallel()

	t.Run("IsValid", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			addr     AddressPort
			expected bool
		}{
			{
				name:     "valid IPv4 address with port",
				addr:     AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
				expected: true,
			},
			{
				name:     "valid IPv6 address with port",
				addr:     AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				expected: true,
			},
			{
				name:     "nil address",
				addr:     AddressPort{Address: nil, Port: 9042},
				expected: false,
			},
			{
				name:     "unspecified IPv4 address",
				addr:     AddressPort{Address: net.IPv4zero, Port: 9042},
				expected: false,
			},
			{
				name:     "unspecified IPv6 address",
				addr:     AddressPort{Address: net.IPv6unspecified, Port: 9042},
				expected: false,
			},
			{
				name:     "zero port",
				addr:     AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 0},
				expected: false,
			},
			{
				name:     "nil address and zero port",
				addr:     AddressPort{Address: nil, Port: 0},
				expected: false,
			},
			{
				name:     "empty AddressPort",
				addr:     AddressPort{},
				expected: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := test.addr.IsValid()
				if result != test.expected {
					t.Errorf("IsValid() = %v, expected %v for %+v", result, test.expected, test.addr)
				}
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		addr1 := AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042}
		addr2 := AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042}
		addr3 := AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 9042}
		addr4 := AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9043}

		tests := []struct {
			name     string
			a        AddressPort
			b        AddressPort
			expected bool
		}{
			{
				name:     "equal addresses and ports",
				a:        addr1,
				b:        addr2,
				expected: true,
			},
			{
				name:     "different addresses, same port",
				a:        addr1,
				b:        addr3,
				expected: false,
			},
			{
				name:     "same address, different ports",
				a:        addr1,
				b:        addr4,
				expected: false,
			},
			{
				name:     "IPv6 addresses equal",
				a:        AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				b:        AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				expected: true,
			},
			{
				name:     "empty",
				a:        AddressPort{},
				b:        AddressPort{},
				expected: true,
			},
			{
				name:     "empty, non-empty",
				a:        AddressPort{},
				b:        AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				expected: false,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := test.a.Equal(test.b)
				if result != test.expected {
					t.Errorf("Equal() = %v, expected %v for a=%+v, b=%+v", result, test.expected, test.a, test.b)
				}
			})
		}
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			addr     AddressPort
			expected string
		}{
			{
				name:     "IPv4 address",
				addr:     AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
				expected: "127.0.0.1:9042",
			},
			{
				name:     "IPv6 address",
				addr:     AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				expected: "::1:9042",
			},
			{
				name:     "different port",
				addr:     AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 8080},
				expected: "192.168.1.1:8080",
			},
			{
				name:     "nil address",
				addr:     AddressPort{Address: nil, Port: 9042},
				expected: "<nil>:9042",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := test.addr.String()
				if result != test.expected {
					t.Errorf("String() = %q, expected %q", result, test.expected)
				}
			})
		}
	})

	t.Run("ToNetAddr", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			addr     AddressPort
			expected string
		}{
			{
				name:     "IPv4 address",
				addr:     AddressPort{Address: net.IPv4(127, 0, 0, 1), Port: 9042},
				expected: "127.0.0.1:9042",
			},
			{
				name:     "IPv6 address",
				addr:     AddressPort{Address: net.ParseIP("::1"), Port: 9042},
				expected: "[::1]:9042",
			},
			{
				name:     "IPv6 address with zone",
				addr:     AddressPort{Address: net.ParseIP("fe80::1"), Port: 9043},
				expected: "[fe80::1]:9043",
			},
			{
				name:     "different port",
				addr:     AddressPort{Address: net.IPv4(192, 168, 1, 1), Port: 8080},
				expected: "192.168.1.1:8080",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				result := test.addr.ToNetAddr()
				if result != test.expected {
					t.Errorf("ToNetAddr() = %q, expected %q", result, test.expected)
				}
			})
		}
	})
}

func TestHostInfoBuilder(t *testing.T) {
	t.Parallel()

	t.Run("Build", func(t *testing.T) {
		t.Run("basic fields", func(t *testing.T) {
			t.Parallel()

			builder := HostInfoBuilder{
				HostId:        "host-123",
				DataCenter:    "dc1",
				Rack:          "rack1",
				Tokens:        []string{"token1", "token2"},
				Port:          9042,
				Workload:      "Analytics",
				DseVersion:    "6.8.0",
				ClusterName:   "test-cluster",
				Partitioner:   "Murmur3Partitioner",
				Hostname:      "node1.example.com",
				SchemaVersion: "schema-v1",
			}

			host := builder.Build()

			if host.HostID() != builder.HostId {
				t.Errorf("HostID() = %q, expected %q", host.HostID(), builder.HostId)
			}
			if host.DataCenter() != builder.DataCenter {
				t.Errorf("DataCenter() = %q, expected %q", host.DataCenter(), builder.DataCenter)
			}
			if host.Rack() != builder.Rack {
				t.Errorf("Rack() = %q, expected %q", host.Rack(), builder.Rack)
			}
			if host.Port() != builder.Port {
				t.Errorf("Port() = %d, expected %d", host.Port(), builder.Port)
			}
			if host.WorkLoad() != builder.Workload {
				t.Errorf("WorkLoad() = %q, expected %q", host.WorkLoad(), builder.Workload)
			}
			if host.DSEVersion() != builder.DseVersion {
				t.Errorf("DSEVersion() = %q, expected %q", host.DSEVersion(), builder.DseVersion)
			}
			if host.ClusterName() != builder.ClusterName {
				t.Errorf("ClusterName() = %q, expected %q", host.ClusterName(), builder.ClusterName)
			}
			if host.Partitioner() != builder.Partitioner {
				t.Errorf("Partitioner() = %q, expected %q", host.Partitioner(), builder.Partitioner)
			}
			if len(host.Tokens()) != len(builder.Tokens) {
				t.Errorf("len(Tokens()) = %d, expected %d", len(host.Tokens()), len(builder.Tokens))
			}
			for i, token := range host.Tokens() {
				if token != builder.Tokens[i] {
					t.Errorf("Tokens()[%d] = %q, expected %q", i, token, builder.Tokens[i])
				}
			}
		})

		t.Run("IP addresses", func(t *testing.T) {
			t.Parallel()

			connectAddr := net.IPv4(192, 168, 1, 1)
			broadcastAddr := net.IPv4(192, 168, 1, 2)
			preferredIP := net.IPv4(192, 168, 1, 3)
			rpcAddr := net.IPv4(192, 168, 1, 4)
			peer := net.IPv4(192, 168, 1, 5)
			listenAddr := net.IPv4(192, 168, 1, 6)

			builder := HostInfoBuilder{
				ConnectAddress:   connectAddr,
				BroadcastAddress: broadcastAddr,
				PreferredIP:      preferredIP,
				RpcAddress:       rpcAddr,
				Peer:             peer,
				ListenAddress:    listenAddr,
				Port:             9042,
			}

			host := builder.Build()

			if !host.UntranslatedConnectAddress().Equal(connectAddr) {
				t.Errorf("UntranslatedConnectAddress() = %v, expected %v", host.UntranslatedConnectAddress(), connectAddr)
			}
			if !host.BroadcastAddress().Equal(broadcastAddr) {
				t.Errorf("BroadcastAddress() = %v, expected %v", host.BroadcastAddress(), broadcastAddr)
			}
			if !host.PreferredIP().Equal(preferredIP) {
				t.Errorf("PreferredIP() = %v, expected %v", host.PreferredIP(), preferredIP)
			}
			if !host.RPCAddress().Equal(rpcAddr) {
				t.Errorf("RPCAddress() = %v, expected %v", host.RPCAddress(), rpcAddr)
			}
			if !host.Peer().Equal(peer) {
				t.Errorf("Peer() = %v, expected %v", host.Peer(), peer)
			}
			if !host.ListenAddress().Equal(listenAddr) {
				t.Errorf("ListenAddress() = %v, expected %v", host.ListenAddress(), listenAddr)
			}
		})

		t.Run("translated addresses", func(t *testing.T) {
			t.Parallel()

			translatedAddrs := &translatedAddresses{
				CQL: AddressPort{
					Address: net.IPv4(10, 0, 0, 1),
					Port:    9042,
				},
				ShardAware: AddressPort{
					Address: net.IPv4(10, 0, 0, 2),
					Port:    19042,
				},
				ShardAwareTLS: AddressPort{
					Address: net.IPv4(10, 0, 0, 3),
					Port:    19043,
				},
			}

			builder := HostInfoBuilder{
				TranslatedAddresses: translatedAddrs,
				Port:                9042,
			}

			host := builder.Build()

			// ConnectAddress should use translated CQL address
			expectedAddr := translatedAddrs.CQL.Address
			if !host.ConnectAddress().Equal(expectedAddr) {
				t.Errorf("ConnectAddress() = %v, expected %v", host.ConnectAddress(), expectedAddr)
			}

			// Verify translated addresses are set
			retrievedAddrs := host.getTranslatedConnectionInfo()
			if retrievedAddrs == nil {
				t.Fatal("getTranslatedConnectionInfo() returned nil")
			}
			if !retrievedAddrs.Equal(translatedAddrs) {
				t.Errorf("translated addresses not equal: got %+v, expected %+v", retrievedAddrs, translatedAddrs)
			}
		})

		t.Run("version", func(t *testing.T) {
			t.Parallel()

			version := cassVersion{
				Major:     4,
				Minor:     0,
				Patch:     3,
				Qualifier: "rc1",
			}

			builder := HostInfoBuilder{
				Version: version,
				Port:    9042,
			}

			host := builder.Build()

			if host.Version() != version {
				t.Errorf("Version() = %+v, expected %+v", host.Version(), version)
			}
		})

		t.Run("empty builder", func(t *testing.T) {
			t.Parallel()

			builder := HostInfoBuilder{}
			host := builder.Build()

			if host.HostID() != "" {
				t.Errorf("HostID() = %q, expected empty string", host.HostID())
			}
			if host.DataCenter() != "" {
				t.Errorf("DataCenter() = %q, expected empty string", host.DataCenter())
			}
			if host.Port() != 0 {
				t.Errorf("Port() = %d, expected 0", host.Port())
			}
			if host.Tokens() != nil {
				t.Errorf("Tokens() = %v, expected nil", host.Tokens())
			}
		})

		t.Run("all fields populated", func(t *testing.T) {
			t.Parallel()

			translatedAddrs := &translatedAddresses{
				CQL: AddressPort{
					Address: net.IPv4(10, 0, 0, 1),
					Port:    9042,
				},
			}

			version := cassVersion{
				Major: 3,
				Minor: 11,
				Patch: 4,
			}

			builder := HostInfoBuilder{
				TranslatedAddresses: translatedAddrs,
				Workload:            "Cassandra",
				HostId:              "uuid-host-456",
				SchemaVersion:       "schema-v2",
				Hostname:            "cassandra-node.local",
				ClusterName:         "production-cluster",
				Partitioner:         "Murmur3Partitioner",
				Rack:                "rack2",
				DseVersion:          "6.8.1",
				DataCenter:          "dc2",
				ConnectAddress:      net.IPv4(192, 168, 2, 1),
				BroadcastAddress:    net.IPv4(192, 168, 2, 2),
				PreferredIP:         net.IPv4(192, 168, 2, 3),
				RpcAddress:          net.IPv4(192, 168, 2, 4),
				Peer:                net.IPv4(192, 168, 2, 5),
				ListenAddress:       net.IPv4(192, 168, 2, 6),
				Tokens:              []string{"token-a", "token-b", "token-c"},
				Version:             version,
				Port:                9043,
			}

			host := builder.Build()

			// Verify all fields
			if host.WorkLoad() != builder.Workload {
				t.Errorf("WorkLoad() = %q, expected %q", host.WorkLoad(), builder.Workload)
			}
			if host.HostID() != builder.HostId {
				t.Errorf("HostID() = %q, expected %q", host.HostID(), builder.HostId)
			}
			if host.ClusterName() != builder.ClusterName {
				t.Errorf("ClusterName() = %q, expected %q", host.ClusterName(), builder.ClusterName)
			}
			if host.Partitioner() != builder.Partitioner {
				t.Errorf("Partitioner() = %q, expected %q", host.Partitioner(), builder.Partitioner)
			}
			if host.Rack() != builder.Rack {
				t.Errorf("Rack() = %q, expected %q", host.Rack(), builder.Rack)
			}
			if host.DSEVersion() != builder.DseVersion {
				t.Errorf("DSEVersion() = %q, expected %q", host.DSEVersion(), builder.DseVersion)
			}
			if host.DataCenter() != builder.DataCenter {
				t.Errorf("DataCenter() = %q, expected %q", host.DataCenter(), builder.DataCenter)
			}
			if !host.UntranslatedConnectAddress().Equal(builder.ConnectAddress) {
				t.Errorf("UntranslatedConnectAddress() = %v, expected %v", host.UntranslatedConnectAddress(), builder.ConnectAddress)
			}
			if !host.BroadcastAddress().Equal(builder.BroadcastAddress) {
				t.Errorf("BroadcastAddress() = %v, expected %v", host.BroadcastAddress(), builder.BroadcastAddress)
			}
			if !host.PreferredIP().Equal(builder.PreferredIP) {
				t.Errorf("PreferredIP() = %v, expected %v", host.PreferredIP(), builder.PreferredIP)
			}
			if !host.RPCAddress().Equal(builder.RpcAddress) {
				t.Errorf("RPCAddress() = %v, expected %v", host.RPCAddress(), builder.RpcAddress)
			}
			if !host.Peer().Equal(builder.Peer) {
				t.Errorf("Peer() = %v, expected %v", host.Peer(), builder.Peer)
			}
			if !host.ListenAddress().Equal(builder.ListenAddress) {
				t.Errorf("ListenAddress() = %v, expected %v", host.ListenAddress(), builder.ListenAddress)
			}
			if host.Version() != builder.Version {
				t.Errorf("Version() = %+v, expected %+v", host.Version(), builder.Version)
			}
			if host.Port() != builder.Port {
				t.Errorf("Port() = %d, expected %d", host.Port(), builder.Port)
			}
			if len(host.Tokens()) != len(builder.Tokens) {
				t.Errorf("len(Tokens()) = %d, expected %d", len(host.Tokens()), len(builder.Tokens))
			}

			// Verify ConnectAddress uses translated address
			if !host.ConnectAddress().Equal(translatedAddrs.CQL.Address) {
				t.Errorf("ConnectAddress() = %v, expected %v", host.ConnectAddress(), translatedAddrs.CQL.Address)
			}
		})

		t.Run("nil IP addresses", func(t *testing.T) {
			t.Parallel()

			builder := HostInfoBuilder{
				ConnectAddress:   nil,
				BroadcastAddress: nil,
				PreferredIP:      nil,
				RpcAddress:       nil,
				Peer:             nil,
				ListenAddress:    nil,
				Port:             9042,
			}

			host := builder.Build()

			if host.UntranslatedConnectAddress() != nil {
				t.Errorf("UntranslatedConnectAddress() = %v, expected nil", host.UntranslatedConnectAddress())
			}
			if host.BroadcastAddress() != nil {
				t.Errorf("BroadcastAddress() = %v, expected nil", host.BroadcastAddress())
			}
			if host.PreferredIP() != nil {
				t.Errorf("PreferredIP() = %v, expected nil", host.PreferredIP())
			}
			if host.RPCAddress() != nil {
				t.Errorf("RPCAddress() = %v, expected nil", host.RPCAddress())
			}
			if host.Peer() != nil {
				t.Errorf("Peer() = %v, expected nil", host.Peer())
			}
			if host.ListenAddress() != nil {
				t.Errorf("ListenAddress() = %v, expected nil", host.ListenAddress())
			}
		})

		t.Run("connect address priority without translated addresses", func(t *testing.T) {
			t.Parallel()

			// Test that ConnectAddress follows the priority order when translated addresses are not set
			tests := []struct {
				name         string
				builder      HostInfoBuilder
				expectedAddr net.IP
				shouldPanic  bool
			}{
				{
					name: "connectAddress takes priority",
					builder: HostInfoBuilder{
						ConnectAddress:   net.IPv4(1, 1, 1, 1),
						RpcAddress:       net.IPv4(2, 2, 2, 2),
						PreferredIP:      net.IPv4(3, 3, 3, 3),
						BroadcastAddress: net.IPv4(4, 4, 4, 4),
						Peer:             net.IPv4(5, 5, 5, 5),
						Port:             9042,
					},
					expectedAddr: net.IPv4(1, 1, 1, 1),
				},
				{
					name: "rpcAddress when connectAddress is nil",
					builder: HostInfoBuilder{
						RpcAddress:       net.IPv4(2, 2, 2, 2),
						PreferredIP:      net.IPv4(3, 3, 3, 3),
						BroadcastAddress: net.IPv4(4, 4, 4, 4),
						Peer:             net.IPv4(5, 5, 5, 5),
						Port:             9042,
					},
					expectedAddr: net.IPv4(2, 2, 2, 2),
				},
				{
					name: "preferredIP when connectAddress and rpcAddress are nil",
					builder: HostInfoBuilder{
						PreferredIP:      net.IPv4(3, 3, 3, 3),
						BroadcastAddress: net.IPv4(4, 4, 4, 4),
						Peer:             net.IPv4(5, 5, 5, 5),
						Port:             9042,
					},
					expectedAddr: net.IPv4(3, 3, 3, 3),
				},
				{
					name: "broadcastAddress when others are nil",
					builder: HostInfoBuilder{
						BroadcastAddress: net.IPv4(4, 4, 4, 4),
						Peer:             net.IPv4(5, 5, 5, 5),
						Port:             9042,
					},
					expectedAddr: net.IPv4(4, 4, 4, 4),
				},
				{
					name: "peer when all others are nil",
					builder: HostInfoBuilder{
						Peer: net.IPv4(5, 5, 5, 5),
						Port: 9042,
					},
					expectedAddr: net.IPv4(5, 5, 5, 5),
				},
				{
					name: "no valid addresses panics",
					builder: HostInfoBuilder{
						Port: 9042,
					},
					shouldPanic: true,
				},
			}

			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					host := test.builder.Build()

					if test.shouldPanic {
						defer func() {
							if r := recover(); r == nil {
								t.Error("ConnectAddress() should have panicked but did not")
							}
						}()
					}

					addr := host.ConnectAddress()
					if !test.shouldPanic && !addr.Equal(test.expectedAddr) {
						t.Errorf("ConnectAddress() = %v, expected %v", addr, test.expectedAddr)
					}
				})
			}
		})
	})
}
