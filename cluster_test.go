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
	"crypto/tls"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql/internal/tests"
)

func TestNewCluster_Defaults(t *testing.T) {
	t.Parallel()

	cfg := NewCluster()
	tests.AssertEqual(t, "cluster config cql version", "3.0.0", cfg.CQLVersion)
	tests.AssertEqual(t, "cluster config timeout", 11*time.Second, cfg.Timeout)
	tests.AssertEqual(t, "cluster config port", 0, cfg.Port)
	tests.AssertEqual(t, "cluster config num-conns", 2, cfg.NumConns)
	tests.AssertEqual(t, "cluster config consistency", Quorum, cfg.Consistency)
	tests.AssertEqual(t, "cluster config max prepared statements", defaultMaxPreparedStmts, cfg.MaxPreparedStmts)
	tests.AssertEqual(t, "cluster config max routing key info", 1000, cfg.MaxRoutingKeyInfo)
	tests.AssertEqual(t, "cluster config page-size", 5000, cfg.PageSize)
	tests.AssertEqual(t, "cluster config default timestamp", true, cfg.DefaultTimestamp)
	tests.AssertEqual(t, "cluster config max wait schema agreement", 60*time.Second, cfg.MaxWaitSchemaAgreement)
	tests.AssertEqual(t, "cluster config reconnect interval", 60*time.Second, cfg.ReconnectInterval)
	tests.AssertTrue(t, "cluster config conviction policy",
		reflect.DeepEqual(&SimpleConvictionPolicy{}, cfg.ConvictionPolicy))
	tests.AssertTrue(t, "cluster config reconnection policy",
		reflect.DeepEqual(&ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second}, cfg.ReconnectionPolicy))
}

func TestNewCluster_WithHosts(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("addr1", "addr2")
	tests.AssertEqual(t, "cluster config hosts length", 2, len(cfg.Hosts))
	tests.AssertEqual(t, "cluster config host 0", "addr1", cfg.Hosts[0])
	tests.AssertEqual(t, "cluster config host 1", "addr2", cfg.Hosts[1])
}

func TestValidateAndInitSSLDoesNotShareTLSConfigBetweenConfigCopies(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("127.0.0.1")
	cfg.SslOpts = &SslOptions{
		Config: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	tlsCfg := *cfg
	if err := tlsCfg.ValidateAndInitSSL(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tlsCfg.getActualTLSConfig() == nil {
		t.Fatal("expected copied TLS config to initialize TLS")
	}
	if cfg.getActualTLSConfig() != nil {
		t.Fatal("expected original config not to retain copied TLS state")
	}

	noTLSCfg := *cfg
	noTLSCfg.SslOpts = nil
	if err := noTLSCfg.ValidateAndInitSSL(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if noTLSCfg.getActualTLSConfig() != nil {
		t.Fatal("expected copied config with nil SslOpts not to use TLS")
	}
}

// TestValidate_HostPortNormalization covers the port-learning logic in Validate():
// when cfg.Port is 0 (not explicitly set) the driver learns it from cfg.Hosts.
func TestValidate_HostPortNormalization(t *testing.T) {
	t.Parallel()

	makeValid := func(hosts ...string) *ClusterConfig {
		cfg := NewCluster(hosts...)
		return cfg
	}

	t.Run("single host with explicit port sets cfg.Port", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
	})

	t.Run("multiple hosts with the same explicit port", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142", "10.0.0.2:9142")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
	})

	t.Run("hosts without explicit port default to 9042", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1", "10.0.0.2")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
	})

	t.Run("mixed: some hosts with port, some without returns error", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142", "10.0.0.2")
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected an error for mixed port/no-port hosts, got nil")
		}
	})

	t.Run("hosts with conflicting explicit ports returns error", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142", "10.0.0.2:9043")
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected an error for conflicting host ports, got nil")
		}
	})

	t.Run("host with invalid port string returns error", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:notaport")
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected an error for invalid port, got nil")
		}
	})

	t.Run("IPv6 bare address without port defaults to 9042", func(t *testing.T) {
		t.Parallel()
		// A bare IPv6 address (no brackets, no port) is not parseable by
		// net.SplitHostPort, so it is treated as a portless entry and the
		// driver falls back to the default port 9042.
		cfg := makeValid("::1")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
	})

	t.Run("IPv6 bracketed address with port sets cfg.Port", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("[::1]:9142")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
	})

	t.Run("multiple IPv6 hosts with the same explicit port", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("[::1]:9142", "[::2]:9142")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
	})

	t.Run("mixed IPv4 and IPv6 hosts with the same port", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142", "[::1]:9142")
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9142, cfg.Port)
	})

	t.Run("mixed IPv4 and IPv6 hosts with conflicting ports returns error", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142", "[::1]:9043")
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected an error for conflicting IPv4/IPv6 host ports, got nil")
		}
	})

	t.Run("user-set Port skips learning from hosts", func(t *testing.T) {
		t.Parallel()
		// User explicitly sets Port=9042 (same as default) while hosts say 9142.
		// The driver must NOT override the explicit Port.
		cfg := makeValid("10.0.0.1:9142")
		cfg.Port = 9042
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
	})

	t.Run("client routes enabled skips learning from hosts", func(t *testing.T) {
		t.Parallel()
		cfg := makeValid("10.0.0.1:9142")
		cfg.ClientRoutesConfig = &ClientRoutesConfig{
			TableName: "system.client_routes",
			Endpoints: []ClientRoutesEndpoint{{ConnectionID: "conn-1", ConnectionAddr: "10.0.0.1:9142"}},
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		tests.AssertEqual(t, "cfg.Port after Validate", 9042, cfg.Port)
	})
}

func TestClusterConfig_translateAddressAndPort_NilTranslator(t *testing.T) {
	t.Parallel()
	hh := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           1234,
	}.Build()
	newAddr, err := translateAddressPort(nil, &hh, AddressPort{
		Address: hh.UntranslatedConnectAddress(),
		Port:    uint16(hh.Port()),
	}, nil)
	tests.AssertNil(t, "should return no error", err)
	tests.AssertTrue(t, "same address as provided", net.ParseIP("10.0.0.1").Equal(newAddr.Address))
	tests.AssertEqual(t, "translated host and port", uint16(1234), newAddr.Port)
}

func TestClusterConfig_translateAddressAndPort_EmptyAddr(t *testing.T) {
	t.Parallel()

	translator := staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	hh := HostInfoBuilder{
		ConnectAddress: []byte{},
		Port:           0,
	}.Build()
	newAddr, err := translateAddressPort(translator, &hh, AddressPort{
		Address: hh.UntranslatedConnectAddress(),
		Port:    uint16(hh.Port()),
	}, nil)
	tests.AssertNil(t, "should return no error", err)
	tests.AssertTrue(t, "translated address is still empty", len(newAddr.Address) == 0)
	tests.AssertEqual(t, "translated port", uint16(0), newAddr.Port)
}

func TestClusterConfig_translateAddressAndPort_Success(t *testing.T) {
	t.Parallel()

	translator := staticAddressTranslator(net.ParseIP("10.10.10.10"), 5432)
	hh := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           2345,
	}.Build()
	newAddr, err := translateAddressPort(translator, &hh, AddressPort{
		Address: hh.UntranslatedConnectAddress(),
		Port:    uint16(hh.Port()),
	}, nil)
	tests.AssertNil(t, "should return no error", err)
	tests.AssertTrue(t, "translated address", net.ParseIP("10.10.10.10").Equal(newAddr.Address))
	tests.AssertEqual(t, "translated port", uint16(5432), newAddr.Port)
}
