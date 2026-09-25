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

	"github.com/gocql/gocql/internal/tests"
)

func TestIdentityAddressTranslator_NilAddrAndZeroPort(t *testing.T) {
	t.Parallel()

	var tr AddressTranslator = IdentityTranslator()
	hostIP := net.ParseIP("")
	if hostIP != nil {
		t.Errorf("expected host ip to be (nil) but was (%+v) instead", hostIP)
	}

	addr, port := tr.Translate(hostIP, 0)
	if addr != nil {
		t.Errorf("expected translated host to be (nil) but was (%+v) instead", addr)
	}
	tests.AssertEqual(t, "translated port", 0, port)
}

func TestIdentityAddressTranslator_HostProvided(t *testing.T) {
	t.Parallel()

	var tr AddressTranslator = IdentityTranslator()
	hostIP := net.ParseIP("10.1.2.3")
	if hostIP == nil {
		t.Error("expected host ip not to be (nil)")
	}

	addr, port := tr.Translate(hostIP, 9042)
	if !hostIP.Equal(addr) {
		t.Errorf("expected translated addr to be (%+v) but was (%+v) instead", hostIP, addr)
	}
	tests.AssertEqual(t, "translated port", 9042, port)
}

func TestTranslateHostAddresses_NoScyllaPorts(t *testing.T) {
	t.Parallel()

	translator := AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		return net.ParseIP("10.10.10.10"), 9142
	})
	host := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           9042,
	}.Build()

	translated, err := translateHostAddresses(translator, &host, nil)

	tests.AssertNil(t, "should return no error", err)
	tests.AssertTrue(t, "translated CQL address", net.ParseIP("10.10.10.10").Equal(translated.CQL.Address))
	tests.AssertEqual(t, "translated CQL port", uint16(9142), translated.CQL.Port)
	tests.AssertTrue(t, "shard aware empty address", len(translated.ShardAware.Address) == 0)
	tests.AssertEqual(t, "shard aware empty port", uint16(0), translated.ShardAware.Port)
	tests.AssertTrue(t, "shard aware tls empty address", len(translated.ShardAwareTLS.Address) == 0)
	tests.AssertEqual(t, "shard aware tls empty port", uint16(0), translated.ShardAwareTLS.Port)
}

// AddressPort.Port is a uint16 but HostInfo.port is an int, so a port outside
// the uint16 range must be reported rather than wrapped: 65536 would otherwise
// translate to 0 and 65537 to 1.
func TestTranslateHostAddresses_PortOutOfRange(t *testing.T) {
	t.Parallel()

	translator := AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		t.Error("translator should not be called for an out-of-range port")
		return addr, port
	})

	for _, port := range []int{0, -1, 65536, 65537, 0x10000 + 9042} {
		host := HostInfoBuilder{
			ConnectAddress: net.ParseIP("10.0.0.1"),
			Port:           port,
		}.Build()

		if _, err := translateHostAddresses(translator, &host, nil); err == nil {
			t.Errorf("port %d: expected an error, got nil", port)
		}
	}

	// A host with a bad port may well be malformed in other ways too, so the
	// error path must not reach for anything that assumes a well-formed host:
	// HostInfo.ConnectAddress panics when there is no valid connect address.
	hostNoAddr := HostInfoBuilder{Port: 70000}.Build()
	if _, err := translateHostAddresses(translator, &hostNoAddr, nil); err == nil {
		t.Error("host without a connect address: expected an error, got nil")
	}
}

// A legacy AddressTranslator returns an int port, which AddressPort narrows to
// uint16. An out-of-range return must be reported rather than wrapped: a host on
// a perfectly valid port would otherwise be dialed on port 1 because the
// translator handed back 65537.
func TestTranslateHostAddresses_TranslatorPortOutOfRange(t *testing.T) {
	t.Parallel()

	for _, translated := range []int{0, -1, 65536, 65537, 0x10000 + 9042} {
		translator := AddressTranslatorFunc(func(addr net.IP, _ int) (net.IP, int) {
			return addr, translated
		})
		host := HostInfoBuilder{
			ConnectAddress: net.ParseIP("10.0.0.1"),
			Port:           9042,
		}.Build()

		got, err := translateHostAddresses(translator, &host, nil)
		if err == nil {
			t.Errorf("translated port %d: expected an error, got port %d", translated, got.CQL.Port)
		}
	}
}

// The shard-aware ports go through the same translator, so an out-of-range
// return has to be caught there too, not only on the CQL address.
func TestTranslateHostAddresses_TranslatorPortOutOfRangeShardAware(t *testing.T) {
	t.Parallel()

	translator := AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
		if port == 19042 {
			return addr, 65536
		}
		return addr, port
	})
	host := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           9042,
	}.Build()
	host.setScyllaFeatures(ScyllaHostFeatures{shardAwarePort: 19042})

	if _, err := translateHostAddresses(translator, &host, nil); err == nil {
		t.Error("expected an error for an out-of-range shard aware port, got nil")
	}
}

// The cql address is dialed unconditionally - both dialers overwrite the address
// they built from host.Port() with the translated one - so a translator that
// zeroes that port has to be reported here. It is reachable through either API:
// AddressTranslatorV2 returns an AddressPort directly, the legacy one by
// returning 0 from Translate.
func TestTranslateHostAddresses_TranslatorZeroesCQLPort(t *testing.T) {
	t.Parallel()

	translators := map[string]AddressTranslator{
		"v1": AddressTranslatorFunc(func(addr net.IP, _ int) (net.IP, int) {
			return addr, 0
		}),
		"v2": AddressTranslatorFuncV2(func(_ string, addr AddressPort) AddressPort {
			return AddressPort{Address: addr.Address, Port: 0}
		}),
	}

	for name, translator := range translators {
		host := HostInfoBuilder{
			ConnectAddress: net.ParseIP("10.0.0.1"),
			Port:           9042,
		}.Build()

		got, err := translateHostAddresses(translator, &host, nil)
		if err == nil {
			t.Errorf("%s: expected an error, got cql address %q", name, got.CQL)
		}
	}
}

// The shard-aware addresses are optional: scyllaDialer only uses them when they
// are IsValid, which a zero port is not. A translator that zeroes one is saying
// "no shard-aware port here", so it must degrade to plain dialing rather than
// failing the whole host the way a zeroed cql port does.
func TestTranslateHostAddresses_TranslatorZeroesShardAwarePort(t *testing.T) {
	t.Parallel()

	translator := AddressTranslatorFuncV2(func(_ string, addr AddressPort) AddressPort {
		if addr.Port == 9042 {
			return addr
		}
		return AddressPort{Address: addr.Address, Port: 0}
	})
	host := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           9042,
	}.Build()
	host.setScyllaFeatures(ScyllaHostFeatures{shardAwarePort: 19042, shardAwarePortTLS: 19043})

	got, err := translateHostAddresses(translator, &host, nil)
	tests.AssertNil(t, "should return no error", err)
	tests.AssertEqual(t, "cql port survives", uint16(9042), got.CQL.Port)
	tests.AssertTrue(t, "shard aware is not usable", !got.ShardAware.IsValid())
	tests.AssertTrue(t, "shard aware tls is not usable", !got.ShardAwareTLS.IsValid())
}

// AddressTranslatorV2 hands back an AddressPort, whose port is already a uint16,
// so it stays exempt from the overflow check on the legacy path.
func TestTranslateHostAddresses_TranslatorV2MaxPort(t *testing.T) {
	t.Parallel()

	translator := AddressTranslatorFuncV2(func(_ string, addr AddressPort) AddressPort {
		return AddressPort{Address: addr.Address, Port: maxPort}
	})
	host := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           9042,
	}.Build()

	got, err := translateHostAddresses(translator, &host, nil)
	tests.AssertNil(t, "should return no error", err)
	tests.AssertEqual(t, "translated CQL port", uint16(maxPort), got.CQL.Port)
}

func TestTranslateHostAddresses_WithScyllaPorts(t *testing.T) {
	t.Parallel()

	translatedIP := net.ParseIP("192.0.2.10")
	translator := AddressTranslatorFuncV2(func(hostID string, addr AddressPort) AddressPort {
		if hostID != "a0000000-0000-0000-0000-000000000001" {
			t.Errorf("expected host id %q, got %q", "a0000000-0000-0000-0000-000000000001", hostID)
		}
		return AddressPort{
			Address: translatedIP,
			Port:    addr.Port + 1,
		}
	})
	host := HostInfoBuilder{
		ConnectAddress: net.ParseIP("10.0.0.1"),
		Port:           9042,
		HostId:         "a0000000-0000-0000-0000-000000000001",
	}.Build()
	host.setScyllaFeatures(ScyllaHostFeatures{
		shardAwarePort:    19042,
		shardAwarePortTLS: 19043,
	})

	translated, err := translateHostAddresses(translator, &host, nil)

	tests.AssertNil(t, "should return no error", err)
	tests.AssertTrue(t, "translated CQL address", translatedIP.Equal(translated.CQL.Address))
	tests.AssertEqual(t, "translated CQL port", uint16(9043), translated.CQL.Port)
	tests.AssertTrue(t, "translated shard aware address", translatedIP.Equal(translated.ShardAware.Address))
	tests.AssertEqual(t, "translated shard aware port", uint16(19043), translated.ShardAware.Port)
	tests.AssertTrue(t, "translated shard aware tls address", translatedIP.Equal(translated.ShardAwareTLS.Address))
	tests.AssertEqual(t, "translated shard aware tls port", uint16(19044), translated.ShardAwareTLS.Port)
}
