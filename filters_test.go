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

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"net"
	"testing"
)

func TestFilter_WhiteList(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1", "127.0.0.2")
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(func() *HostInfo {
			opaqueStorageValue := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValue).InternalRpcAddress = test.addr
			return opaqueStorageValue
		}()) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_AllowAll(t *testing.T) {
	t.Parallel()

	f := AcceptAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), true},
		{net.ParseIP("127.0.0.2"), true},
		{net.ParseIP("127.0.0.3"), true},
	}

	for i, test := range tests {
		if f.Accept(func() *HostInfo {
			opaqueStorageValuex := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuex).InternalConnectAddress = test.addr
			return opaqueStorageValuex
		}()) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_DenyAll(t *testing.T) {
	t.Parallel()

	f := DenyAllFilter()
	tests := [...]struct {
		addr   net.IP
		accept bool
	}{
		{net.ParseIP("127.0.0.1"), false},
		{net.ParseIP("127.0.0.2"), false},
		{net.ParseIP("127.0.0.3"), false},
	}

	for i, test := range tests {
		if f.Accept(func() *HostInfo {
			opaqueStorageValuexx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexx).InternalConnectAddress = test.addr
			return opaqueStorageValuexx
		}()) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}
	}
}

func TestFilter_WhiteList_MatchesRPCAddress(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxx).InternalRpcAddress = net.ParseIP("127.0.0.1")
		return opaqueStorageValuexxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via rpcAddress but wasn't")
	}
}

func TestFilter_WhiteList_MatchesBroadcastAddress(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxx).InternalBroadcastAddress = net.ParseIP("127.0.0.1")
		return opaqueStorageValuexxxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via broadcastAddress but wasn't")
	}
}

func TestFilter_WhiteList_MatchesListenAddress(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxx).InternalListenAddress = net.ParseIP("127.0.0.1")
		return opaqueStorageValuexxxxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via listenAddress but wasn't")
	}
}

func TestFilter_WhiteList_MatchesPeer(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxx).InternalPeer = net.ParseIP("127.0.0.1")
		return opaqueStorageValuexxxxxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via peer but wasn't")
	}
}

func TestFilter_WhiteList_MatchesPreferredIP(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxx).InternalPreferredIP = net.ParseIP("127.0.0.1")
		return opaqueStorageValuexxxxxxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via preferredIP but wasn't")
	}
}

func TestFilter_WhiteList_MatchesTranslatedAddress(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxx).InternalTranslatedAddresses = &translatedAddresses{
			CQL: AddressPort{Address: net.ParseIP("127.0.0.1"), Port: 9042},
		}
		return opaqueStorageValuexxxxxxxx
	}()

	if !f.Accept(host) {
		t.Error("should have been accepted via translatedAddresses but wasn't")
	}
}

func TestFilter_WhiteList_NoMatchWhenNoAddressMatches(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxxxxxx := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalConnectAddress = net.ParseIP("10.0.0.1")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalRpcAddress = net.ParseIP("10.0.0.2")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalBroadcastAddress = net.ParseIP("10.0.0.3")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalListenAddress = net.ParseIP("10.0.0.4")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalPeer = net.ParseIP("10.0.0.5")
		implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxx).InternalPreferredIP = net.ParseIP("10.0.0.6")
		return opaqueStorageValuexxxxxxxxx
	}()

	if f.Accept(host) {
		t.Error("should not have been accepted but was")
	}
}

func TestFilter_WhiteList_EmptyHost(t *testing.T) {
	t.Parallel()

	f := WhiteListHostFilter("127.0.0.1")

	host := func() *HostInfo {
		opaqueStorageValuexxxxxxxxxx := new(HostInfo)
		return opaqueStorageValuexxxxxxxxxx
	}()

	if f.Accept(host) {
		t.Error("empty host should not have been accepted")
	}
}

func TestFilter_DataCenter(t *testing.T) {
	t.Parallel()

	f := DataCenterHostFilter("dc1")
	fDeprecated := DataCentreHostFilter("dc1")

	tests := [...]struct {
		dc     string
		accept bool
	}{
		{"dc1", true},
		{"dc2", false},
	}

	for i, test := range tests {
		if f.Accept(func() *HostInfo {
			opaqueStorageValuexxxxxxxxxxx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxx).InternalDataCenter = test.dc
			return opaqueStorageValuexxxxxxxxxxx
		}()) {
			if !test.accept {
				t.Errorf("%d: should not have been accepted but was", i)
			}
		} else if test.accept {
			t.Errorf("%d: should have been accepted but wasn't", i)
		}

		if f.Accept(func() *HostInfo {
			opaqueStorageValuexxxxxxxxxxxx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxx).InternalDataCenter = test.dc
			return opaqueStorageValuexxxxxxxxxxxx
		}()) != fDeprecated.Accept(func() *HostInfo {
			opaqueStorageValuexxxxxxxxxxxxx := new(HostInfo)
			implEndpoint.HostInfoData(opaqueStorageValuexxxxxxxxxxxxx).InternalDataCenter = test.dc
			return opaqueStorageValuexxxxxxxxxxxxx
		}()) {
			t.Errorf("%d: DataCenterHostFilter and DataCentreHostFilter should be the same", i)
		}
	}
}
