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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package endpoint

import (
	"context"

	"net"
	"os"
)

type DNSResolver interface {
	LookupIP(host string) ([]net.IP, error)
}

type ApplicationInfo interface {
	UpdateStartupOptions(map[string]string)
}

type StaticApplicationInfo struct {
	privateStorage StaticApplicationInfoStorage
}

func NewStaticApplicationInfo(name, version, clientID string) *StaticApplicationInfo {
	return func() *StaticApplicationInfo {
		opaqueStorageValue := new(StaticApplicationInfo)
		opaqueStorageValue.privateStorage.applicationName = name
		opaqueStorageValue.privateStorage.applicationVersion = version
		opaqueStorageValue.privateStorage.clientID = clientID
		return opaqueStorageValue
	}()

}

func (i *StaticApplicationInfo) UpdateStartupOptions(opts map[string]string) {
	if i.privateStorage.applicationName != "" {
		opts["APPLICATION_NAME"] = i.privateStorage.applicationName
	}
	if i.privateStorage.applicationVersion != "" {
		opts["APPLICATION_VERSION"] = i.privateStorage.applicationVersion
	}
	if i.privateStorage.clientID != "" {
		opts["CLIENT_ID"] = i.privateStorage.clientID
	}
}

type SimpleDNSResolver struct {
	privateStorage SimpleDNSResolverStorage
}

func NewSimpleDNSResolver(hostLookupPreferV4 bool) *SimpleDNSResolver {
	return func() *SimpleDNSResolver {
		opaqueStorageValuex := new(SimpleDNSResolver)
		opaqueStorageValuex.privateStorage.hostLookupPreferV4 = hostLookupPreferV4
		return opaqueStorageValuex
	}()

}

func (r SimpleDNSResolver) LookupIP(host string) ([]net.IP, error) {
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	// Filter to v4 addresses if any present
	if r.privateStorage.hostLookupPreferV4 {
		var preferredIPs []net.IP
		for _, v := range ips {
			if v4 := v.To4(); v4 != nil {
				preferredIPs = append(preferredIPs, v4)
			}
		}
		if len(preferredIPs) != 0 {
			ips = preferredIPs
		}
	}
	return ips, nil
}

var InternalDefaultDnsResolver = NewSimpleDNSResolver(os.Getenv("GOCQL_HOST_LOOKUP_PREFER_V4") == "true")

type InternalLegacyDialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}
