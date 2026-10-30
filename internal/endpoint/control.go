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

package endpoint

import (
	"fmt"

	"net"

	"strconv"

	"time"
)

func InternalResolveInitialEndpoint(resolver DNSResolver, addr string, defaultPort int) ([]*HostInfo, error) {
	var port int
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = defaultPort
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
	}

	// Check if host is a literal IP address
	if ip := net.ParseIP(host); ip != nil {
		if InternalValidIpAddr(ip) {
			hb := HostInfoBuilder{
				// Fake hosts for initial endpoints do not need HostID
				Hostname:       host,
				ConnectAddress: ip,
				Port:           port,
			}
			hh := hb.Build()
			return []*HostInfo{&hh}, nil
		}
	}

	// Look up host in DNS
	var ips []net.IP
	var setupDeadline time.Time
	if typed, ok := resolver.(*InternalV2DNSResolver); ok {
		ips, setupDeadline, err = typed.lookupSetup(host)
	} else {
		ips, err = resolver.LookupIP(host)
	}
	if err != nil {
		return nil, err
	} else if len(ips) == 0 {
		return nil, fmt.Errorf("no IP's returned from DNS lookup for %q", addr)
	}

	var hosts []*HostInfo
	for _, ip := range ips {
		if InternalValidIpAddr(ip) {
			hb := HostInfoBuilder{
				// Fake hosts for initial endpoints do not need HostID
				Hostname:       host,
				ConnectAddress: ip,
				Port:           port,
			}
			hh := hb.Build()
			hh.privateStorage.InternalInitialSetupDeadline = setupDeadline
			hosts = append(hosts, &hh)
		}
	}

	if len(hosts) == 0 {
		return nil, fmt.Errorf("no IP's founded for %q", addr)
	}
	return hosts, nil
}
