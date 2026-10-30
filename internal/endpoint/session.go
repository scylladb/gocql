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

import "errors"
import "fmt"
import "github.com/scylladb/gocql/v2/internal/debug"
import logging "github.com/scylladb/gocql/v2/internal/logging"
import "net"

// findTabletReplicasUnsafeForToken returns the raw replica slice for the tablet
// owning the given token. The returned slice must not be modified by callers.
func InternalTranslateAddressPort(addressTranslator AddressTranslator, host *HostInfo, addr AddressPort, logger StdLogger) (AddressPort, error) {
	if addressTranslator == nil || !addr.IsValid() {
		return addr, nil
	}
	translatorV2, ok := addressTranslator.(AddressTranslatorV2)
	if !ok {
		newAddr, newPort := addressTranslator.Translate(addr.Address, int(addr.Port))
		if debug.Enabled {
			logger.Printf("gocql: translated address %q to '%v:%d'", addr, newAddr, newPort)
		}
		if newPort < 1 || newPort > 65535 {
			return addr, fmt.Errorf("invalid translated port %d: port must be between 1 and 65535", newPort)
		}
		return AddressPort{
			Address: newAddr,
			Port:    uint16(newPort),
		}, nil
	}
	newAddr, err := translatorV2.TranslateHost(host, addr)
	if err != nil {
		if debug.Enabled {
			logger.Printf("gocql: failed to translate address %q: %s", addr, err.Error())
		}
		return addr, err
	}
	if debug.Enabled {
		logger.Printf("gocql: translated address %q to %q", addr, newAddr)
	}
	return newAddr, nil
}

func InternalResolveInitialEndpoints(resolver DNSResolver, addrs []string, defaultPort int, logger logging.StdLogger) ([]*HostInfo, error) {
	var hosts []*HostInfo
	var errs []error
	for _, hostaddr := range addrs {
		resolvedHosts, err := InternalResolveInitialEndpoint(resolver, hostaddr, defaultPort)
		if err != nil {
			err = fmt.Errorf("failed to resolve endpoint %q: %w", hostaddr, err)
			errs = append(errs, err)
			logger.Println(err.Error())
			continue
		}
		hosts = append(hosts, resolvedHosts...)
	}
	if len(hosts) == 0 {
		return nil, fmt.Errorf("failed to resolve any of the provided hostnames: %w", errors.Join(errs...))
	}
	return hosts, nil
}

type HostInformation interface {
	Peer() net.IP
	ConnectAddress() net.IP
	UntranslatedConnectAddress() net.IP
	BroadcastAddress() net.IP
	ListenAddress() net.IP
	RPCAddress() net.IP
	PreferredIP() net.IP
	DataCenter() string
	Rack() string
	HostID() string
	WorkLoad() string
	Partitioner() string
	ClusterName() string
	Tokens() []string
	Port() int
	IsUp() bool
	ScyllaShardAwarePort() uint16
	ScyllaShardAwarePortTLS() uint16
	ScyllaShardCount() int
}
