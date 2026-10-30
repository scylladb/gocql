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

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	"errors"
	"fmt"
	"net"

	"time"
)

func InternalCheckSystemSchema(control InternalControlConnection) (bool, error) {
	if control == nil {
		return false, errNoControl
	}
	backend, ok := control.(InternalV2SystemQueryer)
	if !ok {
		return false, typeValidation("schema discovery", "control connection does not support typed queries")
	}
	ctx := context.Background()
	timeout := 60 * time.Second
	if session := control.InternalGetSession(); session != nil {
		ctx = v2SchemaContext(InternalNewSchemaSource(session))
		if session.privateStorage.InternalCfg.MetadataSchemaRequestTimeout > 0 {
			timeout = session.privateStorage.InternalCfg.MetadataSchemaRequestTimeout
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	page, err := backend.InternalQueryV2System(ctx, &attemptRequest{Statement: "SELECT * FROM system_schema.keyspaces", Consistency: One, PageSize: 1, Timeout: timeout})
	if page != nil && page.Owner != nil {
		defer page.Owner.Release()
	}
	if err != nil {
		var server RequestError
		if errors.As(err, &server) && server.GetCode() == ErrCodeSyntax {
			return false, nil
		}
		return false, err
	}
	if page == nil {
		return false, errNoControl
	}
	return true, nil
}

// Given a map that represents a row from either system.local or system.peers
// return as much information as we can in *HostInfo
func InternalHostInfoFromMap(row map[string]any, defaultPort int) (*HostInfo, error) {
	const assertErrorMsg = "Assertion failed for %s"
	var ok bool

	host := *func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		return opaqueStorageValue
	}()

	// Default to our connected port if the cluster doesn't have port information
	for key, value := range row {
		switch key {
		case "data_center":
			implEndpoint.HostInfoData(&host).InternalDataCenter, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "data_center")
			}
		case "rack":
			implEndpoint.HostInfoData(&host).InternalRack, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "rack")
			}
		case "host_id":
			hostId, ok := value.(UUID)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "host_id")
			}
			implEndpoint.HostInfoData(&host).InternalHostId = hostId
		case "release_version":
			version, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "release_version")
			}
			implEndpoint.HostInfoData(&host).InternalVersion.Set(version)
		case "peer":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "peer")
			}
			implEndpoint.HostInfoData(&host).InternalPeer = net.ParseIP(ip)
		case "cluster_name":
			implEndpoint.HostInfoData(&host).InternalClusterName, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "cluster_name")
			}
		case "partitioner":
			implEndpoint.HostInfoData(&host).InternalPartitioner, ok = value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "partitioner")
			}
		case "broadcast_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "broadcast_address")
			}
			implEndpoint.HostInfoData(&host).InternalBroadcastAddress = net.ParseIP(ip)
		case "preferred_ip":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "preferred_ip")
			}
			implEndpoint.HostInfoData(&host).InternalPreferredIP = net.ParseIP(ip)
		case "rpc_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "rpc_address")
			}
			implEndpoint.HostInfoData(&host).InternalRpcAddress = net.ParseIP(ip)
		case "native_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "native_address")
			}
			implEndpoint.HostInfoData(&host).InternalRpcAddress = net.ParseIP(ip)
		case "listen_address":
			ip, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "listen_address")
			}
			implEndpoint.HostInfoData(&host).InternalListenAddress = net.ParseIP(ip)
		case "native_port":
			native_port, ok := value.(int)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "native_port")
			}
			implEndpoint.HostInfoData(&host).InternalPort = native_port
		case "tokens":
			implEndpoint.HostInfoData(&host).InternalTokens, ok = value.([]string)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "tokens")
			}
		case "schema_version":
			schemaVersion, ok := value.(UUID)
			if !ok {
				return nil, fmt.Errorf(assertErrorMsg, "schema_version")
			}
			implEndpoint.HostInfoData(&host).InternalSchemaVersion = schemaVersion.String()
		}
		// TODO(thrawn01): Add 'port'? once CASSANDRA-7544 is complete
		// Not sure what the port field will be called until the JIRA issue is complete
	}

	if implEndpoint.HostInfoData(&host).InternalPort == 0 {
		implEndpoint.HostInfoData(&host).InternalPort = defaultPort
	}
	implEndpoint.HostInfoData(&host).InternalConnectAddress = implEndpoint.InternalHostInfoGetDriverFacingIpAddressLocked(&host)
	return &host, nil
}

func (s *Session) debounceRingRefresh() {
	s.privateStorage.InternalRingRefresher.Debounce()
}

// refreshRing executes a ring refresh immediately and cancels pending debounce ring refresh requests.
func InternalSessionRefreshRingNow(s *Session) error {
	err, ok := <-s.privateStorage.InternalRingRefresher.RefreshNow()
	if !ok {
		return errors.New("could not refresh ring because stop was requested")
	}

	return err
}

func (s *Session) refreshRing() error {
	r := s.privateStorage.InternalHostSource
	r.mu.Lock()
	r.refreshSequence++
	sequence := r.refreshSequence
	epoch := r.eventEpoch
	r.mu.Unlock()
	var hosts []*HostInfo
	var partitioner string
	var err error
	if s.privateStorage.InternalCfg.DisableInitialHostLookup {
		hosts, err = s.v2ContactIdentities(v2SchemaContext(InternalNewSchemaSource(s)), r.InternalGetHostsList())
		if len(hosts) > 0 {
			partitioner = hosts[0].Partitioner()
		}
	} else {
		hosts, partitioner, err = r.GetHostsFromSystem()
	}
	if err != nil {
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Topology: err}
	}
	enabled := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		enabled[h.HostID()] = !InternalClusterSettingsFilterHost(&s.privateStorage.InternalCfg, h)
	}
	r.mu.Lock()
	if r.eventEpoch != epoch || r.refreshSequence != sequence {
		r.mu.Unlock()
		return &PartialRefreshError{Generation: s.Metadata().Generation(), Topology: typeValidation("topology generation", "topology changed during refresh")}
	}
	before := r.InternalHosts
	next := make(map[string]*HostInfo, len(hosts))
	byIP := make(map[string]string, len(hosts))
	var added, removed []*HostInfo
	for _, h := range hosts {
		id := h.HostID()
		if r.tombstones[id] {
			continue
		}
		old := before[id]
		if old != nil && h.UntranslatedConnectAddress().Equal(old.UntranslatedConnectAddress()) && h.Port() == old.Port() && h.DataCenter() == old.DataCenter() && h.Rack() == old.Rack() {
			implEndpoint.InternalHostInfoUpdate(old, h)
			h = old
		} else {
			if old != nil {
				removed = append(removed, old)
			}
			added = append(added, h)
		}
		next[id] = h
		byIP[implEndpoint.InternalHostInfoNodeToNodeAddress(h).String()] = id
	}
	for id, h := range before {
		if next[id] == nil {
			removed = append(removed, h)
		}
	}
	r.InternalHosts = next
	r.hostIPToUUID = byIP
	r.revision++
	r.mu.Unlock()
	// State mutation is complete before notifications or network work. These
	// callbacks run without the topology/pool publication locks.
	for _, h := range removed {
		if s.privateStorage.InternalMetadataDescriber != nil {
			s.privateStorage.InternalMetadataDescriber.RemoveTabletsWithHost(h)
		}
		s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(h))
		s.privateStorage.InternalPolicy.RemoveHost(h)
	}
	InternalSessionPublishV2Metadata(s)
	for _, h := range added {
		if enabled[h.HostID()] {
			s.startPoolFill(h)
		}
	}
	for id, h := range next {
		if !enabled[id] {
			s.privateStorage.InternalPool.InternalRemoveHost(implEndpoint.InternalHostInfoHostUUID(h))
		}
	}
	s.privateStorage.InternalPolicy.SetPartitioner(partitioner)
	if !InternalSessionPublishV2Metadata(s) {
		return s.v2MetadataPublicationError()
	}
	return nil
}
