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
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"time"
)

var (
	ErrCannotFindHost    = errors.New("cannot find host")
	ErrHostAlreadyExists = errors.New("host already exists")
)

type InternalNodeState int32

func (n InternalNodeState) String() string {
	if n == NodeUp {
		return "UP"
	} else if n == NodeDown {
		return "DOWN"
	}
	return fmt.Sprintf("UNKNOWN_%d", n)
}

const (
	NodeUp InternalNodeState = iota
	NodeDown
)

type InternalCassVersion struct {
	Qualifier string
	Major     int
	Minor     int
	Patch     int
}

func (c *InternalCassVersion) Set(v string) error {
	if v == "" {
		return nil
	}

	return c.unmarshal([]byte(v))
}

func (c *InternalCassVersion) unmarshal(data []byte) error {
	v := strings.SplitN(strings.TrimPrefix(strings.TrimSuffix(string(data), "-SNAPSHOT"), "v"), ".", 3)

	if len(v) < 2 {
		return fmt.Errorf("invalid version string: %s", data)
	}

	var err error
	c.Major, err = strconv.Atoi(v[0])
	if err != nil {
		return fmt.Errorf("invalid major version %v: %v", v[0], err)
	}

	if len(v) == 2 {
		vMinor := strings.SplitN(v[1], "-", 2)
		c.Minor, err = strconv.Atoi(vMinor[0])
		if err != nil {
			return fmt.Errorf("invalid minor version %v: %v", vMinor[0], err)
		}
		if len(vMinor) == 2 {
			c.Qualifier = vMinor[1]
		}
		return nil
	}

	c.Minor, err = strconv.Atoi(v[1])
	if err != nil {
		return fmt.Errorf("invalid minor version %v: %v", v[1], err)
	}

	vPatch := strings.SplitN(v[2], "-", 2)
	c.Patch, err = strconv.Atoi(vPatch[0])
	if err != nil {
		return fmt.Errorf("invalid patch version %v: %v", vPatch[0], err)
	}
	if len(vPatch) == 2 {
		c.Qualifier = vPatch[1]
	}
	return nil
}

func (c InternalCassVersion) Before(major, minor, patch int) bool {
	// We're comparing us (cassVersion) with the provided version (major, minor, patch)
	// We return true if our version is lower (comes before) than the provided one.
	if c.Major < major {
		return true
	} else if c.Major == major {
		if c.Minor < minor {
			return true
		} else if c.Minor == minor && c.Patch < patch {
			return true
		}
	}
	return false
}

func (c InternalCassVersion) AtLeast(major, minor, patch int) bool {
	return !c.Before(major, minor, patch)
}

func (c InternalCassVersion) String() string {
	if c.Qualifier != "" {
		return fmt.Sprintf("%d.%d.%d-%v", c.Major, c.Minor, c.Patch, c.Qualifier)
	}
	return fmt.Sprintf("v%d.%d.%d", c.Major, c.Minor, c.Patch)
}

type AddressPort struct {
	Address net.IP
	Port    uint16
}

func (a AddressPort) Equal(o AddressPort) bool {
	return a.Address.Equal(o.Address) && a.Port == o.Port
}

func (a AddressPort) IsValid() bool {
	return len(a.Address) != 0 && !a.Address.IsUnspecified() && a.Port != 0
}

func (a AddressPort) String() string {
	return fmt.Sprintf("%s:%d", a.Address, a.Port)
}

func (a AddressPort) ToNetAddr() string {
	return net.JoinHostPort(a.Address.String(), strconv.Itoa(int(a.Port)))
}

type InternalTranslatedAddresses struct {
	CQL           AddressPort
	ShardAware    AddressPort
	ShardAwareTLS AddressPort
}

func (h InternalTranslatedAddresses) Equal(o *InternalTranslatedAddresses) bool {
	return h.CQL.Equal(o.CQL) && h.ShardAware.Equal(o.ShardAware) && h.ShardAwareTLS.Equal(o.ShardAwareTLS)
}

type HostInfoBuilder struct {
	TranslatedAddresses *InternalTranslatedAddresses
	Workload            string
	HostId              string
	SchemaVersion       string
	Hostname            string
	ClusterName         string
	Partitioner         string
	Rack                string
	DseVersion          string
	DataCenter          string
	ConnectAddress      net.IP
	BroadcastAddress    net.IP
	PreferredIP         net.IP
	RpcAddress          net.IP
	Peer                net.IP
	ListenAddress       net.IP
	Tokens              []string
	Version             InternalCassVersion
	Port                int
}

func (b HostInfoBuilder) Build() HostInfo {
	var hostUUID UUID
	if b.HostId != "" {
		var err error
		hostUUID, err = ParseUUID(b.HostId)
		if err != nil {
			// Fall back: treat as opaque identifier (for tests with non-UUID strings).
			copy(hostUUID[:], b.HostId)
		}
	}
	return *func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		opaqueStorageValue.privateStorage.InternalDseVersion = b.DseVersion
		opaqueStorageValue.privateStorage.InternalHostId = hostUUID
		opaqueStorageValue.privateStorage.InternalDataCenter = b.DataCenter
		opaqueStorageValue.privateStorage.InternalSchemaVersion = b.SchemaVersion
		opaqueStorageValue.privateStorage.InternalHostname = b.Hostname
		opaqueStorageValue.privateStorage.InternalClusterName = b.ClusterName
		opaqueStorageValue.privateStorage.InternalPartitioner = b.Partitioner
		opaqueStorageValue.privateStorage.InternalRack = b.Rack
		opaqueStorageValue.privateStorage.InternalWorkload = b.Workload
		opaqueStorageValue.privateStorage.InternalTokens = b.Tokens
		opaqueStorageValue.privateStorage.InternalPreferredIP = b.PreferredIP
		opaqueStorageValue.privateStorage.InternalBroadcastAddress = b.BroadcastAddress
		opaqueStorageValue.privateStorage.InternalRpcAddress = b.RpcAddress
		opaqueStorageValue.privateStorage.InternalConnectAddress = b.ConnectAddress
		opaqueStorageValue.privateStorage.InternalListenAddress = b.ListenAddress
		opaqueStorageValue.privateStorage.InternalTranslatedAddresses = b.TranslatedAddresses
		opaqueStorageValue.privateStorage.InternalVersion = b.Version
		opaqueStorageValue.privateStorage.InternalPort = b.Port
		opaqueStorageValue.privateStorage.InternalPeer = b.Peer
		return opaqueStorageValue
	}()

}

type HostInfo struct {
	privateStorage HostInfoStorage

	// TODO(zariel): reduce locking maybe, not all values will change, but to ensure
	// that we are thread safe use a mutex to access all fields.

}

func (h *HostInfo) Equal(host *HostInfo) bool {
	if h == host {
		// prevent rlock reentry
		return true
	}

	return InternalHostInfoHostUUID(h) == InternalHostInfoHostUUID(host) && h.ConnectAddress().Equal(host.ConnectAddress()) && h.Port() == host.Port()
}

func (h *HostInfo) Peer() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalPeer
}

func InternalHostInfoInvalidConnectAddr(h *HostInfo) bool {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	addr, _ := InternalHostInfoConnectAddressLocked(h)
	return !InternalValidIpAddr(addr)
}

func InternalValidIpAddr(addr net.IP) bool {
	return addr != nil && !addr.IsUnspecified()
}

func InternalHostInfoConnectAddressLocked(h *HostInfo) (net.IP, string) {
	if h.privateStorage.InternalTranslatedAddresses != nil && h.privateStorage.InternalTranslatedAddresses.CQL.IsValid() {
		return h.privateStorage.InternalTranslatedAddresses.CQL.Address, "connect_address"
	} else if InternalValidIpAddr(h.privateStorage.InternalConnectAddress) {
		return h.privateStorage.InternalConnectAddress, "connect_address"
	} else if InternalValidIpAddr(h.privateStorage.InternalRpcAddress) {
		return h.privateStorage.InternalRpcAddress, "rpc_adress"
	} else if InternalValidIpAddr(h.privateStorage.InternalPreferredIP) {
		// where does perferred_ip get set?
		return h.privateStorage.InternalPreferredIP, "preferred_ip"
	} else if InternalValidIpAddr(h.privateStorage.InternalBroadcastAddress) {
		return h.privateStorage.InternalBroadcastAddress, "broadcast_address"
	} else if InternalValidIpAddr(h.privateStorage.InternalPeer) {
		return h.privateStorage.InternalPeer, "peer"
	}
	return net.IPv4zero, "invalid"
}

func InternalHostInfoGetDriverFacingIpAddressLocked(h *HostInfo) net.IP {
	if InternalValidIpAddr(h.privateStorage.InternalRpcAddress) {
		return h.privateStorage.InternalRpcAddress
	} else if InternalValidIpAddr(h.privateStorage.InternalPreferredIP) {
		return h.privateStorage.InternalPreferredIP
	} else if InternalValidIpAddr(h.privateStorage.InternalBroadcastAddress) {
		return h.privateStorage.InternalBroadcastAddress
	} else if InternalValidIpAddr(h.privateStorage.InternalPeer) {
		return h.privateStorage.InternalPeer
	}
	return net.IPv4zero
}

// nodeToNodeAddress returns address broadcasted between node to nodes.
// It's either `broadcast_address` if host info is read from system.local or `peer` if read from system.peers.
// This IP address is also part of CQL Event emitted on topology/status changes,
// but does not uniquely identify the node in case multiple nodes use the same IP address.
func InternalHostInfoNodeToNodeAddress(h *HostInfo) net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()

	if InternalValidIpAddr(h.privateStorage.InternalBroadcastAddress) {
		return h.privateStorage.InternalBroadcastAddress
	} else if InternalValidIpAddr(h.privateStorage.InternalPeer) {
		return h.privateStorage.InternalPeer
	}
	return net.IPv4zero
}

// Returns the address that should be used to connect to the host.
// If you wish to override this, use an AddressTranslator
func (h *HostInfo) ConnectAddress() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()

	if addr, _ := InternalHostInfoConnectAddressLocked(h); InternalValidIpAddr(addr) {
		return addr
	}
	panic(fmt.Sprintf("no valid connect address for host: %v. Is your cluster configured correctly?", h))
}

func (h *HostInfo) UntranslatedConnectAddress() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalConnectAddress
}

func (h *HostInfo) BroadcastAddress() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalBroadcastAddress
}

func (h *HostInfo) ListenAddress() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalListenAddress
}

func (h *HostInfo) RPCAddress() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalRpcAddress
}

func (h *HostInfo) PreferredIP() net.IP {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalPreferredIP
}

func (h *HostInfo) DataCenter() string {
	h.privateStorage.InternalMu.RLock()
	dc := h.privateStorage.InternalDataCenter
	h.privateStorage.InternalMu.RUnlock()
	return dc
}

func (h *HostInfo) Rack() string {
	h.privateStorage.InternalMu.RLock()
	rack := h.privateStorage.InternalRack
	h.privateStorage.InternalMu.RUnlock()
	return rack
}

func (h *HostInfo) HostID() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	if h.privateStorage.InternalHostId.IsEmpty() {
		return ""
	}
	return h.privateStorage.InternalHostId.String()
}

// hostUUID returns the raw binary host UUID under the read lock.
// Use this instead of direct field access on shared HostInfo to avoid data races.
func InternalHostInfoHostUUID(h *HostInfo) UUID {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalHostId
}

// Deprecated: WorkLoad is a DSE-specific field that is no longer queried
// from system tables. It will always return "" for hosts discovered via
// the driver. Only populated if set explicitly via HostInfoBuilder.
func (h *HostInfo) WorkLoad() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalWorkload
}

// Deprecated: Graph is a DSE-specific field that is no longer queried
// from system tables. It always returns false.
func (h *HostInfo) Graph() bool {
	return false
}

// Deprecated: DSEVersion is a DSE-specific field that is no longer queried
// from system tables. It will always return "" for hosts discovered via
// the driver. Only populated if set explicitly via HostInfoBuilder.
func (h *HostInfo) DSEVersion() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalDseVersion
}

func (h *HostInfo) Partitioner() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	if h.privateStorage.InternalPartitioner != "" {
		return h.privateStorage.InternalPartitioner
	}
	return h.privateStorage.InternalScyllaFeatures.privateStorage.InternalPartitioner
}

func (h *HostInfo) ClusterName() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalClusterName
}

func (h *HostInfo) Version() InternalCassVersion {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalVersion
}

func (h *HostInfo) State() InternalNodeState {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalState
}

func InternalHostInfoSetState(h *HostInfo, state InternalNodeState) *HostInfo {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	if h.privateStorage.InternalState != state {
		h.privateStorage.InternalLifecycleGeneration++
	}
	h.privateStorage.InternalState = state
	return h
}

func (h *HostInfo) Tokens() []string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalTokens
}

func (h *HostInfo) Port() int {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalPort
}

func InternalHostInfoUpdate(h *HostInfo, from *HostInfo) {
	if h == from {
		return
	}
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	from.privateStorage.InternalMu.RLock()
	defer from.privateStorage.InternalMu.RUnlock()

	beforeAddr, _ := InternalHostInfoConnectAddressLocked(h)
	beforeNative := h.privateStorage.InternalConnectAddress.String()
	beforeName := h.privateStorage.InternalHostname
	beforeIdentity := h.privateStorage.InternalHostId
	beforePort := h.privateStorage.InternalPort
	beforeDC, beforeRack := h.privateStorage.InternalDataCenter, h.privateStorage.InternalRack
	defer func() {
		afterAddr, _ := InternalHostInfoConnectAddressLocked(h)
		if beforeNative != h.privateStorage.InternalConnectAddress.String() || beforeName != h.privateStorage.InternalHostname || !beforeAddr.Equal(afterAddr) || beforeIdentity != h.privateStorage.InternalHostId || beforePort != h.privateStorage.InternalPort || beforeDC != h.privateStorage.InternalDataCenter || beforeRack != h.privateStorage.InternalRack {
			h.privateStorage.InternalTransportGeneration++
		}
	}()
	if !h.privateStorage.InternalHostId.IsEmpty() && h.privateStorage.InternalHostId == from.privateStorage.InternalHostId {
		h.privateStorage.InternalTokens = append([]string(nil), from.privateStorage.InternalTokens...)
		h.privateStorage.InternalDataCenter, h.privateStorage.InternalRack = from.privateStorage.InternalDataCenter, from.privateStorage.InternalRack
		h.privateStorage.InternalSchemaVersion = from.privateStorage.InternalSchemaVersion
		if from.privateStorage.InternalPartitioner != "" {
			h.privateStorage.InternalPartitioner = from.privateStorage.InternalPartitioner
		}
		if from.privateStorage.InternalClusterName != "" {
			h.privateStorage.InternalClusterName = from.privateStorage.InternalClusterName
		}
		if from.privateStorage.InternalVersion != (InternalCassVersion{}) {
			h.privateStorage.InternalVersion = from.privateStorage.InternalVersion
		}
	}
	// autogenerated do not update
	if h.privateStorage.InternalPeer == nil {
		h.privateStorage.InternalPeer = from.privateStorage.InternalPeer
	}
	if h.privateStorage.InternalBroadcastAddress == nil {
		h.privateStorage.InternalBroadcastAddress = from.privateStorage.InternalBroadcastAddress
	}
	if h.privateStorage.InternalListenAddress == nil {
		h.privateStorage.InternalListenAddress = from.privateStorage.InternalListenAddress
	}
	if h.privateStorage.InternalRpcAddress == nil {
		h.privateStorage.InternalRpcAddress = from.privateStorage.InternalRpcAddress
	}
	if h.privateStorage.InternalPreferredIP == nil {
		h.privateStorage.InternalPreferredIP = from.privateStorage.InternalPreferredIP
	}
	if h.privateStorage.InternalConnectAddress == nil {
		h.privateStorage.InternalConnectAddress = from.privateStorage.InternalConnectAddress
	}
	if h.privateStorage.InternalPort == 0 {
		h.privateStorage.InternalPort = from.privateStorage.InternalPort
	}
	if h.privateStorage.InternalDataCenter == "" {
		h.privateStorage.InternalDataCenter = from.privateStorage.InternalDataCenter
	}
	if h.privateStorage.InternalRack == "" {
		h.privateStorage.InternalRack = from.privateStorage.InternalRack
	}
	if h.privateStorage.InternalHostId.IsEmpty() {
		h.privateStorage.InternalHostId = from.privateStorage.InternalHostId
	}
	if h.privateStorage.InternalWorkload == "" {
		h.privateStorage.InternalWorkload = from.privateStorage.InternalWorkload
	}
	if h.privateStorage.InternalDseVersion == "" {
		h.privateStorage.InternalDseVersion = from.privateStorage.InternalDseVersion
	}
	if h.privateStorage.InternalPartitioner == "" {
		h.privateStorage.InternalPartitioner = from.privateStorage.InternalPartitioner
	}
	if h.privateStorage.InternalClusterName == "" {
		h.privateStorage.InternalClusterName = from.privateStorage.InternalClusterName
	}
	if h.privateStorage.InternalVersion == (InternalCassVersion{}) {
		h.privateStorage.InternalVersion = from.privateStorage.InternalVersion
	}
	if h.privateStorage.InternalTokens == nil {
		h.privateStorage.InternalTokens = from.privateStorage.InternalTokens
	}
}

func (h *HostInfo) IsUp() bool {
	return h != nil && h.State() == NodeUp
}

// ConnectAddressAndPort returns "{ConnectAddress}:{Port}"
// Deprecated: Use ConnectAddress and Port separately.
func (h *HostInfo) ConnectAddressAndPort() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	addr, _ := InternalHostInfoConnectAddressLocked(h)
	return net.JoinHostPort(addr.String(), strconv.Itoa(h.privateStorage.InternalPort))
}

func (h *HostInfo) String() string {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()

	connectAddr, source := InternalHostInfoConnectAddressLocked(h)
	return fmt.Sprintf("[HostInfo hostname=%q connectAddress=%q peer=%q rpc_address=%q broadcast_address=%q "+
		"preferred_ip=%q connect_addr=%q connect_addr_source=%q "+
		"port=%d data_center=%q rack=%q host_id=%q version=%q state=%s num_tokens=%d]", h.privateStorage.InternalHostname, h.privateStorage.InternalConnectAddress, h.privateStorage.InternalPeer, h.privateStorage.InternalRpcAddress, h.privateStorage.InternalBroadcastAddress, h.privateStorage.InternalPreferredIP, connectAddr, source, h.privateStorage.InternalPort, h.privateStorage.InternalDataCenter, h.privateStorage.InternalRack, h.privateStorage.InternalHostId.String(), h.privateStorage.InternalVersion, h.privateStorage.InternalState, len(h.privateStorage.InternalTokens))
}

func InternalHostInfoSetScyllaFeatures(h *HostInfo, s ScyllaHostFeatures) {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	InternalHostInfoSetScyllaFeaturesLocked(h, s)
}
func InternalHostInfoSetScyllaFeaturesLocked(h *HostInfo, s ScyllaHostFeatures) {
	old := h.privateStorage.InternalScyllaFeatures
	if old.privateStorage.InternalNrShards != s.privateStorage.InternalNrShards || old.privateStorage.InternalMsbIgnore != s.privateStorage.InternalMsbIgnore {
		h.privateStorage.InternalSharderGeneration++
	}
	if old.privateStorage.InternalNrShards > 0 && (old.privateStorage.InternalShardAwarePort != s.privateStorage.InternalShardAwarePort || old.privateStorage.InternalShardAwarePortTLS != s.privateStorage.InternalShardAwarePortTLS) {
		h.privateStorage.InternalTransportGeneration++
	}
	h.privateStorage.InternalScyllaFeatures = s
}

func (h *HostInfo) ScyllaFeatures() ScyllaHostFeatures {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	return h.privateStorage.InternalScyllaFeatures
}

// ScyllaShardAwarePort returns the shard aware port of this host.
// Returns zero if the shard aware port is not known.
func (h *HostInfo) ScyllaShardAwarePort() uint16 {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalScyllaFeatures.ShardAwarePort()
}

// ScyllaShardAwarePortTLS returns the TLS-enabled shard aware port of this host.
// Returns zero if the shard aware port is not known.
func (h *HostInfo) ScyllaShardAwarePortTLS() uint16 {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalScyllaFeatures.ShardAwarePortTLS()
}

// ScyllaShardCount returns count of shards on the node.
func (h *HostInfo) ScyllaShardCount() int {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalScyllaFeatures.ShardsCount()
}

func InternalHostInfoSetTranslatedConnectionInfo(h *HostInfo, info InternalTranslatedAddresses) {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	if h.privateStorage.InternalTranslatedAddresses == nil || !info.Equal(h.privateStorage.InternalTranslatedAddresses) {
		h.privateStorage.InternalTransportGeneration++
	}
	h.privateStorage.InternalTranslatedAddresses = &info
}

func InternalHostInfoGetTranslatedConnectionInfo(h *HostInfo) *InternalTranslatedAddresses {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	return h.privateStorage.InternalTranslatedAddresses
}

// Returns true if we are using system_schema.keyspaces instead of system.schema_keyspaces

func InternalHostInfoTransportEpoch(h *HostInfo) uint64 {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalTransportGeneration
}

// A DNS result belongs to one resolved contact/opening, never a global IP key.
func InternalHostInfoClaimSetupDeadline(h *HostInfo) time.Time {
	h.privateStorage.InternalMu.Lock()
	defer h.privateStorage.InternalMu.Unlock()
	v := h.privateStorage.InternalInitialSetupDeadline
	h.privateStorage.InternalInitialSetupDeadline = time.Time{}
	return v
}

func InternalHostInfoSharderEpoch(h *HostInfo) uint64 {
	h.privateStorage.InternalMu.RLock()
	defer h.privateStorage.InternalMu.RUnlock()
	return h.privateStorage.InternalSharderGeneration
}
