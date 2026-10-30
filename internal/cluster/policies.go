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

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"fmt"

	randv2 "math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const InternalHostInfoListMapThreshold = 20

// hostInfoList is an immutable host list snapshot with optional indexed lookup.
type InternalHostInfoList struct {
	hosts             []*HostInfo
	InternalHostsByID map[UUID]*HostInfo
	InternalHostIDs   []UUID
}

func InternalNewHostInfoList(hosts []*HostInfo) *InternalHostInfoList {
	hosts = hosts[:len(hosts):len(hosts)]
	l := &InternalHostInfoList{
		hosts: hosts,
	}

	if len(hosts) >= InternalHostInfoListMapThreshold {
		hostsByID := make(map[UUID]*HostInfo, len(hosts))
		l.InternalHostsByID = hostsByID
		for _, host := range hosts {
			hostID := implEndpoint.InternalHostInfoHostUUID(host)
			if hostID.IsEmpty() {
				continue
			}
			if _, ok := hostsByID[hostID]; !ok {
				hostsByID[hostID] = host
			}
		}
		return l
	}

	hostIDs := make([]UUID, len(hosts))
	for i, host := range hosts {
		hostIDs[i] = implEndpoint.InternalHostInfoHostUUID(host)
	}
	l.InternalHostIDs = hostIDs

	return l
}

// allHosts returns hosts in this snapshot. Callers must treat the returned slice as read-only.
func (l *InternalHostInfoList) InternalAllHosts() []*HostInfo {
	if l == nil {
		return nil
	}
	return l.hosts
}

// len returns the number of hosts in this snapshot.
func (l *InternalHostInfoList) InternalLen() int {
	if l == nil {
		return 0
	}
	return len(l.hosts)
}

// hostByID finds a host in this snapshot by host_id.
func (l *InternalHostInfoList) InternalHostByID(hostID UUID) *HostInfo {
	if l == nil || hostID.IsEmpty() {
		return nil
	}
	if l.InternalHostsByID != nil {
		return l.InternalHostsByID[hostID]
	}
	for i, id := range l.InternalHostIDs {
		if id == hostID {
			return l.hosts[i]
		}
	}
	return nil
}

// cowHostList implements a copy-on-write host list.
type InternalCowHostList struct {
	list atomic.Pointer[InternalHostInfoList]
	mu   sync.Mutex
}

func (c *InternalCowHostList) String() string {
	return fmt.Sprintf("%+v", c.InternalGet().InternalAllHosts())
}

func (c *InternalCowHostList) InternalGet() *InternalHostInfoList {
	return c.list.Load()
}

// add will add a host if it not already in the list
func (c *InternalCowHostList) InternalAdd(host *HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.InternalGet().InternalAllHosts()

	if n := len(l); n == 0 {
		l = []*HostInfo{host}
	} else {
		newL := make([]*HostInfo, n+1)
		for i := 0; i < n; i++ {
			if host.Equal(l[i]) {
				return false
			}
			newL[i] = l[i]
		}
		newL[n] = host
		l = newL
	}

	c.list.Store(InternalNewHostInfoList(l))
	return true
}

func (c *InternalCowHostList) InternalAddAll(hosts []*HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.InternalGet().InternalAllHosts()
	newL := make([]*HostInfo, len(l), len(l)+len(hosts))
	copy(newL, l)

	added := false
	for _, host := range hosts {
		exists := false
		for _, existing := range newL {
			if host.Equal(existing) {
				exists = true
				break
			}
		}
		if !exists {
			newL = append(newL, host)
			added = true
		}
	}

	if !added {
		return false
	}

	c.list.Store(InternalNewHostInfoList(newL))
	return true
}

func (c *InternalCowHostList) remove(host *HostInfo) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	l := c.InternalGet().InternalAllHosts()
	size := len(l)
	if size == 0 {
		return false
	}

	found := false
	newL := make([]*HostInfo, 0, size)
	for i := 0; i < len(l); i++ {
		if !l[i].Equal(host) {
			newL = append(newL, l[i])
		} else {
			found = true
		}
	}

	if !found {
		return false
	}

	c.list.Store(InternalNewHostInfoList(newL))

	return true
}

type HostStateNotifier interface {
	AddHost(host *HostInfo)
	RemoveHost(host *HostInfo)
	HostUp(host *HostInfo)
	HostDown(host *HostInfo)
}

type KeyspaceUpdateEvent struct {
	Keyspace string
	Change   string
}

type HostTierer interface {
	// HostTier returns an integer specifying how far a host is from the client.
	// Tier must start at 0.
	// The value is used to prioritize closer hosts during host selection.
	// For example this could be:
	// 0 - local rack, 1 - local DC, 2 - remote DC
	// or:
	// 0 - local DC, 1 - remote DC
	HostTier(host *HostInfo) uint

	// This function returns the maximum possible host tier
	MaxHostTier() uint
}

// HostSelectionPolicy is an interface for selecting
// the most appropriate host to execute a given query.
// HostSelectionPolicy instances cannot be shared between sessions.
type HostSelectionPolicy interface {
	HostStateNotifier
	SetPartitioner
	KeyspaceChanged(KeyspaceUpdateEvent)
	Init(*Session)
	// Reset is opprotunity to reset HostSelectionPolicy if Session initilization failed and we want to
	// call HostSelectionPolicy.Init() again with new Session
	Reset()
	IsLocal(host *HostInfo) bool
	// Pick returns an iteration function over selected hosts.
	// Multiple attempts of a single query execution won't call the returned NextHost function concurrently,
	// so it's safe to have internal state without additional synchronization as long as every call to Pick returns
	// a different instance of NextHost.
	// IsOperational checks if host policy can properly work with given Session/Cluster/clusterSettings
	IsOperational(*Session) error
}

type InternalSelectedHost struct {
	InternalInfo  *HostInfo
	InternalToken Token
}

func (host InternalSelectedHost) Info() *HostInfo {
	return host.InternalInfo
}

func (host InternalSelectedHost) Token() Token {
	return host.InternalToken
}

func (host InternalSelectedHost) TokenInt64() (int64Token, bool) {
	return 0, false
}

func (host InternalSelectedHost) Mark(err error) {}

// int64SelectedHost is the fast-path variant of selectedHost: it carries the
// routing token as a raw int64 instead of a boxed Token, so shard-aware conn
// picking consumes it without an interface allocation.
type InternalInt64SelectedHost struct {
	InternalInfo        *HostInfo
	InternalTokenCasted int64Token
}

func (host InternalInt64SelectedHost) Info() *HostInfo {
	return host.InternalInfo
}

func (host InternalInt64SelectedHost) Token() Token {
	return host.InternalTokenCasted
}

func (host InternalInt64SelectedHost) TokenInt64() (int64Token, bool) {
	return host.InternalTokenCasted, true
}

func (host InternalInt64SelectedHost) Mark(err error) {}

func InternalNewSingleHost(info *HostInfo, maxRetries byte, retryDelay time.Duration) *InternalSingleHost {
	return &InternalSingleHost{info: info, maxRetries: maxRetries, delay: retryDelay}
}

type InternalSingleHost struct {
	info       *HostInfo
	delay      time.Duration
	retry      byte
	maxRetries byte
}

func (s *InternalSingleHost) selectHost() SelectedHost {
	if s.retry >= s.maxRetries {
		return nil
	}
	if s.retry > 0 && s.delay > 0 {
		time.Sleep(s.delay)
	}
	s.retry++
	return s
}

func (s InternalSingleHost) Info() *HostInfo { return s.info }

func (s InternalSingleHost) Token() Token { return nil }

func (s InternalSingleHost) Mark(error) {}

// NextHost is an iteration function over picked hosts
type NextHost func() SelectedHost

// RoundRobinHostPolicy is a round-robin load balancing policy, where each host
// is tried sequentially for each query.
func RoundRobinHostPolicy() HostSelectionPolicy {
	return &InternalRoundRobinHostPolicy{}
}

type InternalRoundRobinHostPolicy struct {
	hosts           InternalCowHostList
	lastUsedHostIdx uint64
}

func (r *InternalRoundRobinHostPolicy) IsLocal(*HostInfo) bool              { return true }
func (r *InternalRoundRobinHostPolicy) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (r *InternalRoundRobinHostPolicy) SetPartitioner(partitioner string)   {}
func (r *InternalRoundRobinHostPolicy) Init(*Session)                       {}
func (r *InternalRoundRobinHostPolicy) Reset()                              {}
func (r *InternalRoundRobinHostPolicy) IsOperational(*Session) error        { return nil }

func (r *InternalRoundRobinHostPolicy) AddHost(host *HostInfo) {
	r.hosts.InternalAdd(host)
}

func (r *InternalRoundRobinHostPolicy) RemoveHost(host *HostInfo) {
	r.hosts.remove(host)
}

func (r *InternalRoundRobinHostPolicy) HostUp(host *HostInfo) {
	r.AddHost(host)
}

func (r *InternalRoundRobinHostPolicy) HostDown(host *HostInfo) {
	r.RemoveHost(host)
}

func ShuffleReplicas() func(*InternalTokenAwareHostPolicy) {
	return func(t *InternalTokenAwareHostPolicy) {
		t.privateStorage.InternalShuffleReplicas = true
	}
}

func DontShuffleReplicas() func(*InternalTokenAwareHostPolicy) {
	return func(t *InternalTokenAwareHostPolicy) {
		t.privateStorage.InternalShuffleReplicas = false
	}
}

// AvoidSlowReplicas enabled avoiding slow replicas
//
// TokenAwareHostPolicy normally does not check how busy replica is, with avoidSlowReplicas enabled it avoids replicas
// if they have equal or more than MAX_IN_FLIGHT_THRESHOLD requests in flight
func AvoidSlowReplicas(max_in_flight_threshold int) func(policy *InternalTokenAwareHostPolicy) {
	return func(t *InternalTokenAwareHostPolicy) {
		t.privateStorage.avoidSlowReplicas = true
		MAX_IN_FLIGHT_THRESHOLD = max_in_flight_threshold
	}
}

// NonLocalReplicasFallback enables fallback to replicas that are not considered local.
//
// TokenAwareHostPolicy used with DCAwareHostPolicy fallback first selects replicas by partition key in local DC, then
// falls back to other nodes in the local DC. Enabling NonLocalReplicasFallback causes TokenAwareHostPolicy
// to first select replicas by partition key in local DC, then replicas by partition key in remote DCs and fall back
// to other nodes in local DC.
func NonLocalReplicasFallback() func(policy *InternalTokenAwareHostPolicy) {
	return func(t *InternalTokenAwareHostPolicy) {
		t.privateStorage.InternalNonLocalReplicasFallback = true
	}
}

// TokenAwareHostPolicy is a token aware host selection policy, where hosts are
// selected based on the partition key, so queries are sent to the host which
// owns the partition. Fallback is used when routing information is not available.
func TokenAwareHostPolicy(fallback HostSelectionPolicy, opts ...func(*InternalTokenAwareHostPolicy)) HostSelectionPolicy {
	p := func() *InternalTokenAwareHostPolicy {
		opaqueStorageValue := new(InternalTokenAwareHostPolicy)
		opaqueStorageValue.privateStorage.InternalFallback = fallback
		opaqueStorageValue.privateStorage.InternalShuffleReplicas = true
		return opaqueStorageValue
	}()

	for _, opt := range opts {
		opt(p)
	}
	return p
}

// clusterMeta holds metadata about cluster topology.
// It is used inside atomic.Pointer and shallow copies are used when replacing it,
// so fields should not be modified in-place. Instead, to modify a field a copy of the field should be made
// and the pointer in clusterMeta updated to point to the new value.
type InternalClusterMeta struct {
	// replicas is map[keyspace]map[token]hosts
	replicas  map[string]InternalTokenRingReplicas
	tokenRing *tokenRing
}

var MAX_IN_FLIGHT_THRESHOLD int = 10

type InternalTokenAwareHostPolicy struct {
	privateStorage InternalTokenAwareHostPolicyStorage

	// atomic store for *clusterMeta

	// mu protects writes to hosts, partitioner, metadata.
	// reads can be unlocked as long as they are not used for updating state later.

}

func (t *InternalTokenAwareHostPolicy) Init(s *Session) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	if t.privateStorage.InternalGetKeyspaceMetadata != nil {
		// Init was already called.
		// See https://github.com/scylladb/gocql/issues/94.
		panic("sharing token aware host selection policy between sessions is not supported")
	}
	t.privateStorage.InternalGetKeyspaceMetadata = func(keyspace string) (*KeyspaceMetadata, error) {
		if keyspace == "" {
			return nil, ErrNoKeyspace
		}
		return s.privateStorage.InternalMetadataDescriber.GetKeyspace(keyspace)
	}
	t.privateStorage.InternalGetKeyspaceName = func() string { return s.privateStorage.InternalCfg.Keyspace }
	t.privateStorage.InternalLogger = s.privateStorage.InternalLogger
}

func (t *InternalTokenAwareHostPolicy) Reset() {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()

	// Sharing token aware host selection policy between sessions is not supported
	// but session initialization can failed for some reasons. So in our application
	// may be we want to create new session again.
	// Reset method should be called in Session.Close method
	t.privateStorage.InternalGetKeyspaceMetadata = nil
	t.privateStorage.InternalGetKeyspaceName = nil
	t.privateStorage.InternalLogger = nil
}

func (t *InternalTokenAwareHostPolicy) IsOperational(session *Session) error {
	return t.privateStorage.InternalFallback.IsOperational(session)
}

func (t *InternalTokenAwareHostPolicy) IsLocal(host *HostInfo) bool {
	return t.privateStorage.InternalFallback.IsLocal(host)
}

func (t *InternalTokenAwareHostPolicy) KeyspaceChanged(update KeyspaceUpdateEvent) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()
	meta := t.getMetadataForUpdate()
	t.updateReplicas(meta, update.Keyspace)
	t.privateStorage.metadata.Store(meta)
}

// updateReplicas updates replicas in clusterMeta.
// It must be called with t.mu mutex locked.
// meta must not be nil and it's replicas field will be updated.
func (t *InternalTokenAwareHostPolicy) updateReplicas(meta *InternalClusterMeta, keyspace string) {
	if keyspace == "" || meta == nil || meta.tokenRing == nil {
		return
	}
	newReplicas := make(map[string]InternalTokenRingReplicas, len(meta.replicas))

	ks, err := t.privateStorage.InternalGetKeyspaceMetadata(keyspace)
	if err == nil {
		strat := InternalGetStrategy(ks, t.privateStorage.InternalLogger)
		if strat != nil {
			newReplicas[keyspace] = strat.InternalReplicaMap(meta.tokenRing)
		}
	}

	for ks, replicas := range meta.replicas {
		if ks != keyspace {
			newReplicas[ks] = replicas
		}
	}

	meta.replicas = newReplicas
}

func (t *InternalTokenAwareHostPolicy) SetPartitioner(partitioner string) {
	t.privateStorage.mu.Lock()
	defer t.privateStorage.mu.Unlock()

	if t.privateStorage.partitioner != partitioner {
		t.privateStorage.InternalFallback.SetPartitioner(partitioner)
		t.privateStorage.partitioner = partitioner
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.InternalGet().InternalAllHosts(), t.privateStorage.InternalLogger)
		t.updateReplicas(meta, t.privateStorage.InternalGetKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
}

func (t *InternalTokenAwareHostPolicy) AddHost(host *HostInfo) {
	t.privateStorage.mu.Lock()
	if t.privateStorage.hosts.InternalAdd(host) {
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.InternalGet().InternalAllHosts(), t.privateStorage.InternalLogger)
		t.updateReplicas(meta, t.privateStorage.InternalGetKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
	t.privateStorage.mu.Unlock()
	t.privateStorage.InternalFallback.AddHost(host)
}

func (t *InternalTokenAwareHostPolicy) AddHosts(hosts []*HostInfo) {
	t.privateStorage.mu.Lock()
	t.privateStorage.hosts.InternalAddAll(hosts)

	meta := t.getMetadataForUpdate()
	meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.InternalGet().InternalAllHosts(), t.privateStorage.InternalLogger)
	t.updateReplicas(meta, t.privateStorage.InternalGetKeyspaceName())
	t.privateStorage.metadata.Store(meta)
	t.privateStorage.mu.Unlock()

	for _, host := range hosts {
		t.privateStorage.InternalFallback.AddHost(host)
	}
}

func (t *InternalTokenAwareHostPolicy) RemoveHost(host *HostInfo) {
	t.privateStorage.mu.Lock()
	if t.privateStorage.hosts.remove(host) {
		meta := t.getMetadataForUpdate()
		meta.resetTokenRing(t.privateStorage.partitioner, t.privateStorage.hosts.InternalGet().InternalAllHosts(), t.privateStorage.InternalLogger)
		t.updateReplicas(meta, t.privateStorage.InternalGetKeyspaceName())
		t.privateStorage.metadata.Store(meta)
	}
	t.privateStorage.mu.Unlock()
	t.privateStorage.InternalFallback.RemoveHost(host)
}

func (t *InternalTokenAwareHostPolicy) HostUp(host *HostInfo) {
	t.privateStorage.InternalFallback.HostUp(host)
}

func (t *InternalTokenAwareHostPolicy) HostDown(host *HostInfo) {
	t.privateStorage.InternalFallback.HostDown(host)
}

// getMetadataReadOnly returns current cluster metadata.
// Metadata uses copy on write, so the returned value should be only used for reading.
// To obtain a copy that could be updated, use getMetadataForUpdate instead.
func (t *InternalTokenAwareHostPolicy) getMetadataReadOnly() *InternalClusterMeta {
	return t.privateStorage.metadata.Load()
}

// getMetadataForUpdate returns clusterMeta suitable for updating.
// It is a SHALLOW copy of current metadata in case it was already set or new empty clusterMeta otherwise.
// This function should be called with t.mu mutex locked and the mutex should not be released before
// storing the new metadata.
func (t *InternalTokenAwareHostPolicy) getMetadataForUpdate() *InternalClusterMeta {
	metaReadOnly := t.getMetadataReadOnly()
	meta := new(InternalClusterMeta)
	if metaReadOnly != nil {
		*meta = *metaReadOnly
	}
	return meta
}

// resetTokenRing creates a new tokenRing.
// It must be called with t.mu locked.
func (m *InternalClusterMeta) resetTokenRing(partitioner string, hosts []*HostInfo, logger StdLogger) {
	if partitioner == "" {
		// partitioner not yet set
		return
	}

	// create a new token ring
	tokenRing, err := newTokenRing(partitioner, hosts)
	if err != nil {
		logger.Printf("Unable to update the token ring due to error: %s", err)
		return
	}

	// replace the token ring
	m.tokenRing = tokenRing
}

// hostSet is a small set optimized for tracking hosts returned by the
// token-aware iterator. Uses an inline array for RF <= 9 (3 DCs × RF=3),
// spilling to a map for larger replica sets.
type InternalHostSet struct {
	InternalOverflow map[*HostInfo]struct{}
	arr              [9]*HostInfo
	n                int
}

func (s *InternalHostSet) InternalAdd(h *HostInfo) {
	if s.n < len(s.arr) {
		s.arr[s.n] = h
		s.n++
		return
	}
	if s.InternalOverflow == nil {
		s.InternalOverflow = make(map[*HostInfo]struct{})
		for i := range s.n {
			s.InternalOverflow[s.arr[i]] = struct{}{}
		}
	}
	s.InternalOverflow[h] = struct{}{}
}

func (s *InternalHostSet) InternalContains(h *HostInfo) bool {
	if s.InternalOverflow != nil {
		_, ok := s.InternalOverflow[h]
		return ok
	}
	for i := range s.n {
		if s.arr[i] == h {
			return true
		}
	}
	return false
}

// shuffleHostsInPlace shuffles the given slice in-place using math/rand/v2.
func InternalShuffleHostsInPlace(hosts []*HostInfo) {
	randv2.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
}

// partitionHealthy performs an in-place stable partition of replicas, moving
// healthy (non-busy) hosts to the front while preserving relative order.
func InternalPartitionHealthy(replicas []*HostInfo, s *Session) {
	n := len(replicas)
	if n <= 1 {
		return
	}

	// Snapshot IsBusy to avoid TOCTOU races between counting and placement.
	var busyBuf [9]bool
	var busy []bool
	if n <= len(busyBuf) {
		busy = busyBuf[:n]
	} else {
		busy = make([]bool, n)
	}

	healthyCount := 0
	for i, h := range replicas {
		busy[i] = InternalHostBusy(s, h)
		if !busy[i] {
			healthyCount++
		}
	}

	if healthyCount == 0 || healthyCount == n {
		return // all same category, nothing to do
	}

	var buf [9]*HostInfo
	var tmp []*HostInfo
	if n <= len(buf) {
		tmp = buf[:n]
	} else {
		tmp = make([]*HostInfo, n)
	}
	copy(tmp, replicas)

	hi, ui := 0, healthyCount
	for i, h := range tmp {
		if !busy[i] {
			replicas[hi] = h
			hi++
		} else {
			replicas[ui] = h
			ui++
		}
	}
}

// Fast path: use the raw int64 hash when available, avoiding a Token-boxing
// allocation. Falls back to Hash() otherwise (see int64Hasher in token.go).

// Presized to the known upper bound.

// Zero-copy: replicas must not be mutated below unless needsMutation is true.

// Rare fallback (no tablet/replica-map match): GetHostForToken needs a
// boxed Token, so box only here instead of on every query.

// Cache IsLWT() once: it is read on both the shuffle and avoid-slow-replicas
// paths below, and computing it can take a lock on the query routing info.

// fallback

// filter the token aware selected hosts from the fallback hosts

type InternalDcAwareRR struct {
	local             string
	localHosts        InternalCowHostList
	remoteHosts       InternalCowHostList
	lastUsedHostIdx   uint64
	disableDCFailover bool
}

type InternalDcFailoverDisabledPolicy interface {
	setDCFailoverDisabled()
}

type InternalDcAwarePolicyOption func(p InternalDcFailoverDisabledPolicy)

func HostPolicyOptionDisableDCFailover(p InternalDcFailoverDisabledPolicy) {
	p.setDCFailoverDisabled()
}

// DCAwareRoundRobinPolicy is a host selection policies which will prioritize and
// return hosts which are in the local datacentre before returning hosts in all
// other datercentres
func DCAwareRoundRobinPolicy(localDC string, opts ...InternalDcAwarePolicyOption) HostSelectionPolicy {
	p := &InternalDcAwareRR{local: localDC, disableDCFailover: false}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (d *InternalDcAwareRR) setDCFailoverDisabled() {
	d.disableDCFailover = true
}

// dcFailoverDisabled reports whether this policy was constructed with
// HostPolicyOptionDisableDCFailover. Used by driver_config.go to report
// query.load-balancing.policy.fallback-to-non-preferred-nodes.
func (d *InternalDcAwareRR) dcFailoverDisabled() bool {
	return d.disableDCFailover
}

// localDatacenter reports the datacenter this policy prioritizes. Used by
// driver_config.go to report query.load-balancing.node-preference.
func (d *InternalDcAwareRR) localDatacenter() string {
	return d.local
}

func (d *InternalDcAwareRR) Init(*Session)                       {}
func (d *InternalDcAwareRR) Reset()                              {}
func (d *InternalDcAwareRR) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (d *InternalDcAwareRR) SetPartitioner(p string)             {}

func (d *InternalDcAwareRR) IsOperational(session *Session) error {
	if session.privateStorage.InternalCfg.privateStorage.disableInit || session.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		return nil
	}

	hosts := session.privateStorage.InternalHostSource.InternalGetHostsList()
	for _, host := range hosts {
		if !InternalClusterSettingsFilterHost(&session.privateStorage.InternalCfg, host) && host.DataCenter() == d.local {
			// Policy can work properly only if there is at least one host from target DC
			// No need to check host status, since it could be down due to the outage
			// We only need to make sure that policy is not misconfigured with wrong DC
			return nil
		}
	}

	return fmt.Errorf("gocql: datacenter %s in the policy was not found in the topology - probable DC aware policy misconfiguration", d.local)
}

func (d *InternalDcAwareRR) IsLocal(host *HostInfo) bool {
	return host.DataCenter() == d.local
}

func (d *InternalDcAwareRR) AddHost(host *HostInfo) {
	if d.IsLocal(host) {
		d.localHosts.InternalAdd(host)
	} else {
		d.remoteHosts.InternalAdd(host)
	}
}

func (d *InternalDcAwareRR) RemoveHost(host *HostInfo) {
	if d.IsLocal(host) {
		d.localHosts.remove(host)
	} else {
		d.remoteHosts.remove(host)
	}
}

func (d *InternalDcAwareRR) HostUp(host *HostInfo)   { d.AddHost(host) }
func (d *InternalDcAwareRR) HostDown(host *HostInfo) { d.RemoveHost(host) }

// This function is supposed to be called in a fashion
// roundRobbin(offset, hostsPriority1, hostsPriority2, hostsPriority3 ... )
//
// E.g. for DC-naive strategy:
// roundRobbin(offset, allHosts)
//
// For tiered and DC-aware strategy:
// roundRobbin(offset, localHosts, remoteHosts)
func InternalRoundRobbin(shift int, hosts ...[]*HostInfo) NextHost {
	currentLayer := 0
	currentlyObserved := 0

	return func() SelectedHost {
		// iterate over layers
		for {
			if currentLayer == len(hosts) {
				return nil
			}

			currentLayerSize := len(hosts[currentLayer])

			// iterate over hosts within a layer
			for {
				currentlyObserved++
				if currentlyObserved > currentLayerSize {
					currentLayer++
					currentlyObserved = 0
					break
				}

				h := hosts[currentLayer][(shift+currentlyObserved)%currentLayerSize]

				if h.IsUp() {
					return InternalSelectedHost{InternalInfo: h}
				}

			}
		}
	}
}

// RackAwareRoundRobinPolicy is a host selection policies which will prioritize and
// return hosts which are in the local rack, before hosts in the local datacenter but
// a different rack, before hosts in all other datacenters

type InternalRackAwareRR struct {
	localDC   string
	localRack string
	hosts     []InternalCowHostList
	// lastUsedHostIdx keeps the index of the last used host.
	// It is accessed atomically and needs to be aligned to 64 bits, so we
	// keep it first in the struct. Do not move it or add new struct members
	// before it.
	lastUsedHostIdx   uint64
	disableDCFailover bool
}

func RackAwareRoundRobinPolicy(localDC string, localRack string, opts ...InternalDcAwarePolicyOption) HostSelectionPolicy {
	p := &InternalRackAwareRR{localDC: localDC, localRack: localRack, hosts: make([]InternalCowHostList, 3), disableDCFailover: false}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (d *InternalRackAwareRR) Init(*Session)                       {}
func (d *InternalRackAwareRR) Reset()                              {}
func (d *InternalRackAwareRR) KeyspaceChanged(KeyspaceUpdateEvent) {}
func (d *InternalRackAwareRR) SetPartitioner(p string)             {}

func (d *InternalRackAwareRR) IsOperational(session *Session) error {
	if session.privateStorage.InternalCfg.privateStorage.disableInit || session.privateStorage.InternalCfg.privateStorage.InternalDisableControlConn {
		return nil
	}
	hosts := session.privateStorage.InternalHostSource.InternalGetHostsList()
	for _, host := range hosts {
		if !InternalClusterSettingsFilterHost(&session.privateStorage.InternalCfg, host) && host.DataCenter() == d.localDC && host.Rack() == d.localRack {
			// Policy can work properly only if there is at least one host from target DC+Rack
			// No need to check host status, since it could be down due to the outage
			// We only need to make sure that policy is not misconfigured with wrong DC+Rack
			return nil
		}
	}
	return fmt.Errorf("gocql: rack %s/%s was not found in the topology - probable Rack aware policy misconfiguration", d.localDC, d.localRack)
}

func (d *InternalRackAwareRR) MaxHostTier() uint {
	return 2
}

func (d *InternalRackAwareRR) setDCFailoverDisabled() {
	d.disableDCFailover = true
}

// dcFailoverDisabled reports whether this policy was constructed with
// HostPolicyOptionDisableDCFailover. Used by driver_config.go to report
// query.load-balancing.policy.fallback-to-non-preferred-nodes.
func (d *InternalRackAwareRR) dcFailoverDisabled() bool {
	return d.disableDCFailover
}

// localDatacenter and localRackName report the datacenter/rack this policy
// prioritizes. Used by driver_config.go to report
// query.load-balancing.node-preference.
func (d *InternalRackAwareRR) localDatacenter() string {
	return d.localDC
}

func (d *InternalRackAwareRR) localRackName() string {
	return d.localRack
}

func (d *InternalRackAwareRR) HostTier(host *HostInfo) uint {
	if host.DataCenter() == d.localDC {
		if host.Rack() == d.localRack {
			return 0
		} else {
			return 1
		}
	} else {
		return 2
	}
}

func (d *InternalRackAwareRR) IsLocal(host *HostInfo) bool {
	return d.HostTier(host) == 0
}

func (d *InternalRackAwareRR) AddHost(host *HostInfo) {
	dist := d.HostTier(host)
	d.hosts[dist].InternalAdd(host)
}

func (d *InternalRackAwareRR) RemoveHost(host *HostInfo) {
	dist := d.HostTier(host)
	d.hosts[dist].remove(host)
}

func (d *InternalRackAwareRR) HostUp(host *HostInfo)   { d.AddHost(host) }
func (d *InternalRackAwareRR) HostDown(host *HostInfo) { d.RemoveHost(host) }

// ReadyPolicy defines a policy for when a HostSelectionPolicy can be used. After
// each host connects during session initialization, the Ready method will be
// called. If you only need a single Host to be up you can wrap a
// HostSelectionPolicy policy with SingleHostReadyPolicy.
type ReadyPolicy interface {
	Ready() bool
}

// SingleHostReadyPolicy wraps a HostSelectionPolicy and returns Ready after a
// single host has been added via HostUp
func SingleHostReadyPolicy(p HostSelectionPolicy) *InternalSingleHostReadyPolicy {
	return &InternalSingleHostReadyPolicy{
		HostSelectionPolicy: p,
	}
}

type InternalSingleHostReadyPolicy struct {
	HostSelectionPolicy
	ready    bool
	readyMux sync.Mutex
}

func (s *InternalSingleHostReadyPolicy) HostUp(host *HostInfo) {
	s.HostSelectionPolicy.HostUp(host)

	s.readyMux.Lock()
	s.ready = true
	s.readyMux.Unlock()
}

func (s *InternalSingleHostReadyPolicy) Ready() bool {
	s.readyMux.Lock()
	ready := s.ready
	s.readyMux.Unlock()
	if !ready {
		return false
	}

	// in case the wrapped policy is also a ReadyPolicy, defer to that
	if rdy, ok := s.HostSelectionPolicy.(ReadyPolicy); ok {
		return rdy.Ready()
	}
	return true
}

func InternalHostBusy(s *Session, h *HostInfo) bool {
	pool, ok := s.privateStorage.InternalPool.InternalGetPool(h)
	return ok && h != nil && pool.InFlight() >= MAX_IN_FLIGHT_THRESHOLD
}
