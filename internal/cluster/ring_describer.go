// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"
	"fmt"
	"sync"
)

// Polls system.peers at a specific interval to find new hosts
type InternalRingDescriber struct {
	InternalControl InternalControlConnection
	InternalLogger  StdLogger
	InternalCfg     *InternalClusterSettings
	// hosts are the set of all hosts in the cassandra ring that we know of.
	// key of map is host_id.
	InternalHosts map[string]*HostInfo
	// hostIPToUUID maps host native address to host_id.
	hostIPToUUID       map[string]string
	prevPartitioner    string
	prevHosts          []*HostInfo
	mu                 sync.RWMutex
	readMu             sync.Mutex
	revision           uint64
	eventEpoch         uint64
	refreshSequence    uint64
	tombstones         map[string]bool
	tombstoneAddresses map[string]string
	diagnostics        []error
}

func (r *InternalRingDescriber) setControlConn(c InternalControlConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.InternalControl = c
}

// Ask the control node for the local host info
func (r *InternalRingDescriber) getLocalHostInfo(ctx context.Context, conn ConnInterface) (*HostInfo, error) {
	hosts, err := InternalReadV2Topology(ctx, conn, qrySystemLocal, r.InternalCfg.Port)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve local host info: %w", err)
	}
	if len(hosts) != 1 || implEndpoint.InternalHostInfoHostUUID(hosts[0]).IsEmpty() || implEndpoint.InternalHostInfoInvalidConnectAddr(hosts[0]) {
		return nil, typeValidation("local topology", "expected one valid local identity")
	}
	return hosts[0], nil
}

func (r *InternalRingDescriber) InternalGetClusterPeerInfo(ctx context.Context, c ConnInterface) ([]*HostInfo, error) {
	stmt := qrySystemPeersCassandra
	if c.InternalGetIsSchemaV2() {
		stmt = qrySystemPeersV2
	} else if c.InternalIsScyllaConn() {
		stmt = qrySystemPeers
	}
	hosts, err := InternalReadV2Topology(ctx, c, stmt, r.InternalCfg.Port)
	if err != nil {
		return nil, err
	}
	valid := hosts[:0]
	for _, h := range hosts {
		if InternalIsValidPeer(h) {
			valid = append(valid, h)
			continue
		}
		cause := typeValidation("peer topology", "incomplete peer %s", h.HostID())
		r.mu.Lock()
		known := r.InternalHosts[h.HostID()] != nil
		for _, old := range r.prevHosts {
			if implEndpoint.InternalHostInfoHostUUID(old) == implEndpoint.InternalHostInfoHostUUID(h) || implEndpoint.InternalHostInfoNodeToNodeAddress(old).Equal(implEndpoint.InternalHostInfoNodeToNodeAddress(h)) {
				known = true
				break
			}
		}
		if len(r.diagnostics) < 64 {
			r.diagnostics = append(r.diagnostics, cause)
		}
		r.mu.Unlock()
		if known {
			return nil, cause
		}

	}
	return valid, nil
}

func InternalGetPeersFromQuerySystemPeers(querySystemPeerRows []map[string]any, defaultPort int, logger StdLogger) ([]*HostInfo, error) {
	var peers []*HostInfo

	for _, row := range querySystemPeerRows {
		// extract all available info about the peer
		host, err := InternalHostInfoFromMap(row, defaultPort)
		if err != nil {
			return nil, err
		} else if !InternalIsValidPeer(host) {
			// If it's not a valid peer
			logger.Printf("Found invalid peer '%s' "+
				"Likely due to a gossip or snitch issue, this host will be ignored", host)
			continue
		} else if InternalIsZeroToken(host) {
			continue
		}

		peers = append(peers, host)
	}

	return peers, nil
}

// Return true if the host is a valid peer
func InternalIsValidPeer(host *HostInfo) bool {
	return !(len(host.RPCAddress()) == 0 || implEndpoint.HostInfoData(host).InternalHostId.IsEmpty() || implEndpoint.HostInfoData(host).InternalDataCenter == "" || implEndpoint.HostInfoData(host).InternalRack == "")
}

func InternalIsZeroToken(host *HostInfo) bool {
	return len(implEndpoint.HostInfoData(host).InternalTokens) == 0
}

// GetHostsFromSystem returns a list of hosts found via queries to system.local and system.peers
func (r *InternalRingDescriber) GetHostsFromSystem() ([]*HostInfo, string, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	r.mu.Lock()
	r.diagnostics = nil
	r.mu.Unlock()
	r.mu.RLock()
	control := r.InternalControl
	epoch := r.eventEpoch
	previous := r.prevHosts
	partitionerBefore := r.prevPartitioner
	r.mu.RUnlock()

	if control == nil {
		return previous, partitionerBefore, errNoControl
	}
	ch := control.InternalGetConn()
	if ch == nil || ch.InternalConn == nil {
		return previous, partitionerBefore, errNoControl
	}

	var (
		localHost *HostInfo
		peerHosts []*HostInfo
		wg        sync.WaitGroup
		errMu     sync.Mutex
		firstErr  error
	)

	ctx, cancel := context.WithCancel(v2SchemaContext(InternalNewSchemaSource(control.InternalGetSession())))
	defer cancel()

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		defer errMu.Unlock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		var localErr error
		localHost, localErr = r.getLocalHostInfo(ctx, ch.InternalConn)
		recordErr(localErr)
	}()
	go func() {
		defer wg.Done()
		var peerErr error
		peerHosts, peerErr = r.InternalGetClusterPeerInfo(ctx, ch.InternalConn)
		recordErr(peerErr)
	}()
	wg.Wait()

	if firstErr != nil {
		return previous, partitionerBefore, firstErr
	}

	hosts := []*HostInfo{localHost}
	hosts = append(hosts, peerHosts...)
	seen := make(map[UUID]bool, len(hosts))
	for _, h := range hosts {
		if seen[implEndpoint.InternalHostInfoHostUUID(h)] {
			return previous, partitionerBefore, typeValidation("topology", "duplicate host identity %s", h.HostID())
		}
		seen[implEndpoint.InternalHostInfoHostUUID(h)] = true
	}

	var partitioner string
	if len(hosts) > 0 {
		partitioner = hosts[0].Partitioner()
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.eventEpoch != epoch {
		return previous, partitionerBefore, typeValidation("topology generation", "topology changed during read")
	}
	filtered := hosts[:0]
	for _, host := range hosts {
		if !r.tombstones[host.HostID()] {
			filtered = append(filtered, host)
		}
	}
	hosts = filtered
	r.prevHosts = hosts
	r.prevPartitioner = partitioner

	return hosts, partitioner, nil
}

func (r *InternalRingDescriber) InternalGetHostByIP(ip string) (*HostInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hi, ok := r.hostIPToUUID[ip]
	return r.InternalHosts[hi], ok
}

func (r *InternalRingDescriber) InternalGetHost(hostID string) *HostInfo {
	r.mu.RLock()
	host := r.InternalHosts[hostID]
	r.mu.RUnlock()
	return host
}

func (r *InternalRingDescriber) InternalGetHostsList() []*HostInfo {
	r.mu.RLock()
	hosts := make([]*HostInfo, 0, len(r.InternalHosts))
	for _, host := range r.InternalHosts {
		hosts = append(hosts, host)
	}
	r.mu.RUnlock()
	return hosts
}

func (r *InternalRingDescriber) getHostsMap() map[string]*HostInfo {
	r.mu.RLock()
	hosts := make(map[string]*HostInfo, len(r.InternalHosts))
	for k, v := range r.InternalHosts {
		hosts[k] = v
	}
	r.mu.RUnlock()
	return hosts
}

func (r *InternalRingDescriber) InternalAddOrUpdate(host *HostInfo) *HostInfo {
	if existingHost, ok := r.InternalAddHostIfMissing(host); ok {
		implEndpoint.InternalHostInfoUpdate(existingHost, host)
		host = existingHost
	}
	return host
}

func (r *InternalRingDescriber) InternalAddHostIfMissing(host *HostInfo) (*HostInfo, bool) {
	if !validIpAddr(host.ConnectAddress()) {
		panic(fmt.Sprintf("invalid host: %v", host))
	}
	hostID := host.HostID()

	r.mu.Lock()
	if r.InternalHosts == nil {
		r.InternalHosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	existing, ok := r.InternalHosts[hostID]
	if !ok {
		r.revision++
		r.InternalHosts[hostID] = host
		r.hostIPToUUID[implEndpoint.InternalHostInfoNodeToNodeAddress(host).String()] = hostID
		existing = host
	}
	r.mu.Unlock()
	return existing, ok
}

func (r *InternalRingDescriber) InternalRemoveHost(hostID string) bool {
	r.mu.Lock()
	if r.InternalHosts == nil {
		r.InternalHosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	h, ok := r.InternalHosts[hostID]
	if ok {
		delete(r.hostIPToUUID, implEndpoint.InternalHostInfoNodeToNodeAddress(h).String())
	}
	delete(r.InternalHosts, hostID)
	r.revision++
	r.mu.Unlock()
	return ok
}
