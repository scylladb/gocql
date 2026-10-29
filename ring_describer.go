package gocql

import (
	"context"
	"fmt"
	"sync"
)

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	control controlConnection
	logger  StdLogger
	cfg     *ClusterConfig
	// hosts are the set of all hosts in the cassandra ring that we know of.
	// key of map is host_id.
	hosts map[string]*HostInfo
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

func (r *ringDescriber) setControlConn(c controlConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.control = c
}

// Ask the control node for the local host info
func (r *ringDescriber) getLocalHostInfo(ctx context.Context, conn ConnInterface) (*HostInfo, error) {
	hosts, err := readV2Topology(ctx, conn, qrySystemLocal, r.cfg.Port)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve local host info: %w", err)
	}
	if len(hosts) != 1 || hosts[0].hostUUID().IsEmpty() || hosts[0].invalidConnectAddr() {
		return nil, typeValidation("local topology", "expected one valid local identity")
	}
	return hosts[0], nil
}

func (r *ringDescriber) getClusterPeerInfo(ctx context.Context, c ConnInterface) ([]*HostInfo, error) {
	stmt := qrySystemPeersCassandra
	if c.getIsSchemaV2() {
		stmt = qrySystemPeersV2
	} else if c.isScyllaConn() {
		stmt = qrySystemPeers
	}
	hosts, err := readV2Topology(ctx, c, stmt, r.cfg.Port)
	if err != nil {
		return nil, err
	}
	valid := hosts[:0]
	for _, h := range hosts {
		if isValidPeer(h) {
			valid = append(valid, h)
			continue
		}
		cause := typeValidation("peer topology", "incomplete peer %s", h.HostID())
		r.mu.Lock()
		known := r.hosts[h.HostID()] != nil
		for _, old := range r.prevHosts {
			if old.hostUUID() == h.hostUUID() || old.nodeToNodeAddress().Equal(h.nodeToNodeAddress()) {
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

func getPeersFromQuerySystemPeers(querySystemPeerRows []map[string]any, defaultPort int, logger StdLogger) ([]*HostInfo, error) {
	var peers []*HostInfo

	for _, row := range querySystemPeerRows {
		// extract all available info about the peer
		host, err := hostInfoFromMap(row, defaultPort)
		if err != nil {
			return nil, err
		} else if !isValidPeer(host) {
			// If it's not a valid peer
			logger.Printf("Found invalid peer '%s' "+
				"Likely due to a gossip or snitch issue, this host will be ignored", host)
			continue
		} else if isZeroToken(host) {
			continue
		}

		peers = append(peers, host)
	}

	return peers, nil
}

// Return true if the host is a valid peer
func isValidPeer(host *HostInfo) bool {
	return !(len(host.RPCAddress()) == 0 ||
		host.hostId.IsEmpty() ||
		host.dataCenter == "" ||
		host.rack == "")
}

func isZeroToken(host *HostInfo) bool {
	return len(host.tokens) == 0
}

// GetHostsFromSystem returns a list of hosts found via queries to system.local and system.peers
func (r *ringDescriber) GetHostsFromSystem() ([]*HostInfo, string, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	r.mu.Lock()
	r.diagnostics = nil
	r.mu.Unlock()
	r.mu.RLock()
	control := r.control
	epoch := r.eventEpoch
	previous := r.prevHosts
	partitionerBefore := r.prevPartitioner
	r.mu.RUnlock()

	if control == nil {
		return previous, partitionerBefore, errNoControl
	}
	ch := control.getConn()
	if ch == nil || ch.conn == nil {
		return previous, partitionerBefore, errNoControl
	}

	var (
		localHost *HostInfo
		peerHosts []*HostInfo
		wg        sync.WaitGroup
		errMu     sync.Mutex
		firstErr  error
	)

	ctx, cancel := context.WithCancel(v2SchemaContext(control.getSession()))
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
		localHost, localErr = r.getLocalHostInfo(ctx, ch.conn)
		recordErr(localErr)
	}()
	go func() {
		defer wg.Done()
		var peerErr error
		peerHosts, peerErr = r.getClusterPeerInfo(ctx, ch.conn)
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
		if seen[h.hostUUID()] {
			return previous, partitionerBefore, typeValidation("topology", "duplicate host identity %s", h.HostID())
		}
		seen[h.hostUUID()] = true
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

func (r *ringDescriber) getHostByIP(ip string) (*HostInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	hi, ok := r.hostIPToUUID[ip]
	return r.hosts[hi], ok
}

func (r *ringDescriber) getHost(hostID string) *HostInfo {
	r.mu.RLock()
	host := r.hosts[hostID]
	r.mu.RUnlock()
	return host
}

func (r *ringDescriber) getHostsList() []*HostInfo {
	r.mu.RLock()
	hosts := make([]*HostInfo, 0, len(r.hosts))
	for _, host := range r.hosts {
		hosts = append(hosts, host)
	}
	r.mu.RUnlock()
	return hosts
}

func (r *ringDescriber) getHostsMap() map[string]*HostInfo {
	r.mu.RLock()
	hosts := make(map[string]*HostInfo, len(r.hosts))
	for k, v := range r.hosts {
		hosts[k] = v
	}
	r.mu.RUnlock()
	return hosts
}

func (r *ringDescriber) addOrUpdate(host *HostInfo) *HostInfo {
	if existingHost, ok := r.addHostIfMissing(host); ok {
		existingHost.update(host)
		host = existingHost
	}
	return host
}

func (r *ringDescriber) addHostIfMissing(host *HostInfo) (*HostInfo, bool) {
	if !validIpAddr(host.ConnectAddress()) {
		panic(fmt.Sprintf("invalid host: %v", host))
	}
	hostID := host.HostID()

	r.mu.Lock()
	if r.hosts == nil {
		r.hosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	existing, ok := r.hosts[hostID]
	if !ok {
		r.revision++
		r.hosts[hostID] = host
		r.hostIPToUUID[host.nodeToNodeAddress().String()] = hostID
		existing = host
	}
	r.mu.Unlock()
	return existing, ok
}

func (r *ringDescriber) removeHost(hostID string) bool {
	r.mu.Lock()
	if r.hosts == nil {
		r.hosts = make(map[string]*HostInfo)
	}
	if r.hostIPToUUID == nil {
		r.hostIPToUUID = make(map[string]string)
	}

	h, ok := r.hosts[hostID]
	if ok {
		delete(r.hostIPToUUID, h.nodeToNodeAddress().String())
	}
	delete(r.hosts, hostID)
	r.revision++
	r.mu.Unlock()
	return ok
}
