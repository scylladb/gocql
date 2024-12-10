package gocql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
)

// Polls system.peers at a specific interval to find new hosts
type ringDescriber struct {
	control         controlConnection
	cfg             *ClusterConfig
	logger          StdLogger
	mu              sync.Mutex
	prevHosts       []*HostInfo
	prevPartitioner string
}

func (r *ringDescriber) setControlConn(c controlConnection) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.control = c
}

// Ask the control node for the local host info
func (r *ringDescriber) getLocalHostInfo(conn ConnInterface) (*HostInfo, error) {
	iter := conn.querySystem(context.TODO(), qrySystemLocal)

	if iter == nil {
		return nil, errNoControl
	}

	host, err := hostInfoFromIter(iter, nil, r.cfg.Port, r.cfg.translateAddressPort)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve local host info: %w", err)
	}
	return host, nil
}

// Ask the control node for host info on all it's known peers
func (r *ringDescriber) getClusterPeerInfo(localHost *HostInfo, c ConnInterface) ([]*HostInfo, error) {
	var iter *Iter
	if c.getIsSchemaV2() {
		iter = c.querySystem(context.TODO(), qrySystemPeersV2)
	} else {
		iter = c.querySystem(context.TODO(), qrySystemPeers)
	}

	if iter == nil {
		return nil, errNoControl
	}

	rows, err := iter.SliceMap()
	if err != nil {
		// TODO(zariel): make typed error
		return nil, fmt.Errorf("unable to fetch peer host info: %s", err)
	}

	return getPeersFromQuerySystemPeers(rows, r.cfg.Port, r.cfg.translateAddressPort, r.logger)
}

func getPeersFromQuerySystemPeers(querySystemPeerRows []map[string]interface{}, port int, translateAddressPort func(addr net.IP, port int) (net.IP, int), logger StdLogger) ([]*HostInfo, error) {
	var peers []*HostInfo

	for _, row := range querySystemPeerRows {
		// extract all available info about the peer
		host, err := hostInfoFromMap(row, &HostInfo{port: port}, translateAddressPort)
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
		host.hostId == "" ||
		host.dataCenter == "" ||
		host.rack == "")
}

func isZeroToken(host *HostInfo) bool {
	return len(host.tokens) == 0
}

// GetHosts returns a list of hosts found via queries to system.local and system.peers
func (r *ringDescriber) GetHosts() ([]*HostInfo, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.control == nil {
		return r.prevHosts, r.prevPartitioner, errNoControl
	}

	ch := r.control.getConn()
	localHost, err := r.getLocalHostInfo(ch.conn)
	if err != nil {
		return r.prevHosts, r.prevPartitioner, err
	}

	peerHosts, err := r.getClusterPeerInfo(localHost, ch.conn)
	if err != nil {
		return r.prevHosts, r.prevPartitioner, err
	}

	var hosts []*HostInfo
	if !isZeroToken(localHost) {
		hosts = []*HostInfo{localHost}
	}
	hosts = append(hosts, peerHosts...)

	var partitioner string
	if len(hosts) > 0 {
		partitioner = hosts[0].Partitioner()
	}

	return hosts, partitioner, nil
}

// Given an ip/port return HostInfo for the specified ip/port
func (r *ringDescriber) getHostInfo(hostID UUID) (*HostInfo, error) {
	var host *HostInfo
	for _, table := range []string{"system.peers", "system.local"} {
		ch := r.control.getConn()
		var iter *Iter
		if ch.host.HostID() == hostID.String() {
			host = ch.host
			iter = nil
		}

		if table == "system.peers" {
			if ch.conn.getIsSchemaV2() {
				iter = ch.conn.querySystem(context.TODO(), qrySystemPeersV2)
			} else {
				iter = ch.conn.querySystem(context.TODO(), qrySystemPeers)
			}
		} else {
			iter = ch.conn.query(context.TODO(), fmt.Sprintf("SELECT * FROM %s", table))
		}

		if iter != nil {
			rows, err := iter.SliceMap()
			if err != nil {
				return nil, err
			}

			for _, row := range rows {
				h, err := hostInfoFromMap(row, &HostInfo{port: r.cfg.Port}, r.cfg.translateAddressPort)
				if err != nil {
					return nil, err
				}

				if h.HostID() == hostID.String() {
					host = h
					break
				}
			}
		}
	}

	if host == nil {
		return nil, errors.New("unable to fetch host info: invalid control connection")
	} else if host.invalidConnectAddr() {
		return nil, fmt.Errorf("host ConnectAddress invalid ip=%v: %v", host.connectAddress, host)
	}

	return host, nil
}
