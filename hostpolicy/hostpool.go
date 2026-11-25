package hostpolicy

import (
	"fmt"
	"sync"

	"github.com/hailocab/go-hostpool"

	"github.com/gocql/gocql"
)

// HostPool is a host policy which uses the bitly/go-hostpool library
// to distribute queries between hosts and prevent sending queries to
// unresponsive hosts. When creating the host pool that is passed to the policy
// use an empty slice of hosts as the hostpool will be populated later by gocql.
// Peer names exposed by the supplied pool are host IDs or opaque registration
// keys for hosts without IDs, rather than connection addresses. Treat these names
// as opaque identifiers when logging or collecting per-peer statistics.
// An ID-less contact point and an identified node at the same address remain
// separate registrations until the contact point is explicitly removed.
// See below for examples of usage:
//
//	// Create host selection policy using a simple host pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPool(hostpool.New(nil))
//
//	// Create host selection policy using an epsilon greedy pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPool(
//	    hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
//	)
func HostPool(hp hostpool.HostPool) gocql.HostSelectionPolicy {
	return &hostPoolHostPolicy{hostMap: map[string]*gocql.HostInfo{}, hp: hp}
}

type hostPoolHostPolicy struct {
	hp      hostpool.HostPool
	hostMap map[string]*gocql.HostInfo
	nextKey uint64
	mu      sync.RWMutex
}

// hostKey is called with r.mu held.
func (r *hostPoolHostPolicy) hostKey(host *gocql.HostInfo, pending map[string]*gocql.HostInfo) string {
	if id := host.HostID(); id != "" {
		return id
	}
	// Match exact registrations before considering any equivalent endpoint.
	for _, hosts := range []map[string]*gocql.HostInfo{r.hostMap, pending} {
		for key, registered := range hosts {
			if registered == host {
				return key
			}
		}
	}
	address := host.ConnectAddress()
	// Canonicalize this batch before consulting aliases in the previous map.
	for _, hosts := range []map[string]*gocql.HostInfo{pending, r.hostMap} {
		for key, registered := range hosts {
			if registered.HostID() == "" && registered.ConnectAddress().Equal(address) {
				return key
			}
		}
	}
	r.nextKey++
	return fmt.Sprintf("gocql-host-%d", r.nextKey)
}

func (r *hostPoolHostPolicy) Init(*gocql.Session)                       {}
func (r *hostPoolHostPolicy) Reset()                                    {}
func (r *hostPoolHostPolicy) IsOperational(*gocql.Session) error        { return nil }
func (r *hostPoolHostPolicy) KeyspaceChanged(gocql.KeyspaceUpdateEvent) {}
func (r *hostPoolHostPolicy) SetPartitioner(string)                     {}
func (r *hostPoolHostPolicy) IsLocal(*gocql.HostInfo) bool              { return true }

func (r *hostPoolHostPolicy) SetHosts(hosts []*gocql.HostInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	peers := make([]string, 0, len(hosts))
	hostMap := make(map[string]*gocql.HostInfo, len(hosts))

	for _, host := range hosts {
		key := r.hostKey(host, hostMap)
		if _, present := hostMap[key]; !present {
			peers = append(peers, key)
		}
		hostMap[key] = host
	}

	r.hp.SetHosts(peers)
	r.hostMap = hostMap
}

func (r *hostPoolHostPolicy) AddHost(host *gocql.HostInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.hostKey(host, nil)

	// Refresh host information without resetting the registered backend peer.
	if h, ok := r.hostMap[key]; ok && h != nil {
		r.hostMap[key] = host
		return
	}
	// otherwise, add the host to the map
	r.hostMap[key] = host
	// and construct a new peer list to give to the HostPool
	hosts := make([]string, 0, len(r.hostMap))
	for addr := range r.hostMap {
		hosts = append(hosts, addr)
	}

	r.hp.SetHosts(hosts)
}

func (r *hostPoolHostPolicy) RemoveHost(host *gocql.HostInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := r.hostKey(host, nil)

	if _, ok := r.hostMap[key]; !ok {
		return
	}

	delete(r.hostMap, key)
	hosts := make([]string, 0, len(r.hostMap))
	for key := range r.hostMap {
		hosts = append(hosts, key)
	}

	r.hp.SetHosts(hosts)
}

func (r *hostPoolHostPolicy) HostUp(host *gocql.HostInfo) {
	r.AddHost(host)
}

func (r *hostPoolHostPolicy) HostDown(host *gocql.HostInfo) {
	r.RemoveHost(host)
}

func (r *hostPoolHostPolicy) Pick(qry gocql.ExecutableQuery) gocql.NextHost {
	return func() gocql.SelectedHost {
		r.mu.RLock()
		defer r.mu.RUnlock()

		if len(r.hostMap) == 0 {
			return nil
		}

		hostR := r.hp.Get()
		host, ok := r.hostMap[hostR.Host()]
		if !ok {
			return nil
		}

		return selectedHostPoolHost{
			policy: r,
			info:   host,
			hostR:  hostR,
		}
	}
}

// selectedHostPoolHost is a host returned by the hostPoolHostPolicy and
// implements the SelectedHost interface
type selectedHostPoolHost struct {
	policy *hostPoolHostPolicy
	info   *gocql.HostInfo
	hostR  hostpool.HostPoolResponse
}

func (host selectedHostPoolHost) Info() *gocql.HostInfo {
	return host.info
}

func (host selectedHostPoolHost) Token() gocql.Token {
	return nil
}

func (host selectedHostPoolHost) Mark(err error) {
	key := host.hostR.Host()

	host.policy.mu.RLock()
	defer host.policy.mu.RUnlock()

	if _, ok := host.policy.hostMap[key]; !ok {
		// host was removed between pick and mark
		return
	}

	host.hostR.Mark(err)
}
