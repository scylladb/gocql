//go:build integration
// +build integration

package gocql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
)

type mockDNSResolver struct {
	lock sync.RWMutex
	data map[string][]net.IP
}

func newMockDNSResolver() *mockDNSResolver {
	return &mockDNSResolver{
		data: make(map[string][]net.IP),
		lock: sync.RWMutex{},
	}
}

func (r *mockDNSResolver) LookupIP(host string) ([]net.IP, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	ips, _ := r.data[host]
	if len(ips) == 0 {
		return nil, &net.DNSError{Err: errors.New("no IP addresses").Error(), Name: host}
	}
	return ips, nil
}

func (r *mockDNSResolver) Update(host string, ips ...net.IP) {
	r.lock.Lock()
	r.data[host] = ips
	defer r.lock.Unlock()
}

func (r *mockDNSResolver) Delete(hosts ...string) {
	r.lock.Lock()
	for _, host := range hosts {
		delete(r.data, host)
	}
	defer r.lock.Unlock()
}

func MustIP(ip string) net.IP {
	out := net.ParseIP(ip)
	if out == nil {
		panic("failed to parse IP: " + ip)
	}
	return out
}

func TestDNS(t *testing.T) {
	t.Parallel()

	checkIfSessionWorking := func(t *testing.T, cluster Config, hosts []string) {
		s, err := Connect(context.Background(), cluster)
		if err != nil {
			t.Fatalf("failed to create session: %v", err)
		}
		defer s.Close()

		err = s.RefreshMetadata(context.Background())
		if err != nil {
			t.Fatalf("failed to refresh ring: %v", err)
		}

		rows, err := v2Query(context.Background(), s, "select * from system.peers")
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		if err != nil {
			t.Fatalf("failed to execute query: %v", err)
		}
		ringHosts := s.Metadata().Hosts()
		if len(ringHosts) != len(hosts) {
			t.Fatalf("wrong number of hosts: got %d, want %d", len(ringHosts), len(hosts))
		}
	}

	OneDNSPerNode := func(c *Config) {
		r := newMockDNSResolver()
		var dnsRecords []string
		for id, host := range c.ContactPoints {
			dns := fmt.Sprintf("node%d.cluster.local", id+1)
			dnsRecords = append(dnsRecords, dns)
			r.Update(dns, MustIP(host))
		}
		c.DNSResolver = r
		c.ContactPoints = dnsRecords
	}

	OneDNSPerCluster := func(c *Config) {
		r := newMockDNSResolver()
		var hostIPs []net.IP
		for _, host := range c.ContactPoints {
			hostIPs = append(hostIPs, MustIP(host))
		}
		r.Update("cluster.local", hostIPs...)
		c.DNSResolver = r
		c.ContactPoints = []string{"cluster.local"}
	}

	OneDNSPerClusterFirstBroken := func(c *Config) {
		r := newMockDNSResolver()
		var hostIPs []net.IP
		for _, host := range c.ContactPoints {
			hostIPs = append(hostIPs, MustIP(host))
		}
		hostIPs[0] = MustIP("0.0.0.0")
		r.Update("cluster.local", hostIPs...)
		c.DNSResolver = r
		c.ContactPoints = []string{"cluster.local"}
	}

	WithAddressTranslator := func(c *Config) {
		var toAddresses []net.IP
		var fromAddresses []net.IP
		var clusterHosts []string
		for _, host := range c.ContactPoints {
			ip := MustIP(host)

			var fromAddress net.IP
			if ip.To4().String() == ip.String() {
				ip = ip.To4()
				fromAddress = net.IPv4(ip[0], ip[1], ip[2]+1, ip[3])
			} else {
				fromAddress = net.IP{ip[0], ip[1], ip[2], ip[3], ip[4], ip[5], ip[6], ip[7], ip[8], ip[9], ip[10], ip[11], ip[12] + 1, ip[13], ip[14], ip[15]}
			}
			toAddresses = append(toAddresses, ip)
			fromAddresses = append(fromAddresses, fromAddress)
			clusterHosts = append(clusterHosts, fromAddress.String())
		}

		c.AddressTranslator = AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
			for id, host := range fromAddresses {
				if host.Equal(addr) {
					return toAddresses[id], port
				}
			}
			for _, host := range toAddresses {
				if host.Equal(addr) {
					return addr, port
				}
			}
			panic("failed to translate address")
		})
		c.ContactPoints = clusterHosts
	}

	testCases := []struct {
		name        string
		clusterMods []func(*Config)
	}{
		{
			name:        "OneDNSPerNode",
			clusterMods: []func(*Config){OneDNSPerNode},
		},
		{
			name:        "OneDNSPerCluster",
			clusterMods: []func(*Config){OneDNSPerCluster},
		},
		{
			name:        "AddressTranslator+OneDNSPerNode",
			clusterMods: []func(*Config){WithAddressTranslator, OneDNSPerNode},
		},
		{
			name:        "AddressTranslator+OneDNSPerCluster",
			clusterMods: []func(*Config){WithAddressTranslator, OneDNSPerCluster},
		},
		{
			name:        "OneDNSPerClusterFirstBroken",
			clusterMods: []func(*Config){OneDNSPerClusterFirstBroken},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := createV2Cluster(tc.clusterMods...)
			checkIfSessionWorking(t, cluster, getClusterHosts())
		})
	}
}
