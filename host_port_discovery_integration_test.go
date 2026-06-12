//go:build integration
// +build integration

package gocql

import (
	"net"
	"strconv"
	"testing"
)

// TestHostPortDiscoveryExplicitPort is a regression test for
// https://github.com/scylladb/gocql/issues/900.
//
// It reproduces the scenario where a ScyllaDB cluster exposes CQL on a
// non-default port (e.g. the TLS port 9142) while cfg.Port is left at its
// default value (9042). The port is supplied explicitly via cfg.Hosts entries
// using the host:port form. After the regression, peer discovery from
// system.local/system.peers assigned cfg.Port (9042) to all discovered hosts
// instead of the port the control connection was actually established on,
// causing subsequent data connections to fail.
func TestHostPortDiscoveryExplicitPort(t *testing.T) {
	if *flagDistribution == "cassandra" {
		t.Skip("skipping; the separate CQL TLS port scenario is ScyllaDB-specific")
	}
	t.Parallel()

	const (
		sslPort       = 9142
		plaintextPort = 9042
	)

	// sslOpts mirrors the client material the Makefile provisions under
	// testdata/pki for the ScyllaDB cluster (client auth is required).
	newSslOpts := func() *SslOptions {
		return &SslOptions{
			CertPath:               "testdata/pki/gocql.crt",
			KeyPath:                "testdata/pki/gocql.key",
			CaPath:                 "testdata/pki/ca.crt",
			EnableHostVerification: false,
		}
	}

	hostsWithPort := func(hosts []string, port int) []string {
		out := make([]string, 0, len(hosts))
		for _, h := range hosts {
			host := h
			if hostOnly, _, err := net.SplitHostPort(h); err == nil {
				host = hostOnly
			}
			out = append(out, net.JoinHostPort(host, strconv.Itoa(port)))
		}
		return out
	}

	baseHosts := getClusterHosts()

	// Probe: verify the cluster is reachable over TLS on 9142 using a correct
	// configuration. If it is not (e.g. a node started without
	// native_transport_port_ssl / client encryption), skip rather than fail, so
	// the test does not produce false negatives in non-TLS environments.
	probe := createCluster()
	probe.Port = sslPort
	probe.Hosts = hostsWithPort(baseHosts, sslPort)
	probe.SslOpts = newSslOpts()
	probeSession, err := probe.CreateSession()
	if err != nil {
		t.Skipf("skipping; cluster not reachable over TLS on port %d: %v", sslPort, err)
	}
	probeSession.Close()

	// Buggy scenario: cfg.Port stays at the default plaintext port while the
	// explicit TLS port is provided only through cfg.Hosts entries. With the
	// regression present, discovered hosts get cfg.Port (9042) and data
	// connections fail; with the fix they inherit the control connection's port.
	cluster := createCluster()
	cluster.Port = plaintextPort
	cluster.Hosts = hostsWithPort(baseHosts, sslPort)
	cluster.SslOpts = newSslOpts()

	session, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession failed; the explicit port from cfg.Hosts was likely not propagated to discovered hosts: %v", err)
	}
	defer session.Close()

	ringHosts := session.hostSource.getHostsList()
	if len(ringHosts) == 0 {
		t.Fatal("expected at least one host in the ring")
	}

	for _, host := range ringHosts {
		if host.Port() != sslPort {
			t.Errorf("host %s discovered with port %d, expected %d (port from cfg.Hosts must be propagated, not cfg.Port)",
				host.ConnectAddress(), host.Port(), sslPort)
		}
	}

	// Exercise the data plane to confirm pool connections succeed on the TLS
	// port. Before the fix, data connections went to cfg.Port (9042) with TLS
	// enabled and failed with a TLS handshake error.
	var releaseVersion string
	if err := session.Query("SELECT release_version FROM system.local").
		Consistency(One).Scan(&releaseVersion); err != nil {
		t.Fatalf("data-plane query failed (connections likely used the wrong port): %v", err)
	}
}
