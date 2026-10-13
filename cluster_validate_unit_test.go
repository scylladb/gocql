//go:build unit
// +build unit

package gocql

import (
	"net"
	"strings"
	"testing"
	"time"
)

// TestValidateRejects walks every check in ClusterConfig.Validate that turns a
// misconfiguration into an error, so a user sees it at CreateSession rather than
// as a confusing failure later.
func TestValidateRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(cfg *ClusterConfig)
		wantErr string
	}{
		{"NoHosts", func(c *ClusterConfig) { c.Hosts = nil }, ErrNoHosts.Error()},
		{"AuthenticatorAndAuthProvider", func(c *ClusterConfig) {
			c.Authenticator = PasswordAuthenticator{}
			c.AuthProvider = func(*HostInfo) (Authenticator, error) { return nil, nil }
		}, "both Authenticator and AuthProvider"},
		{"NilInitialReconnectionPolicy", func(c *ClusterConfig) { c.InitialReconnectionPolicy = nil }, "InitialReconnectionPolicy is nil"},
		{"InitialReconnectionPolicyNoRetries", func(c *ClusterConfig) {
			c.InitialReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 0}
		}, "InitialReconnectionPolicy.GetMaxRetries"},
		{"NilReconnectionPolicy", func(c *ClusterConfig) { c.ReconnectionPolicy = nil }, "ReconnectionPolicy is nil"},
		// hostConnPool.connect makes GetMaxRetries attempts; with none every pool
		// fill fails.
		{"ReconnectionPolicyNoRetries", func(c *ClusterConfig) {
			c.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 0}
		}, "ReconnectionPolicy.GetMaxRetries"},
		{"NegativePageSize", func(c *ClusterConfig) { c.PageSize = -1 }, "PageSize"},
		{"NegativeMaxRoutingKeyInfo", func(c *ClusterConfig) { c.MaxRoutingKeyInfo = -1 }, "MaxRoutingKeyInfo"},
		{"NegativeMaxPreparedStmts", func(c *ClusterConfig) { c.MaxPreparedStmts = -1 }, "MaxPreparedStmts"},
		{"NegativeSocketKeepalive", func(c *ClusterConfig) { c.SocketKeepalive = -time.Second }, "SocketKeepalive"},
		{"NegativeMaxRequestsPerConn", func(c *ClusterConfig) { c.MaxRequestsPerConn = -1 }, "MaxRequestsPerConn"},
		{"NegativeNumConns", func(c *ClusterConfig) { c.NumConns = -1 }, "NumConns"},
		{"PortTooLarge", func(c *ClusterConfig) { c.Port = 65536 }, "valid port number"},
		{"NegativePort", func(c *ClusterConfig) { c.Port = -1 }, "valid port number"},
		{"MixedHostPorts", func(c *ClusterConfig) { c.Hosts = []string{"10.0.0.1:9042", "10.0.0.2"} }, "all include a port or none"},
		{"NegativeWriteTimeout", func(c *ClusterConfig) { c.WriteTimeout = -time.Second }, "WriteTimeout"},
		{"NegativeTimeout", func(c *ClusterConfig) { c.Timeout = -time.Second }, "Timeout should be"},
		{"NegativeConnectTimeout", func(c *ClusterConfig) { c.ConnectTimeout = -time.Second }, "ConnectTimeout"},
		{"NegativeMetadataSchemaRequestTimeout", func(c *ClusterConfig) { c.MetadataSchemaRequestTimeout = -time.Second }, "MetadataSchemaRequestTimeout"},
		{"NegativeWriteCoalesceWaitTime", func(c *ClusterConfig) { c.WriteCoalesceWaitTime = -time.Second }, "WriteCoalesceWaitTime"},
		{"NegativeReconnectInterval", func(c *ClusterConfig) { c.ReconnectInterval = -time.Second }, "ReconnectInterval"},
		{"NegativeMaxWaitSchemaAgreement", func(c *ClusterConfig) { c.MaxWaitSchemaAgreement = -time.Second }, "MaxWaitSchemaAgreement"},
		{"NonSerialSerialConsistency", func(c *ClusterConfig) { c.SerialConsistency = Quorum }, "SERIAL or LOCAL_SERIAL"},
		{"NilDNSResolver", func(c *ClusterConfig) { c.DNSResolver = nil }, "DNSResolver is empty"},
		{"NegativeMaxExcessShardConnectionsRate", func(c *ClusterConfig) { c.MaxExcessShardConnectionsRate = -1 }, "MaxExcessShardConnectionsRate"},
		{"ClientRoutesWithAddressTranslator", func(c *ClusterConfig) {
			c.ClientRoutesConfig = &ClientRoutesConfig{}
			c.AddressTranslator = AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) { return addr, port })
		}, "should not be set at the same time"},
		{"InvalidClientRoutesConfig", func(c *ClusterConfig) { c.ClientRoutesConfig = &ClientRoutesConfig{} }, "ClientRoutesConfig is invalid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := NewCluster("10.0.0.1")
			cfg.Logger = nopLogger{}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("baseline config invalid: %v", err)
			}

			cfg = NewCluster("10.0.0.1")
			cfg.Logger = nopLogger{}
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() = %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateAcceptsBoundaryValues(t *testing.T) {
	t.Parallel()

	for _, serial := range []Consistency{Serial, LocalSerial} {
		cfg := NewCluster("10.0.0.1")
		cfg.Logger = nopLogger{}
		cfg.SerialConsistency = serial
		cfg.Port = 65535
		if err := cfg.Validate(); err != nil {
			t.Fatalf("SerialConsistency=%v Port=65535: %v", serial, err)
		}
	}
}

// With no port anywhere, Validate fills in the CQL default; with client routes
// it does so without looking at the hosts, whose ports are entry points only.
func TestValidateDefaultsPort(t *testing.T) {
	t.Parallel()

	cfg := NewCluster("10.0.0.1")
	cfg.Logger = nopLogger{}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Port != defaultCQLPort {
		t.Fatalf("Port = %d, want %d", cfg.Port, defaultCQLPort)
	}

	cfg = NewCluster("10.0.0.1:19042", "10.0.0.2")
	cfg.Logger = nopLogger{}
	cfg.ClientRoutesConfig = &ClientRoutesConfig{}
	err := cfg.Validate()
	// The empty routes config is itself invalid; what matters is that mixed host
	// ports did not fail first and the port was defaulted.
	if err == nil || strings.Contains(err.Error(), "all include a port") {
		t.Fatalf("Validate() = %v, want the client routes error, not a host port error", err)
	}
	if cfg.Port != defaultCQLPort {
		t.Fatalf("Port = %d, want %d with client routes", cfg.Port, defaultCQLPort)
	}
}
