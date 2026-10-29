// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"sigs.k8s.io/yaml"

	"github.com/scylladb/gocql/v2"
)

// NewCloudCluster reads a cloud bundle into the typed driver configuration.
// Options are applied before installing the datacenter routing hooks, so a
// custom Dialer is used by both direct and proxied cloud connections. Changing
// Config.Dialer after this builder returns does not replace the captured dialer.
func NewCloudCluster(bundlePath string, options ...func(*gocql.Config)) (gocql.Config, error) {
	connConf := &ConnectionConfig{}

	bundleFile, err := os.ReadFile(bundlePath)
	if err != nil {
		return gocql.Config{}, fmt.Errorf("can't open bundle path: %w", err)
	}

	if err := yaml.Unmarshal(bundleFile, connConf); err != nil {
		return gocql.Config{}, fmt.Errorf("can't decode bundle file at %q: %w", bundlePath, err)
	}

	if _, ok := connConf.Contexts[connConf.CurrentContext]; !ok {
		return gocql.Config{}, fmt.Errorf("current context points to unknown context")
	}

	confContext := connConf.Contexts[connConf.CurrentContext]

	if _, ok := connConf.AuthInfos[confContext.AuthInfoName]; !ok {
		return gocql.Config{}, fmt.Errorf("context %q auth info points to unknown authinfo", connConf.CurrentContext)
	}

	if _, ok := connConf.Datacenters[confContext.DatacenterName]; !ok {
		return gocql.Config{}, fmt.Errorf("context %q datacenter points to unknown datacenter", connConf.CurrentContext)
	}

	authInfo := connConf.AuthInfos[confContext.AuthInfoName]

	caPool, err := connConf.GetRootCAPool()
	if err != nil {
		return gocql.Config{}, fmt.Errorf("can't create root CA pool: %w", err)
	}

	cc := gocql.DefaultConfig(connConf.GetInitialContactPoints()...)
	cc.Port = 443

	// Initial contact points trust the configured datacenter certificate authorities.
	// Skip verifying TLS if any of DC requires it.
	insecureSkipVerify := false
	for _, dc := range connConf.Datacenters {
		if dc.InsecureSkipTLSVerify {
			insecureSkipVerify = true
			break
		}
	}

	cc.TLS = &tls.Config{
		RootCAs: caPool,
		GetClientCertificate: func(info *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return connConf.GetClientCertificate()
		},
		InsecureSkipVerify: insecureSkipVerify,
	}
	cc.Auth = gocql.PasswordAuth(authInfo.Username, authInfo.Password)
	// Cloud routes all traffic through SNI rather than source-port sharding.
	cc.Pool.ShardAwarePort = false
	cc.Pool.ConnectionsPerHost = 2
	cc.CoalesceWait = 0
	profile := cc.DefaultProfile.Snapshot()
	profile.Consistency = gocql.Quorum

	if connConf.Parameters != nil {
		if connConf.Parameters.DefaultConsistency != "" {
			if !validateConsistency(connConf.Parameters.DefaultConsistency, allowedConsistencies) {
				return gocql.Config{}, fmt.Errorf("invalid value of default consistency %q, values can be one of: %v", connConf.Parameters.DefaultConsistency, allowedConsistencies)
			}
			if err := profile.Consistency.UnmarshalText([]byte(connConf.Parameters.DefaultConsistency)); err != nil {
				return gocql.Config{}, fmt.Errorf("unmarshal default consistency: %w", err)
			}
		}
		if connConf.Parameters.DefaultSerialConsistency != "" {
			if !validateConsistency(connConf.Parameters.DefaultSerialConsistency, allowedSerialConsistencies) {
				return gocql.Config{}, fmt.Errorf("invalid value of default serial consistency %q, values can be one of: %v", connConf.Parameters.DefaultSerialConsistency, allowedSerialConsistencies)
			}
			var serial gocql.Consistency
			if err := serial.UnmarshalText([]byte(connConf.Parameters.DefaultSerialConsistency)); err != nil {
				return gocql.Config{}, fmt.Errorf("unmarshal default serial consistency: %w", err)
			}
			profile.SerialConsistency = gocql.Option[gocql.Consistency]{Value: serial, Set: true}
		}
	}

	if err := cc.DefaultProfile.Replace(profile); err != nil {
		return gocql.Config{}, fmt.Errorf("cloud execution profile: %w", err)
	}
	for _, option := range options {
		option(&cc)
	}
	dialer := cc.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: cc.ConnectTimeout, KeepAlive: cc.SocketKeepalive}
	}
	hostDialer := NewSniHostDialer(connConf, dialer)
	cc.DialHost = hostDialer.DialHost
	cc.TLSForHost = hostDialer.TLSForHost
	return cc, nil
}

func validateConsistency(c ConsistencyString, allowed []ConsistencyString) bool {
	for _, ac := range allowed {
		if ac == c {
			return true
		}
	}
	return false
}

var allowedSerialConsistencies = []ConsistencyString{
	DefaultSerialConsistency,
	DefaultLocalSerialConsistency,
}
var allowedConsistencies = []ConsistencyString{
	DefaultThreeConsistency,
	DefaultOneConsistency,
	DefaultTwoConsistency,
	DefaultAnyConsistency,
	DefaultQuorumConsistency,
	DefaultAllConsistency,
	DefaultLocalQuorumConsistency,
	DefaultEachQuorumConsistency,
	DefaultLocalOneConsistency,
}
