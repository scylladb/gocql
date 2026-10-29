// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"

	"golang.org/x/net/proxy"

	"github.com/scylladb/gocql/v2"
)

// SniHostDialer routes immutable host snapshots to their datacenter's proxy.
// TLSForHost supplies UUID-based SNI and the matching datacenter certificates.
type SniHostDialer struct {
	connConfig *ConnectionConfig
	dialer     gocql.Dialer
}

func NewSniHostDialer(connConfig *ConnectionConfig, dialer gocql.Dialer) *SniHostDialer {
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	return &SniHostDialer{connConfig: connConfig, dialer: dialer}
}

func (s *SniHostDialer) datacenter(host gocql.HostSnapshot) (*Datacenter, error) {
	if host.ID == (gocql.UUID{}) {
		dc, err := s.connConfig.GetCurrentDatacenterConfig()
		if err != nil {
			return nil, fmt.Errorf("can't get current datacenter config: %w", err)
		}
		return dc, nil
	}
	dc, ok := s.connConfig.Datacenters[host.Datacenter]
	if !ok {
		return nil, fmt.Errorf("datacenter %q configuration not found in connection bundle", host.Datacenter)
	}
	return dc, nil
}

// DialHost opens the raw connection; Connect applies TLSForHost and performs the
// TLS handshake. The supplied context bounds both direct and SOCKS dialing.
func (s *SniHostDialer) DialHost(ctx context.Context, host gocql.HostSnapshot) (net.Conn, error) {
	dc, err := s.datacenter(host)
	if err != nil {
		return nil, err
	}
	dialer := s.dialer
	if dc.ProxyURL != "" {
		u, err := url.Parse(dc.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("can't parse proxy URL %q: %w", dc.ProxyURL, err)
		}
		base := dialer
		d, err := proxy.FromURL(u, proxyDialerFunc(func(network, address string) (net.Conn, error) {
			return base.DialContext(ctx, network, address)
		}))
		if err != nil {
			return nil, fmt.Errorf("can't create proxy dialer: %w", err)
		}
		contextDialer, ok := d.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("cloud proxy %q does not support context dialing", dc.ProxyURL)
		}
		dialer = contextDialer
	}
	server := dc.Server
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "443")
	}
	conn, err := dialer.DialContext(ctx, "tcp", server)
	if err != nil {
		return nil, fmt.Errorf("can't connect to %q: %w", server, err)
	}
	return conn, nil
}

func (s *SniHostDialer) TLSForHost(host gocql.HostSnapshot) (*tls.Config, error) {
	if host.ID == (gocql.UUID{}) {
		return s.initialTLS()
	}
	dc, err := s.datacenter(host)
	if err != nil {
		return nil, err
	}
	certificate, err := s.connConfig.GetClientCertificate()
	if err != nil {
		return nil, fmt.Errorf("can't get client certificate from configuration: %w", err)
	}
	roots, err := s.connConfig.GetDatacenterCAPool(host.Datacenter)
	if err != nil {
		return nil, fmt.Errorf("can't get root CA from configuration: %w", err)
	}
	return &tls.Config{
		ServerName:         fmt.Sprintf("%s.%s", host.ID, dc.NodeDomain),
		RootCAs:            roots,
		InsecureSkipVerify: dc.InsecureSkipTLSVerify,
		Certificates:       []tls.Certificate{*certificate},
	}, nil
}

func (s *SniHostDialer) initialTLS() (*tls.Config, error) {
	certificate, err := s.connConfig.GetClientCertificate()
	if err != nil {
		return nil, fmt.Errorf("can't get client certificate from configuration: %w", err)
	}
	roots, err := s.connConfig.GetRootCAPool()
	if err != nil {
		return nil, fmt.Errorf("can't get root CA from configuration: %w", err)
	}
	dc, err := s.datacenter(gocql.HostSnapshot{})
	if err != nil {
		return nil, err
	}
	serverName := dc.NodeDomain
	if serverName == "" {
		serverName = dc.Server
	}
	insecure := false
	for _, dc := range s.connConfig.Datacenters {
		insecure = insecure || dc.InsecureSkipTLSVerify
	}
	return &tls.Config{
		ServerName:         serverName,
		RootCAs:            roots,
		InsecureSkipVerify: insecure,
		Certificates:       []tls.Certificate{*certificate},
	}, nil
}

type proxyDialerFunc func(network, addr string) (net.Conn, error)

func (d proxyDialerFunc) Dial(network, addr string) (net.Conn, error) {
	return d(network, addr)
}
