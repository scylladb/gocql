// Copyright (C) 2021 ScyllaDB

package scyllacloud

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

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
		dialer = gocql.DefaultDialer{}
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
func (s *SniHostDialer) DialHost(ctx context.Context, target gocql.DialTarget) (net.Conn, error) {
	dc, err := s.datacenter(target.Host)
	if err != nil {
		return nil, err
	}
	var rawMu sync.Mutex
	var proxyRaw net.Conn
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		t := target
		t.Network, t.Address = network, address
		return s.dialer.DialContext(ctx, t)
	}
	if dc.ProxyURL != "" {
		u, err := url.Parse(dc.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("can't parse proxy URL %q: %w", dc.ProxyURL, err)
		}
		base := dial
		d, err := proxy.FromURL(u, proxyDialerFunc(func(network, address string) (net.Conn, error) {
			conn, err := base(ctx, network, address)
			if err != nil {
				if conn != nil {
					conn.Close()
				}
				return nil, err
			}
			rawMu.Lock()
			proxyRaw = conn
			rawMu.Unlock()
			return conn, nil
		}))
		if err != nil {
			return nil, fmt.Errorf("can't create proxy dialer: %w", err)
		}
		contextDialer, ok := d.(proxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("cloud proxy %q does not support context dialing", dc.ProxyURL)
		}
		dial = contextDialer.DialContext
	}
	server := dc.Server
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "443")
	}
	conn, err := dial(ctx, "tcp", server)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, fmt.Errorf("can't connect to %q: %w", server, err)
	}
	rawMu.Lock()
	raw := proxyRaw
	rawMu.Unlock()
	if raw != nil {
		return &cloudSocketConn{Conn: conn, socket: raw}, nil
	}
	return conn, nil
}

func (s *SniHostDialer) TLSForHost(ctx context.Context, host gocql.HostSnapshot) (*tls.Config, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// Proxy IO wrappers may hide the raw TCP method set. Preserve actual socket
// controls from the raw dial, so the driver applies its requested settings to
// the real underlying connection rather than silently skipping them.
type cloudSocketConn struct {
	net.Conn
	socket net.Conn
}

func (c *cloudSocketConn) unsupported(name string) error {
	return &gocql.UnsupportedSettingError{Setting: name, Reason: "cloud raw dialer does not expose TCP controls"}
}
func (c *cloudSocketConn) SetNoDelay(v bool) error {
	if s, ok := c.socket.(interface{ SetNoDelay(bool) error }); ok {
		return s.SetNoDelay(v)
	}
	return c.unsupported("TCP_NODELAY")
}
func (c *cloudSocketConn) SetKeepAlive(v bool) error {
	if s, ok := c.socket.(interface{ SetKeepAlive(bool) error }); ok {
		return s.SetKeepAlive(v)
	}
	return c.unsupported("TCP keepalive")
}
func (c *cloudSocketConn) SetKeepAlivePeriod(v time.Duration) error {
	if s, ok := c.socket.(interface{ SetKeepAlivePeriod(time.Duration) error }); ok {
		return s.SetKeepAlivePeriod(v)
	}
	return c.unsupported("TCP keepalive period")
}
func (c *cloudSocketConn) SetReadBuffer(v int) error {
	if s, ok := c.socket.(interface{ SetReadBuffer(int) error }); ok {
		return s.SetReadBuffer(v)
	}
	return c.unsupported("receive buffer")
}
func (c *cloudSocketConn) SetWriteBuffer(v int) error {
	if s, ok := c.socket.(interface{ SetWriteBuffer(int) error }); ok {
		return s.SetWriteBuffer(v)
	}
	return c.unsupported("send buffer")
}
func (c *cloudSocketConn) SetLinger(v int) error {
	if s, ok := c.socket.(interface{ SetLinger(int) error }); ok {
		return s.SetLinger(v)
	}
	return c.unsupported("linger")
}
