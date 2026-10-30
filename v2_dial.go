/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/netip"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// DialTarget is a resolved raw connection target. Hooks must honor the selected
// endpoint and source, or return an error. TLS and protocol setup follow dialing.
type DialTarget struct {
	Host             HostSnapshot
	Network, Address string
	LocalAddress     netip.AddrPort
	ShardID          Option[uint32]
}

type Dialer interface {
	DialContext(context.Context, DialTarget) (net.Conn, error)
}

// DialerFunc adapts a context-aware raw connection hook to Dialer.
type DialerFunc func(context.Context, DialTarget) (net.Conn, error)

func (f DialerFunc) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	return f(ctx, target)
}

// DefaultDialer opens the selected target with Go's network dialer. It also
// supports Unix targets; TCP source selection is never applied to Unix sockets.
type DefaultDialer struct{ ConnectTimeout, SocketKeepalive time.Duration }

func (d DefaultDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	keepalive := d.SocketKeepalive
	if keepalive == 0 {
		keepalive = -1
	}
	n := net.Dialer{Timeout: d.ConnectTimeout, KeepAlive: keepalive}
	if target.LocalAddress.IsValid() {
		if target.Network != "tcp" && target.Network != "tcp4" && target.Network != "tcp6" {
			return nil, typeValidation("local address", "source selection requires TCP")
		}
		n.LocalAddr = net.TCPAddrFromAddrPort(target.LocalAddress)
	}
	return n.DialContext(ctx, target.Network, target.Address)
}

type v2TLSHostDialer struct {
	unixHostID func() UUID
	legacy     *clusterSettings
	config     Config
}

func (d *v2TLSHostDialer) selectedTLS(ctx context.Context, host HostSnapshot) (*tls.Config, error) {
	c := d.config.TLS
	if d.config.TLSForHost != nil {
		v, err := d.config.TLSForHost(ctx, cloneV2HostSnapshot(host))
		if err != nil {
			return nil, err
		}
		if v != nil {
			c = v
		}
		if c == nil {
			return nil, typeValidation("TLSForHost", "TLS hook supplied no configuration and no inherited TLS configuration exists")
		}
	}
	if c != nil {
		c = cloneV2TLSConfig(c)
	}
	if d.config.ClientRoutes != nil && d.config.ClientRoutes.TLS && (c == nil || c.InsecureSkipVerify && c.VerifyConnection == nil && c.VerifyPeerCertificate == nil) {
		return nil, typeValidation("client routes TLS", "usable verified TLS required")
	}
	return c, nil
}
func (d *v2TLSHostDialer) raw(ctx context.Context, target DialTarget) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var c net.Conn
	var err error
	if d.config.DialHost != nil {
		c, err = d.config.DialHost(ctx, cloneV2DialTarget(target))
	} else if d.config.Dialer != nil {
		c, err = d.config.Dialer.DialContext(ctx, cloneV2DialTarget(target))
	} else {
		c, err = (DefaultDialer{d.config.ConnectTimeout, d.config.SocketKeepalive}).DialContext(ctx, target)
	}
	if err != nil {
		if c != nil {
			c.Close()
		}
		return nil, err
	}
	if c == nil {
		return nil, typeValidation("dial", "hook returned nil connection")
	}
	if _, ok := c.(*tls.Conn); ok {
		c.Close()
		return nil, typeValidation("dial", "hook must return a raw connection")
	}
	if target.LocalAddress.IsValid() {
		a, ok := c.LocalAddr().(*net.TCPAddr)
		if !ok || (target.LocalAddress.Port() != 0 && a.Port != int(target.LocalAddress.Port())) || (!target.LocalAddress.Addr().IsUnspecified() && a.AddrPort().Addr().Unmap() != target.LocalAddress.Addr().Unmap()) {
			c.Close()
			return nil, &UnsupportedSettingError{Setting: "dial source", Reason: "hook did not honor selected source " + target.LocalAddress.String()}
		}
	}
	if err = applyV2SocketOptions(c, d.config, target.Network); err != nil {
		c.Close()
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func applyV2SocketOptions(c net.Conn, config Config, network string) error {
	if network == "unix" || network == "unixpacket" {
		return nil
	}
	_, realTCP := c.LocalAddr().(*net.TCPAddr)
	unsupported := func(setting string) error {
		return &UnsupportedSettingError{Setting: setting, Reason: "raw TCP connection wrapper does not expose requested socket controls"}
	}
	if realTCP {
		noDelay, ok := c.(interface{ SetNoDelay(bool) error })
		if !ok {
			return unsupported("TCP_NODELAY")
		}
		if err := noDelay.SetNoDelay(config.NoDelay); err != nil {
			return err
		}
		keepalive, ok := c.(interface{ SetKeepAlive(bool) error })
		if !ok {
			return unsupported("TCP keepalive")
		}
		if err := keepalive.SetKeepAlive(config.SocketKeepalive > 0); err != nil {
			return err
		}
		if config.SocketKeepalive > 0 {
			period, ok := c.(interface{ SetKeepAlivePeriod(time.Duration) error })
			if !ok {
				return unsupported("TCP keepalive period")
			}
			if err := period.SetKeepAlivePeriod(config.SocketKeepalive); err != nil {
				return err
			}
		}
	}
	if config.ReceiveBuffer.Set {
		setter, ok := c.(interface{ SetReadBuffer(int) error })
		if !ok {
			return unsupported("receive buffer")
		}
		if err := setter.SetReadBuffer(config.ReceiveBuffer.Value); err != nil {
			return err
		}
	}
	if config.SendBuffer.Set {
		setter, ok := c.(interface{ SetWriteBuffer(int) error })
		if !ok {
			return unsupported("send buffer")
		}
		if err := setter.SetWriteBuffer(config.SendBuffer.Value); err != nil {
			return err
		}
	}
	if config.Linger.Set {
		setter, ok := c.(interface{ SetLinger(int) error })
		if !ok {
			return unsupported("linger")
		}
		if err := setter.SetLinger(int(config.Linger.Value / time.Second)); err != nil {
			return err
		}
	}
	if !realTCP && config.SocketKeepalive > 0 {
		return unsupported("TCP keepalive")
	}
	return nil
}

func (d *v2TLSHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	return d.dial(ctx, host, -1, 0)
}
func (d *v2TLSHostDialer) DialShard(ctx context.Context, host *HostInfo, shard, shards int) (*DialedHost, error) {
	return d.dial(ctx, host, shard, shards)
}
func (d *v2TLSHostDialer) dial(ctx context.Context, host *HostInfo, shard, shards int) (*DialedHost, error) {
	epoch := host.transportEpoch()
	sharderEpoch := host.sharderEpoch()
	snapshot, err := snapshotV2Host(nil, host)
	if err != nil {
		return nil, err
	}
	if path, e := unixContact(d.config); e != nil {
		return nil, e
	} else if path != "" {
		snapshot.Address = netip.AddrPort{}
		if d.unixHostID != nil {
			if local := d.unixHostID(); !local.IsEmpty() && !snapshot.ID.IsEmpty() && local != snapshot.ID {
				return nil, &UnsupportedSettingError{Setting: "Unix remote host", Reason: "one local socket cannot represent a remote node"}
			}
		}
		c, e := d.raw(ctx, DialTarget{Host: snapshot, Network: "unix", Address: path})
		if e != nil {
			return nil, e
		}
		return &DialedHost{Conn: c, targetEpoch: epoch, sharderEpoch: sharderEpoch}, nil
	}
	tlsConfig, err := d.selectedTLS(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	address := net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(host.Port()))
	logicalAddress := logicalV2TLSAddress(host)
	translated := host.getTranslatedConnectionInfo()
	if translated != nil && translated.CQL.IsValid() {
		address = translated.CQL.ToNetAddr()
	}
	if routes := d.config.ClientRoutes; routes != nil && routes.SNIProxyAddress != "" {
		proxy, port, err := net.SplitHostPort(routes.SNIProxyAddress)
		if err != nil {
			proxy = routes.SNIProxyAddress
			_, port, _ = net.SplitHostPort(address)
		}
		resolver := &v2DNSResolver{base: d.config.DNSResolver, timeout: d.config.DNSTimeout}
		ips, err := resolver.LookupIPContext(ctx, proxy)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, typeValidation("SNI proxy", "no resolved addresses")
		}
		address = net.JoinHostPort(ips[0].String(), port)
	}
	target := DialTarget{Host: snapshot, Network: "tcp", Address: address}
	if d.config.LocalAddress.IsValid() {
		target.LocalAddress = netip.AddrPortFrom(d.config.LocalAddress, 0)
	}
	var c net.Conn
	var selectedShard Option[uint32]
	if shards > 0 && d.config.Pool.ShardAwarePort && d.config.Pool.ConnectionsPerHost == 0 {
		if shard < 0 || shard >= shards {
			return nil, typeValidation("shard", "target out of range")
		}
		endpoint := ""
		if translated != nil {
			if tlsConfig != nil && translated.ShardAwareTLS.IsValid() {
				endpoint = translated.ShardAwareTLS.ToNetAddr()
			} else if tlsConfig == nil && translated.ShardAware.IsValid() {
				endpoint = translated.ShardAware.ToNetAddr()
			}
		}
		if endpoint != "" {
			for port := int(d.config.Pool.SourcePortStart); port <= int(d.config.Pool.SourcePortEnd); port++ {
				if port%shards != shard {
					continue
				}
				attempt := target
				attempt.Address = endpoint
				attempt.ShardID = Option[uint32]{Value: uint32(shard), Set: true}
				ip := d.config.LocalAddress
				if !ip.IsValid() {
					ip = netip.IPv4Unspecified()
					if snapshot.Address.Addr().Is6() {
						ip = netip.IPv6Unspecified()
					}
				}
				attempt.LocalAddress = netip.AddrPortFrom(ip, uint16(port))
				c, err = d.raw(ctx, attempt)
				if err == nil {
					selectedShard = attempt.ShardID
					break
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !isLocalAddrInUseErr(err) {
					break
				}
			}
		}
	}
	if c == nil {
		c, err = d.raw(ctx, target)
		if err != nil {
			return nil, err
		}
	}
	if tlsConfig != nil && tlsConfig.ServerName == "" && !snapshot.Address.IsValid() {
		c.Close()
		return nil, typeValidation("TLS identity", "missing logical host name")
	}
	result, err := WrapTLS(ctx, c, logicalAddress, tlsConfig)
	if err != nil {
		return nil, err
	}
	result.shardTarget = selectedShard
	result.targetEpoch = epoch
	result.sharderEpoch = sharderEpoch
	result.validateTarget = func(ctx context.Context) error {
		current, err := snapshotV2Host(nil, host)
		if err != nil {
			return err
		}
		if current.ID != snapshot.ID || current.Address != snapshot.Address || current.Datacenter != snapshot.Datacenter || current.Rack != snapshot.Rack || logicalV2TLSAddress(host) != logicalAddress {
			return typeValidation("dial target generation", "host identity/endpoint changed during opening")
		}
		if d.config.TLSForHost != nil {
			now, err := d.selectedTLS(ctx, current)
			if err != nil {
				return err
			}
			if !equalV2TLSConfig(tlsConfig, now) {
				return typeValidation("dial target generation", "TLS/SNI target changed during opening")
			}
		}
		return nil
	}
	return result, nil
}

// Parsed keys, callbacks, session caches and provider objects remain shared by
// their documented concurrency contracts. Mutable certificate/trust containers
// and wire bytes are copied before any connection uses them.
func cloneV2TLSConfig(source *tls.Config) *tls.Config {
	if source == nil {
		return nil
	}
	c := source.Clone()
	if source.RootCAs != nil {
		c.RootCAs = source.RootCAs.Clone()
	}
	if source.ClientCAs != nil {
		c.ClientCAs = source.ClientCAs.Clone()
	}
	c.NextProtos = slices.Clone(source.NextProtos)
	c.CipherSuites = slices.Clone(source.CipherSuites)
	c.CurvePreferences = slices.Clone(source.CurvePreferences)
	cloneCert := func(v tls.Certificate) tls.Certificate {
		v.Certificate = slices.Clone(v.Certificate)
		for i, b := range v.Certificate {
			v.Certificate[i] = slices.Clone(b)
		}
		v.OCSPStaple = slices.Clone(v.OCSPStaple)
		v.SignedCertificateTimestamps = slices.Clone(v.SignedCertificateTimestamps)
		for i, b := range v.SignedCertificateTimestamps {
			v.SignedCertificateTimestamps[i] = slices.Clone(b)
		}
		v.SupportedSignatureAlgorithms = slices.Clone(v.SupportedSignatureAlgorithms)
		if v.Leaf != nil && len(v.Leaf.Raw) > 0 {
			if leaf, err := x509.ParseCertificate(slices.Clone(v.Leaf.Raw)); err == nil {
				v.Leaf = leaf
			}
		}
		return v
	}
	c.Certificates = slices.Clone(source.Certificates)
	for i, v := range c.Certificates {
		c.Certificates[i] = cloneCert(v)
	}
	if source.NameToCertificate != nil {
		c.NameToCertificate = make(map[string]*tls.Certificate, len(source.NameToCertificate))
		for n, v := range source.NameToCertificate {
			if v != nil {
				copy := cloneCert(*v)
				c.NameToCertificate[n] = &copy
			}
		}
	}
	return c
}

// UnsupportedSettingError identifies a requested setting without support on
// the selected transport. It never indicates a silently changed setting.
type UnsupportedSettingError struct{ Setting, Reason string }

func (e *UnsupportedSettingError) Error() string {
	return "gocql: unsupported " + e.Setting + ": " + e.Reason
}
func unixContact(config Config) (string, error) {
	path := ""
	for _, point := range config.ContactPoints {
		if strings.HasPrefix(point, "unix:") || filepath.IsAbs(point) {
			if len(config.ContactPoints) != 1 {
				return "", typeValidation("contact points", "Unix and TCP contacts cannot be mixed")
			}
			path = strings.TrimPrefix(point, "unix:")
			if !filepath.IsAbs(path) {
				return "", typeValidation("Unix contact point", "socket path must be absolute")
			}
		}
	}
	if path != "" {
		if config.Metadata.InitialDiscovery {
			return "", &UnsupportedSettingError{Setting: "Unix discovery", Reason: "one local socket requires InitialDiscovery=false"}
		}
		if config.Protocol != 3 {
			return "", &UnsupportedSettingError{Setting: "Unix protocol", Reason: "explicit native protocol 3 is required"}
		}
		if config.TLS != nil || config.TLSForHost != nil {
			return "", &UnsupportedSettingError{Setting: "Unix TLS", Reason: "maintenance sockets carry plaintext native protocol"}
		}
		if config.LocalAddress.IsValid() {
			return "", &UnsupportedSettingError{Setting: "Unix local address", Reason: "source address/port selection requires TCP"}
		}
	}
	return path, nil
}

func cloneV2HostSnapshot(v HostSnapshot) HostSnapshot { v.Tokens = slices.Clone(v.Tokens); return v }
func cloneV2DialTarget(v DialTarget) DialTarget       { v.Host = cloneV2HostSnapshot(v.Host); return v }

func equalV2TLSConfig(a, b *tls.Config) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.ServerName != b.ServerName || a.InsecureSkipVerify != b.InsecureSkipVerify || a.MinVersion != b.MinVersion || a.MaxVersion != b.MaxVersion || !slices.Equal(a.NextProtos, b.NextProtos) || !slices.Equal(a.CipherSuites, b.CipherSuites) || !slices.Equal(a.CurvePreferences, b.CurvePreferences) || len(a.Certificates) != len(b.Certificates) {
		return false
	}
	if a.RootCAs == nil || b.RootCAs == nil {
		if a.RootCAs != b.RootCAs {
			return false
		}
	} else if !a.RootCAs.Equal(b.RootCAs) {
		return false
	}
	if a.ClientCAs == nil || b.ClientCAs == nil {
		if a.ClientCAs != b.ClientCAs {
			return false
		}
	} else if !a.ClientCAs.Equal(b.ClientCAs) {
		return false
	}
	for i, c := range a.Certificates {
		other := b.Certificates[i]
		if len(c.Certificate) != len(other.Certificate) || !bytes.Equal(c.OCSPStaple, other.OCSPStaple) {
			return false
		}
		for j, der := range c.Certificate {
			if !bytes.Equal(der, other.Certificate[j]) {
				return false
			}
		}
	}
	return true
}

func logicalV2TLSAddress(host *HostInfo) string {
	host.mu.RLock()
	defer host.mu.RUnlock()
	name := host.hostname
	if name == "" {
		switch {
		case validIpAddr(host.connectAddress):
			name = host.connectAddress.String()
		case validIpAddr(host.rpcAddress):
			name = host.rpcAddress.String()
		case validIpAddr(host.peer):
			name = host.peer.String()
		default:
			name = host.broadcastAddress.String()
		}
	}
	return net.JoinHostPort(name, strconv.Itoa(host.port))
}
