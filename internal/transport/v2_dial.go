// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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

package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	"net"
	"net/netip"

	"slices"
	"strconv"

	"time"
)

type InternalV2TLSHostDialer struct {
	InternalUnixHostID func() UUID
	InternalConfig     Config
}

func (d *InternalV2TLSHostDialer) InternalSelectedTLS(ctx context.Context, host HostSnapshot) (*tls.Config, error) {
	c := d.InternalConfig.TLS
	if d.InternalConfig.TLSForHost != nil {
		v, err := d.InternalConfig.TLSForHost(ctx, InternalCloneV2HostSnapshot(host))
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
	if d.InternalConfig.ClientRoutes != nil && d.InternalConfig.ClientRoutes.TLS && (c == nil || c.InsecureSkipVerify && c.VerifyConnection == nil && c.VerifyPeerCertificate == nil) {
		return nil, typeValidation("client routes TLS", "usable verified TLS required")
	}
	return c, nil
}
func (d *InternalV2TLSHostDialer) raw(ctx context.Context, target DialTarget) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var c net.Conn
	var err error
	if d.InternalConfig.DialHost != nil {
		c, err = d.InternalConfig.DialHost(ctx, InternalCloneV2DialTarget(target))
	} else if d.InternalConfig.Dialer != nil {
		c, err = d.InternalConfig.Dialer.DialContext(ctx, InternalCloneV2DialTarget(target))
	} else {
		c, err = (DefaultDialer{ConnectTimeout: d.InternalConfig.ConnectTimeout, SocketKeepalive: d.InternalConfig.SocketKeepalive}).DialContext(ctx, target)
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
	if err = InternalApplyV2SocketOptions(c, d.InternalConfig, target.Network); err != nil {
		c.Close()
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}
func InternalApplyV2SocketOptions(c net.Conn, config Config, network string) error {
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

func (d *InternalV2TLSHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	return d.dial(ctx, host, -1, 0)
}
func (d *InternalV2TLSHostDialer) DialShard(ctx context.Context, host *HostInfo, shard, shards int) (*DialedHost, error) {
	return d.dial(ctx, host, shard, shards)
}
func (d *InternalV2TLSHostDialer) dial(ctx context.Context, host *HostInfo, shard, shards int) (*DialedHost, error) {
	epoch := implEndpoint.InternalHostInfoTransportEpoch(host)
	sharderEpoch := implEndpoint.InternalHostInfoSharderEpoch(host)
	snapshot, err := implEndpoint.SnapshotHost(host)
	if err != nil {
		return nil, err
	}
	if path, e := unixContact(d.InternalConfig); e != nil {
		return nil, e
	} else if path != "" {
		snapshot.Address = netip.AddrPort{}
		if d.InternalUnixHostID != nil {
			if local := d.InternalUnixHostID(); !local.IsEmpty() && !snapshot.ID.IsEmpty() && local != snapshot.ID {
				return nil, &UnsupportedSettingError{Setting: "Unix remote host", Reason: "one local socket cannot represent a remote node"}
			}
		}
		c, e := d.raw(ctx, DialTarget{Host: snapshot, Network: "unix", Address: path})
		if e != nil {
			return nil, e
		}
		return func() *DialedHost {
			opaqueStorageValue := new(DialedHost)
			opaqueStorageValue.Conn = c
			implEndpoint.DialedHostData(opaqueStorageValue).InternalTargetEpoch = epoch
			implEndpoint.DialedHostData(opaqueStorageValue).InternalSharderEpoch = sharderEpoch
			return opaqueStorageValue
		}(), nil
	}
	tlsConfig, err := d.InternalSelectedTLS(ctx, snapshot)
	if err != nil {
		return nil, err
	}
	address := net.JoinHostPort(host.ConnectAddress().String(), strconv.Itoa(host.Port()))
	logicalAddress := InternalLogicalV2TLSAddress(host)
	translated := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translated != nil && translated.CQL.IsValid() {
		address = translated.CQL.ToNetAddr()
	}
	if routes := d.InternalConfig.ClientRoutes; routes != nil && routes.SNIProxyAddress != "" {
		proxy, port, err := net.SplitHostPort(routes.SNIProxyAddress)
		if err != nil {
			proxy = routes.SNIProxyAddress
			_, port, _ = net.SplitHostPort(address)
		}
		resolver := &v2DNSResolver{InternalBase: d.InternalConfig.DNSResolver, InternalTimeout: d.InternalConfig.DNSTimeout}
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
	if d.InternalConfig.LocalAddress.IsValid() {
		target.LocalAddress = netip.AddrPortFrom(d.InternalConfig.LocalAddress, 0)
	}
	var c net.Conn
	var selectedShard Option[uint32]
	if shards > 0 && d.InternalConfig.Pool.ShardAwarePort && d.InternalConfig.Pool.ConnectionsPerHost == 0 {
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
			for port := int(d.InternalConfig.Pool.SourcePortStart); port <= int(d.InternalConfig.Pool.SourcePortEnd); port++ {
				if port%shards != shard {
					continue
				}
				attempt := target
				attempt.Address = endpoint
				attempt.ShardID = Option[uint32]{Value: uint32(shard), Set: true}
				ip := d.InternalConfig.LocalAddress
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
				if !InternalIsLocalAddrInUseErr(err) {
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
	implEndpoint.DialedHostData(result).InternalShardTarget = selectedShard
	implEndpoint.DialedHostData(result).InternalTargetEpoch = epoch
	implEndpoint.DialedHostData(result).InternalSharderEpoch = sharderEpoch
	implEndpoint.DialedHostData(result).InternalValidateTarget = func(ctx context.Context) error {
		current, err := implEndpoint.SnapshotHost(host)
		if err != nil {
			return err
		}
		if current.ID != snapshot.ID || current.Address != snapshot.Address || current.Datacenter != snapshot.Datacenter || current.Rack != snapshot.Rack || InternalLogicalV2TLSAddress(host) != logicalAddress {
			return typeValidation("dial target generation", "host identity/endpoint changed during opening")
		}
		if d.InternalConfig.TLSForHost != nil {
			now, err := d.InternalSelectedTLS(ctx, current)
			if err != nil {
				return err
			}
			if !InternalEqualV2TLSConfig(tlsConfig, now) {
				return typeValidation("dial target generation", "TLS/SNI target changed during opening")
			}
		}
		return nil
	}
	return result, nil
}

func InternalCloneV2HostSnapshot(v HostSnapshot) HostSnapshot {
	v.Tokens = slices.Clone(v.Tokens)
	return v
}
func InternalCloneV2DialTarget(v DialTarget) DialTarget {
	v.Host = InternalCloneV2HostSnapshot(v.Host)
	return v
}

func InternalEqualV2TLSConfig(a, b *tls.Config) bool {
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

func InternalLogicalV2TLSAddress(host *HostInfo) string {
	implEndpoint.HostInfoData(host).InternalMu.RLock()
	defer implEndpoint.HostInfoData(host).InternalMu.RUnlock()
	name := implEndpoint.HostInfoData(host).InternalHostname
	if name == "" {
		switch {
		case validIpAddr(implEndpoint.HostInfoData(host).InternalConnectAddress):
			name = implEndpoint.HostInfoData(host).InternalConnectAddress.String()
		case validIpAddr(implEndpoint.HostInfoData(host).InternalRpcAddress):
			name = implEndpoint.HostInfoData(host).InternalRpcAddress.String()
		case validIpAddr(implEndpoint.HostInfoData(host).InternalPeer):
			name = implEndpoint.HostInfoData(host).InternalPeer.String()
		default:
			name = implEndpoint.HostInfoData(host).InternalBroadcastAddress.String()
		}
	}
	return net.JoinHostPort(name, strconv.Itoa(implEndpoint.HostInfoData(host).InternalPort))
}
