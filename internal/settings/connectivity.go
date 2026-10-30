// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import "github.com/scylladb/gocql/v2/internal/connectivity"

// InternalConnectivityConfig projects the validated immutable configuration.
// It transfers only settings/descriptors; request/result payloads are absent.
func InternalConnectivityConfig(c Config) connectivity.Config {
	return connectivity.Config{
		ContactPoints:     c.ContactPoints,
		Protocol:          c.Protocol,
		ConnectTimeout:    c.ConnectTimeout,
		DNSTimeout:        c.DNSTimeout,
		KeepaliveInterval: c.KeepaliveInterval,
		KeepaliveTimeout:  c.KeepaliveTimeout,
		SocketKeepalive:   c.SocketKeepalive,
		NoDelay:           c.NoDelay,
		ReceiveBuffer:     c.ReceiveBuffer,
		SendBuffer:        c.SendBuffer,
		Linger:            c.Linger,
		LocalAddress:      c.LocalAddress,
		DialHost:          c.DialHost,
		Dialer:            c.Dialer,
		DNSResolver:       c.DNSResolver,
		TLS:               c.TLS,
		TLSForHost:        c.TLSForHost,
		Pool:              c.Pool,
		ClientRoutes:      c.ClientRoutes,
		Metadata: connectivity.MetadataIO{
			InitialDiscovery: c.Metadata.InitialDiscovery,
			RequestTimeout:   c.Metadata.RequestTimeout,
			ServerTimeout:    c.Metadata.ServerTimeout,
		},
		Reconnect: c.Reconnect,
	}
}
func InternalConnectivityConfigPointer(c Config) *connectivity.Config {
	v := InternalConnectivityConfig(c)
	return &v
}
