// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package connectivity contains physical connection/pool contracts without
// execution policies, routing implementations or schema loaders.
package connectivity

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"time"

	"github.com/scylladb/gocql/v2/internal/cqltype"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	"github.com/scylladb/gocql/v2/internal/model"
)

type Option[T any] = cqltype.Option[T]
type Nullable[T any] = cqltype.Nullable[T]
type Bytes = cqltype.Bytes
type HostSnapshot = model.HostSnapshot
type PoolOptions struct {
	ConnectionsPerHost, ConnectionsPerShard int
	MaxExcessPerShard                       int
	MaxRequestsPerConnection                int
	MaxOrphanedStreams                      int
	ShardAwarePort                          bool
	SourcePortStart, SourcePortEnd          uint16
}

type ReconnectPolicy interface {
	Delay(attempt int) (time.Duration, bool)
}

type ProtocolLimits struct {
	MaxFrameBytes, MaxDecompressedBytes int64
	MaxMetadataDepth                    int
	MaxPagingStates                     int
	MaxPagingStateBytes                 int64
}

// MetadataIO contains only the budgets required for encoded metadata I/O.
type MetadataIO struct {
	InitialDiscovery bool
	RequestTimeout   time.Duration
	ServerTimeout    Option[time.Duration]
}

type ClientRoutesOptions struct {
	ConnectionIDs     []string
	HostnameOverrides map[string]string
	TLS               bool
	SNIProxyAddress   string
}

type AuthProvider interface {
	Start(context.Context, HostSnapshot, string) (AuthExchange, Nullable[[]byte], error)
}

type AuthExchange interface {
	Challenge(context.Context, Nullable[Bytes]) (Nullable[[]byte], error)
	Success(context.Context, Nullable[Bytes]) error
	Close()
}

type ConvictionPolicy interface {
	// Implementations should return `true` if the host should be convicted, `false` otherwise.
	AddFailure(error error, host *implEndpoint.HostInfo) bool
	// Implementations should clear out any convictions or state regarding the host.
	Reset(host *implEndpoint.HostInfo)
}
type Config struct {
	ContactPoints                                                                    []string
	Protocol                                                                         int
	ConnectTimeout, DNSTimeout, KeepaliveInterval, KeepaliveTimeout, SocketKeepalive time.Duration
	NoDelay                                                                          bool
	ReceiveBuffer, SendBuffer                                                        Option[int]
	Linger                                                                           Option[time.Duration]
	LocalAddress                                                                     netip.Addr
	DialHost                                                                         func(context.Context, implEndpoint.DialTarget) (net.Conn, error)
	Dialer                                                                           implEndpoint.Dialer
	DNSResolver                                                                      implEndpoint.DNSResolver
	TLS                                                                              *tls.Config
	TLSForHost                                                                       func(context.Context, HostSnapshot) (*tls.Config, error)
	Pool                                                                             PoolOptions
	ClientRoutes                                                                     *ClientRoutesOptions
	Metadata                                                                         MetadataIO
	Reconnect                                                                        ReconnectPolicy
}
