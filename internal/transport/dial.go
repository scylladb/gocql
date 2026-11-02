/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package transport

import "context"
import "bufio"
import "fmt"
import "time"
import "github.com/scylladb/gocql/v2/internal/streams"
import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
import "crypto/tls"
import "net"
import "net/netip"
import "strconv"
import "strings"

// Opening captures session-owned settings before socket admission.
type Opening struct {
	Context                       context.Context
	Hooks                         InternalConnectionHooks
	Identity                      InternalStartupIdentity
	Reporter                      InternalStartupReporter
	MetadataTimeout, CoalesceWait time.Duration
	PoolPolicy                    InternalConnectionPoolPolicy
	MaxRequests                   int
	FrameObserver                 FrameHeaderObserver
	StreamObserver                StreamObserver
}

func newStreamIDs(limit int) *streams.IDGenerator {
	if limit > 0 {
		return streams.NewLimited(limit)
	}
	return streams.New()
}
func Dial(ctx context.Context, host *HostInfo, cfg *ConnConfig, errorHandler ConnErrorHandler,
	shardID, nrShards int, opening Opening) (*Conn, error) {
	lifetimeCtx := ctx
	if value, ok := ctx.Value(InternalConnectionLifetimeKey{}).(context.Context); ok {
		lifetimeCtx = value
	}
	if cfg.ConnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.ConnectTimeout)
		defer cancel()
	}
	if cfg.ProtoVersion < 0 || cfg.ProtoVersion > 127 {
		return nil, fmt.Errorf("invalid protocol version %d: must be between 0 and 127", cfg.ProtoVersion)
	}

	shardDialer, ok := cfg.HostDialer.(implEndpoint.ShardDialer)
	var (
		dialedHost *DialedHost
		err        error
	)

	isShardAware := false
	if ok && nrShards > 0 {
		isShardAware = true
		dialedHost, err = shardDialer.DialShard(ctx, host, shardID, nrShards)
	} else {
		dialedHost, err = cfg.HostDialer.DialHost(ctx, host)
	}

	if err != nil {
		return nil, err
	}

	if cfg.TypedConfig != nil {
		isShardAware = implEndpoint.DialedHostData(dialedHost).InternalShardTarget.Set
	}
	openingDeadline, _ := ctx.Deadline()
	connectionCtx, cancel := context.WithCancel(lifetimeCtx)
	c := &Conn{
		InternalR: &connReader{
			InternalConn: dialedHost.Conn,
			InternalR:    bufio.NewReaderSize(dialedHost.Conn, 8<<10),
			transferable: true,
		},
		InternalCfg:                 cfg,
		InternalCalls:               make(map[int]*InternalCallReq),
		InternalVersion:             uint8(cfg.ProtoVersion),
		InternalIsShardAware:        isShardAware,
		InternalRequestedShard:      implEndpoint.DialedHostData(dialedHost).InternalShardTarget,
		InternalTargetEpoch:         implEndpoint.DialedHostData(dialedHost).InternalTargetEpoch,
		InternalOpeningSharderEpoch: implEndpoint.DialedHostData(dialedHost).InternalSharderEpoch,
		InternalOpeningDeadline:     openingDeadline,
		InternalValidateTarget:      implEndpoint.DialedHostData(dialedHost).InternalValidateTarget,
		InternalAddr:                dialedHost.Conn.RemoteAddr().String(),
		InternalErrorHandler:        errorHandler,
		InternalCompressor:          cfg.Compressor,
		InternalHooks:               opening.Hooks,
		InternalStartupIdentity:     opening.Identity,
		InternalStartupReporter:     opening.Reporter,
		InternalMetadataTimeout:     opening.MetadataTimeout,
		InternalCoalesceWait:        opening.CoalesceWait,
		InternalPoolPolicy:          opening.PoolPolicy,
		InternalStreams:             newStreamIDs(opening.MaxRequests),
		InternalHost:                host,
		InternalIsSchemaV2:          true, // Try using "system.peers_v2" until proven otherwise
		InternalFrameObserver:       opening.FrameObserver,
		InternalW: &InternalDeadlineContextWriter{
			InternalW:         dialedHost.Conn,
			InternalSemaphore: make(chan struct{}, 1),
			InternalQuit:      make(chan struct{}),
		},
		InternalCtx:            connectionCtx,
		InternalCancel:         cancel,
		InternalLogger:         cfg.InternalLogger(),
		InternalStreamObserver: opening.StreamObserver,
	}
	c.InternalSetSystemRequestTimeout(cfg.ConnectTimeout)

	if err := c.InternalInit(ctx, dialedHost); err != nil {
		cancel()
		c.Close()
		return nil, err
	}

	return c, nil
}

type InternalDefaultHostDialer struct {
	dialer    legacyDialer
	tlsConfig *tls.Config
}

func (hd *InternalDefaultHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	ip := host.ConnectAddress()
	port := host.Port()

	if !validIpAddr(ip) {
		return nil, fmt.Errorf("host missing connect ip address: %v", ip)
	} else if port == 0 {
		return nil, fmt.Errorf("host missing port: %v", port)
	}

	addr := net.JoinHostPort(ip.String(), strconv.Itoa(port))
	translatedInfo := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translatedInfo != nil {
		addr = translatedInfo.CQL.ToNetAddr()
	}

	conn, err := hd.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return WrapTLS(ctx, conn, addr, hd.tlsConfig)
}

func InternalTlsConfigForAddr(tlsConfig *tls.Config, addr string) *tls.Config {

	if !tlsConfig.InsecureSkipVerify && tlsConfig.ServerName == "" {
		hostname, _, err := net.SplitHostPort(addr)
		if err != nil {
			hostname = strings.Trim(addr, "[]")
		}
		if ip, err := netip.ParseAddr(hostname); err == nil {
			hostname = ip.WithZone("").String()
		}

		tlsConfig = tlsConfig.Clone()
		tlsConfig.ServerName = hostname
	}
	return tlsConfig
}

func WrapTLS(ctx context.Context, conn net.Conn, addr string, tlsConfig *tls.Config) (*DialedHost, error) {
	if tlsConfig != nil {
		tlsConfig := InternalTlsConfigForAddr(tlsConfig, addr)
		tconn := tls.Client(conn, tlsConfig)
		if err := tconn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, err
		}
		conn = tconn
	}

	return &DialedHost{
		Conn:            conn,
		DisableCoalesce: tlsConfig != nil,
	}, nil
}
