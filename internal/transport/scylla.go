// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import

// isScyllaConn checks if conn is connected to a ScyllaDB node.
"context"
import "crypto/tls"
import "errors"
import "fmt"
import "net"
import "strconv"
import "syscall"
import "github.com/scylladb/gocql/v2/internal/debug"

func (c *Conn) InternalIsScyllaConn() bool {
	return c.InternalGetScyllaSupported().IsPresent()
}

type InternalScyllaDialer struct {
	InternalDialer    legacyDialer
	InternalLogger    StdLogger
	InternalTlsConfig *tls.Config
}

func (sd *InternalScyllaDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
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

	conn, err := sd.InternalDialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return WrapTLS(ctx, conn, addr, sd.InternalTlsConfig)
}

func (sd *InternalScyllaDialer) DialShard(ctx context.Context, host *HostInfo, shardID, nrShards int) (*DialedHost, error) {
	ip := host.ConnectAddress()
	port := host.Port()

	if !validIpAddr(ip) {
		return nil, fmt.Errorf("host missing connect ip address: %v", ip)
	} else if port == 0 {
		return nil, fmt.Errorf("host missing port: %v", port)
	}

	iter := InternalNewScyllaPortIterator(shardID, nrShards)
	addr := net.JoinHostPort(ip.String(), strconv.Itoa(port))
	shardAwareAddr := ""
	translatedInfo := implEndpoint.InternalHostInfoGetTranslatedConnectionInfo(host)
	if translatedInfo != nil {
		addr = translatedInfo.CQL.ToNetAddr()
		if sd.InternalTlsConfig != nil {
			if translatedInfo.ShardAwareTLS.IsValid() {
				shardAwareAddr = translatedInfo.ShardAwareTLS.ToNetAddr()
			}
		} else if translatedInfo.ShardAware.IsValid() {
			shardAwareAddr = translatedInfo.ShardAware.ToNetAddr()
		}
	}

	if debug.Enabled {
		sd.InternalLogger.Printf("scylla: connecting to shard %d", shardID)
	}

	conn, err := sd.dialShardAware(ctx, addr, shardAwareAddr, iter)
	if err != nil {
		return nil, err
	}

	return WrapTLS(ctx, conn, addr, sd.InternalTlsConfig)
}

func (sd *InternalScyllaDialer) dialShardAware(ctx context.Context, addr, shardAwareAddr string, iter *InternalScyllaPortIterator) (net.Conn, error) {
	for {
		port, ok := iter.Next()
		if !ok {

			return sd.InternalDialer.DialContext(ctx, "tcp", addr)
		}

		ctxWithPort := context.WithValue(ctx, scyllaSourcePortCtx{}, port)
		conn, err := sd.InternalDialer.DialContext(ctxWithPort, "tcp", shardAwareAddr)

		if InternalIsLocalAddrInUseErr(err) {

			continue
		} else if err != nil {
			conn, err := sd.InternalDialer.DialContext(ctx, "tcp", addr)
			if err == nil {

				if debug.Enabled {
					sd.InternalLogger.Printf(
						"scylla: %s couldn't connect to shard-aware address while the non-shard-aware address %s is available; this might be an issue with ",
						addr,
						shardAwareAddr,
					)
				}
			}
			return conn, err
		}
		return conn, err
	}
}

var ErrScyllaSourcePortAlreadyInUse = errors.New("scylla: source port is already in use")

func InternalIsLocalAddrInUseErr(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE) || errors.Is(err, ErrScyllaSourcePortAlreadyInUse)
}

type InternalScyllaPortIterator struct {
	currentPort int
	shardCount  int
}

const (
	InternalScyllaPortBasedBalancingMin = 0x8000
	InternalScyllaPortBasedBalancingMax = 0xFFFF
)

func InternalNewScyllaPortIterator(shardID, shardCount int) *InternalScyllaPortIterator {
	if shardCount == 0 {
		panic("shardCount cannot be 0")
	}

	port := InternalScyllaPortBasedBalancingMin - InternalScyllaShardForSourcePort(InternalScyllaPortBasedBalancingMin, shardCount) + shardID
	if port < InternalScyllaPortBasedBalancingMin {
		port += shardCount
	}

	return &InternalScyllaPortIterator{
		currentPort: port,
		shardCount:  shardCount,
	}
}

func (spi *InternalScyllaPortIterator) Next() (uint16, bool) {
	if spi == nil {
		return 0, false
	}

	p := spi.currentPort

	if p > InternalScyllaPortBasedBalancingMax {
		return 0, false
	}

	spi.currentPort += spi.shardCount
	return uint16(p), true
}

func InternalScyllaShardForSourcePort(sourcePort uint16, shardCount int) int {
	return int(sourcePort) % shardCount
}
