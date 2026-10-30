package gocql

import (
	"context"
	"crypto/tls"
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"
	pool "github.com/scylladb/gocql/v2/internal/pool"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"net"
	"time"
)

type driverConnPicker = pool.ConnPicker
type ConstantReconnectionPolicy = pool.ConstantReconnectionPolicy

var ErrScyllaSourcePortAlreadyInUse = implTransport.ErrScyllaSourcePortAlreadyInUse

type ExponentialReconnectionPolicy = pool.ExponentialReconnectionPolicy
type HostInformation = implEndpoint.HostInformation
type HostPoolInfo = pool.HostPoolInfo
type NoReconnectionPolicy = pool.NoReconnectionPolicy
type Partitioner = implEndpoint.Partitioner
type ReconnectionPolicy = pool.ReconnectionPolicy
type SelectedHost = implEndpoint.SelectedHost
type SetHosts = pool.SetHosts
type SetPartitioner = pool.SetPartitioner
type SetTablets = pool.SetTablets
type Token = implEndpoint.Token

func WrapTLS(a0 context.Context, a1 net.Conn, a2 string, a3 *tls.Config) (*DialedHost, error) {
	return implTransport.WrapTLS(a0, a1, a2, a3)
}
func applyV2SocketOptions(a0 net.Conn, a1 Config, a2 string) error {
	return implTransport.InternalApplyV2SocketOptions(a0, a1, a2)
}
func cloneV2DialTarget(a0 DialTarget) DialTarget { return implTransport.InternalCloneV2DialTarget(a0) }
func cloneV2HostSnapshot(a0 HostSnapshot) HostSnapshot {
	return implTransport.InternalCloneV2HostSnapshot(a0)
}
func closeConns(a0 ...*driverConn) { pool.InternalCloseConns(a0...) }

type defaultConnPicker = pool.InternalDefaultConnPicker
type defaultHostDialer = implTransport.InternalDefaultHostDialer

func equalV2TLSConfig(a0 *tls.Config, a1 *tls.Config) bool {
	return implTransport.InternalEqualV2TLSConfig(a0, a1)
}
func getExponentialTime(a0 time.Duration, a1 time.Duration, a2 int) time.Duration {
	return pool.InternalGetExponentialTime(a0, a1, a2)
}

type hostConnPool = pool.InternalHostConnPool
type hostToken = implEndpoint.InternalHostToken
type int64ConnPicker = pool.InternalInt64ConnPicker
type int64Hasher = implEndpoint.InternalInt64Hasher
type int64Token = implEndpoint.InternalInt64Token
type int64TokenSelectedHost = implEndpoint.InternalInt64TokenSelectedHost

func isHeavyLoaded(a0 *driverConn) bool       { return pool.InternalIsHeavyLoaded(a0) }
func isLocalAddrInUseErr(a0 error) bool       { return implTransport.InternalIsLocalAddrInUseErr(a0) }
func logicalV2TLSAddress(a0 *HostInfo) string { return implTransport.InternalLogicalV2TLSAddress(a0) }

var maxHashInt = implEndpoint.InternalMaxHashInt

type murmur3Partitioner = implEndpoint.InternalMurmur3Partitioner

func newDefaultConnPicker(a0 int) *defaultConnPicker { return pool.InternalNewDefaultConnPicker(a0) }
func newHostPool(a0 poolServices, a1 *HostInfo, a2 int, a3 string) *hostConnPool {
	return pool.InternalNewHostPool(a0, a1, a2, a3)
}
func newScyllaConnPicker(a0 *driverConn, a1 StdLogger) *scyllaConnPicker {
	return pool.InternalNewScyllaConnPicker(a0, a1)
}
func newScyllaPortIterator(a0 int, a1 int) *scyllaPortIterator {
	return implTransport.InternalNewScyllaPortIterator(a0, a1)
}
func newTokenRing(a0 string, a1 []*HostInfo) (*tokenRing, error) {
	return implEndpoint.InternalNewTokenRing(a0, a1)
}
func newTypedConnPicker(a0 *driverConn, a1 PoolOptions) (*typedConnPicker, error) {
	return pool.InternalNewTypedConnPicker(a0, a1)
}

type nopConnPicker = pool.InternalNopConnPicker
type orderedPartitioner = implEndpoint.InternalOrderedPartitioner
type orderedToken = implEndpoint.InternalOrderedToken

func parseInt64Token(a0 string) int64Token { return implEndpoint.InternalParseInt64Token(a0) }

type policyConnPool = pool.InternalPolicyConnPool

const poolBroken = pool.InternalPoolBroken
const poolClosed = pool.InternalPoolClosed
const poolInitializing = pool.InternalPoolInitializing
const poolReady = pool.InternalPoolReady

type poolServices = pool.InternalPoolServices
type poolState = pool.InternalPoolState
type randomPartitioner = implEndpoint.InternalRandomPartitioner
type randomToken = implEndpoint.InternalRandomToken

const scyllaCDCExtensionName = implEndpoint.InternalScyllaCDCExtensionName
const scyllaCDCLogTableNameSuffix = implEndpoint.InternalScyllaCDCLogTableNameSuffix
const scyllaCDCMaxSupportedVersion = implEndpoint.InternalScyllaCDCMaxSupportedVersion
const scyllaCDCMinSupportedVersion = implEndpoint.InternalScyllaCDCMinSupportedVersion
const scyllaCDCMinToken = implEndpoint.InternalScyllaCDCMinToken
const scyllaCDCPartitionKeyLength = implEndpoint.InternalScyllaCDCPartitionKeyLength

type scyllaCDCPartitioner = implEndpoint.InternalScyllaCDCPartitioner

const scyllaCDCPartitionerFullName = implEndpoint.InternalScyllaCDCPartitionerFullName
const scyllaCDCPartitionerName = implEndpoint.InternalScyllaCDCPartitionerName
const scyllaCDCVersionMask = implEndpoint.InternalScyllaCDCVersionMask

type scyllaConnPicker = pool.InternalScyllaConnPicker
type scyllaDialer = implTransport.InternalScyllaDialer

const scyllaPortBasedBalancingMax = implTransport.InternalScyllaPortBasedBalancingMax
const scyllaPortBasedBalancingMin = implTransport.InternalScyllaPortBasedBalancingMin

type scyllaPortIterator = implTransport.InternalScyllaPortIterator

const scyllaShardAwarePortFallbackDuration = pool.InternalScyllaShardAwarePortFallbackDuration

func scyllaShardForSourcePort(a0 uint16, a1 int) int {
	return implTransport.InternalScyllaShardForSourcePort(a0, a1)
}
func tlsConfigForAddr(a0 *tls.Config, a1 string) *tls.Config {
	return implTransport.InternalTlsConfigForAddr(a0, a1)
}

type tokenRing = implEndpoint.InternalTokenRing
type typedConnPicker = pool.InternalTypedConnPicker
type v2TLSHostDialer = implTransport.InternalV2TLSHostDialer
