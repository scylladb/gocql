// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

import (
	"context"
	"fmt"
	"github.com/scylladb/gocql/v2/internal/debug"
	logging "github.com/scylladb/gocql/v2/internal/logging"
	"net"
	"strconv"
	"strings"
)

// V2's version block follows the pinned extension, independent of protocol v3/4/5.
type InternalTabletsRoutingV2Ext struct{}

func (InternalTabletsRoutingV2Ext) InternalName() string { return tabletsRoutingV2 }

func (InternalTabletsRoutingV2Ext) InternalSerialize() map[string]string {
	return map[string]string{tabletsRoutingV2: ""}
}

func InternalTypedTabletExtensions(exts []cqlProtocolExtension, supported map[string][]string) []cqlProtocolExtension {
	if _, ok := supported[tabletsRoutingV2]; !ok {
		return exts
	}
	filtered := exts[:0]
	for _, ext := range exts {
		if ext.InternalName() != tabletsRoutingV1 {
			filtered = append(filtered, ext)
		}
	}
	return append(filtered, InternalTabletsRoutingV2Ext{})
}

type ScyllaConnectionFeatures struct {
	ScyllaHostFeatures
	privateStorage ScyllaConnectionFeaturesStorage
	// Comes from SCYLLA_SHARD

}

func (f ScyllaConnectionFeatures) Shard() int {
	return f.privateStorage.InternalShard
}

type ScyllaHostFeatures struct {
	privateStorage ScyllaHostFeaturesStorage
	// Comes from SCYLLA_PARTITIONER

	// Comes from SCYLLA_SHARDING_ALGORITHM

	// Comes from SCYLLA_NR_SHARDS

	// Comes from SCYLLA_SHARDING_IGNORE_MSB

	// Comes from SCYLLA_LWT_ADD_METADATA_MARK.LWT_OPTIMIZATION_META_BIT_MASK

	// Comes from SCYLLA_RATE_LIMIT_ERROR.ERROR_CODE

	// Comes from SCYLLA_SHARD_AWARE_PORT

	// Comes from SCYLLA_SHARD_AWARE_PORT_SSL

	// Comes from SCYLLA_USE_METADATA_ID
	// Signals that host supports proper prepared statement metadata invalidation read more at:
	// https://github.com/scylladb/scylladb/issues/20860
	// https://github.com/scylladb/scylladb/pull/23292

	// True when the node is identified as ScyllaDB; stays true even when
	// shard awareness is disabled on the server.

}

// IsPresent reports whether the host is a ScyllaDB node.
func (f ScyllaHostFeatures) IsPresent() bool {
	return f.privateStorage.InternalIsScylla
}

func (f ScyllaHostFeatures) Partitioner() string {
	return f.privateStorage.InternalPartitioner
}

func (f ScyllaHostFeatures) ShardingAlgorithm() string {
	return f.privateStorage.InternalShardingAlgorithm
}

func (f ScyllaHostFeatures) ShardsCount() int {
	return f.privateStorage.InternalNrShards
}

func (f ScyllaHostFeatures) MSBIgnore() uint64 {
	return f.privateStorage.InternalMsbIgnore
}

func (f ScyllaHostFeatures) LWTFlagMask() int {
	return f.privateStorage.InternalLwtFlagMask
}

func (f ScyllaHostFeatures) ShardAwarePort() uint16 {
	return f.privateStorage.InternalShardAwarePort
}

func (f ScyllaHostFeatures) ShardAwarePortTLS() uint16 {
	return f.privateStorage.InternalShardAwarePortTLS
}

func (f ScyllaHostFeatures) RateLimitErrorCode() int {
	return f.privateStorage.InternalRateLimitErrorCode
}

func (f ScyllaHostFeatures) IsMetadataIDSupported() bool {
	return f.privateStorage.InternalIsMetadataIDSupported
}

// CQL Protocol extension interface for Scylla.
// Each extension is identified by a name and defines a way to serialize itself
// in STARTUP message payload.
type InternalCqlProtocolExtension interface {
	InternalName() string
	InternalSerialize() map[string]string
}

func InternalFindCQLProtoExtByName(exts []InternalCqlProtocolExtension, name string) InternalCqlProtocolExtension {
	for i := range exts {
		if exts[i].InternalName() == name {
			return exts[i]
		}
	}
	return nil
}

// Top-level keys used for serialization/deserialization of CQL protocol
// extensions in SUPPORTED/STARTUP messages.
// Each key identifies a single extension.
const (
	InternalLwtAddMetadataMarkKey = "SCYLLA_LWT_ADD_METADATA_MARK"
	InternalRateLimitError        = "SCYLLA_RATE_LIMIT_ERROR"
	InternalTabletsRoutingV1      = "TABLETS_ROUTING_V1"
	InternalTabletsRoutingV2      = "TABLETS_ROUTING_V2_EXPERIMENTAL"
	InternalScyllaUseMetadataID   = "SCYLLA_USE_METADATA_ID"
)

// "tabletsRoutingV1" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to send a tablet information in `custom_payload`.
//
// Implements cqlProtocolExtension interface.
type InternalTabletsRoutingV1Ext struct {
}

// Factory function to deserialize and create an `tabletsRoutingV1Ext` instance
// from SUPPORTED message payload.
func InternalNewTabletsRoutingV1Ext(supported map[string][]string) *InternalTabletsRoutingV1Ext {
	if _, found := supported[InternalTabletsRoutingV1]; found {
		return &InternalTabletsRoutingV1Ext{}
	}
	return nil
}

func (ext *InternalTabletsRoutingV1Ext) InternalSerialize() map[string]string {
	return map[string]string{
		InternalTabletsRoutingV1: "",
	}
}

func (ext *InternalTabletsRoutingV1Ext) InternalName() string {
	return InternalTabletsRoutingV1
}

// "Rate limit" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to send a special kind of error.
//
// Implements cqlProtocolExtension interface.
type InternalRateLimitExt struct {
	InternalRateLimitErrorCode int
}

// Factory function to deserialize and create an `rateLimitExt` instance
// from SUPPORTED message payload.
func InternalNewRateLimitExt(supported map[string][]string, logger logging.StdLogger) *InternalRateLimitExt {
	const rateLimitErrorCode = "ERROR_CODE"

	if v, found := supported[InternalRateLimitError]; found {
		for i := range v {
			splitVal := strings.SplitN(v[i], "=", 2)
			if len(splitVal) != 2 {
				continue
			}
			if splitVal[0] == rateLimitErrorCode {
				var (
					err       error
					errorCode int
				)
				if errorCode, err = strconv.Atoi(splitVal[1]); err != nil {
					if debug.Enabled {
						logger.Printf("scylla: failed to parse %s value %v: %s", rateLimitErrorCode, splitVal[1], err)
					}
					return nil
				}
				return &InternalRateLimitExt{
					InternalRateLimitErrorCode: errorCode,
				}
			}
		}
	}
	return nil
}

func (ext *InternalRateLimitExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalRateLimitError: "",
	}
}

func (ext *InternalRateLimitExt) InternalName() string {
	return InternalRateLimitError
}

// "LWT prepared statements metadata mark" CQL Protocol Extension.
// This extension, if enabled (properly negotiated), allows Scylla server
// to set a special bit in prepared statements metadata, which would indicate
// whether the statement at hand is LWT statement or not.
//
// This is further used to consistently choose primary replicas in a predefined
// order for these queries, which can reduce contention over hot keys and thus
// increase LWT performance.
//
// Implements cqlProtocolExtension interface.
type InternalLwtAddMetadataMarkExt struct {
	InternalLwtOptMetaBitMask int
}

// Factory function to deserialize and create an `lwtAddMetadataMarkExt` instance
// from SUPPORTED message payload.
func InternalNewLwtAddMetaMarkExt(supported map[string][]string, logger logging.StdLogger) *InternalLwtAddMetadataMarkExt {
	const lwtOptMetaBitMaskKey = "LWT_OPTIMIZATION_META_BIT_MASK"

	if v, found := supported[InternalLwtAddMetadataMarkKey]; found {
		for i := range v {
			splitVal := strings.SplitN(v[i], "=", 2)
			if len(splitVal) != 2 {
				continue
			}
			if splitVal[0] == lwtOptMetaBitMaskKey {
				var (
					err     error
					bitMask int
				)
				value, parseErr := strconv.ParseUint(splitVal[1], 10, 32)
				bitMask, err = int(value), parseErr
				if err != nil {
					if debug.Enabled {
						logger.Printf("scylla: failed to parse %s value %v: %s", lwtOptMetaBitMaskKey, splitVal[1], err)
					}
					return nil
				}
				return &InternalLwtAddMetadataMarkExt{
					InternalLwtOptMetaBitMask: bitMask,
				}
			}
		}
	}
	return nil
}

func (ext *InternalLwtAddMetadataMarkExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalLwtAddMetadataMarkKey: fmt.Sprintf("LWT_OPTIMIZATION_META_BIT_MASK=%d", uint32(ext.InternalLwtOptMetaBitMask)),
	}
}

func (ext *InternalLwtAddMetadataMarkExt) InternalName() string {
	return InternalLwtAddMetadataMarkKey
}

// "SCYLLA_USE_METADATA_ID" CQL Protocol Extension.
// Enables storing and updating metadata IDs for prepared statements, similar to CQL v5.
// When negotiated, the driver tracks metadata changes and updates cached metadata accordingly.
type InternalScyllaUseMetadataIDExt struct {
}

// newScyllaUseMetadataIDExt returns the extension only for a protocol v4 connection,
// which is the whole of what the extension is: a backport of the v5 result metadata
// id to v4. Version matters because opting in changes the wire format — EXECUTE
// carries a [short bytes] result metadata id and RESULT/Prepared answers with one
// (see writeExecuteFrame and parseResultPrepared) — so on v3, where that field is not
// defined, the opt-in would desynchronise a connection the driver otherwise supports
// (newFramer accepts v3, and a non-zero clusterSettings.ProtoVersion skips
// discoverProtocol). On v5 the field is already mandatory and Conn.tracksResultMetadataID
// is true without the extension, so asking for it again would be redundant.
func InternalNewScyllaUseMetadataIDExt(supported map[string][]string, version byte) *InternalScyllaUseMetadataIDExt {
	if version&protoVersionMask != protoVersion4 {
		return nil
	}
	if _, found := supported[InternalScyllaUseMetadataID]; found {
		return &InternalScyllaUseMetadataIDExt{}
	}
	return nil
}

// name implements cqlProtocolExtension.
func (ext *InternalScyllaUseMetadataIDExt) InternalName() string {
	return InternalScyllaUseMetadataID
}

// serialize implements cqlProtocolExtension.
func (ext *InternalScyllaUseMetadataIDExt) InternalSerialize() map[string]string {
	return map[string]string{
		InternalScyllaUseMetadataID: "",
	}
}

func InternalParseSupported(supported map[string][]string, logger logging.StdLogger) ScyllaConnectionFeatures {
	const (
		scyllaShard             = "SCYLLA_SHARD"
		scyllaNrShards          = "SCYLLA_NR_SHARDS"
		scyllaPartitioner       = "SCYLLA_PARTITIONER"
		scyllaShardingAlgorithm = "SCYLLA_SHARDING_ALGORITHM"
		scyllaShardingIgnoreMSB = "SCYLLA_SHARDING_IGNORE_MSB"
		scyllaShardAwarePort    = "SCYLLA_SHARD_AWARE_PORT"
		scyllaShardAwarePortSSL = "SCYLLA_SHARD_AWARE_PORT_SSL"
	)

	var (
		si  ScyllaConnectionFeatures
		err error
	)

	if s, ok := supported[scyllaShard]; ok && len(s) > 0 {
		if si.privateStorage.InternalShard, err = strconv.Atoi(s[0]); err != nil {
			if debug.Enabled {
				logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShard, s, err)
			}
		}
	}
	if s, ok := supported[scyllaNrShards]; ok && len(s) > 0 {
		if si.ScyllaHostFeatures.privateStorage.InternalNrShards, err = strconv.Atoi(s[0]); err != nil {
			if debug.Enabled {
				logger.Printf("scylla: failed to parse %s value %v: %s", scyllaNrShards, s, err)
			}
		}
	}
	if s, ok := supported[scyllaShardingIgnoreMSB]; ok && len(s) > 0 {
		if si.ScyllaHostFeatures.privateStorage.InternalMsbIgnore, err = strconv.ParseUint(s[0], 10, 64); err != nil {
			if debug.Enabled {
				logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardingIgnoreMSB, s, err)
			}
		}
	}

	if s, ok := supported[scyllaPartitioner]; ok && len(s) > 0 {
		si.ScyllaHostFeatures.privateStorage.InternalPartitioner = s[0]
	}
	if s, ok := supported[scyllaShardingAlgorithm]; ok && len(s) > 0 {
		si.ScyllaHostFeatures.privateStorage.InternalShardingAlgorithm = s[0]
	}
	if s, ok := supported[scyllaShardAwarePort]; ok && len(s) > 0 {
		if shardAwarePort, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if debug.Enabled {
				logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardAwarePort, s, err)
			}
		} else {
			si.ScyllaHostFeatures.privateStorage.InternalShardAwarePort = uint16(shardAwarePort)
		}
	}
	if s, ok := supported[scyllaShardAwarePortSSL]; ok && len(s) > 0 {
		if shardAwarePortTLS, err := strconv.ParseUint(s[0], 10, 16); err != nil {
			if debug.Enabled {
				logger.Printf("scylla: failed to parse %s value %v: %s", scyllaShardAwarePortSSL, s, err)
			}
		} else {
			si.ScyllaHostFeatures.privateStorage.InternalShardAwarePortTLS = uint16(shardAwarePortTLS)
		}
	}

	if lwtInfo := InternalNewLwtAddMetaMarkExt(supported, logger); lwtInfo != nil {
		si.ScyllaHostFeatures.privateStorage.InternalLwtFlagMask = lwtInfo.InternalLwtOptMetaBitMask
	}

	if rateLimitInfo := InternalNewRateLimitExt(supported, logger); rateLimitInfo != nil {
		si.ScyllaHostFeatures.privateStorage.InternalRateLimitErrorCode = rateLimitInfo.InternalRateLimitErrorCode
	}

	if _, ok := supported[InternalScyllaUseMetadataID]; ok {
		si.ScyllaHostFeatures.privateStorage.InternalIsMetadataIDSupported = true
	}

	_, hasLWT := supported[InternalLwtAddMetadataMarkKey]
	_, hasRateLimit := supported[InternalRateLimitError]
	si.ScyllaHostFeatures.privateStorage.InternalIsScylla = hasLWT || hasRateLimit || si.ScyllaHostFeatures.privateStorage.InternalIsMetadataIDSupported || si.ScyllaHostFeatures.privateStorage.InternalNrShards != 0

	complete := true
	for _, key := range []string{scyllaShard, scyllaNrShards, scyllaPartitioner, scyllaShardingAlgorithm, scyllaShardingIgnoreMSB} {
		v, ok := supported[key]
		if !ok || len(v) == 0 || v[0] == "" {
			complete = false
		}
	}
	if !complete || si.privateStorage.InternalShard < 0 || si.privateStorage.InternalShard >= si.ScyllaHostFeatures.privateStorage.InternalNrShards || si.ScyllaHostFeatures.privateStorage.InternalNrShards > 65535 || si.ScyllaHostFeatures.privateStorage.InternalMsbIgnore > 63 || si.ScyllaHostFeatures.privateStorage.InternalPartitioner != "org.apache.cassandra.dht.Murmur3Partitioner" || si.ScyllaHostFeatures.privateStorage.InternalShardingAlgorithm != "biased-token-round-robin" || si.ScyllaHostFeatures.privateStorage.InternalNrShards <= 0 {
		if debug.Enabled {
			logger.Printf("scylla: unsupported sharding configuration, partitioner=%s, algorithm=%s, no_shards=%d, msb_ignore=%d", si.ScyllaHostFeatures.privateStorage.InternalPartitioner, si.ScyllaHostFeatures.privateStorage.InternalShardingAlgorithm, si.ScyllaHostFeatures.privateStorage.InternalNrShards, si.ScyllaHostFeatures.privateStorage.InternalMsbIgnore)
		}
		si.privateStorage.InternalShard = 0
		si.ScyllaHostFeatures.privateStorage.InternalNrShards = 0
		si.ScyllaHostFeatures.privateStorage.InternalMsbIgnore = 0
		si.ScyllaHostFeatures.privateStorage.InternalShardingAlgorithm = ""
	}

	return si
}

// parseCQLProtocolExtensions turns the server's SUPPORTED multimap into the
// extensions this connection will opt into. version is the connection's protocol
// version: an extension whose wire effect is version-specific is gated on it here,
// which is the single point that feeds both the STARTUP opt-in (startupCoordinator.startup)
// and the framer config (connFramers.initCache), so the two cannot disagree.
func InternalParseCQLProtocolExtensions(supported map[string][]string, version byte, logger logging.StdLogger) []InternalCqlProtocolExtension {
	exts := []InternalCqlProtocolExtension{}

	lwtExt := InternalNewLwtAddMetaMarkExt(supported, logger)
	if lwtExt != nil {
		exts = append(exts, lwtExt)
	}

	rateLimitExt := InternalNewRateLimitExt(supported, logger)
	if rateLimitExt != nil {
		exts = append(exts, rateLimitExt)
	}

	tabletsExt := InternalNewTabletsRoutingV1Ext(supported)
	if tabletsExt != nil {
		exts = append(exts, tabletsExt)
	}

	metadataIDExt := InternalNewScyllaUseMetadataIDExt(supported, version)
	if metadataIDExt != nil {
		exts = append(exts, metadataIDExt)
	}

	return exts
}

// ShardDialer is like HostDialer but is shard-aware.
// If the driver wants to connect to a specific shard, it will call DialShard,
// otherwise it will call DialHost.
type ShardDialer interface {
	HostDialer

	// DialShard establishes a connection to the specified shard ID out of nrShards.
	// The returned connection must be directly usable for CQL protocol,
	// specifically DialShard is responsible also for setting up the TLS session if needed.
	DialShard(ctx context.Context, host *HostInfo, shardID, nrShards int) (*DialedHost, error)
}

// ScyllaShardAwareDialer wraps a net.Dialer, but uses a source port specified by gocql when connecting.
//
// Unlike in the case standard native transport ports, gocql can choose which shard will handle
// a new connection by connecting from a specific source port. If you are using your own net.Dialer
// in clusterSettings, you can use ScyllaShardAwareDialer to "upgrade" it so that it connects
// from the source port chosen by gocql.
//
// Please note that ScyllaShardAwareDialer overwrites the LocalAddr field in order to choose
// the right source port for connection.
type ScyllaShardAwareDialer struct {
	net.Dialer
}

func (d *ScyllaShardAwareDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := ScyllaGetSourcePort(ctx)
	if sourcePort == 0 {
		return d.Dialer.DialContext(ctx, network, addr)
	}
	dialerWithLocalAddr := d.Dialer
	dialerWithLocalAddr.LocalAddr, err = net.ResolveTCPAddr(network, fmt.Sprintf(":%d", sourcePort))
	if err != nil {
		return nil, err
	}

	return dialerWithLocalAddr.DialContext(ctx, network, addr)
}

type InternalScyllaSourcePortCtx struct{}

// ScyllaGetSourcePort returns the source port that should be used when connecting to a node.
//
// Unlike in the case standard native transport ports, gocql can choose which shard will handle
// a new connection at the shard-aware port by connecting from a specific source port. Therefore,
// if you are using a custom Dialer and your nodes expose shard-aware ports, your dialer should
// use the source port specified by gocql.
//
// If this function returns 0, then your dialer can use any source port.
//
// If you aren't using a custom dialer, gocql will use a default one which uses appropriate source port.
// If you are using net.Dialer, consider wrapping it in a gocql.ScyllaShardAwareDialer.
func ScyllaGetSourcePort(ctx context.Context) uint16 {
	sourcePort, _ := ctx.Value(InternalScyllaSourcePortCtx{}).(uint16)
	return sourcePort
}
