// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

type ScyllaHostFeaturesStorage struct {

	// Comes from SCYLLA_PARTITIONER
	InternalPartitioner string
	// Comes from SCYLLA_SHARDING_ALGORITHM
	InternalShardingAlgorithm string
	// Comes from SCYLLA_NR_SHARDS
	InternalNrShards int
	// Comes from SCYLLA_SHARDING_IGNORE_MSB
	InternalMsbIgnore uint64
	// Comes from SCYLLA_LWT_ADD_METADATA_MARK.LWT_OPTIMIZATION_META_BIT_MASK
	InternalLwtFlagMask int
	// Comes from SCYLLA_RATE_LIMIT_ERROR.ERROR_CODE
	InternalRateLimitErrorCode int
	// Comes from SCYLLA_SHARD_AWARE_PORT
	InternalShardAwarePort uint16
	// Comes from SCYLLA_SHARD_AWARE_PORT_SSL
	InternalShardAwarePortTLS uint16
	// Comes from SCYLLA_USE_METADATA_ID
	// Signals that host supports proper prepared statement metadata invalidation read more at:
	// https://github.com/scylladb/scylladb/issues/20860
	// https://github.com/scylladb/scylladb/pull/23292
	InternalIsMetadataIDSupported bool
	// True when the node is identified as ScyllaDB; stays true even when
	// shard awareness is disabled on the server.
	InternalIsScylla bool
}

func ScyllaHostFeaturesData(v *ScyllaHostFeatures) *ScyllaHostFeaturesStorage {
	return &v.privateStorage
}
func ScyllaHostFeaturesValueData(v ScyllaHostFeatures) ScyllaHostFeaturesStorage {
	return v.privateStorage
}
