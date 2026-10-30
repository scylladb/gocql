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

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

func InternalCheckedV2PoolBudget(options PoolOptions, shards int) (int, error) {
	if shards < 1 || shards > math.MaxUint16 {
		return 0, typeValidation("shard count", "must be 1..65535")
	}
	if options.ConnectionsPerHost < 0 || options.ConnectionsPerShard < 0 || options.MaxExcessPerShard < 0 {
		return 0, typeValidation("pool counts", "must not be negative")
	}
	if options.ConnectionsPerHost > 0 {
		return options.ConnectionsPerHost, nil
	}
	if options.ConnectionsPerShard == 0 {
		return 0, typeValidation("pool target", "per-host or per-shard count required")
	}
	if options.ConnectionsPerShard > math.MaxInt/shards || options.MaxExcessPerShard > math.MaxInt/shards {
		return 0, typeValidation("pool target", "connection/excess count arithmetic overflow")
	}
	target := options.ConnectionsPerShard * shards
	excess := options.MaxExcessPerShard * shards
	if excess > math.MaxInt-target {
		return 0, typeValidation("pool target", "combined target/excess count arithmetic overflow")
	}
	return target, nil
}
func InternalValidateV2ConnectionSharding(info ScyllaConnectionFeatures) error {
	if implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalNrShards == 0 {
		if implEndpoint.ScyllaConnectionFeaturesData(&info).InternalShard != 0 || implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalMsbIgnore != 0 {
			return typeValidation("sharding", "unsharded connection has shard-specific parameters")
		}
		return nil
	}
	if implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalNrShards < 1 || implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalNrShards > math.MaxUint16 || implEndpoint.ScyllaConnectionFeaturesData(&info).InternalShard < 0 || implEndpoint.ScyllaConnectionFeaturesData(&info).InternalShard >= implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalNrShards || implEndpoint.ScyllaHostFeaturesData(&info.ScyllaHostFeatures).InternalMsbIgnore > 63 {
		return typeValidation("sharding", "invalid count, shard ID or ignored MSB count")
	}
	return nil
}

// Validate hostile SUPPORTED values before any parser indexes a list or picker
// allocates from an advertised count. Flag-only optional extensions may use an
// empty value list; sharding's required single-value parameters may not.
func InternalValidateV2Supported(values map[string][]string, options PoolOptions) error {
	keys := []string{"SCYLLA_SHARD", "SCYLLA_NR_SHARDS", "SCYLLA_PARTITIONER", "SCYLLA_SHARDING_ALGORITHM", "SCYLLA_SHARDING_IGNORE_MSB"}
	present := false
	for _, key := range keys {
		if _, ok := values[key]; ok {
			present = true
		}
	}
	shards := 1
	if present {
		for _, key := range keys {
			v, ok := values[key]
			if !ok || len(v) != 1 || v[0] == "" {
				return &ProtocolError{Stage: "SUPPORTED sharding", Err: fmt.Errorf("missing/ambiguous %s", key)}
			}
		}
		count, err := strconv.ParseUint(values["SCYLLA_NR_SHARDS"][0], 10, 16)
		if err != nil || count == 0 {
			return &ProtocolError{Stage: "SUPPORTED sharding", Err: fmt.Errorf("invalid 16-bit nonzero shard count")}
		}
		shard, err := strconv.ParseUint(values["SCYLLA_SHARD"][0], 10, 16)
		if err != nil || shard >= count {
			return &ProtocolError{Stage: "SUPPORTED sharding", Err: fmt.Errorf("shard ID outside advertised count")}
		}
		msb, err := strconv.ParseUint(values["SCYLLA_SHARDING_IGNORE_MSB"][0], 10, 6)
		if err != nil || msb > 63 {
			return &ProtocolError{Stage: "SUPPORTED sharding", Err: fmt.Errorf("ignored MSB count outside 0..63")}
		}
		shards = int(count)
	}
	for _, key := range []string{"SCYLLA_SHARD_AWARE_PORT", "SCYLLA_SHARD_AWARE_PORT_SSL"} {
		if v, ok := values[key]; ok {
			if len(v) != 1 {
				return &ProtocolError{Stage: "SUPPORTED port", Err: fmt.Errorf("missing/ambiguous %s", key)}
			}
			if _, err := strconv.ParseUint(v[0], 10, 16); err != nil {
				return &ProtocolError{Stage: "SUPPORTED port", Err: err}
			}
		}
	}
	for key, param := range map[string]string{rateLimitError: "ERROR_CODE", lwtAddMetadataMarkKey: "LWT_OPTIMIZATION_META_BIT_MASK"} {
		for _, entry := range values[key] {
			name, value, ok := strings.Cut(entry, "=")
			if name != param {
				continue
			}
			if !ok || value == "" {
				return &ProtocolError{Stage: "SUPPORTED extension", Err: fmt.Errorf("invalid %s", param)}
			}
			if key == lwtAddMetadataMarkKey {
				if _, err := strconv.ParseUint(value, 10, 32); err != nil {
					return &ProtocolError{Stage: "SUPPORTED extension", Err: err}
				}
			} else {
				if _, err := strconv.ParseInt(value, 10, 32); err != nil {
					return &ProtocolError{Stage: "SUPPORTED extension", Err: err}
				}
			}
		}
	}
	_, err := InternalCheckedV2PoolBudget(options, shards)
	return err
}
