// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package endpoint

type ScyllaConnectionFeaturesStorage struct {

	// Comes from SCYLLA_SHARD
	InternalShard int
}

func ScyllaConnectionFeaturesData(v *ScyllaConnectionFeatures) *ScyllaConnectionFeaturesStorage {
	return &v.privateStorage
}
