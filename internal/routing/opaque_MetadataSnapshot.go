// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

func SnapshotData(v *MetadataSnapshot) *SnapshotState       { return &v.state }
func NewMetadataSnapshot(v SnapshotState) *MetadataSnapshot { return &MetadataSnapshot{state: v} }
