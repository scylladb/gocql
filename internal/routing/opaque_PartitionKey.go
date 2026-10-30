// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

func PartitionKeyData(v *PartitionKey) *PartitionKeyState { return &v.state }
func NewPartitionKey(v PartitionKeyState) *PartitionKey   { return &PartitionKey{state: v} }
