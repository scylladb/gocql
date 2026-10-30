// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
)

type TracerEnhancedStorage struct {
	session  *Session
	traceIDs [][]byte
	mu       sync.Mutex
}

func TracerEnhancedData(v *TracerEnhanced) *TracerEnhancedStorage { return &v.privateStorage }
