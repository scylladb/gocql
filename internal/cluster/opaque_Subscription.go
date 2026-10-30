// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"sync"
)

type SubscriptionStorage struct {
	mu     sync.Mutex
	events chan Event
	closed bool
	filter EventFilter
	parent *InternalV2PeripheralState
}

func SubscriptionData(v *Subscription) *SubscriptionStorage { return &v.privateStorage }
