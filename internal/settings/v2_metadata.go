// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"time"
)

type LifecycleObserver interface{ ObserveLifecycle(LifecycleEvent) }
type LifecycleEvent struct {
	Kind, Stage string
	HostID      UUID
	Stream      int16
	At          time.Time
	Err         error
}
