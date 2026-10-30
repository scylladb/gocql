// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package settings

import (
	"sync/atomic"
)

type ProfileHandleStorage struct {
	current atomic.Pointer[Profile]
}

func ProfileHandleData(v *ProfileHandle) *ProfileHandleStorage { return &v.privateStorage }
