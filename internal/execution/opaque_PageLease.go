// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"github.com/scylladb/gocql/v2/internal/wire"
	"sync"
)

type PageLeaseStorage struct {
	once  sync.Once
	owner *wire.Lease
}

func PageLeaseData(v *PageLease) *PageLeaseStorage { return &v.privateStorage }
