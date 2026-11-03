// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

type BoundStorage[R any] struct {
	InternalRequest InternalExecutionRequest
	InternalRows    RowDecoder[R]
	plans           *InternalResultPlanCache[R]
	results         *queryResultStorage[R]
}

func BoundData[R any](v *Bound[R]) *BoundStorage[R] { return &v.privateStorage }
