// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

type PreparedStorage[P, R any] struct {
	InternalKind   StatementKind
	InternalState  *InternalPreparedState
	InternalParams ParamPlan[P]
	InternalRows   RowDecoder[R]
	InternalPlans  *InternalResultPlanCache[R]
}

func PreparedData[P any, R any](v *Prepared[P, R]) *PreparedStorage[P, R] { return &v.privateStorage }
