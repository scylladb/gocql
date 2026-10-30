// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

type BatchItemStorage struct {
	InternalRequest *InternalExecutionRequest
}

func BatchItemData(v *BatchItem) *BatchItemStorage { return &v.privateStorage }
