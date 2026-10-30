// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metadata

type AggregateMetadataStorage struct {
	InternalStateFunc string
	InternalFinalFunc string
}

func AggregateMetadataData(v *AggregateMetadata) *AggregateMetadataStorage { return &v.privateStorage }
