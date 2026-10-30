// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import (
	"github.com/scylladb/gocql/v2/tablets"
)

type MetadataStorage struct {
	InternalTabletsMetadata  *tablets.CowTabletList
	InternalKeyspaceMetadata cowKeyspaceMetadataMap
}

func MetadataData(v *Metadata) *MetadataStorage { return &v.privateStorage }
