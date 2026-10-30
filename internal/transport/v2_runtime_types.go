// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	model "github.com/scylladb/gocql/v2/internal/model"
)

type InternalResponsePage struct {
	InternalResponseData
	InternalMetadata *model.ResultSchemaIdentity
}
