// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

var ErrKeyspaceDoesNotExist = runtimeerrors.ErrKeyspaceDoesNotExist

var cqlNativeTypes = InternalCqlNativeTypes

type metadataTable = model.TableKey
