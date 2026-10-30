// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

type HostInfo = implEndpoint.HostInfo

type UnsupportedSettingError = runtimeerrors.UnsupportedSettingError
