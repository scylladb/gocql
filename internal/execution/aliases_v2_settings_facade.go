// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	implSettings "github.com/scylladb/gocql/v2/internal/settings"
)

type Config = implSettings.Config

type Observer = implSettings.Observer

type Profile = implSettings.Profile
type ProfileHandle = implSettings.ProfileHandle

type RequestEvent = implSettings.RequestEvent
type RetryPolicy = implSettings.RetryPolicy

type UnsupportedSettingError = implSettings.UnsupportedSettingError

func defaultProfile() Profile { return implSettings.InternalDefaultProfile() }

func validConsistency(a0 Consistency) bool { return implSettings.InternalValidConsistency(a0) }
