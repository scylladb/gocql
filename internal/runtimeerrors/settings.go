// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

type UnsupportedSettingError struct{ Setting, Reason string }

func (e *UnsupportedSettingError) Error() string {
	return "gocql: unsupported " + e.Setting + ": " + e.Reason
}
