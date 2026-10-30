// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package runtimeerrors

import "errors"

var ErrNoControl = errors.New("gocql: no control connection available")
