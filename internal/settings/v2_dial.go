// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package settings

import (
	"crypto/tls"

	connectivity "github.com/scylladb/gocql/v2/internal/connectivity"
	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
)

// Parsed keys, callbacks, session caches and provider objects remain shared by
// their documented concurrency contracts. Mutable certificate/trust containers
// and wire bytes are copied before any connection uses them.
func InternalCloneV2TLSConfig(source *tls.Config) *tls.Config {
	return connectivity.CloneTLSConfig(source)
}

type UnsupportedSettingError = runtimeerrors.UnsupportedSettingError

func InternalUnixContact(c Config) (string, error) {
	return connectivity.UnixContact(InternalConnectivityConfig(c))
}
