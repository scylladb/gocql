/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package endpoint

import (
	"context"

	"net"
)

// HostDialer allows customizing connection to cluster nodes.
type HostDialer interface {
	// DialHost establishes a connection to the host.
	// The returned connection must be directly usable for CQL protocol,
	// specifically DialHost is responsible also for setting up the TLS session if needed.
	// DialHost should disable write coalescing if the returned net.Conn does not support writev.
	// As of Go 1.18, only plain TCP connections support writev, TLS sessions should disable coalescing.
	// You can use WrapTLS helper function if you don't need to override the TLS setup.
	DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error)
}

// DialedHost contains information about established connection to a host.
type DialedHost struct {

	// Conn used to communicate with the server.
	Conn net.Conn

	// DisableCoalesce disables write coalescing for the Conn.
	// If true, the effect is the same as if WriteCoalesceWaitTime was configured to 0.
	DisableCoalesce bool
	privateStorage  DialedHostStorage
}
