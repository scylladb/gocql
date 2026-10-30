// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

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

// Native protocol v5 ("modern framing") transport segments. The wire codec lives
// in internal/segment, which the record/replay dialers share; what remains here is
// the one thing that cannot move, because it needs this package's Compressor
// interface.

package transport

// resolveSegmentCompressor narrows the negotiated compressor once, into
// c.segCompressor, so the receive path does not repeat the assertion for every
// segment it reads.
//
// It must run after the COMPRESSION negotiation in startupCoordinator.startup — the
// last write to c.compressor, which clears it if the server refused the requested
// algorithm — and before the first segment is exchanged. Those two points are closer
// together than they look: an AUTHENTICATE response sets startupCompleted, so the
// AUTH_RESPONSE exchange that follows is already written and read as v5 segments, well
// before Conn.initFramerCache runs at the end of startupCoordinator.options. Resolving
// alongside the other snapshots taken there would leave an authenticated, compressed
// connection decoding its auth exchange with the uncompressed layout.
//
// Only v5+ resolves. Below it the segment codec is never reached, and a compressor
// without segment support — SnappyCompressor — is a valid configuration that must not
// be refused here.
//
// The error cannot fire on a connection that got this far: clusterSettings.Validate
// rejects a compressor without segment support on v5 before anything is dialed. It is
// reported rather than ignored because the alternative is a connection that silently
// reads the wrong segment layout, and failing the handshake is where a misconfigured
// connection should die — not mid-stream, on a path whose callers can only turn it
// into a per-request error.
func (c *Conn) InternalResolveSegmentCompressor() error {
	if c.InternalVersion <= protoVersion4 {
		return nil
	}

	segComp, err := asSegmentCompressor(c.InternalCompressor)
	if err != nil {
		return err
	}
	c.InternalSegCompressor = segComp

	return nil
}
