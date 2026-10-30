// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

// PageStateOwned transfers immutable token ownership inside the runtime.
func PageStateOwned(token []byte, kind uint8) PageState {
	return PageState{token: token, kind: kind}
}
func PageToken(p PageState) []byte { return p.token }
func PageKind(p PageState) uint8   { return p.kind }
