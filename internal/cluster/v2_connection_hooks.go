// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
)

type InternalSessionConnectionHooks struct{ session *Session }

func InternalNewConnectionHooks(s *Session) implTransport.InternalConnectionHooks {
	return InternalSessionConnectionHooks{s}
}
func (h InternalSessionConnectionHooks) InternalStartConnectionWorker(f func()) bool {
	return InternalSessionPeripheral(h.session).InternalStartWorker(f)
}
func (h InternalSessionConnectionHooks) InternalConnectionWorkersDone() <-chan struct{} {
	return InternalSessionPeripheral(h.session).InternalDone
}
func (h InternalSessionConnectionHooks) InternalHandleConnectionEvent(ctx context.Context, f frame, release func()) bool {
	return h.session.handleEventOwnedContext(ctx, f, release)
}
func InternalConnectionOwnedBy(c *Conn, s *Session) bool {
	h, ok := c.InternalHooks.(InternalSessionConnectionHooks)
	return ok && h.session == s
}
