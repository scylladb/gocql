// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

// Package transport provides real connection peers and focused transport faults.
package transport

import (
	"context"
	"github.com/scylladb/gocql/v2/internal/wire"
	"net"
	"testing"

	"github.com/scylladb/gocql/v2/internal/logging"
	"github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	runtime "github.com/scylladb/gocql/v2/internal/transport"
)

// NewPair creates the actual multiplexed transport over a private socket pair.
// The caller drives the protocol peer, including short reads and failures.
func NewPair(t testing.TB, proto byte, compressor protocol.Compressor) (*runtime.Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	c := &runtime.Conn{InternalVersion: proto, InternalCtx: ctx, InternalCancel: cancel, InternalR: &runtime.InternalConnReader{InternalConn: client}, InternalCalls: make(map[int]*runtime.InternalCallReq), InternalStreams: streams.New(), InternalLogger: logging.InternalNopLogger{}, InternalCompressor: compressor, InternalErrorHandler: runtime.InternalConnErrorHandlerFn(func(*runtime.Conn, error, bool) {})}
	c.InternalW = &runtime.InternalDeadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
	if proto == 5 && compressor != nil {
		c.InternalSegCompressor = compressor.(segment.Compressor)
	}
	if c.InternalCfg == nil {
		c.InternalCfg = new(runtime.ConnConfig)
	}
	c.InternalCfg.InternalReceiveCache = new(wire.ReceiveCache)
	t.Cleanup(c.InternalCfg.InternalReceiveCache.Close)
	c.InternalInitFramerCache()
	t.Cleanup(func() { c.Close(); server.Close() })
	return c, server
}

// Fail closes the real connection through its normal poison/retirement path.
func Fail(c *runtime.Conn, err error) { c.InternalCloseWithError(err) }
