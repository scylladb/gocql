// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import implTransport "github.com/scylladb/gocql/v2/internal/transport"

// Conn exposes connection I/O without exposing the transport's runtime state.
type Conn struct{ core *implTransport.Conn }

func (c *Conn) Read(p []byte) (int, error)        { return c.core.Read(p) }
func (c *Conn) Write(p []byte) (int, error)       { return c.core.Write(p) }
func (c *Conn) Close()                            { c.core.Close() }
func (c *Conn) Closed() bool                      { return c.core.Closed() }
func (c *Conn) Address() string                   { return c.core.Address() }
func (c *Conn) AvailableStreams() int             { return c.core.AvailableStreams() }
func (c *Conn) UseKeyspace(keyspace string) error { return c.core.UseKeyspace(keyspace) }
