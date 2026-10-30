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

package protocol

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"io"
	"sync"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// The budget is shared by all live metadata/control objects of a reserved
// connection. Replacing an object does not reset accounting before release.
type StreamMetadataBudget struct {
	mu          sync.Mutex
	Limit, live int64
}

func (b *StreamMetadataBudget) Reserve(n int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n < 0 || n > b.Limit-b.live {
		return &ProtocolError{Stage: "stream metadata budget", Err: wire.ErrLimit}
	}
	b.live += n
	return nil
}

type StreamMetadataCursor struct {
	Scope    *wire.Stream
	Budget   *StreamMetadataBudget
	Used     int64
	released sync.Once
}

func (b *StreamMetadataBudget) Release(n int64) {
	b.mu.Lock()
	b.live -= n
	b.mu.Unlock()
}
func (b *StreamMetadataBudget) Bytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.live
}
func (b *StreamMetadataBudget) SetLimit(limit int64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.live > limit {
		return wire.ErrLimit
	}
	b.Limit = limit
	return nil
}
func (c *StreamMetadataCursor) Release() {
	c.released.Do(func() { c.Budget.Release(c.Used) })
}
func (c *StreamMetadataCursor) reserve(n int64) error {
	if err := c.Budget.Reserve(n); err != nil {
		return err
	}
	c.Used += n
	return nil
}
func (c *StreamMetadataCursor) copy(n int64) ([]byte, error) {
	if n < 0 || n > c.Scope.Remaining() {
		return nil, io.ErrUnexpectedEOF
	}
	if err := c.reserve(n); err != nil {
		return nil, err
	}
	b := make([]byte, int(n))
	if err := c.Scope.ReadFull(b); err != nil {
		return nil, err
	}
	return b, nil
}
func (c *StreamMetadataCursor) Take(n int64) (wire.Chain, error) {
	b, err := c.copy(n)
	if err != nil {
		return wire.Chain{}, err
	}
	return wire.NewChain(wire.Borrow(b))
}
func (c *StreamMetadataCursor) Uint8() (uint8, error)   { return c.Scope.Uint8() }
func (c *StreamMetadataCursor) Uint16() (uint16, error) { return c.Scope.Uint16() }
func (c *StreamMetadataCursor) Uint32() (uint32, error) { return c.Scope.Uint32() }
func (c *StreamMetadataCursor) Uint64() (uint64, error) { return c.Scope.Uint64() }
func (c *StreamMetadataCursor) Remaining() int64        { return c.Scope.Remaining() }
func (c *StreamMetadataCursor) RequireEnd() error       { return c.Scope.RequireEnd() }

func (c *StreamMetadataCursor) CopyMetadata(n int64) ([]byte, error) { return c.copy(n) }
func (c *StreamMetadataCursor) ReserveMetadata(n int64) error        { return c.reserve(n) }
