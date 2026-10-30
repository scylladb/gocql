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

package schemafence

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"sync"
	"sync/atomic"
)

// Prepared epochs are dependency-local clocks. Snapshot publication numbers and
// opaque server metadata IDs are deliberately not schema clocks.
type InternalPreparedDependencyKey struct {
	InternalKind                   byte
	InternalKeyspace, InternalName string
}
type InternalPreparedSchemaTracker struct {
	InternalMu     sync.RWMutex
	sequence       atomic.Uint64
	InternalEpochs map[InternalPreparedDependencyKey]*atomic.Uint64
}
type InternalPreparedEpochRead struct {
	clock *atomic.Uint64
	value uint64
}
type InternalPreparedSchemaStamp []InternalPreparedEpochRead

func (s InternalPreparedSchemaStamp) InternalCurrent() bool {
	for _, e := range s {
		if e.clock.Load() != e.value {
			return false
		}
	}
	return true
}
func (s InternalPreparedSchemaStamp) InternalEqual(other InternalPreparedSchemaStamp) bool {
	if len(s) != len(other) {
		return false
	}
	for i, e := range s {
		if e != other[i] {
			return false
		}
	}
	return true
}

type InternalPreparedDependencies struct {
	mu              sync.RWMutex
	InternalTracker *InternalPreparedSchemaTracker
	clocks          []*atomic.Uint64
}

func (d *InternalPreparedDependencies) InternalSnapshot() InternalPreparedSchemaStamp {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := make(InternalPreparedSchemaStamp, len(d.clocks))
	for i, c := range d.clocks {
		s[i] = InternalPreparedEpochRead{c, c.Load()}
	}
	return s
}

func (t *InternalPreparedSchemaTracker) InternalStart() uint64 { return t.sequence.Load() }
func (t *InternalPreparedSchemaTracker) InternalBump(keys ...InternalPreparedDependencyKey) {
	t.InternalMu.Lock()
	defer t.InternalMu.Unlock()
	sequence := t.sequence.Add(1)
	for _, k := range keys {
		c := t.InternalEpochs[k]
		if c == nil {
			c = new(atomic.Uint64)
			t.InternalEpochs[k] = c
		}
		c.Store(sequence)
	}
}
func (t *InternalPreparedSchemaTracker) InternalDependencies(keyspace string, parameters, columns cqltype.Columns) *InternalPreparedDependencies {
	keys := []InternalPreparedDependencyKey{{InternalKind: 'p'}}
	seen := make(map[InternalPreparedDependencyKey]bool)
	add := func(k InternalPreparedDependencyKey) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	knownTable := false
	visited := make(map[*cqltype.DataTypeDescriptor]bool)
	var typ func(cqltype.DataType)
	typ = func(v cqltype.DataType) {
		d := cqltype.DescriptorOf(v)
		if d == nil || visited[d] {
			return
		}
		visited[d] = true
		if d.Kind == cqltype.TypeUDT {
			add(InternalPreparedDependencyKey{'k', d.Keyspace, ""})
			add(InternalPreparedDependencyKey{'t', d.Keyspace, d.Name})
		}
		typ(d.Key)
		typ(d.Element)
		for _, f := range cqltype.ColumnSlice(d.Fields) {
			typ(f.Type)
		}
	}
	for _, set := range []cqltype.Columns{parameters, columns} {
		for _, c := range cqltype.ColumnSlice(set) {
			ks := c.Keyspace
			if ks == "" {
				ks = keyspace
			}
			if c.Table != "" {
				knownTable = true
				add(InternalPreparedDependencyKey{'b', ks, c.Table})
				add(InternalPreparedDependencyKey{'k', ks, ""})
				add(InternalPreparedDependencyKey{'f', ks, ""})
			}
			typ(c.Type)
		}
	}
	if !knownTable {
		add(InternalPreparedDependencyKey{'a', keyspace, ""})
	}
	t.InternalMu.Lock()
	defer t.InternalMu.Unlock()
	d := &InternalPreparedDependencies{InternalTracker: t, clocks: make([]*atomic.Uint64, len(keys))}
	for i, k := range keys {
		c := t.InternalEpochs[k]
		if c == nil {
			c = new(atomic.Uint64)
			t.InternalEpochs[k] = c
		}
		d.clocks[i] = c
	}
	return d
}
func (d *InternalPreparedDependencies) InternalSince(start uint64) InternalPreparedSchemaStamp {
	stamp := d.InternalSnapshot()
	for i := range stamp {
		if stamp[i].value > start {
			stamp[i].value = start
		}
	}
	return stamp
}

// extend registers newly observed result UDTs without invalidating prior pages.
func (d *InternalPreparedDependencies) InternalExtend(keyspace string, parameters, columns cqltype.Columns) {
	other := d.InternalTracker.InternalDependencies(keyspace, parameters, columns)
	d.mu.Lock()
	defer d.mu.Unlock()
	seen := make(map[*atomic.Uint64]bool, len(d.clocks))
	for _, c := range d.clocks {
		seen[c] = true
	}
	for _, c := range other.clocks {
		if !seen[c] {
			d.clocks = append(d.clocks, c)
			seen[c] = true
		}
	}
}
