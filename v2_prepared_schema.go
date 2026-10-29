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

package gocql

import (
	"slices"
	"sync"
	"sync/atomic"
)

// Prepared epochs are dependency-local clocks. Snapshot publication numbers and
// opaque server metadata IDs are deliberately not schema clocks.
type preparedDependencyKey struct {
	kind           byte
	keyspace, name string
}
type preparedSchemaTracker struct {
	mu       sync.RWMutex
	sequence atomic.Uint64
	epochs   map[preparedDependencyKey]*atomic.Uint64
}
type preparedEpochRead struct {
	clock *atomic.Uint64
	value uint64
}
type preparedSchemaStamp []preparedEpochRead

func (s preparedSchemaStamp) current() bool {
	for _, e := range s {
		if e.clock.Load() != e.value {
			return false
		}
	}
	return true
}
func (s preparedSchemaStamp) equal(other preparedSchemaStamp) bool {
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

type preparedDependencies struct {
	mu      sync.RWMutex
	tracker *preparedSchemaTracker
	clocks  []*atomic.Uint64
}

func (d *preparedDependencies) snapshot() preparedSchemaStamp {
	if d == nil {
		return nil
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := make(preparedSchemaStamp, len(d.clocks))
	for i, c := range d.clocks {
		s[i] = preparedEpochRead{c, c.Load()}
	}
	return s
}
func (rt *sessionRuntime) schemaTracker() *preparedSchemaTracker {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.schema == nil {
		rt.schema = &preparedSchemaTracker{epochs: make(map[preparedDependencyKey]*atomic.Uint64)}
	}
	return rt.schema
}
func (t *preparedSchemaTracker) start() uint64 { return t.sequence.Load() }
func (t *preparedSchemaTracker) bump(keys ...preparedDependencyKey) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sequence := t.sequence.Add(1)
	for _, k := range keys {
		c := t.epochs[k]
		if c == nil {
			c = new(atomic.Uint64)
			t.epochs[k] = c
		}
		c.Store(sequence)
	}
}
func (t *preparedSchemaTracker) dependencies(keyspace string, parameters, columns Columns) *preparedDependencies {
	keys := []preparedDependencyKey{{kind: 'p'}}
	seen := make(map[preparedDependencyKey]bool)
	add := func(k preparedDependencyKey) {
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	knownTable := false
	visited := make(map[*dataTypeDescriptor]bool)
	var typ func(DataType)
	typ = func(v DataType) {
		d := v.descriptor
		if d == nil || visited[d] {
			return
		}
		visited[d] = true
		if d.kind == TypeUDT {
			add(preparedDependencyKey{'k', d.keyspace, ""})
			add(preparedDependencyKey{'t', d.keyspace, d.name})
		}
		typ(d.key)
		typ(d.element)
		for _, f := range d.fields.columns {
			typ(f.Type)
		}
	}
	for _, set := range []Columns{parameters, columns} {
		for _, c := range set.columns {
			ks := c.Keyspace
			if ks == "" {
				ks = keyspace
			}
			if c.Table != "" {
				knownTable = true
				add(preparedDependencyKey{'b', ks, c.Table})
				add(preparedDependencyKey{'k', ks, ""})
				add(preparedDependencyKey{'f', ks, ""})
			}
			typ(c.Type)
		}
	}
	if !knownTable {
		add(preparedDependencyKey{'a', keyspace, ""})
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	d := &preparedDependencies{tracker: t, clocks: make([]*atomic.Uint64, len(keys))}
	for i, k := range keys {
		c := t.epochs[k]
		if c == nil {
			c = new(atomic.Uint64)
			t.epochs[k] = c
		}
		d.clocks[i] = c
	}
	return d
}
func (d *preparedDependencies) since(start uint64) preparedSchemaStamp {
	stamp := d.snapshot()
	for i := range stamp {
		if stamp[i].value > start {
			stamp[i].value = start
		}
	}
	return stamp
}
func (s *Session) invalidatePreparedSchema(e Event) {
	t := s.typedRuntime().schemaTracker()
	keys := []preparedDependencyKey{{'a', "", ""}, {'a', e.Keyspace, ""}}
	switch e.Target {
	case "TABLE", "VIEW":
		keys = append(keys, preparedDependencyKey{'b', e.Keyspace, e.Object})
	case "TYPE":
		keys = append(keys, preparedDependencyKey{'t', e.Keyspace, e.Object})
	case "FUNCTION", "AGGREGATE":
		keys = append(keys, preparedDependencyKey{'f', e.Keyspace, ""})
	default:
		keys = append(keys, preparedDependencyKey{'k', e.Keyspace, ""})
	}
	t.bump(keys...)
}
func (s *Session) updatePreparedSchemaSnapshot(old, next *MetadataSnapshot) {
	if old == nil {
		return
	}
	t := s.typedRuntime().schemaTracker()
	if old.partitioner != next.partitioner {
		t.bump(preparedDependencyKey{kind: 'p'})
	}
	names := make(map[string]bool, len(old.keyspaces)+len(next.keyspaces))
	for n := range old.keyspaces {
		names[n] = true
	}
	for n := range next.keyspaces {
		names[n] = true
	}
	for n := range names {
		a, b := old.keyspaces[n], next.keyspaces[n]
		if a == b {
			continue
		}
		if a == nil || b == nil {
			if a != b {
				s.invalidatePreparedSchema(Event{Keyspace: n, Target: "KEYSPACE"})
			}
			continue
		}
		tables := make(map[string]bool, len(a.Tables)+len(b.Tables))
		for n := range a.Tables {
			tables[n] = true
		}
		for n := range b.Tables {
			tables[n] = true
		}
		for name := range tables {
			if !a.Tables[name].Equals(b.Tables[name]) {
				s.invalidatePreparedSchema(Event{Keyspace: n, Object: name, Target: "TABLE"})
			}
		}
		types := make(map[string]bool, len(a.Types)+len(b.Types))
		for n := range a.Types {
			types[n] = true
		}
		for n := range b.Types {
			types[n] = true
		}
		for name := range types {
			x, y := a.Types[name], b.Types[name]
			same := x == y || x != nil && y != nil && x.Keyspace == y.Keyspace && x.Name == y.Name && slices.Equal(x.FieldNames, y.FieldNames) && slices.Equal(x.FieldTypes, y.FieldTypes)
			if !same {
				s.invalidatePreparedSchema(Event{Keyspace: n, Object: name, Target: "TYPE"})
			}
		}
		// Function dependencies are not exposed by PREPARED, so use their keyspace
		// scope conservatively; unrelated table/type publications stay isolated.
		if !equalPreparedFunctions(a.Functions, b.Functions) || !equalPreparedAggregates(a.Aggregates, b.Aggregates) {
			s.invalidatePreparedSchema(Event{Keyspace: n, Target: "FUNCTION"})
		}
	}
}
func equalPreparedFunction(a, b FunctionMetadata) bool {
	return a.Keyspace == b.Keyspace && a.Name == b.Name && a.Body == b.Body && a.Language == b.Language && a.ReturnType == b.ReturnType && a.CalledOnNullInput == b.CalledOnNullInput && slices.Equal(a.ArgumentTypes, b.ArgumentTypes) && slices.Equal(a.ArgumentNames, b.ArgumentNames)
}
func equalPreparedFunctions(a, b map[string]*FunctionMetadata) bool {
	if len(a) != len(b) {
		return false
	}
	for k, x := range a {
		y := b[k]
		if x == nil || y == nil {
			if x != y {
				return false
			}
			continue
		}
		if !equalPreparedFunction(*x, *y) {
			return false
		}
	}
	return true
}
func equalPreparedAggregates(a, b map[string]*AggregateMetadata) bool {
	if len(a) != len(b) {
		return false
	}
	for k, x := range a {
		y := b[k]
		if x == nil || y == nil {
			if x != y {
				return false
			}
			continue
		}
		if x.Keyspace != y.Keyspace || x.Name != y.Name || x.InitCond != y.InitCond || x.ReturnType != y.ReturnType || x.StateType != y.StateType || x.stateFunc != y.stateFunc || x.finalFunc != y.finalFunc || !slices.Equal(x.ArgumentTypes, y.ArgumentTypes) || !equalPreparedFunction(x.FinalFunc, y.FinalFunc) || !equalPreparedFunction(x.StateFunc, y.StateFunc) {
			return false
		}
	}
	return true
}
func (s *Session) preparedPartitionerName(keyspace, table string) string {
	m := s.Metadata()
	name := m.partitioner
	if ks := m.keyspaces[keyspace]; ks != nil {
		if t := ks.Tables[table]; t != nil && t.Options.Partitioner != "" {
			name = t.Options.Partitioner
		}
	}
	return name
}

// extend registers newly observed result UDTs without invalidating prior pages.
func (d *preparedDependencies) extend(keyspace string, parameters, columns Columns) {
	other := d.tracker.dependencies(keyspace, parameters, columns)
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
func (s *preparedState) cacheCurrent() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.initialEpoch.current()
}
func (rt *sessionRuntime) beginPreparedWorker(s *Session) bool {
	rt.prepareWorkersMu.Lock()
	defer rt.prepareWorkersMu.Unlock()
	if rt.prepareWorkersClosed || s.Closed() {
		return false
	}
	rt.prepareWorkers.Add(1)
	return true
}
func (s *Session) stopPreparedWorkers() *sessionRuntime {
	s.typedMu.Lock()
	rt := s.typedState
	s.typedMu.Unlock()
	if rt != nil {
		rt.prepareWorkersMu.Lock()
		rt.prepareWorkersClosed = true
		rt.prepareWorkersMu.Unlock()
	}
	return rt
}

// Remote schema observations have no total order. Coalesce the need for an
// authoritative typed refresh instead of picking a winner by opaque server ID.
func (s *preparedState) noteMetadataConflict() error {
	if s.session == nil {
		return nil
	}
	if s.session.Closed() {
		return ErrClosed
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	return s.queueMetadataConflict()
}
func (s *preparedState) queueMetadataConflict() error {
	p := s.session.peripheral()
	s.conflictDirty.Store(true)
	p.mu.Lock()
	epoch := p.schemaReconciled.Load() + 1
	_, pending := p.preparedRefreshScopes[s.info.Keyspace]
	p.mu.Unlock()
	if s.conflictEpoch.Load() == epoch && pending {
		return nil
	}
	if err := s.session.scheduleV2PreparedRefresh(s.info.Keyspace); err != nil {
		s.conflictEpoch.Store(0)
		return err
	}
	s.conflictEpoch.Store(epoch)
	s.conflictRefresh.Store(true)
	target := "TABLE"
	if s.info.Table == "" {
		target = "KEYSPACE"
	}
	s.session.invalidatePreparedSchema(Event{Keyspace: s.info.Keyspace, Object: s.info.Table, Target: target})
	s.session.markV2SchemaStale(s.info.Keyspace, false)
	return nil
}
func (s *preparedState) reconcilePendingConflict() error {
	if !s.conflictDirty.Load() || s.session == nil {
		return nil
	}
	s.conflictMu.Lock()
	defer s.conflictMu.Unlock()
	p := s.session.peripheral()
	p.mu.Lock()
	_, pending := p.preparedRefreshScopes[s.info.Keyspace]
	p.mu.Unlock()
	if pending {
		return nil
	}
	if s.conflictEpoch.Load() != 0 {
		s.conflictDirty.Store(false)
		return nil
	}
	return s.queueMetadataConflict()
}
