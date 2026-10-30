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

package cluster

import implMetadata "github.com/scylladb/gocql/v2/internal/metadata"

import (
	routing "github.com/scylladb/gocql/v2/internal/routing"

	"slices"
)

func InternalSessionInvalidatePreparedSchema(s *Session, e Event) {
	t := InternalSessionSchemaTracker(s)
	keys := []preparedDependencyKey{{InternalKind: 'a', InternalKeyspace: "", InternalName: ""}, {InternalKind: 'a', InternalKeyspace: e.Keyspace, InternalName: ""}}
	switch e.Target {
	case "TABLE", "VIEW":
		keys = append(keys, preparedDependencyKey{InternalKind: 'b', InternalKeyspace: e.Keyspace, InternalName: e.Object})
	case "TYPE":
		keys = append(keys, preparedDependencyKey{InternalKind: 't', InternalKeyspace: e.Keyspace, InternalName: e.Object})
	case "FUNCTION", "AGGREGATE":
		keys = append(keys, preparedDependencyKey{InternalKind: 'f', InternalKeyspace: e.Keyspace, InternalName: ""})
	default:
		keys = append(keys, preparedDependencyKey{InternalKind: 'k', InternalKeyspace: e.Keyspace, InternalName: ""})
	}
	t.InternalBump(keys...)
}
func InternalSessionUpdatePreparedSchemaSnapshot(s *Session, old, next *MetadataSnapshot) {
	if old == nil {
		return
	}
	t := InternalSessionSchemaTracker(s)
	if routing.SnapshotData(old).InternalPartitioner != routing.SnapshotData(next).InternalPartitioner {
		t.InternalBump(preparedDependencyKey{InternalKind: 'p'})
	}
	names := make(map[string]bool, len(routing.SnapshotData(old).InternalKeyspaces)+len(routing.SnapshotData(next).InternalKeyspaces))
	for n := range routing.SnapshotData(old).InternalKeyspaces {
		names[n] = true
	}
	for n := range routing.SnapshotData(next).InternalKeyspaces {
		names[n] = true
	}
	for n := range names {
		a, b := routing.SnapshotData(old).InternalKeyspaces[n], routing.SnapshotData(next).InternalKeyspaces[n]
		if a == b {
			continue
		}
		if a == nil || b == nil {
			if a != b {
				InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: n, Target: "KEYSPACE"})
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
				InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: n, Object: name, Target: "TABLE"})
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
				InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: n, Object: name, Target: "TYPE"})
			}
		}
		// Function dependencies are not exposed by PREPARED, so use their keyspace
		// scope conservatively; unrelated table/type publications stay isolated.
		if !InternalEqualPreparedFunctions(a.Functions, b.Functions) || !InternalEqualPreparedAggregates(a.Aggregates, b.Aggregates) {
			InternalSessionInvalidatePreparedSchema(s, Event{Keyspace: n, Target: "FUNCTION"})
		}
	}
}
func InternalEqualPreparedFunction(a, b FunctionMetadata) bool {
	return a.Keyspace == b.Keyspace && a.Name == b.Name && a.Body == b.Body && a.Language == b.Language && a.ReturnType == b.ReturnType && a.CalledOnNullInput == b.CalledOnNullInput && slices.Equal(a.ArgumentTypes, b.ArgumentTypes) && slices.Equal(a.ArgumentNames, b.ArgumentNames)
}
func InternalEqualPreparedFunctions(a, b map[string]*FunctionMetadata) bool {
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
		if !InternalEqualPreparedFunction(*x, *y) {
			return false
		}
	}
	return true
}
func InternalEqualPreparedAggregates(a, b map[string]*AggregateMetadata) bool {
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
		if x.Keyspace != y.Keyspace || x.Name != y.Name || x.InitCond != y.InitCond || x.ReturnType != y.ReturnType || x.StateType != y.StateType || implMetadata.AggregateMetadataData(x).InternalStateFunc != implMetadata.AggregateMetadataData(y).InternalStateFunc || implMetadata.AggregateMetadataData(x).InternalFinalFunc != implMetadata.AggregateMetadataData(y).InternalFinalFunc || !slices.Equal(x.ArgumentTypes, y.ArgumentTypes) || !InternalEqualPreparedFunction(x.FinalFunc, y.FinalFunc) || !InternalEqualPreparedFunction(x.StateFunc, y.StateFunc) {
			return false
		}
	}
	return true
}
func InternalSessionPreparedPartitionerName(s *Session, keyspace, table string) string {
	m := s.Metadata()
	if table == "" {
		return ""
	}
	ks := routing.SnapshotData(m).InternalKeyspaces[keyspace]
	if ks == nil {
		return ""
	}
	name := routing.SnapshotData(m).InternalPartitioner
	if t := ks.Tables[table]; t != nil {
		if t.Options.Partitioner != "" {
			name = t.Options.Partitioner
		}
		return name
	}
	if v := ks.Views[table]; v != nil {
		if v.Options.Partitioner != "" {
			name = v.Options.Partitioner
		}
		return name
	}
	// Metadata-disabled/filtered/minimal scopes cannot attest a table's hasher.
	return ""
}
