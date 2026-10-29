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
	"context"
	"errors"
	"fmt"
	"slices"
)

const maxPreparedRefreshScopes = 64

// Register only affected keyspaces. A full queue fails the logical operation
// visibly; it neither drops required work nor enables a broader schema refresh.
func (s *Session) scheduleV2PreparedRefresh(keyspace string) error {
	p := s.peripheral()
	p.mu.Lock()
	if p.closed || s.Closed() {
		p.mu.Unlock()
		return ErrClosed
	}
	if _, exists := p.preparedRefreshScopes[keyspace]; !exists && len(p.preparedRefreshScopes) >= maxPreparedRefreshScopes {
		p.mu.Unlock()
		return &ProtocolError{Stage: "prepared schema reconciliation", Err: fmt.Errorf("pending keyspace limit %d exceeded", maxPreparedRefreshScopes)}
	}
	if p.preparedRefreshScopes == nil {
		p.preparedRefreshScopes = make(map[string]uint64)
	}
	p.preparedRefreshVersion++
	p.preparedRefreshScopes[keyspace] = p.preparedRefreshVersion
	p.mu.Unlock()
	s.startV2SchemaRetry()
	return nil
}

// Required reconciliation validates actual typed schema independently of
// optional background metadata publication. Known scopes never enumerate peers'
// unrelated keyspaces; an unknown statement scope alone requires enumeration.
func (s *Session) reconcileV2PreparedScope(ctx context.Context, keyspace string) error {
	if keyspace == "" {
		rows, err := queryV2SchemaRows(ctx, s, `SELECT keyspace_name, durable_writes, replication FROM system_schema.keyspaces`, nil, v2DecodeKeyspace())
		if err != nil {
			return err
		}
		for _, row := range rows {
			if row.name == "" {
				return typeValidation("keyspace schema", "missing keyspace identity in required reconciliation")
			}
			if err := s.reconcileV2PreparedScope(ctx, row.name); err != nil {
				return err
			}
		}
		return nil
	}
	p := s.peripheral()
	p.mu.Lock()
	epoch := p.schemaEpoch[keyspace]
	config := p.config
	p.mu.Unlock()
	base, err := getV2KeyspaceMetadata(ctx, s, keyspace)
	absent := errors.Is(err, ErrKeyspaceDoesNotExist)
	if err != nil && !absent {
		return err
	}
	var schema, minimal *KeyspaceMetadata
	if !absent {
		copyBase := *base
		minimal = &copyBase
		schema, err = fetchV2Keyspace(ctx, s, base)
		if err != nil {
			return err
		}
	}
	publish := config.Metadata.Mode != MetadataDisabled && (len(config.Metadata.Keyspaces) == 0 || slices.Contains(config.Metadata.Keyspaces, keyspace))
	p.mu.Lock()
	if p.schemaEpoch[keyspace] != epoch {
		p.mu.Unlock()
		return &PartialRefreshError{Keyspaces: map[string]error{keyspace: errors.New("schema changed during prepared reconciliation")}}
	}
	if publish && s.metadataDescriber != nil {
		if absent {
			s.metadataDescriber.metadata.keyspaceMetadata.removeKeyspace(keyspace)
		} else {
			if config.Metadata.Mode == MetadataMinimal {
				schema = minimal
			}
			s.metadataDescriber.metadata.keyspaceMetadata.set(keyspace, schema)
		}
	}
	delete(p.schemaStale, keyspace)
	p.mu.Unlock()
	if absent {
		s.invalidatePreparedSchema(Event{Keyspace: keyspace, Target: "KEYSPACE"})
	}
	if publish {
		s.publishV2Metadata()
	}
	return nil
}
