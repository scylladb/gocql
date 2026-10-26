/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"math/rand/v2"
	"strings"
	"sync/atomic"

	"github.com/gocql/gocql/tablets"
)

func (c *Conn) isTabletsRoutingV2() bool {
	return atomic.LoadInt32(&c.tabletsRoutingV2) == 1
}

func (s *Session) findTabletForToken(keyspace, table string, token int64) (tablets.TabletEntry, bool) {
	if s == nil || s.Closed() || s.metadataDescriber == nil || s.metadataDescriber.metadata == nil {
		return tablets.TabletEntry{}, false
	}
	return s.metadataDescriber.metadata.tabletsMetadata.FindTabletForToken(keyspace, table, token)
}

// sampleTabletVersion encodes one randomly selected nibble of a uint64 hash.
// The high nibble identifies the block, starting at the least significant bits.
func sampleTabletVersion(version uint64) byte {
	block := rand.UintN(16)
	return byte(block<<4) | byte(version>>(block*4))&0xf
}

// tabletVersionBlock uses this attempt's already encoded bound values. It avoids
// preparing on another connection or invoking a Bind callback a second time.
// Missing routing information still needs a byte: random feedback eventually
// elicits a fresh tablet hint even on a cold cache.
func (s *Session) tabletVersionBlock(q *Query, meta *preparedMetadata, values []queryValues, keyspace, table string) byte {
	cold := byte(rand.UintN(256))
	key := q.routingKey
	if key == nil {
		if len(meta.pkeyColumns) == 0 {
			return cold
		}
		var composite [256]byte
		key = composite[:0]
		for _, idx := range meta.pkeyColumns {
			if idx < 0 || idx >= len(values) || values[idx].isUnset || values[idx].value == nil {
				return cold
			}
			value := values[idx].value
			if len(meta.pkeyColumns) == 1 {
				key = value
				break
			}
			if len(value) > 65535 {
				return cold
			}
			key = append(key, byte(len(value)>>8), byte(len(value)))
			key = append(key, value...)
			key = append(key, 0)
		}
	}
	partitioner := q.GetCustomPartitioner()
	if partitioner == nil {
		partitioner = murmur3Partitioner{}
	}
	var token int64
	if p, ok := partitioner.(int64Hasher); ok {
		token = p.hashInt64(key)
	} else if t, ok := partitioner.Hash(key).(int64Token); ok {
		token = int64(t)
	} else {
		return cold
	}
	tablet, ok := s.findTabletForToken(keyspace, table, token)
	if !ok {
		return cold
	}
	version, known := tablet.Version()
	if !known {
		return cold
	}
	return sampleTabletVersion(version)
}

func (s *Session) prefersTabletLeader(q ExecutableQuery) bool {
	// Batch leader awareness requires its own routing-key semantics.
	query, ok := q.(*Query)
	if !ok {
		return false
	}
	md, ok := s.metadataDescriber.metadata.keyspaceMetadata.getKeyspace(q.Keyspace())
	if !ok || md == nil || md.ConsistencyMode != "global" {
		return false
	}
	if q.GetConsistency() == One || q.GetConsistency() == LocalOne {
		return !isTabletRead(query.stmt)
	}
	return true
}

func isTabletRead(stmt string) bool {
	// CQL permits comments before SELECT. An eventual read must retain normal
	// replica routing even when a query starts with comments.
	for {
		stmt = strings.TrimSpace(stmt)
		if strings.HasPrefix(stmt, "--") || strings.HasPrefix(stmt, "//") {
			i := strings.IndexByte(stmt, '\n')
			if i < 0 {
				return false
			}
			stmt = stmt[i+1:]
		} else if strings.HasPrefix(stmt, "/*") {
			i := strings.Index(stmt[2:], "*/")
			if i < 0 {
				return false
			}
			stmt = stmt[i+4:]
		} else {
			break
		}
	}
	return strings.EqualFold(stmtKeyword(stmt), "select")
}

// tabletLeaderAllowed keeps the fallback policy's exclusions authoritative.
// Built-in policies need no query-plan allocation or host scan.
func (t *tokenAwareHostPolicy) tabletLeaderAllowed(q ExecutableQuery, leader *HostInfo) bool {
	if leader == nil {
		return false
	}
	if s := q.GetSession(); s != nil && s.cfg.filterHost(leader) {
		return false
	}
	switch p := t.fallback.(type) {
	case *roundRobinHostPolicy:
		return true
	case *dcAwareRR:
		return !p.disableDCFailover || p.IsLocal(leader)
	case *rackAwareRR:
		return !p.disableDCFailover || leader.DataCenter() == p.localDC
	default:
		// Custom policies can exclude nodes for reasons beyond locality.
		for next := t.fallback.Pick(q); ; {
			host := next()
			if host == nil {
				return false
			}
			if host.Info().HostID() == leader.HostID() {
				return true
			}
		}
	}
}
