// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"encoding/binary"
	"fmt"

	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/tablets"
)

// parseTypedTabletHint consumes the tuple<bigint,bigint,list<tuple<uuid,int>>>
// custom payload without materializing a potentially fragmented payload.
func InternalParseTypedTabletHint(value Bytes, keyspace, table string) (tablets.TabletInfo, error) {
	return InternalParseTypedTabletPayload(value, keyspace, table, false)
}

func InternalParseTypedTabletPayload(value Bytes, keyspace, table string, v2 bool) (tablets.TabletInfo, error) {
	if err := cqltype.CheckBytesAlive(value); err != nil {
		return tablets.TabletInfo{}, err
	}
	p := &typedParser{Cursor: wire.NewCursor(cqltype.BytesChain(value))}
	field := func(p *typedParser) *typedParser {
		length := p.I32()
		if length < 0 {
			p.Check(fmt.Errorf("gocql: tablet hint contains null or unset field"))
			return &typedParser{Cursor: wire.NewCursor(wire.Chain{}), Err: p.Err}
		}
		chain := p.Take(int64(length))
		return &typedParser{Cursor: wire.NewCursor(chain), Err: p.Err}
	}
	bigint := func(p *typedParser) int64 {
		child := field(p)
		n, err := child.Cursor.Uint64()
		child.Check(err)
		child.Check(child.Cursor.RequireEnd())
		p.Check(child.Err)
		return int64(n)
	}
	first, last := bigint(p), bigint(p)
	list := field(p)
	count := list.Count(4)
	if count > 1000 {
		list.Check(fmt.Errorf("gocql: unreasonable tablet replica count %d", count))
		count = 0
	}
	replicas := make([]tablets.ReplicaInfo, count)
	for i := range replicas {
		tuple := field(list)
		id := field(tuple)
		hi, err := id.Cursor.Uint64()
		id.Check(err)
		lo, err := id.Cursor.Uint64()
		id.Check(err)
		id.Check(id.Cursor.RequireEnd())
		tuple.Check(id.Err)
		var uuid [16]byte
		binary.BigEndian.PutUint64(uuid[:8], hi)
		binary.BigEndian.PutUint64(uuid[8:], lo)
		shard := field(tuple)
		shardID := shard.I32()
		shard.Check(shard.Cursor.RequireEnd())
		tuple.Check(shard.Err)
		if shardID < 0 {
			tuple.Check(fmt.Errorf("gocql: negative tablet shard %d", shardID))
		}
		tuple.Check(tuple.Cursor.RequireEnd())
		list.Check(tuple.Err)
		replicas[i] = tablets.NewReplicaInfo(tablets.HostUUID(uuid), shardID)
	}
	list.Check(list.Cursor.RequireEnd())
	p.Check(list.Err)
	var version uint64
	if v2 {
		version = uint64(bigint(p))
	}
	p.Check(p.Cursor.RequireEnd())
	if p.Err != nil {
		return tablets.TabletInfo{}, p.Err
	}
	hint, err := tablets.NewTabletInfo(keyspace, table, first, last, replicas)
	if v2 {
		hint = hint.WithVersion(version)
	}
	return hint, err
}
