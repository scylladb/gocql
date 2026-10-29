package gocql

import (
	"encoding/binary"
	"fmt"

	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/tablets"
)

// parseTypedTabletHint consumes the tuple<bigint,bigint,list<tuple<uuid,int>>>
// custom payload without materializing a potentially fragmented payload.
func parseTypedTabletHint(value Bytes, keyspace, table string) (tablets.TabletInfo, error) {
	if err := value.checkAlive(); err != nil {
		return tablets.TabletInfo{}, err
	}
	p := &typedParser{cursor: wire.NewCursor(value.chain)}
	field := func(p *typedParser) *typedParser {
		length := p.i32()
		if length < 0 {
			p.check(fmt.Errorf("gocql: tablet hint contains null or unset field"))
			return &typedParser{cursor: wire.NewCursor(wire.Chain{}), err: p.err}
		}
		chain := p.take(int64(length))
		return &typedParser{cursor: wire.NewCursor(chain), err: p.err}
	}
	bigint := func(p *typedParser) int64 {
		child := field(p)
		n, err := child.cursor.Uint64()
		child.check(err)
		child.check(child.cursor.RequireEnd())
		p.check(child.err)
		return int64(n)
	}
	first, last := bigint(p), bigint(p)
	list := field(p)
	count := list.count(4)
	if count > 1000 {
		list.check(fmt.Errorf("gocql: unreasonable tablet replica count %d", count))
		count = 0
	}
	replicas := make([]tablets.ReplicaInfo, count)
	for i := range replicas {
		tuple := field(list)
		id := field(tuple)
		hi, err := id.cursor.Uint64()
		id.check(err)
		lo, err := id.cursor.Uint64()
		id.check(err)
		id.check(id.cursor.RequireEnd())
		tuple.check(id.err)
		var uuid [16]byte
		binary.BigEndian.PutUint64(uuid[:8], hi)
		binary.BigEndian.PutUint64(uuid[8:], lo)
		shard := field(tuple)
		shardID := shard.i32()
		shard.check(shard.cursor.RequireEnd())
		tuple.check(shard.err)
		if shardID < 0 {
			tuple.check(fmt.Errorf("gocql: negative tablet shard %d", shardID))
		}
		tuple.check(tuple.cursor.RequireEnd())
		list.check(tuple.err)
		replicas[i] = tablets.NewReplicaInfo(tablets.HostUUID(uuid), shardID)
	}
	list.check(list.cursor.RequireEnd())
	p.check(list.err)
	p.check(p.cursor.RequireEnd())
	if p.err != nil {
		return tablets.TabletInfo{}, p.err
	}
	return tablets.NewTabletInfo(keyspace, table, first, last, replicas)
}
