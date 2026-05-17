//go:build unit
// +build unit

package gocql

import (
	"encoding/binary"
	"testing"

	"github.com/gocql/gocql/tablets"
)

// buildTabletHintPayload constructs a synthetic tablets-routing-v1 payload
// in the CQL wire format: tuple<bigint, bigint, list<tuple<uuid, int>>>.
func buildTabletHintPayload(firstToken, lastToken int64, numReplicas int) []byte {
	// Pre-calculate size.
	replicaTupleSize := 4 + 16 + 4 + 4 // len+uuid + len+int32
	listBodySize := 4 + numReplicas*(4+replicaTupleSize)
	totalSize := (4 + 8) + (4 + 8) + (4 + listBodySize)

	buf := make([]byte, totalSize)
	off := 0

	// first_token
	binary.BigEndian.PutUint32(buf[off:], 8)
	off += 4
	binary.BigEndian.PutUint64(buf[off:], uint64(firstToken))
	off += 8

	// last_token
	binary.BigEndian.PutUint32(buf[off:], 8)
	off += 4
	binary.BigEndian.PutUint64(buf[off:], uint64(lastToken))
	off += 8

	// list header
	binary.BigEndian.PutUint32(buf[off:], uint32(listBodySize))
	off += 4
	binary.BigEndian.PutUint32(buf[off:], uint32(numReplicas))
	off += 4

	for i := 0; i < numReplicas; i++ {
		// inner tuple length
		binary.BigEndian.PutUint32(buf[off:], uint32(replicaTupleSize))
		off += 4
		// uuid length + uuid
		binary.BigEndian.PutUint32(buf[off:], 16)
		off += 4
		buf[off+15] = byte(i + 1) // unique UUID per replica
		off += 16
		// shard length + shard
		binary.BigEndian.PutUint32(buf[off:], 4)
		off += 4
		binary.BigEndian.PutUint32(buf[off:], uint32(i))
		off += 4
	}

	return buf
}

// unmarshalTabletHintReflect is the original reflection-based parser, kept for benchmarking.
func unmarshalTabletHintReflect(hint []byte, v uint8, keyspace, table string) (tablets.TabletInfo, error) {
	tabletBuilder := tablets.NewTabletInfoBuilder()
	err := Unmarshal(TupleTypeInfo{
		NativeType: NativeType{proto: v, typ: TypeTuple},
		Elems: []TypeInfo{
			NativeType{typ: TypeBigInt},
			NativeType{typ: TypeBigInt},
			CollectionType{
				NativeType: NativeType{proto: v, typ: TypeList},
				Elem: TupleTypeInfo{
					NativeType: NativeType{proto: v, typ: TypeTuple},
					Elems: []TypeInfo{
						NativeType{proto: v, typ: TypeUUID},
						NativeType{proto: v, typ: TypeInt},
					}},
			},
		},
	}, hint, []any{&tabletBuilder.FirstToken, &tabletBuilder.LastToken, &tabletBuilder.Replicas})
	if err != nil {
		return tablets.TabletInfo{}, err
	}
	tabletBuilder.KeyspaceName = keyspace
	tabletBuilder.TableName = table
	return tabletBuilder.Build()
}

func BenchmarkUnmarshalTabletHint(b *testing.B) {
	payload := buildTabletHintPayload(-1000, 1000, 3)

	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := parseTabletHintDirect(payload, "ks", "tbl")
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Reflect", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := unmarshalTabletHintReflect(payload, 4, "ks", "tbl")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestParseTabletHintDirectMatchesReflect(t *testing.T) {
	payload := buildTabletHintPayload(-9876543210, 1234567890, 3)

	direct, err := parseTabletHintDirect(payload, "ks", "tbl")
	if err != nil {
		t.Fatal(err)
	}

	reflected, err := unmarshalTabletHintReflect(payload, 4, "ks", "tbl")
	if err != nil {
		t.Fatal(err)
	}

	if direct.KeyspaceName() != reflected.KeyspaceName() {
		t.Fatalf("keyspace mismatch: %s vs %s", direct.KeyspaceName(), reflected.KeyspaceName())
	}
	if direct.TableName() != reflected.TableName() {
		t.Fatalf("table mismatch: %s vs %s", direct.TableName(), reflected.TableName())
	}
	if direct.FirstToken() != reflected.FirstToken() {
		t.Fatalf("firstToken mismatch: %d vs %d", direct.FirstToken(), reflected.FirstToken())
	}
	if direct.LastToken() != reflected.LastToken() {
		t.Fatalf("lastToken mismatch: %d vs %d", direct.LastToken(), reflected.LastToken())
	}

	dr := direct.Replicas()
	rr := reflected.Replicas()
	if len(dr) != len(rr) {
		t.Fatalf("replica count mismatch: %d vs %d", len(dr), len(rr))
	}
	for i := range dr {
		if dr[i].HostID() != rr[i].HostID() || dr[i].ShardID() != rr[i].ShardID() {
			t.Fatalf("replica %d mismatch: %v vs %v", i, dr[i], rr[i])
		}
	}
}
