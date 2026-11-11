//go:build unit
// +build unit

package tablets

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
)

// hintBytes encodes one [bytes] value; nil encodes CQL null.
func hintBytes(v []byte) []byte {
	if v == nil {
		return []byte{0xff, 0xff, 0xff, 0xff}
	}
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(v))), v...)
}

func hintBigInt(v int64) []byte {
	return hintBytes(binary.BigEndian.AppendUint64(nil, uint64(v)))
}

func hintReplica(uuid HostUUID, shard int32) []byte {
	return hintBytes(append(hintBytes(uuid[:]), hintBytes(binary.BigEndian.AppendUint32(nil, uint32(shard)))...))
}

// hintPayload encodes tuple<bigint, bigint, list<tuple<uuid, int>>> as the
// tablets-routing-v1 custom payload carries it.
func hintPayload(first, last int64, replicas ...[]byte) []byte {
	list := binary.BigEndian.AppendUint32(nil, uint32(len(replicas)))
	for _, r := range replicas {
		list = append(list, r...)
	}
	out := append(hintBigInt(first), hintBigInt(last)...)
	return append(out, hintBytes(list)...)
}

var (
	hintHost1 = MustParseHostUUID("00000000-0000-0000-0000-000000000001")
	hintHost2 = MustParseHostUUID("deadbeef-cafe-4bad-8000-0123456789ab")
)

func TestParseHint(t *testing.T) {
	t.Parallel()

	data := hintPayload(-100, 200, hintReplica(hintHost1, 0), hintReplica(hintHost2, 7))
	tablet, err := ParseHint(data, "ks", "tbl")
	if err != nil {
		t.Fatal(err)
	}
	if tablet.KeyspaceName() != "ks" || tablet.TableName() != "tbl" {
		t.Fatalf("target = %s.%s, want ks.tbl", tablet.KeyspaceName(), tablet.TableName())
	}
	if tablet.FirstToken() != -100 || tablet.LastToken() != 200 {
		t.Fatalf("range = [%d, %d], want [-100, 200]", tablet.FirstToken(), tablet.LastToken())
	}
	replicas := tablet.Replicas()
	if len(replicas) != 2 {
		t.Fatalf("got %d replicas, want 2", len(replicas))
	}
	if replicas[0].HostUUIDValue() != hintHost1 || replicas[0].ShardID() != 0 ||
		replicas[1].HostUUIDValue() != hintHost2 || replicas[1].ShardID() != 7 {
		t.Fatalf("replicas = %v, want [%v:0 %v:7]", replicas, hintHost1, hintHost2)
	}
}

func TestParseHintNoReplicas(t *testing.T) {
	t.Parallel()

	tablet, err := ParseHint(hintPayload(1, 1), "ks", "tbl")
	if err != nil {
		t.Fatal(err)
	}
	if len(tablet.Replicas()) != 0 {
		t.Fatalf("replicas = %v, want none", tablet.Replicas())
	}
}

func TestParseHintRejects(t *testing.T) {
	t.Parallel()

	good := hintReplica(hintHost1, 3)
	count := func(n int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(n)) }
	withList := func(list []byte) []byte {
		return append(append(hintBigInt(0), hintBigInt(1)...), hintBytes(list)...)
	}

	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{"Empty", nil, "unexpected end"},
		{"TruncatedTokens", hintBigInt(0)[:6], "unexpected end"},
		{"MissingList", append(hintBigInt(0), hintBigInt(1)...), "unexpected end"},
		{"WrongTokenSize", append(append(hintBytes([]byte{1, 2}), hintBigInt(1)...), hintBytes(count(0))...), "bigint"},
		{"NullList", withList(nil), "unexpected end"},
		{"NegativeCount", withList(count(-1)), "unreasonable replica count"},
		{"HugeCount", withList(count(1001)), "unreasonable replica count"},
		{"CountExceedsReplicas", withList(append(count(2), good...)), "unexpected end"},
		{"NullReplica", withList(append(count(1), hintBytes(nil)...)), "replica 0"},
		{"ShortUUID", withList(append(count(1), hintBytes(append(hintBytes(make([]byte, 15)), hintBytes(count(0))...))...)), "replica 0"},
		{"MissingShard", withList(append(count(1), hintBytes(hintBytes(hintHost1[:]))...)), "replica 0"},
		{"InvertedRange", hintPayload(5, 4, good), "invalid token range"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseHint(tc.data, "ks", "tbl"); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ParseHint error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestParseHostUUID(t *testing.T) {
	t.Parallel()

	const s = "deadbeef-cafe-4bad-8000-0123456789ab"
	u, err := ParseHostUUID(s)
	if err != nil {
		t.Fatal(err)
	}
	want := HostUUID{0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0x4b, 0xad, 0x80, 0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab}
	if u != want {
		t.Fatalf("ParseHostUUID(%q) = %x, want %x", s, u, want)
	}
	if u.String() != s {
		t.Fatalf("String() = %q, want %q", u.String(), s)
	}
	if upper, err := ParseHostUUID(strings.ToUpper(s)); err != nil || upper != want {
		t.Fatalf("upper-case hex: %x, %v", upper, err)
	}
	if !(HostUUID{}).IsEmpty() || u.IsEmpty() {
		t.Fatal("IsEmpty is only true for the zero UUID")
	}
}

func TestParseHostUUIDRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, in, wantErr string
	}{
		{"Empty", "", "invalid UUID length"},
		{"NoDashes", "deadbeefcafe4bad80000123456789ab", "invalid UUID length"},
		{"TooLong", "deadbeef-cafe-4bad-8000-0123456789abc", "invalid UUID length"},
		// Right length, but a dash shifted one position at each boundary.
		{"DashAt9", "deadbeefc-afe-4bad-8000-0123456789ab", "invalid UUID format"},
		{"DashAt12", "deadbeef-caf-e4bad-8000-0123456789ab", "invalid UUID format"},
		{"DashAt19", "deadbeef-cafe-4bad8-000-0123456789ab", "invalid UUID format"},
		{"DashAt22", "deadbeef-cafe-4bad-800-00123456789ab", "invalid UUID format"},
		{"BadHexFirstGroup", "deadbeeg-cafe-4bad-8000-0123456789ab", "invalid UUID hex"},
		{"BadHexLastGroup", "deadbeef-cafe-4bad-8000-0123456789az", "invalid UUID hex"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseHostUUID(tc.in); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ParseHostUUID(%q) error = %v, want %q", tc.in, err, tc.wantErr)
			}
		})
	}

	defer func() {
		if recover() == nil {
			t.Fatal("MustParseHostUUID did not panic on invalid input")
		}
	}()
	MustParseHostUUID("not a uuid")
}

func TestParseHintV2Version(t *testing.T) {
	for _, version := range []uint64{0, 1, 1 << 63, ^uint64(0)} {
		t.Run(fmt.Sprintf("%x", version), func(t *testing.T) {
			data := append(hintPayload(-100, 200, hintReplica(hintHost1, 7)), hintBigInt(int64(version))...)
			tablet, err := ParseHintV2(data, "CaseKS", "CaseTable")
			if err != nil {
				t.Fatal(err)
			}
			if got, known := tablet.Version(); !known || got != version {
				t.Fatalf("version: %x, %v; want %x", got, known, version)
			}
			cache := NewCowTabletList()
			defer cache.Close()
			cache.AddTablet(tablet)
			cache.Flush()
			entry, ok := cache.FindTabletForToken("CaseKS", "CaseTable", 1)
			if !ok {
				t.Fatal("tablet not cached")
			}
			if got, known := entry.Version(); !known || got != version {
				t.Fatalf("entry version: %x, %v", got, known)
			}
			if got, known := cache.Get()[0].Version(); !known || got != version {
				t.Fatalf("snapshot version: %x, %v", got, known)
			}
			cache.AddTablet(mustParseV1(t, hintPayload(-100, 200, hintReplica(hintHost2, 2))))
			cache.Flush()
			entry, _ = cache.FindTabletForToken("CaseKS", "CaseTable", 1)
			if _, known := entry.Version(); known {
				t.Fatal("V1 update kept stale V2 version/leader ordering")
			}
		})
	}
}

func mustParseV1(t *testing.T, data []byte) TabletInfo {
	t.Helper()
	tablet, err := ParseHint(data, "CaseKS", "CaseTable")
	if err != nil {
		t.Fatal(err)
	}
	return tablet
}

func TestParseHintV2RejectsMissingVersion(t *testing.T) {
	base := hintPayload(-100, 200, hintReplica(hintHost1, 7))
	for _, data := range [][]byte{base, append(bytes.Clone(base), hintBigInt(1)[:10]...), append(bytes.Clone(base), hintBytes(nil)...), append(bytes.Clone(base), hintBytes([]byte{1})...)} {
		if _, err := ParseHintV2(data, "ks", "t"); err == nil {
			t.Fatalf("accepted invalid V2 hint: %x", data)
		}
	}
}
