//go:build all || unit

package gocql

import (
	"math"
	"testing"
)

func nt(t Type) NativeType { return NativeType{proto: protoVersion4, typ: t} }

func TestCollectionElemWireSize(t *testing.T) {
	fixed := map[Type]int{
		TypeTinyInt: 1, TypeBoolean: 1,
		TypeSmallInt: 2,
		TypeInt:      4, TypeFloat: 4, TypeDate: 4,
		TypeBigInt: 8, TypeDouble: 8, TypeTimestamp: 8, TypeCounter: 8, TypeTime: 8,
		TypeUUID: 16, TypeTimeUUID: 16,
	}
	for typ, want := range fixed {
		if got := collectionElemWireSize(nt(typ)); got != want {
			t.Errorf("collectionElemWireSize(%v) = %d, want %d", typ, got, want)
		}
	}
	// Variable-length types report 0 so callers fall back to an estimate.
	for _, typ := range []Type{TypeVarchar, TypeText, TypeAscii, TypeBlob, TypeInet, TypeCustom} {
		if got := collectionElemWireSize(nt(typ)); got != 0 {
			t.Errorf("collectionElemWireSize(%v) = %d, want 0 (variable-length)", typ, got)
		}
	}
}

// TestCollectionEntrySizeNoEstimate pins the decision not to guess at
// variable-length element sizes: an overshooting hint pushes the buffer past
// marshalBufMaxCap, so putMarshalBuf drops it and the pool stops working.
// Callers must see 0 here and skip the hint entirely.
func TestCollectionEntrySizeNoEstimate(t *testing.T) {
	for _, typ := range []Type{TypeVarchar, TypeText, TypeAscii, TypeBlob, TypeInet, TypeCustom} {
		if got := collectionEntrySize(nt(typ)); got != 0 {
			t.Errorf("collectionEntrySize(%v) = %d, want 0 so no hint is applied", typ, got)
		}
	}
	for _, typ := range []Type{TypeInt, TypeUUID, TypeBoolean, TypeSmallInt, TypeBigInt} {
		if got := collectionEntrySize(nt(typ)); got <= 0 {
			t.Errorf("collectionEntrySize(%v) = %d, want the exact wire size", typ, got)
		}
	}
}

// TestCollectionSizeHintNeverOvershoots is the property that makes hinting
// safe: the hint must never exceed what the encoding actually needs. An
// overshoot inflates the buffer past marshalBufMaxCap for no reason, and
// putMarshalBuf then drops it instead of returning it to the pool. (A genuinely
// large collection exceeding that cap is unavoidable and fine — the waste only
// matters when the hint is bigger than the data.)
func TestCollectionSizeHintNeverOvershoots(t *testing.T) {
	for _, elemSize := range []int{1, 2, 4, 8, 16} {
		for _, n := range []int{0, 1, 10, 1000, 100_000, math.MaxInt32} {
			perEntry := 4 + elemSize
			exact := 4 + int64(n)*int64(perEntry)
			got := int64(collectionSizeHint(n, perEntry))
			if got > exact {
				t.Fatalf("collectionSizeHint(%d, %d) = %d, overshoots exact size %d", n, perEntry, got, exact)
			}
		}
	}
}

// TestCollectionSizeHintCapped is the safety property: the hint is computed
// from an element count before any element has been marshalled, so a bogus or
// hostile count must neither overflow nor request an enormous allocation.
func TestCollectionSizeHintCapped(t *testing.T) {
	cases := []struct {
		name     string
		n        int
		perEntry int
		want     int
	}{
		{"empty", 0, 12, 4},
		{"negative count", -1, 12, 4},
		{"no per-entry size", 10, 0, 4},
		{"small exact", 10, 12, 4 + 120},
		{"at the cap boundary", (maxCollectionPreallocBytes - 4) / 12, 12, 4 + ((maxCollectionPreallocBytes-4)/12)*12},
		{"past the cap", maxCollectionPreallocBytes, 12, maxCollectionPreallocBytes},
		{"huge count", math.MaxInt32, 20, maxCollectionPreallocBytes},
		{"overflow attempt", math.MaxInt, 64, maxCollectionPreallocBytes},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := collectionSizeHint(tc.n, tc.perEntry)
			if got != tc.want {
				t.Fatalf("collectionSizeHint(%d, %d) = %d, want %d", tc.n, tc.perEntry, got, tc.want)
			}
			if got < 0 || got > maxCollectionPreallocBytes {
				t.Fatalf("hint %d outside [0, %d]", got, maxCollectionPreallocBytes)
			}
		})
	}
}

// TestMarshalMapRangeMatchesReflect checks the MapRange rewrite still produces
// a well-formed map body. Entry order is unspecified for CQL maps and Go map
// iteration is randomised, so this decodes the result rather than comparing
// bytes.
func TestMarshalMapRangeMatchesReflect(t *testing.T) {
	info := CollectionType{NativeType: nt(TypeMap), Key: nt(TypeVarchar), Elem: nt(TypeInt)}
	src := map[string]int32{"a": 1, "b": 2, "c": 3, "": 0}

	data, err := Marshal(info, src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got map[string]int32
	if err := Unmarshal(info, data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got) != len(src) {
		t.Fatalf("round trip produced %d entries, want %d", len(got), len(src))
	}
	for k, v := range src {
		if got[k] != v {
			t.Fatalf("key %q: got %d, want %d", k, got[k], v)
		}
	}
}

// TestMarshalCollectionOversizedCountDoesNotPrealloc guards the cap end to
// end: a large element count must not allocate proportionally before the
// elements are actually marshalled.
func TestMarshalCollectionOversizedCountDoesNotPrealloc(t *testing.T) {
	info := CollectionType{NativeType: nt(TypeList), Elem: nt(TypeBigInt)}
	// Zero-sized elements: a large count costs almost nothing to build, but
	// an uncapped hint would ask for n*12 bytes up front.
	src := make([]int64, 200_000)

	data, err := Marshal(info, src)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want := 4 + len(src)*(4+8)
	if len(data) != want {
		t.Fatalf("encoded %d bytes, want %d", len(data), want)
	}
}
