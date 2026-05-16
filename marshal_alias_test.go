//go:build all || unit

package gocql

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/gocql/gocql/internal/tests/mock"
)

// The list fast paths must allocate a fresh backing array per decode. Reusing
// the one already in *dst is tempting — the destination is usually a loop
// variable — but it aliases every result the caller kept from an earlier
// decode into the same variable. Iter.Scan documents that it copies into dest,
// and gocql publishes no "valid until the next Scan" contract, so these tests
// pin the copying behaviour against a future reuse optimization.

func aliasListWire(elems ...[]byte) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(elems)))
	for _, e := range elems {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], uint32(len(e)))
		out = append(out, l[:]...)
		out = append(out, e...)
	}
	return out
}

func aliasListType(elem Type) CollectionType {
	return CollectionType{
		NativeType: NativeType{proto: protoVersion4, typ: TypeList},
		Elem:       NativeType{proto: protoVersion4, typ: elem},
	}
}

// TestUnmarshalListDoesNotAliasRetainedResult decodes twice into one variable
// and checks the first result is untouched.
func TestUnmarshalListDoesNotAliasRetainedResult(t *testing.T) {
	t.Run("uuid", func(t *testing.T) {
		first, second := make([]byte, 16), make([]byte, 16)
		first[0], second[0] = 0x11, 0x22

		var dst []UUID
		if err := Unmarshal(aliasListType(TypeUUID), aliasListWire(first), &dst); err != nil {
			t.Fatal(err)
		}
		retained := dst
		if err := Unmarshal(aliasListType(TypeUUID), aliasListWire(second), &dst); err != nil {
			t.Fatal(err)
		}
		if retained[0][0] != 0x11 {
			t.Fatalf("second decode overwrote the retained slice: got %#v, want first byte 0x11", retained[0])
		}
	})

	t.Run("string", func(t *testing.T) {
		var dst []string
		if err := Unmarshal(aliasListType(TypeVarchar), aliasListWire([]byte("first")), &dst); err != nil {
			t.Fatal(err)
		}
		retained := dst
		if err := Unmarshal(aliasListType(TypeVarchar), aliasListWire([]byte("secnd")), &dst); err != nil {
			t.Fatal(err)
		}
		if retained[0] != "first" {
			t.Fatalf("second decode overwrote the retained slice: got %q, want %q", retained[0], "first")
		}
	})

	// []int and the vector decoders reuse via a differently-named local, so
	// they are covered explicitly rather than by inspection.
	t.Run("int", func(t *testing.T) {
		enc := func(v int32) []byte {
			var b [4]byte
			binary.BigEndian.PutUint32(b[:], uint32(v))
			return b[:]
		}
		var dst []int
		if err := Unmarshal(aliasListType(TypeInt), aliasListWire(enc(11)), &dst); err != nil {
			t.Fatal(err)
		}
		retained := dst
		if err := Unmarshal(aliasListType(TypeInt), aliasListWire(enc(22)), &dst); err != nil {
			t.Fatal(err)
		}
		if retained[0] != 11 {
			t.Fatalf("second decode overwrote the retained slice: got %d, want 11", retained[0])
		}
	})

	t.Run("vector_float32", func(t *testing.T) {
		vt := VectorType{
			NativeType: NewCustomType(protoVersion4, TypeCustom, apacheCassandraTypePrefix+"VectorType"),
			SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
			Dimensions: 2,
		}
		enc := func(a, b float32) []byte {
			out := make([]byte, 8)
			binary.BigEndian.PutUint32(out[0:], math.Float32bits(a))
			binary.BigEndian.PutUint32(out[4:], math.Float32bits(b))
			return out
		}
		var dst []float32
		if err := Unmarshal(vt, enc(1, 2), &dst); err != nil {
			t.Fatal(err)
		}
		retained := dst
		if err := Unmarshal(vt, enc(9, 9), &dst); err != nil {
			t.Fatal(err)
		}
		if retained[0] != 1 {
			t.Fatalf("second decode overwrote the retained vector: got %v, want 1", retained[0])
		}
	})

	// dim==0 is the easy site to miss: no bytes are written, but handing
	// back the caller's array lets a later append clobber retained memory.
	t.Run("vector_dim0", func(t *testing.T) {
		vt := VectorType{
			NativeType: NewCustomType(protoVersion4, TypeCustom, apacheCassandraTypePrefix+"VectorType"),
			SubType:    NativeType{proto: protoVersion4, typ: TypeFloat},
			Dimensions: 0,
		}
		shared := make([]float32, 1, 4)
		shared[0] = 42
		dst := shared[:0]
		if err := Unmarshal(vt, []byte{}, &dst); err != nil {
			t.Fatal(err)
		}
		dst = append(dst, 7) // must land in fresh storage
		if shared[0] != 42 {
			t.Fatalf("append through the decoded slice clobbered retained memory: shared[0]=%v", shared[0])
		}
	})

	t.Run("int64", func(t *testing.T) {
		enc := func(v int64) []byte {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v))
			return b[:]
		}
		var dst []int64
		if err := Unmarshal(aliasListType(TypeBigInt), aliasListWire(enc(1)), &dst); err != nil {
			t.Fatal(err)
		}
		retained := dst
		if err := Unmarshal(aliasListType(TypeBigInt), aliasListWire(enc(2)), &dst); err != nil {
			t.Fatal(err)
		}
		if retained[0] != 1 {
			t.Fatalf("second decode overwrote the retained slice: got %d, want 1", retained[0])
		}
	})
}

// TestScanLoopRetainsDistinctCollectionSlices exercises the same property
// through Iter.Scan, which is how callers actually hit it.
func TestScanLoopRetainsDistinctCollectionSlices(t *testing.T) {
	wire := func(elems ...string) []byte {
		raw := make([][]byte, len(elems))
		for i, e := range elems {
			raw[i] = []byte(e)
		}
		return aliasListWire(raw...)
	}

	col := ColumnInfo{Name: "tags", TypeInfo: aliasListType(TypeVarchar)}
	iter := &Iter{
		meta:    resultMetadata{columns: []ColumnInfo{col}, actualColCount: 1},
		framer:  &mock.MockFramer{Data: [][]byte{wire("row-one"), wire("row-two"), wire("row-three")}},
		numRows: 3,
	}

	var tags []string
	var all [][]string
	for iter.Scan(&tags) {
		all = append(all, tags)
	}

	want := []string{"row-one", "row-two", "row-three"}
	if len(all) != len(want) {
		t.Fatalf("scanned %d rows, want %d", len(all), len(want))
	}
	for i, got := range all {
		if got[0] != want[i] {
			t.Errorf("row %d: retained slice holds %q, want %q", i, got[0], want[i])
		}
	}
}
