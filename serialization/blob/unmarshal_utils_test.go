//go:build unit
// +build unit

package blob

import (
	"bytes"
	"testing"
)

func TestDecBytesArrayBackedSlice(t *testing.T) {
	t.Parallel()

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}

	var arr [12]byte
	slice := arr[:]

	if err := DecBytes(data, &slice); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(arr[:], data) {
		t.Fatalf("expected underlying array to be %v, got %v", data, arr)
	}
	if !bytes.Equal(slice, data) {
		t.Fatalf("expected slice to be %v, got %v", data, slice)
	}
}

func TestDecBytesArrayBackedSliceViaUnmarshal(t *testing.T) {
	t.Parallel()

	type ObjectID [12]byte

	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}
	var id ObjectID
	pkSlice := id[:]

	if err := Unmarshal(data, &pkSlice); err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(id[:], data) {
		t.Fatalf("expected underlying array to be %v, got %v", data, id)
	}
}

func TestDecBytesNil(t *testing.T) {
	t.Parallel()

	existing := []byte{1, 2, 3}
	if err := DecBytes(nil, &existing); err != nil {
		t.Fatal(err)
	}
	if existing != nil {
		t.Fatalf("expected nil, got %v", existing)
	}
}

func TestDecBytesEmpty(t *testing.T) {
	t.Parallel()

	var dest []byte
	if err := DecBytes(make([]byte, 0), &dest); err != nil {
		t.Fatal(err)
	}
	if dest == nil {
		t.Fatal("expected non-nil empty slice for non-nil empty input")
	}
	if len(dest) != 0 {
		t.Fatalf("expected empty slice, got %v", dest)
	}
}

func TestDecBytesPreallocated(t *testing.T) {
	t.Parallel()

	data := []byte{0xAA, 0xBB, 0xCC}
	dest := make([]byte, 5)
	if err := DecBytes(data, &dest); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(dest, data) {
		t.Fatalf("expected %v, got %v", data, dest)
	}
}

func TestDecBytesNilReference(t *testing.T) {
	t.Parallel()

	if err := DecBytes([]byte{1}, nil); err == nil {
		t.Fatal("expected error for nil reference")
	}
}
