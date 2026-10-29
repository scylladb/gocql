package gocql

import (
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type foundationCustomBlob struct {
	checks, encodes, decodes *int
	reject, errorAfterWrite  bool
}

func (c foundationCustomBlob) CheckType(t DataType) error {
	if c.checks != nil {
		*c.checks++
	}
	if c.reject || t.Kind() != TypeBlob {
		return errors.New("unexpected type")
	}
	return nil
}
func (c foundationCustomBlob) Encode(w *ValueWriter, v []byte) error {
	if c.encodes != nil {
		*c.encodes++
	}
	if err := w.BorrowBytes(v); err != nil {
		return err
	}
	if c.errorAfterWrite {
		return errors.New("custom encoder failure")
	}
	return nil
}
func (c foundationCustomBlob) Decode(r ValueReader) ([]byte, error) {
	if c.decodes != nil {
		*c.decodes++
	}
	return r.Bytes().OwnedBytes(), nil
}
func (c foundationCustomBlob) Clone(v []byte) []byte { return append([]byte(nil), v...) }
func TestV2FoundationPublicCodecChecks(t *testing.T) {
	typ := foundationScalar(t, TypeBlob)
	checks, encodes, decodes := 0, 0, 0
	codec := foundationCustomBlob{checks: &checks, encodes: &encodes, decodes: &decodes}
	encoded, err := EncodeValueBorrowed(typ, codec, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 || encodes != 1 {
		t.Fatalf("encode checks/calls: %d/%d", checks, encodes)
	}
	value, err := DecodeValue(codec, encoded)
	if err != nil || string(value) != "value" {
		t.Fatalf("decode: %q %v", value, err)
	}
	if checks != 2 || decodes != 1 {
		t.Fatalf("decode checks/calls: %d/%d", checks, decodes)
	}
	codec.reject = true
	if _, err := EncodeValueBorrowed(typ, codec, nil); err == nil || encodes != 1 {
		t.Fatal("rejected type entered encoder")
	}
	if _, err := DecodeValue(codec, encoded); err == nil || decodes != 1 {
		t.Fatal("rejected type entered decoder")
	}
	codec.reject = false
	codec.errorAfterWrite = true
	failed, err := EncodeValueBorrowed(typ, codec, []byte("partial"))
	if err == nil || failed.Type().descriptor != nil {
		t.Fatal("failed encoder published a value")
	}
	var nilEncoder Encoder[[]byte]
	if _, err := EncodeValueBorrowed(typ, nilEncoder, nil); err == nil {
		t.Fatal("nil encoder accepted")
	}
	var nilDecoder Decoder[[]byte]
	if _, err := DecodeValue(nilDecoder, encoded); err == nil {
		t.Fatal("nil decoder accepted")
	}
}
func TestV2FoundationOwnedAndBorrowedEncoding(t *testing.T) {
	typ := foundationScalar(t, TypeBlob)
	source := []byte("source")
	borrowed, err := EncodeValueBorrowed(typ, BytesEncoder, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Bytes().each(func(s wire.Span) error {
		if &s.Bytes()[0] != &source[0] {
			t.Error("borrowed encode copied payload")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	owned, err := EncodeValueOwned(typ, BytesEncoder, source)
	if err != nil {
		t.Fatal(err)
	}
	clone := borrowed.Clone()
	source[0] = 'X'
	if got := string(owned.Bytes().OwnedBytes()); got != "source" {
		t.Fatalf("owned data retained source: %q", got)
	}
	if got := string(clone.Bytes().OwnedBytes()); got != "source" {
		t.Fatalf("cloned data retained source: %q", got)
	}
	// An owned parent propagates its mode through nested child writers.
	source = []byte("nested")
	builder := wire.NewBuilder(0)
	w := newOwnedValueWriter(builder, typ, false)
	if err := w.Child(typ, func(child *ValueWriter) error { return child.BorrowBytes(source) }); err != nil {
		t.Fatal(err)
	}
	if err := finishValueWriter(w); err != nil {
		t.Fatal(err)
	}
	chain, _ := builder.Freeze()
	source[0] = 'X'
	parent, err := readValue(wire.NewCursor(chain), typ, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	child, err := parent.Cursor().Child(typ)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(child.Bytes().OwnedBytes()); got != "nested" {
		t.Fatalf("owned child retained source: %q", got)
	}
}
func TestV2FoundationEncodedValueStatesAndLifetime(t *testing.T) {
	blob := foundationScalar(t, TypeBlob)
	integer := foundationScalar(t, TypeInt)
	for _, test := range []struct {
		state CellState
		body  []byte
		typ   DataType
	}{{Null, []byte{1}, blob}, {Unset, []byte{1}, blob}, {Empty, []byte{1}, integer}, {CellState(42), nil, blob}, {Empty, nil, blob}} {
		if _, err := NewEncodedValue(test.typ, test.state, BorrowBytes(test.body)); err == nil {
			t.Fatalf("invalid state/body accepted: %+v", test)
		}
	}
	empty, err := EncodeValueBorrowed(integer, EmptyCodec(Int32), EmptyValueOf[int32]{Empty: true})
	if err != nil {
		t.Fatal(err)
	}
	if empty.State() != Empty || empty.Bytes().Len() != 0 {
		t.Fatal("explicit empty state lost")
	}
	decoded, err := DecodeValue(EmptyCodec(Int32), empty)
	if err != nil || !decoded.Empty {
		t.Fatalf("empty decode: %+v %v", decoded, err)
	}
	unset, err := NewEncodedValue(blob, Unset, BorrowBytes(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeValue(BlobCodec, unset); err == nil {
		t.Fatal("unset result accepted")
	}
	chain := foundationChain(t, []byte("leased"))
	lease := wire.NewLease(chain, nil)
	leased, err := NewEncodedValue(blob, Present, Bytes{chain: chain, lease: lease})
	if err != nil {
		t.Fatal(err)
	}
	detached := leased.Clone()
	lease.Release()
	if _, err := DecodeValue(BlobCodec, leased); !errors.Is(err, ErrReleased) {
		t.Fatalf("decode released: %v", err)
	}
	if _, err := NewEncodedValue(blob, Present, leased.Bytes()); !errors.Is(err, ErrReleased) {
		t.Fatalf("construct released: %v", err)
	}
	foundationPanicReleased(t, func() { leased.Clone() })
	if got, err := DecodeValue(BlobCodec, detached); err != nil || string(got.OwnedBytes()) != "leased" {
		t.Fatalf("owned decode: %v", err)
	}
}

func TestV2FoundationOversizedValues(t *testing.T) {
	typ := foundationScalar(t, TypeBlob)
	// Reuse immutable storage to represent 2 GiB without allocating its payload.
	block := make([]byte, 1<<16)
	spans := make([]wire.Span, 1<<15)
	for i := range spans {
		spans[i] = wire.Borrow(block)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewEncodedValue(typ, Present, Bytes{chain: chain}); err == nil {
		t.Fatal("oversized standalone cell accepted")
	}
	builder := wire.NewBuilder(0)
	w := newValueWriter(builder, typ, false)
	if err := w.borrowView(Bytes{chain: chain}); err != nil {
		t.Fatal(err)
	}
	if err := finishValueWriter(w); err == nil {
		t.Fatal("oversized framed cell accepted")
	}
	if builder.Len() != 0 {
		t.Fatal("oversized cell was not rolled back")
	}
}
