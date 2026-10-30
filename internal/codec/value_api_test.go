// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"testing"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type foundationCustomBlob struct {
	checks, encodes, decodes *int
	reject, errorAfterWrite  bool
}

func (c foundationCustomBlob) CheckType(t cqltype.DataType) error {
	if c.checks != nil {
		*c.checks++
	}
	if c.reject || t.Kind() != cqltype.TypeBlob {
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
	typ := FoundationScalar(t, cqltype.TypeBlob)
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
	if err == nil || cqltype.DescriptorOf(failed.Type()) != nil {
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
	typ := FoundationScalar(t, cqltype.TypeBlob)
	source := []byte("source")
	borrowed, err := EncodeValueBorrowed(typ, BytesEncoder, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := cqltype.EachBytes(borrowed.Bytes(), func(s wire.Span) error {
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
	if err := FinishValueWriter(w); err != nil {
		t.Fatal(err)
	}
	chain, _ := builder.Freeze()
	source[0] = 'X'
	parent, err := ReadValue(wire.NewCursor(chain), typ, nil, false)
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
	blob := FoundationScalar(t, cqltype.TypeBlob)
	integer := FoundationScalar(t, cqltype.TypeInt)
	for _, test := range []struct {
		state cqltype.CellState
		body  []byte
		typ   cqltype.DataType
	}{{cqltype.Null, []byte{1}, blob}, {cqltype.Unset, []byte{1}, blob}, {cqltype.Empty, []byte{1}, integer}, {cqltype.CellState(42), nil, blob}, {cqltype.Empty, nil, blob}} {
		if _, err := NewEncodedValue(test.typ, test.state, cqltype.BorrowBytes(test.body)); err == nil {
			t.Fatalf("invalid state/body accepted: %+v", test)
		}
	}
	empty, err := EncodeValueBorrowed(integer, EmptyCodec(Int32), cqltype.EmptyValueOf[int32]{Empty: true})
	if err != nil {
		t.Fatal(err)
	}
	if empty.State() != cqltype.Empty || empty.Bytes().Len() != 0 {
		t.Fatal("explicit empty state lost")
	}
	decoded, err := DecodeValue(EmptyCodec(Int32), empty)
	if err != nil || !decoded.Empty {
		t.Fatalf("empty decode: %+v %v", decoded, err)
	}
	unset, err := NewEncodedValue(blob, cqltype.Unset, cqltype.BorrowBytes(nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeValue(BlobCodec, unset); err == nil {
		t.Fatal("unset result accepted")
	}
	chain := FoundationChain(t, []byte("leased"))
	lease := wire.NewLease(chain, nil)
	leased, err := NewEncodedValue(blob, cqltype.Present, cqltype.BytesView(chain, lease))
	if err != nil {
		t.Fatal(err)
	}
	detached := leased.Clone()
	lease.Release()
	if _, err := DecodeValue(BlobCodec, leased); !errors.Is(err, cqltype.ErrReleased) {
		t.Fatalf("decode released: %v", err)
	}
	if _, err := NewEncodedValue(blob, cqltype.Present, leased.Bytes()); !errors.Is(err, cqltype.ErrReleased) {
		t.Fatalf("construct released: %v", err)
	}
	foundationPanicReleased(t, func() { leased.Clone() })
	if got, err := DecodeValue(BlobCodec, detached); err != nil || string(got.OwnedBytes()) != "leased" {
		t.Fatalf("owned decode: %v", err)
	}
}

func TestV2FoundationOversizedValues(t *testing.T) {
	typ := FoundationScalar(t, cqltype.TypeBlob)
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
	if _, err := NewEncodedValue(typ, cqltype.Present, cqltype.BytesView(chain, nil)); err == nil {
		t.Fatal("oversized standalone cell accepted")
	}
	builder := wire.NewBuilder(0)
	w := NewValueWriter(builder, typ, false)
	if err := w.borrowView(cqltype.BytesView(chain, nil)); err != nil {
		t.Fatal(err)
	}
	if err := FinishValueWriter(w); err == nil {
		t.Fatal("oversized framed cell accepted")
	}
	if builder.Len() != 0 {
		t.Fatal("oversized cell was not rolled back")
	}
}

// codecTestMust unwraps checked constructors for known-valid fixture values.
func CodecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
