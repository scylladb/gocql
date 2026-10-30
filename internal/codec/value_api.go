// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wire"
	"math"
)

// EncodedValue contains one checked value body, its complete type, and cell
// state. Borrowed payloads remain subject to their source lifetime.

type EncodedValue struct {
	typ   cqltype.DataType
	state cqltype.CellState
	body  cqltype.Bytes
}

// NewEncodedValue checks the descriptor and state/body framing. Scalar content
// is checked by the selected decoder when DecodeValue is called.
func NewEncodedValue(typ cqltype.DataType, state cqltype.CellState, body cqltype.Bytes) (EncodedValue, error) {
	if err := cqltype.ValidateDataType(typ); err != nil {
		return EncodedValue{}, err
	}
	if err := cqltype.CheckBytesAlive(body); err != nil {
		return EncodedValue{}, err
	}
	if body.Len() > math.MaxInt32 {
		return EncodedValue{}, cqltype.TypeValidation("value", "cell body exceeds signed 32-bit length")
	}
	if state > cqltype.Empty {
		return EncodedValue{}, cqltype.TypeValidation("state", "unknown cell state %d", state)
	}
	if state != cqltype.Present && body.Len() != 0 {
		return EncodedValue{}, cqltype.TypeValidation("value", "state %d cannot contain payload bytes", state)
	}
	if state == cqltype.Empty && !emptySupported(typ) {
		return EncodedValue{}, cqltype.TypeValidation("type", "%s does not support empty values", typ)
	}
	return EncodedValue{typ: typ, state: state, body: body}, nil
}
func (v EncodedValue) Type() cqltype.DataType   { return v.typ }
func (v EncodedValue) State() cqltype.CellState { return v.state }
func (v EncodedValue) Bytes() cqltype.Bytes     { return v.body }

// Clone detaches all encoded storage. It panics with ErrReleased after release.
func (v EncodedValue) Clone() EncodedValue {
	return EncodedValue{typ: v.typ, state: v.state, body: v.body.Clone()}
}

// EncodeValueBorrowed encodes one value without copying borrowed input payloads.
// The caller must keep those payloads immutable while the encoded value is used.
func EncodeValueBorrowed[T any](typ cqltype.DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return encodeValue(typ, encoder, value, false)
}

// EncodeValueOwned encodes one value and copies every borrowed input payload.
func EncodeValueOwned[T any](typ cqltype.DataType, encoder Encoder[T], value T) (EncodedValue, error) {
	return encodeValue(typ, encoder, value, true)
}
func encodeValue[T any](typ cqltype.DataType, encoder Encoder[T], value T, owned bool) (EncodedValue, error) {
	if err := cqltype.ValidateDataType(typ); err != nil {
		return EncodedValue{}, err
	}
	if encoder == nil {
		return EncodedValue{}, cqltype.TypeValidation("encoder", "nil encoder")
	}
	compiled, err := compileValueEncoder(encoder, typ)
	if err != nil {
		return EncodedValue{}, err
	}
	builder := wire.NewBuilder(0)
	writer := NewValueWriter(builder, typ, true)
	writer.ownBorrowed = owned
	if err := compiled.Encode(writer, value); err != nil {
		abortValueWriter(writer)
		return EncodedValue{}, err
	}
	if err := FinishValueWriter(writer); err != nil {
		return EncodedValue{}, err
	}
	chain, err := builder.Freeze()
	if err != nil {
		return EncodedValue{}, err
	}
	// Slice skips only the enclosing length, retaining the existing payload spans.
	body, err := chain.Slice(4, chain.Len()-4)
	if err != nil {
		return EncodedValue{}, err
	}
	return EncodedValue{typ: typ, state: writer.state, body: cqltype.BytesView(body, nil)}, nil
}

// DecodeValue checks the requested type before invoking the decoder. Unset is
// an input-binding state and is never admitted as a result value.
func DecodeValue[T any](decoder Decoder[T], value EncodedValue) (T, error) {
	var zero T
	if err := cqltype.ValidateDataType(value.typ); err != nil {
		return zero, err
	}
	if decoder == nil {
		return zero, cqltype.TypeValidation("decoder", "nil decoder")
	}
	compiled, err := compileValueDecoder(decoder, value.typ)
	if err != nil {
		return zero, err
	}
	if err := cqltype.CheckBytesAlive(value.body); err != nil {
		return zero, err
	}
	if value.state == cqltype.Unset {
		return zero, cqltype.TypeValidation("state", "unset cannot be decoded as a result")
	}
	return compiled.Decode(NewValueReader(value.typ, value.state, cqltype.BytesChain(value.body), cqltype.BytesLease(value.body)))
}
