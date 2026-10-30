// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	"math/big"

	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
)

// Varint is a signed, arbitrary precision integer. Its encoded bytes can borrow
// segmented page storage; use VarintCodec.Clone to detach them. The zero value
// represents zero.

type Varint = cqltype.Varint
type Decimal = cqltype.Decimal

func VarintFromBytes(b []byte) (Varint, error)        { return cqltype.VarintFromBytes(b) }
func VarintFromBigInt(v *big.Int) Varint              { return cqltype.VarintFromBigInt(v) }
func canonicalVarint(b cqltype.Bytes) (Varint, error) { return cqltype.CanonicalVarint(b) }
func cloneVarint(v Varint) Varint                     { return cqltype.VarintView(cqltype.VarintBytes(v).Clone()) }
func encodeVarint(w *ValueWriter, v Varint) error {
	if err := cqltype.CheckBytesAlive(cqltype.VarintBytes(v)); err != nil {
		return err
	}
	if cqltype.VarintBytes(v).Len() == 0 {
		return w.Uint8(0)
	}
	return w.borrowView(cqltype.VarintBytes(v))
}
func decodeVarint(r ValueReader) (Varint, error) { return canonicalVarint(r.Bytes()) }

var VarintCodec Codec[Varint] = scalarCodec[Varint]{kind: cqltype.TypeVarint, encode: encodeVarint, decode: decodeVarint, clone: cloneVarint}
var DecimalCodec Codec[Decimal] = scalarCodec[Decimal]{kind: cqltype.TypeDecimal,
	encode: func(w *ValueWriter, v Decimal) error {
		if err := w.Uint32(uint32(v.Scale)); err != nil {
			return err
		}
		return encodeVarint(w, v.Unscaled)
	},
	decode: func(r ValueReader) (Decimal, error) {
		if r.Bytes().Len() < 5 {
			return Decimal{}, errors.New("gocql: decimal requires a scale and nonempty varint")
		}
		c := r.cursorValue()
		scale, err := c.Uint32()
		if err != nil {
			return Decimal{}, err
		}
		b, err := c.Take(c.Remaining())
		if err != nil {
			return Decimal{}, err
		}
		v, err := canonicalVarint(b)
		if err != nil {
			return Decimal{}, err
		}
		return Decimal{Scale: int32(scale), Unscaled: v}, nil
	},
	clone: func(v Decimal) Decimal { return Decimal{Scale: v.Scale, Unscaled: cloneVarint(v.Unscaled)} },
}
