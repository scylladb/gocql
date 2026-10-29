// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"errors"
	"math/big"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Varint is a signed, arbitrary precision integer. Its encoded bytes can borrow
// segmented page storage; use VarintCodec.Clone to detach them. The zero value
// represents zero.
type Varint struct{ bytes Bytes }

// Decimal represents Unscaled * 10^-Scale without rounding.
type Decimal struct {
	Scale    int32
	Unscaled Varint
}

// VarintFromBytes borrows a nonempty, big-endian two's complement integer.
// Redundant sign bytes are removed by slicing descriptors, without payload copies.
func VarintFromBytes(b []byte) (Varint, error) {
	if len(b) == 0 {
		return Varint{}, errors.New("gocql: varint requires at least one byte")
	}
	return canonicalVarint(BorrowBytes(b))
}
func canonicalVarint(b Bytes) (Varint, error) {
	if err := b.checkAlive(); err != nil {
		return Varint{}, err
	}
	if b.Len() == 0 {
		return Varint{}, errors.New("gocql: varint requires at least one byte")
	}
	var first byte
	seen := false
	var trim int64
	done := false
	err := b.each(func(s wire.Span) error {
		if done {
			return nil
		}
		for _, v := range s.Bytes() {
			if !seen {
				first = v
				seen = true
				continue
			}
			if (first == 0 && v&0x80 == 0) || (first == 0xff && v&0x80 != 0) {
				trim++
				first = v
				continue
			}
			done = true
			break
		}
		return nil
	})
	if err != nil {
		return Varint{}, err
	}
	if trim > 0 {
		b, err = b.Slice(trim, b.Len()-trim)
		if err != nil {
			return Varint{}, err
		}
	}
	return Varint{bytes: b}, nil
}

// VarintFromBigInt materializes an owned two's complement representation. Nil
// and zero both produce the zero value.
func VarintFromBigInt(v *big.Int) Varint {
	if v == nil || v.Sign() == 0 {
		return Varint{}
	}
	if v.Sign() > 0 {
		b := v.Bytes()
		if b[0]&0x80 != 0 {
			b = append([]byte{0}, b...)
		}
		return Varint{bytes: BorrowBytes(b)}
	}
	// -x has the bits of ^(x-1). Include a sign byte when x-1 has its high bit set.
	magnitude := new(big.Int).Neg(v)
	magnitude.Sub(magnitude, big.NewInt(1))
	b := magnitude.Bytes()
	if len(b) == 0 {
		b = []byte{0}
	} else if b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	for i := range b {
		b[i] = ^b[i]
	}
	return Varint{bytes: BorrowBytes(b)}
}

// BigInt explicitly materializes an independent arbitrary precision integer.
func (v Varint) BigInt() *big.Int {
	if v.bytes.Len() == 0 {
		if err := v.bytes.checkAlive(); err != nil {
			panic(err)
		}
		return new(big.Int)
	}
	b := v.bytes.OwnedBytes()
	if b[0]&0x80 == 0 {
		return new(big.Int).SetBytes(b)
	}
	for i := range b {
		b[i] = ^b[i]
	}
	n := new(big.Int).SetBytes(b)
	n.Add(n, big.NewInt(1))
	return n.Neg(n)
}
func cloneVarint(v Varint) Varint { return Varint{bytes: v.bytes.Clone()} }
func encodeVarint(w *ValueWriter, v Varint) error {
	if err := v.bytes.checkAlive(); err != nil {
		return err
	}
	if v.bytes.Len() == 0 {
		return w.Uint8(0)
	}
	return w.borrowView(v.bytes)
}
func decodeVarint(r ValueReader) (Varint, error) { return canonicalVarint(r.Bytes()) }

var VarintCodec Codec[Varint] = scalarCodec[Varint]{kind: TypeVarint, encode: encodeVarint, decode: decodeVarint, clone: cloneVarint}
var DecimalCodec Codec[Decimal] = scalarCodec[Decimal]{kind: TypeDecimal,
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
