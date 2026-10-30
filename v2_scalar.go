// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"unicode/utf8"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// Counter is a CQL counter, distinct from a bigint.
type Counter int64

// TimeUUID is a version 1 UUID.
type TimeUUID UUID

// scalarCodec contains immutable operations shared by all calls to a codec.
type scalarCodec[T any] struct {
	kind        Type
	alternative Type
	encode      func(*ValueWriter, T) error
	decode      func(ValueReader) (T, error)
	clone       func(T) T
}

func checkScalarType(t DataType, kind, alternative Type) error {
	if t.Kind() != kind && (alternative == 0 || t.Kind() != alternative) {
		return fmt.Errorf("gocql: codec for %s cannot use %s", kind, t.Kind())
	}
	return nil
}
func (c scalarCodec[T]) CheckType(t DataType) error { return checkScalarType(t, c.kind, c.alternative) }

func (c scalarCodec[T]) Encode(w *ValueWriter, v T) error {
	if err := c.CheckType(w.Type()); err != nil {
		return err
	}
	return c.encode(w, v)
}
func (c scalarCodec[T]) Decode(r ValueReader) (T, error) {
	var zero T
	if err := c.CheckType(r.Type()); err != nil {
		return zero, err
	}
	if err := requirePresent(r); err != nil {
		return zero, err
	}
	return c.decode(r)
}
func (c scalarCodec[T]) Clone(v T) T {
	if c.clone != nil {
		return c.clone(v)
	}
	return v
}

func requirePresent(r ValueReader) error {
	if r.err != nil {
		return r.err
	}
	if r.State() != Present {
		return fmt.Errorf("gocql: nonnullable %s cannot decode cell state %d", r.Type().Kind(), r.State())
	}
	return r.Bytes().checkAlive()
}
func fixedCursor(r ValueReader, width int64) (ValueCursor, error) {
	if r.Bytes().Len() != width {
		return ValueCursor{}, fmt.Errorf("gocql: %s requires %d bytes, got %d", r.Type().Kind(), width, r.Bytes().Len())
	}
	return r.cursorValue(), nil
}

type signedScalar interface {
	~int8 | ~int16 | ~int32 | ~int64
}

func integerCodec[T signedScalar](kind Type, width int64) Codec[T] {
	return scalarCodec[T]{kind: kind,
		encode: func(w *ValueWriter, v T) error {
			switch width {
			case 1:
				return w.Uint8(uint8(v))
			case 2:
				return w.Uint16(uint16(v))
			case 4:
				return w.Uint32(uint32(v))
			default:
				return w.Uint64(uint64(v))
			}
		},
		decode: func(r ValueReader) (T, error) {
			c, err := fixedCursor(r, width)
			if err != nil {
				return 0, err
			}
			switch width {
			case 1:
				v, e := c.Uint8()
				return T(int8(v)), e
			case 2:
				v, e := c.Uint16()
				return T(int16(v)), e
			case 4:
				v, e := c.Uint32()
				return T(int32(v)), e
			default:
				v, e := c.Uint64()
				return T(int64(v)), e
			}
		},
	}
}

var (
	Bool         Codec[bool]    = scalarCodec[bool]{kind: TypeBoolean, encode: encodeBool, decode: decodeBool}
	Int8         Codec[int8]    = integerCodec[int8](TypeTinyInt, 1)
	Int16        Codec[int16]   = integerCodec[int16](TypeSmallInt, 2)
	Int32        Codec[int32]   = integerCodec[int32](TypeInt, 4)
	Int64        Codec[int64]   = integerCodec[int64](TypeBigInt, 8)
	CounterCodec Codec[Counter] = integerCodec[Counter](TypeCounter, 8)
	Float32      Codec[float32] = scalarCodec[float32]{kind: TypeFloat,
		encode: func(w *ValueWriter, v float32) error { return w.Uint32(math.Float32bits(v)) },
		decode: func(r ValueReader) (float32, error) {
			c, e := fixedCursor(r, 4)
			if e != nil {
				return 0, e
			}
			v, e := c.Uint32()
			return math.Float32frombits(v), e
		},
	}
	Float64 Codec[float64] = scalarCodec[float64]{kind: TypeDouble,
		encode: func(w *ValueWriter, v float64) error { return w.Uint64(math.Float64bits(v)) },
		decode: func(r ValueReader) (float64, error) {
			c, e := fixedCursor(r, 8)
			if e != nil {
				return 0, e
			}
			v, e := c.Uint64()
			return math.Float64frombits(v), e
		},
	}
	UUIDCodec     Codec[UUID]       = scalarCodec[UUID]{kind: TypeUUID, encode: encodeUUID, decode: decodeUUID}
	TimeUUIDCodec Codec[TimeUUID]   = scalarCodec[TimeUUID]{kind: TypeTimeUUID, encode: encodeTimeUUID, decode: decodeTimeUUID}
	InetCodec     Codec[netip.Addr] = scalarCodec[netip.Addr]{kind: TypeInet, encode: encodeInet, decode: decodeInet}
	StringEncoder Encoder[string]   = stringEncoder{}
	ASCIIEncoder  Encoder[string]   = stringEncoder{ascii: true}
	BytesEncoder  Encoder[[]byte]   = bytesEncoder{}
	TextCodec     Codec[Text]       = textCodec{}
	ASCIICodec    Codec[Text]       = textCodec{ascii: true}
	BlobCodec     Codec[Bytes]      = scalarCodec[Bytes]{kind: TypeBlob,
		encode: func(w *ValueWriter, v Bytes) error { return w.borrowView(v) },
		decode: func(r ValueReader) (Bytes, error) { return r.Bytes(), nil },
		clone:  func(v Bytes) Bytes { return v.Clone() },
	}
)

func encodeBool(w *ValueWriter, v bool) error {
	if v {
		return w.Uint8(1)
	}
	return w.Uint8(0)
}
func decodeBool(r ValueReader) (bool, error) {
	c, e := fixedCursor(r, 1)
	if e != nil {
		return false, e
	}
	v, e := c.Uint8()
	if e != nil {
		return false, e
	}
	if v > 1 {
		return false, errors.New("gocql: boolean must be 0 or 1")
	}
	return v == 1, nil
}
func encodeUUID(w *ValueWriter, v UUID) error {
	if err := w.Uint64(binary.BigEndian.Uint64(v[:8])); err != nil {
		return err
	}
	return w.Uint64(binary.BigEndian.Uint64(v[8:]))
}
func decodeUUID(r ValueReader) (UUID, error) {
	var v UUID
	c, e := fixedCursor(r, 16)
	if e != nil {
		return v, e
	}
	hi, e := c.Uint64()
	if e != nil {
		return v, e
	}
	lo, e := c.Uint64()
	if e != nil {
		return v, e
	}
	binary.BigEndian.PutUint64(v[:8], hi)
	binary.BigEndian.PutUint64(v[8:], lo)
	return v, nil
}
func encodeTimeUUID(w *ValueWriter, v TimeUUID) error {
	if UUID(v).Version() != 1 {
		return errors.New("gocql: timeuuid must have version 1")
	}
	return encodeUUID(w, UUID(v))
}
func decodeTimeUUID(r ValueReader) (TimeUUID, error) {
	v, e := decodeUUID(r)
	if e != nil {
		return TimeUUID{}, e
	}
	if v.Version() != 1 {
		return TimeUUID{}, errors.New("gocql: timeuuid must have version 1")
	}
	return TimeUUID(v), nil
}
func encodeInet(w *ValueWriter, v netip.Addr) error {
	if !v.IsValid() || v.Zone() != "" {
		return errors.New("gocql: inet requires a valid address without a zone")
	}
	if v.Is4() {
		a := v.As4()
		return w.Uint32(binary.BigEndian.Uint32(a[:]))
	}
	a := v.As16()
	if e := w.Uint64(binary.BigEndian.Uint64(a[:8])); e != nil {
		return e
	}
	return w.Uint64(binary.BigEndian.Uint64(a[8:]))
}
func decodeInet(r ValueReader) (netip.Addr, error) {
	c := r.cursorValue()
	switch r.Bytes().Len() {
	case 4:
		v, e := c.Uint32()
		if e != nil {
			return netip.Addr{}, e
		}
		var a [4]byte
		binary.BigEndian.PutUint32(a[:], v)
		return netip.AddrFrom4(a), nil
	case 16:
		hi, e := c.Uint64()
		if e != nil {
			return netip.Addr{}, e
		}
		lo, e := c.Uint64()
		if e != nil {
			return netip.Addr{}, e
		}
		var a [16]byte
		binary.BigEndian.PutUint64(a[:8], hi)
		binary.BigEndian.PutUint64(a[8:], lo)
		return netip.AddrFrom16(a), nil
	default:
		return netip.Addr{}, errors.New("gocql: inet requires 4 or 16 bytes")
	}
}

type stringEncoder struct{ ascii bool }

func checkTextType(t DataType, ascii bool) error {
	if ascii {
		if t.Kind() == TypeAscii {
			return nil
		}
	} else if t.Kind() == TypeText || t.Kind() == TypeVarchar {
		return nil
	}
	return fmt.Errorf("gocql: incompatible text type %s", t.Kind())
}
func (c stringEncoder) CheckType(t DataType) error { return checkTextType(t, c.ascii) }
func (c stringEncoder) Encode(w *ValueWriter, v string) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	if c.ascii {
		for i := 0; i < len(v); i++ {
			if v[i] >= utf8.RuneSelf {
				return errors.New("gocql: invalid ASCII")
			}
		}
	} else if !utf8.ValidString(v) {
		return errors.New("gocql: invalid UTF-8")
	}
	return w.BorrowString(v)
}

type bytesEncoder struct{}

func (bytesEncoder) CheckType(t DataType) error {
	if t.Kind() != TypeBlob {
		return fmt.Errorf("gocql: blob codec cannot use %s", t.Kind())
	}
	return nil
}
func (c bytesEncoder) Encode(w *ValueWriter, v []byte) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	return w.BorrowBytes(v)
}

type textCodec struct{ ascii bool }

func (c textCodec) CheckType(t DataType) error { return checkTextType(t, c.ascii) }
func (c textCodec) Encode(w *ValueWriter, v Text) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	if e := validateText(v.Bytes(), c.ascii); e != nil {
		return e
	}
	return w.borrowView(v.Bytes())
}
func (c textCodec) Decode(r ValueReader) (Text, error) {
	if e := c.CheckType(r.Type()); e != nil {
		return Text{}, e
	}
	if e := requirePresent(r); e != nil {
		return Text{}, e
	}
	if e := validateText(r.Bytes(), c.ascii); e != nil {
		return Text{}, e
	}
	return Text{bytes: r.Bytes()}, nil
}
func (textCodec) Clone(v Text) Text { return v.Clone() }

// textValidation shares incremental UTF-8/ASCII rules between borrowed spans
// and final-storage streaming. It never constructs a string or retains a chunk.
type textValidation struct {
	ascii     bool
	remaining int
	low, high byte
}

func newTextValidation(ascii bool) textValidation {
	return textValidation{ascii: ascii, low: 0x80, high: 0xbf}
}
func (v *textValidation) write(b []byte) error {
	for _, c := range b {
		if v.ascii {
			if c >= utf8.RuneSelf {
				return errors.New("gocql: invalid ASCII")
			}
			continue
		}
		if v.remaining > 0 {
			if c < v.low || c > v.high {
				return errors.New("gocql: invalid UTF-8")
			}
			v.remaining--
			v.low, v.high = 0x80, 0xbf
			continue
		}
		switch {
		case c < 0x80:
		case c >= 0xc2 && c <= 0xdf:
			v.remaining = 1
		case c >= 0xe0 && c <= 0xef:
			v.remaining = 2
			if c == 0xe0 {
				v.low = 0xa0
			}
			if c == 0xed {
				v.high = 0x9f
			}
		case c >= 0xf0 && c <= 0xf4:
			v.remaining = 3
			if c == 0xf0 {
				v.low = 0x90
			}
			if c == 0xf4 {
				v.high = 0x8f
			}
		default:
			return errors.New("gocql: invalid UTF-8")
		}
	}
	return nil
}
func (v *textValidation) finish() error {
	if v.remaining != 0 {
		return errors.New("gocql: truncated UTF-8")
	}
	return nil
}
func validateText(b Bytes, ascii bool) error {
	v := newTextValidation(ascii)
	if err := b.each(func(s wire.Span) error { return v.write(s.Bytes()) }); err != nil {
		return err
	}
	return v.finish()
}

// NamedScalar adapts a named application type through explicitly typed conversions.
func NamedScalar[A, B any](base Codec[B], to func(A) B, from func(B) A) Codec[A] {
	return namedScalar[A, B]{base: base, to: to, from: from}
}

type namedScalar[A, B any] struct {
	base    Codec[B]
	to      func(A) B
	from    func(B) A
	encoder Encoder[B]
	decoder Decoder[B]
	typ     DataType
}

func (c namedScalar[A, B]) CheckType(t DataType) error {
	if c.typ.descriptor != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled mapped scalar metadata changed")
	}
	if c.base == nil || c.to == nil || c.from == nil {
		return errors.New("gocql: incomplete named scalar adapter")
	}
	return c.base.CheckType(t)
}
func (c namedScalar[A, B]) Encode(w *ValueWriter, v A) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	encoder := c.encoder
	if encoder == nil {
		encoder = c.base
	}
	return encoder.Encode(w, c.to(v))
}
func (c namedScalar[A, B]) Decode(r ValueReader) (A, error) {
	var zero A
	if e := c.CheckType(r.Type()); e != nil {
		return zero, e
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v, e := decoder.Decode(r)
	if e != nil {
		return zero, e
	}
	return c.from(v), nil
}
func (c namedScalar[A, B]) Clone(v A) A { return c.from(c.base.Clone(c.to(v))) }

func NullableCodec[T any](base Codec[T]) Codec[Nullable[T]] {
	return nullableCodec[T]{nullableEncoder[T]{base: base}, nullableDecoder[T]{base: base}}
}
func NullableEncoder[T any](base Encoder[T]) Encoder[Nullable[T]] {
	return nullableEncoder[T]{base: base}
}
func NullableDecoder[T any](base Decoder[T]) Decoder[Nullable[T]] {
	return nullableDecoder[T]{base: base}
}

type nullableEncoder[T any] struct {
	base Encoder[T]
	typ  DataType
}

func (c nullableEncoder[T]) CheckType(t DataType) error {
	if c.typ.descriptor != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil nullable encoder")
	}
	return c.base.CheckType(t)
}
func (c nullableEncoder[T]) Encode(w *ValueWriter, v Nullable[T]) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	if !v.Valid {
		return w.Null()
	}
	return c.base.Encode(w, v.Value)
}

type nullableDecoder[T any] struct {
	base Decoder[T]
	typ  DataType
}

func (c nullableDecoder[T]) CheckType(t DataType) error {
	if c.typ.descriptor != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil nullable decoder")
	}
	return c.base.CheckType(t)
}
func (c nullableDecoder[T]) Decode(r ValueReader) (Nullable[T], error) {
	if e := c.CheckType(r.Type()); e != nil {
		return Nullable[T]{}, e
	}
	if r.err != nil {
		return Nullable[T]{}, r.err
	}
	if r.State() == Null {
		if e := r.Bytes().checkAlive(); e != nil {
			return Nullable[T]{}, e
		}
		return Nullable[T]{}, nil
	}
	v, e := c.base.Decode(r)
	if e != nil {
		return Nullable[T]{}, e
	}
	return Nullable[T]{Value: v, Valid: true}, nil
}
func (c nullableDecoder[T]) Clone(v Nullable[T]) Nullable[T] {
	if v.Valid {
		v.Value = c.base.Clone(v.Value)
		return v
	}
	return Nullable[T]{}
}

type nullableCodec[T any] struct {
	nullableEncoder[T]
	nullableDecoder[T]
}

func (c nullableCodec[T]) CheckType(t DataType) error          { return c.nullableEncoder.CheckType(t) }
func BindEncoder[T any](base Encoder[T]) Encoder[BindValue[T]] { return bindEncoder[T]{base: base} }

type bindEncoder[T any] struct {
	base Encoder[T]
	typ  DataType
}

func (c bindEncoder[T]) CheckType(t DataType) error {
	if c.typ.descriptor != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil bind encoder")
	}
	return c.base.CheckType(t)
}
func (c bindEncoder[T]) Encode(w *ValueWriter, v BindValue[T]) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	switch v.State {
	case Present:
		return c.base.Encode(w, v.Value)
	case Null:
		return w.Null()
	case Unset:
		return w.Unset()
	default:
		return errors.New("gocql: invalid bind state")
	}
}

func EmptyCodec[T any](base Codec[T]) Codec[EmptyValueOf[T]] { return emptyCodec[T]{base: base} }

type emptyCodec[T any] struct {
	base    Codec[T]
	encoder Encoder[T]
	decoder Decoder[T]
	typ     DataType
}

func emptySupported(t DataType) bool {
	switch t.Kind() {
	case TypeBoolean, TypeTinyInt, TypeSmallInt, TypeInt, TypeBigInt, TypeFloat, TypeDouble, TypeDate, TypeTime, TypeTimestamp, TypeUUID, TypeTimeUUID, TypeInet, TypeVarint, TypeDecimal, TypeTuple:
		return true
	}
	return false
}
func (c emptyCodec[T]) CheckType(t DataType) error {
	if c.typ.descriptor != nil {
		if c.typ.Equal(t) {
			return nil
		}
		return typeValidation("type", "compiled wrapper metadata changed")
	}
	if c.base == nil {
		return errors.New("gocql: nil empty codec")
	}
	if e := c.base.CheckType(t); e != nil {
		return e
	}
	if !emptySupported(t) {
		return fmt.Errorf("gocql: %s does not support Empty", t.Kind())
	}
	return nil
}
func (c emptyCodec[T]) Encode(w *ValueWriter, v EmptyValueOf[T]) error {
	if e := c.CheckType(w.Type()); e != nil {
		return e
	}
	if v.Empty {
		return w.emptyValue()
	}
	if c.encoder != nil {
		return c.encoder.Encode(w, v.Value)
	}
	return c.base.Encode(w, v.Value)
}
func (c emptyCodec[T]) Decode(r ValueReader) (EmptyValueOf[T], error) {
	if e := c.CheckType(r.Type()); e != nil {
		return EmptyValueOf[T]{}, e
	}
	if r.err != nil {
		return EmptyValueOf[T]{}, r.err
	}
	if r.State() == Empty || (r.State() == Present && r.Bytes().Len() == 0) {
		if e := r.Bytes().checkAlive(); e != nil {
			return EmptyValueOf[T]{}, e
		}
		if r.Bytes().Len() != 0 {
			return EmptyValueOf[T]{}, errors.New("gocql: Empty has a payload")
		}
		return EmptyValueOf[T]{Empty: true}, nil
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	v, e := decoder.Decode(r)
	if e != nil {
		return EmptyValueOf[T]{}, e
	}
	return EmptyValueOf[T]{Value: v}, nil
}
func (c emptyCodec[T]) Clone(v EmptyValueOf[T]) EmptyValueOf[T] {
	if !v.Empty {
		v.Value = c.base.Clone(v.Value)
		return v
	}
	return EmptyValueOf[T]{Empty: true}
}

func (c nullableEncoder[T]) compileValueEncoder(t DataType) (Encoder[Nullable[T]], error) {
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c nullableDecoder[T]) compileValueDecoder(t DataType) (Decoder[Nullable[T]], error) {
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c nullableDecoder[T]) DecodeInto(r ValueReader, dst *Nullable[T]) error {
	if dst == nil {
		return typeValidation("destination", "nil nullable destination")
	}
	*dst = Nullable[T]{}
	if err := c.CheckType(r.Type()); err != nil {
		return err
	}
	if r.err != nil {
		return r.err
	}
	if r.State() == Null {
		return r.Bytes().checkAlive()
	}
	if err := decodeValueInto(c.base, r, &dst.Value); err != nil {
		return err
	}
	dst.Valid = true
	return nil
}
func (c bindEncoder[T]) compileValueEncoder(t DataType) (Encoder[BindValue[T]], error) {
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.base, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) compileValueEncoder(t DataType) (Encoder[EmptyValueOf[T]], error) {
	if err := c.CheckType(t); err != nil {
		return nil, err
	}
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) compileValueDecoder(t DataType) (Decoder[EmptyValueOf[T]], error) {
	if err := c.CheckType(t); err != nil {
		return nil, err
	}
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}
func (c emptyCodec[T]) DecodeInto(r ValueReader, dst *EmptyValueOf[T]) error {
	if dst == nil {
		return typeValidation("destination", "nil empty destination")
	}
	*dst = EmptyValueOf[T]{}
	if err := c.CheckType(r.Type()); err != nil {
		return err
	}
	if r.err != nil {
		return r.err
	}
	if r.State() == Empty || (r.State() == Present && r.Bytes().Len() == 0) {
		if err := r.Bytes().checkAlive(); err != nil {
			return err
		}
		dst.Empty = true
		return nil
	}
	decoder := c.decoder
	if decoder == nil {
		decoder = c.base
	}
	return decodeValueInto(decoder, r, &dst.Value)
}

func (c namedScalar[A, B]) compileValueEncoder(t DataType) (Encoder[A], error) {
	if c.to == nil || c.from == nil {
		return nil, typeValidation("codec", "nil scalar mapping")
	}
	base, err := compileValueEncoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.encoder, c.typ = base, t
	return c, nil
}
func (c namedScalar[A, B]) compileValueDecoder(t DataType) (Decoder[A], error) {
	if c.to == nil || c.from == nil {
		return nil, typeValidation("codec", "nil scalar mapping")
	}
	base, err := compileValueDecoder(c.base, t)
	if err != nil {
		return nil, err
	}
	c.decoder, c.typ = base, t
	return c, nil
}
