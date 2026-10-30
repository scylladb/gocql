// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package codec

import (
	"errors"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"math"
	"math/bits"
	"time"
)

// Date counts calendar days relative to 1970-01-01.

type Date = cqltype.Date
type TimeOfDay = cqltype.TimeOfDay
type Timestamp = cqltype.Timestamp

const nanosecondsPerDay int64 = 24 * 60 * 60 * 1_000_000_000

func DateFromTime(t time.Time) (Date, error)           { return cqltype.DateFromTime(t) }
func TimestampFromTime(t time.Time) (Timestamp, error) { return cqltype.TimestampFromTime(t) }

var DateCodec Codec[Date] = scalarCodec[Date]{kind: cqltype.TypeDate,
	encode: func(w *ValueWriter, v Date) error { return w.Uint32(uint32(v) ^ 0x80000000) },
	decode: func(r ValueReader) (Date, error) {
		c, e := fixedCursor(r, 4)
		if e != nil {
			return 0, e
		}
		v, e := c.Uint32()
		return Date(int32(v ^ 0x80000000)), e
	},
}
var TimeCodec Codec[TimeOfDay] = scalarCodec[TimeOfDay]{kind: cqltype.TypeTime,
	encode: func(w *ValueWriter, v TimeOfDay) error {
		if int64(v) < 0 || int64(v) >= nanosecondsPerDay {
			return errors.New("gocql: time outside one day")
		}
		return w.Uint64(uint64(v))
	},
	decode: func(r ValueReader) (TimeOfDay, error) {
		c, e := fixedCursor(r, 8)
		if e != nil {
			return 0, e
		}
		v, e := c.Uint64()
		if e != nil {
			return 0, e
		}
		if v >= uint64(nanosecondsPerDay) {
			return 0, errors.New("gocql: time outside one day")
		}
		return TimeOfDay(v), nil
	},
}
var TimestampCodec Codec[Timestamp] = integerCodec[Timestamp](cqltype.TypeTimestamp, 8)
var DurationCodec Codec[cqltype.Duration] = scalarCodec[cqltype.Duration]{kind: cqltype.TypeDuration, encode: encodeDuration, decode: decodeDuration}

func durationSignsValid(v cqltype.Duration) bool {
	hasNegative := v.Months < 0 || v.Days < 0 || v.Nanoseconds < 0
	hasPositive := v.Months > 0 || v.Days > 0 || v.Nanoseconds > 0
	return !(hasNegative && hasPositive)
}
func encodeDuration(w *ValueWriter, v cqltype.Duration) error {
	if !durationSignsValid(v) {
		return errors.New("gocql: duration components must have compatible signs")
	}
	if err := writeSignedVInt(w, int64(v.Months)); err != nil {
		return err
	}
	if err := writeSignedVInt(w, int64(v.Days)); err != nil {
		return err
	}
	return writeSignedVInt(w, v.Nanoseconds)
}
func writeSignedVInt(w *ValueWriter, v int64) error {
	return scalarWriteUnsignedVInt(w, (uint64(v)<<1)^uint64(v>>63))
}
func scalarWriteUnsignedVInt(w *ValueWriter, v uint64) error {
	if v < 128 {
		return w.Uint8(uint8(v))
	}
	extra := (bits.Len64(v) - 1) / 7
	if extra > 8 {
		extra = 8
	}
	if extra == 8 {
		if err := w.Uint8(0xff); err != nil {
			return err
		}
	} else {
		prefix := byte(0xff << uint(8-extra))
		if err := w.Uint8(prefix | byte(v>>uint(extra*8))); err != nil {
			return err
		}
	}
	for i := extra - 1; i >= 0; i-- {
		if err := w.Uint8(byte(v >> uint(i*8))); err != nil {
			return err
		}
	}
	return nil
}

// scalarVInt owns the native scalar conversion shared by buffered and streaming
// readers. I/O-specific loops feed bytes without allocating cursor adapters.
type scalarVInt struct {
	remaining int
	value     uint64
}

func newScalarVInt(first byte) scalarVInt {
	extra := bits.LeadingZeros8(^first)
	return scalarVInt{remaining: extra, value: uint64(first & byte(0xff>>uint(extra)))}
}
func (v *scalarVInt) add(b byte)   { v.value = v.value<<8 | uint64(b); v.remaining-- }
func (v scalarVInt) signed() int64 { return int64(v.value>>1) ^ -int64(v.value&1) }
func scalarReadUnsignedVInt(c *ValueCursor) (uint64, error) {
	first, err := c.Uint8()
	if err != nil {
		return 0, err
	}
	v := newScalarVInt(first)
	for v.remaining > 0 {
		next, err := c.Uint8()
		if err != nil {
			return 0, err
		}
		v.add(next)
	}
	return v.value, nil
}
func readSignedVInt(c *ValueCursor) (int64, error) {
	value, err := scalarReadUnsignedVInt(c)
	return int64(value>>1) ^ -int64(value&1), err
}
func decodeDuration(r ValueReader) (cqltype.Duration, error) {
	c := r.cursorValue()
	months, e := readSignedVInt(&c)
	if e != nil {
		return cqltype.Duration{}, e
	}
	days, e := readSignedVInt(&c)
	if e != nil {
		return cqltype.Duration{}, e
	}
	nanos, e := readSignedVInt(&c)
	if e != nil {
		return cqltype.Duration{}, e
	}
	if e := c.RequireEnd(); e != nil {
		return cqltype.Duration{}, e
	}
	return durationFromComponents(months, days, nanos)
}

// durationFromComponents is shared by buffered and incremental decoding.
func durationFromComponents(months, days, nanos int64) (cqltype.Duration, error) {
	if months < math.MinInt32 || months > math.MaxInt32 || days < math.MinInt32 || days > math.MaxInt32 {
		return cqltype.Duration{}, errors.New("gocql: duration months or days overflow int32")
	}
	v := cqltype.Duration{Months: int32(months), Days: int32(days), Nanoseconds: nanos}
	if !durationSignsValid(v) {
		return cqltype.Duration{}, errors.New("gocql: duration components must have compatible signs")
	}
	return v, nil
}
