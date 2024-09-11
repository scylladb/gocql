package gocql

import (
	"testing"
	"time"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsTimestamp(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTimestamp}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:   "[nil]refs",
				Mods:   mods.Non,
				Data:   nil,
				Values: tests.NilRefs(int64(0), mods.Int16(0), time.Duration(0), time.Time{}),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Custom,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{int64(0), time.Duration(0), time.Time{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludeMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{int64(0), time.Duration(0), time.Time{}},
			},
			{
				Name:   "zeros",
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{int64(0), time.Duration(0), time.UnixMilli(0).UTC()},
			},
			{
				Name: "9223372036854775807",
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: []interface{}{
					int64(9223372036854775807),
					time.Duration(9223372036854775807),
					time.UnixMilli(9223372036854775807).UTC(),
				},
			},
			{
				Name: "-9223372036854775808",
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{
					int64(-9223372036854775808),
					time.Duration(-9223372036854775808),
					time.UnixMilli(-9223372036854775808).UTC(),
				},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalTimestampMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

	cases := &tests.UnmarshalCases{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mods.Default,
		Cases: []*tests.MustFailUnmarshal{
			{
				Name:         "big_data",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:         "small_data1",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:         "small_data2",
				Data:         []byte("\x00"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}

	cases.GetModified().Run(t)
}
