package gocql

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsTime(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

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
				Values: tests.NilRefs(int64(0), mods.Int16(0), time.Duration(0)),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Custom,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludeMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "zeros",
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: []interface{}{int64(0), time.Duration(0)},
			},
			{
				Name:   "86399999999999",
				Data:   []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff"),
				Values: []interface{}{int64(86399999999999), time.Duration(86399999999999)},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalTimeMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailMarshal{
				{
					Name:       "big_vals1",
					MarshalIns: []interface{}{int64(86300000000001), time.Duration(86300000000001)},
					Issue:      "the error is not returned",
				},
				{
					Name:       "big_vals2",
					MarshalIns: []interface{}{int64(math.MaxInt64), time.Duration(math.MaxInt64)},
					Issue:      "the error is not returned",
				},
				{
					Name:       "small_vals1",
					MarshalIns: []interface{}{int64(-1), time.Duration(-1)},
					Issue:      "the error is not returned",
				},
				{
					Name:       "small_vals2",
					MarshalIns: []interface{}{int64(math.MinInt64), time.Duration(math.MinInt64)},
					Issue:      "the error is not returned",
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Default,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name:         "big_data",
					Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
					UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
					Issue:        "https://github.com/scylladb/gocql/issues/246",
				},
				{
					Name:         "small_data1",
					Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
					UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
					Issue:        "https://github.com/scylladb/gocql/issues/252",
				},
				{
					Name:         "small_data2",
					Data:         []byte("\x00"),
					UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
					Issue:        "https://github.com/scylladb/gocql/issues/252",
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
