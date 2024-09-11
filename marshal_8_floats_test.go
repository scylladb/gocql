package gocql

import (
	"gopkg.in/inf.v0"
	"math"
	"math/big"
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalFloat(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeFloat}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:   "[nil]refs",
				Mods:   mods.Custom,
				Data:   nil,
				Values: tests.NilRefs(float32(0), mods.Float32(0)),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Value: float32(0),
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: float32(0),
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00\x00\x00\x00"),
				Value: float32(0),
			},
			{Data: []byte("\x7f\x7f\xff\xff"), Value: float32(math.MaxFloat32)},
			{Data: []byte("\x00\x00\x00\x01"), Value: float32(math.SmallestNonzeroFloat32)},
			{Data: []byte("\x7f\x80\x00\x00"), Value: float32(math.Inf(1))},
			{Data: []byte("\xff\x80\x00\x00"), Value: float32(math.Inf(-1))},
			{Data: []byte("\x7f\xc0\x00\x00"), Value: float32(math.NaN())},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalFloatMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeFloat}

	cases := &tests.UnmarshalCases{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mods.Default,
		Cases: []*tests.MustFailUnmarshal{
			{
				Name:        "big_data",
				Data:        []byte("\x7f\x7f\xff\xff\xff"),
				UnmarshalIn: float32(0),
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "small_data",
				Data:        []byte("\x7f\x7f\xff"),
				UnmarshalIn: float32(0),
				Issue:       "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalDouble(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDouble}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:  "[nil]refs",
				Mods:  mods.Custom,
				Data:  nil,
				Value: tests.NilRef(float64(0)),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Value: float64(0),
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: float64(0),
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Value: float64(0),
			},
			{
				Mods:  mods.Custom,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Value: float64(0),
			},
			{Data: []byte("\x7f\xef\xff\xff\xff\xff\xff\xff"), Value: float64(math.MaxFloat64)},
			{Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x01"), Value: float64(math.SmallestNonzeroFloat64)},
			{Data: []byte("\x7f\xf0\x00\x00\x00\x00\x00\x00"), Value: float64(math.Inf(1))},
			{Data: []byte("\xff\xf0\x00\x00\x00\x00\x00\x00"), Value: float64(math.Inf(-1))},
			{Data: []byte("\x7f\xf8\x00\x00\x00\x00\x00\x01"), Value: float64(math.NaN())},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalDoubleMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDouble}

	cases := &tests.UnmarshalCases{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mods.Default,
		Cases: []*tests.MustFailUnmarshal{
			{
				Name:        "big_data",
				Data:        []byte("\x7f\xef\xff\xff\xff\xff\xff\xff\xff"),
				UnmarshalIn: float64(0),
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "small_data",
				Data:        []byte("\x7f\xef\xff\xff\xff\xff\xff"),
				UnmarshalIn: float64(0),
				Issue:       "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalDecimal(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDecimal}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Name:  "[nil]refs",
				Mods:  mods.Non,
				Data:  nil,
				Value: tests.NilRef(inf.Dec{}),
			},
			{
				Name:           "unmarshal nil data",
				Mods:           mods.Non,
				Funcs:          funcs.ExcludeMarshal(),
				Data:           nil,
				Value:          inf.Dec{},
				IssueUnmarshal: "error inf.Dec needs at least 4 bytes, while value has only 0",
			},
			{
				Name:           "unmarshal zero data",
				Funcs:          funcs.ExcludeMarshal(),
				Data:           make([]byte, 0),
				Value:          inf.Dec{},
				IssueUnmarshal: "error inf.Dec needs at least 4 bytes, while value has only 0",
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00\x00\x00\x00\x00"),
				Value: inf.Dec{},
			},
			{
				Data:  []byte("\x7f\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Value: *inf.NewDec(int64(math.MaxInt64), inf.Scale(int32(math.MaxInt32))),
			},
			{
				Data:  []byte("\x80\x00\x00\x00\x80\x00\x00\x00\x00\x00\x00\x00"),
				Value: *inf.NewDec(int64(math.MinInt64), inf.Scale(int32(math.MinInt32))),
			},
			{
				Data:  []byte("\x00\x00\x00\x00\x00"),
				Value: *inf.NewDec(int64(0), inf.Scale(0)),
			},
			{
				Data:  []byte("\x7f\xff\xff\xff\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Value: *inf.NewDecBig(big.NewInt(int64(math.MaxInt64)), inf.Scale(int32(math.MaxInt32))),
			},
		},
	}
	cases.GetModified().Run(t)
}

func TestMarshalDecimalMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDecimal}

	cases := &tests.UnmarshalCases{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mods.Ref,
		Cases: []*tests.MustFailUnmarshal{
			{
				Name:        "big_data",
				Data:        []byte("\x00\x00\x00\x00\xff\x00\xff"),
				UnmarshalIn: inf.Dec{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "small_data1",
				Data:        []byte("\x00"),
				UnmarshalIn: inf.Dec{},
			},
			{
				Name:        "small_data2",
				Data:        []byte("\x00\x00\x00"),
				UnmarshalIn: inf.Dec{},
			},
		},
	}

	cases.GetModified().Run(t)
}
