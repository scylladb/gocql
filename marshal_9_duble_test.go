package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"math"
	"testing"
)

func TestMarshalDouble(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDouble}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:  "[nil]refs",
				Mods:  mod.Custom,
				Data:  nil,
				Value: mod.NilRef(float64(0)),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Value: float64(0),
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Value: float64(0),
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Value: float64(0),
			},
			{
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
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

	cases.Gen().RunGroup(t)
}
