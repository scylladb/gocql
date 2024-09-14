package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"math"
	"testing"
)

func TestMarshalFloat(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeFloat}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:   "[nil]refs",
				Mods:   mod.Custom,
				Data:   nil,
				Values: mod.NilRefs(float32(0), mod.Float32(0)),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mod.Custom,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Value: float32(0),
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
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

	cases.Gen().RunGroup(t)
}
