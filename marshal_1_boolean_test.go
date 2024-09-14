package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalBoolean(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeBoolean}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:   "(nil)refs",
				Mods:   mod.Non,
				Data:   nil,
				Values: mod.NilRefs(false, mod.Bool(false)),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludedMarshal(),
				Mods:  mod.Custom,
				Data:  nil,
				Value: false,
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Value: false,
			},
			{
				Name:  "unmarshal ff",
				Funcs: funcs.ExcludedMarshal(),
				Data:  []byte("\xff"),
				Value: true,
			},
			{
				Name:  "zeros",
				Data:  []byte("\x00"),
				Value: false,
			},
			{
				Data:  []byte("\x01"),
				Value: true,
			},
		},
	}

	cases.Gen().RunGroup(t)
}
