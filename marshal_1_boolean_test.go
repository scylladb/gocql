package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalBoolean(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeBoolean}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:   "(nil)refs",
				Mods:   mods.Non,
				Data:   nil,
				Values: tests.NilRefs(false, mods.Bool(false)),
			},
			{
				Name:  "unmarshal nil data",
				Funcs: funcs.ExcludeMarshal(),
				Mods:  mods.Custom,
				Data:  nil,
				Value: false,
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: false,
			},
			{
				Name:  "unmarshal ff",
				Funcs: funcs.ExcludeMarshal(),
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

	cases.GetModified().Run(t)
}

func TestMarshalBooleanMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeBoolean}

	tCase := &tests.MustFailUnmarshal{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		Name:         "big data",
		Data:         []byte("\x80\x00"),
		UnmarshalIns: []interface{}{false},
		Mods:         mods.Default,
		Issue:        "https://github.com/scylladb/gocql/issues/246",
	}

	tCase.GetModified().Run(t)
}
