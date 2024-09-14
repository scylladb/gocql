package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalAscii(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeAscii}

	var nilBytes []byte = nil

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
				Values: mod.NilRefs(make([]byte, 0), ""),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mod.Custom,
				Funcs:  funcs.ExcludedMarshal(),
				Data:   nil,
				Values: []interface{}{nilBytes, ""},
			},
			{
				Name:           "unmarshal zero data to bytes",
				Funcs:          funcs.ExcludedMarshal(),
				Data:           make([]byte, 0),
				Value:          nilBytes,
				IssueUnmarshal: "for not custom types - unmarshalls data with zero bytes into value with nil bytes slice",
			},
			{
				Name:  "unmarshal zero data to string",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Value: "",
			},
			{
				Name:   "marshal zero data",
				Funcs:  funcs.ExcludedUnmarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{make([]byte, 0), ""},
			},
			{
				Name:   "text",
				Data:   []byte("test text string"),
				Values: []interface{}{[]byte("test text string"), "test text string"},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
