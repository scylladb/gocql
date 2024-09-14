package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalTexts(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeVarchar},
		{proto: 4, typ: TypeText},
		{proto: 4, typ: TypeBlob},
	}

	var nilBytes []byte = nil

	cases := serialization.Group{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mod.Default,
		Sets: []*serialization.Set{
			{
				Name:   "[nil]refs",
				Mods:   mod.Non,
				Data:   nil,
				Values: mod.NilRefs(make([]byte, 0), "", make(mod.Bytes, 0), mod.String("")),
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
				Data:   []byte("$test text string$"),
				Values: []interface{}{[]byte("$test text string$"), "$test text string$"},
			},
		},
	}

	for _, tType := range tTypes {
		tCases := cases.Copy()
		tCases.Name = tType.String()
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases.Gen().RunGroup(t)
	}
}
