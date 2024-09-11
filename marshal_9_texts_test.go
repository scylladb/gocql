package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalTexts(t *testing.T) {
	tTypes := []NativeType{
		{proto: 4, typ: TypeVarchar},
		{proto: 4, typ: TypeText},
		{proto: 4, typ: TypeBlob},
	}

	var nilBytes []byte = nil

	cases := tests.Serialization{
		Funcs:       funcs.Default(nil, nil),
		DefaultMods: mods.Default,
		Cases: []*tests.Case{
			{
				Name:   "[nil]refs",
				Mods:   mods.Non,
				Data:   nil,
				Values: tests.NilRefs(make([]byte, 0), "", make(mods.Bytes, 0), mods.String("")),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Custom,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{nilBytes, ""},
			},
			{
				Name:           "unmarshal zero data to bytes",
				Funcs:          funcs.ExcludeMarshal(),
				Data:           make([]byte, 0),
				Value:          nilBytes,
				IssueUnmarshal: "for not custom types - unmarshalls data with zero bytes into value with nil bytes slice",
			},
			{
				Name:  "unmarshal zero data to string",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: "",
			},
			{
				Name:   "marshal zero data",
				Funcs:  funcs.ExcludeUnmarshal(),
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
		tCases.Funcs.Marshal = func(i interface{}) ([]byte, error) { return Marshal(tType, i) }
		tCases.Funcs.Unmarshal = func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) }

		tCases = tCases.GetModified()
		t.Run(tType.String(), func(t *testing.T) {
			tCases.Run(t)
		})
	}
}

func TestMarshalAscii(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeAscii}

	var nilBytes []byte = nil

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
				Values: tests.NilRefs(make([]byte, 0), ""),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Custom,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{nilBytes, ""},
			},
			{
				Name:           "unmarshal zero data to bytes",
				Funcs:          funcs.ExcludeMarshal(),
				Data:           make([]byte, 0),
				Value:          nilBytes,
				IssueUnmarshal: "for not custom types - unmarshalls data with zero bytes into value with nil bytes slice",
			},
			{
				Name:  "unmarshal zero data to string",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: "",
			},
			{
				Name:   "marshal zero data",
				Funcs:  funcs.ExcludeUnmarshal(),
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

	cases.GetModified().Run(t)
}

func TestMarshalAsciiMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeAscii}

	cases := &tests.UnmarshalCases{
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mods.Default,
		Cases: []*tests.MustFailUnmarshal{
			{
				Name:        "corrupt_data_str1",
				Data:        []byte{255},
				UnmarshalIn: new(string),
				Issue:       "the error is not returned",
			},
			{
				Name:        "corrupt_data_str2",
				Data:        []byte{127, 255, 127},
				UnmarshalIn: "",
				Issue:       "the error is not returned",
			},
			{
				Name:        "corrupt_data_bytes1",
				Data:        []byte{255},
				UnmarshalIn: "",
				Issue:       "the error is not returned",
			},
			{
				Name:        "corrupt_data_bytes2",
				Data:        []byte{127, 255, 127},
				UnmarshalIn: "",
				Issue:       "the error is not returned",
			},
		},
	}

	cases.GetModified().Run(t)
}
