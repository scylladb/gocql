package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
)

func TestMarshalAsciiMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeAscii}

	cases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
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

	cases.Gen().RunGroup(t)
}
