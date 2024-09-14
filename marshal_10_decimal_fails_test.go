package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"gopkg.in/inf.v0"
	"testing"
)

func TestMarshalDecimalMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDecimal}

	cases := &unmarshal.Group{
		Name: tType.Type().String(),
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mod.Ref,
		Sets: []*unmarshal.Set{
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

	cases.Gen().RunGroup(t)
}
