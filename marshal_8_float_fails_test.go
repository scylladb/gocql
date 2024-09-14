package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
)

func TestMarshalFloatMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeFloat}

	cases := &unmarshal.Group{
		Name: tType.Type().String(),
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
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

	cases.Gen().RunGroup(t)
}
