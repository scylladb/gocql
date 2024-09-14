package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
)

func TestMarshalDoubleMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDouble}

	cases := &unmarshal.Group{
		Name: tType.Type().String(),
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name:        "big_data",
				Data:        []byte("\x7f\xef\xff\xff\xff\xff\xff\xff\xff"),
				UnmarshalIn: float64(0),
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "small_data",
				Data:        []byte("\x7f\xef\xff\xff\xff\xff\xff"),
				UnmarshalIn: float64(0),
				Issue:       "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}

	cases.Gen().RunGroup(t)
}
