package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
)

func TestMarshalBooleanMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeBoolean}
	tCase := unmarshal.Set{
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		Name:         "big data",
		Data:         []byte("\x80\x00"),
		UnmarshalIns: []interface{}{false},
		Mods:         mod.Default,
		Issue:        "https://github.com/scylladb/gocql/issues/246",
	}

	tCase.Gen().RunGroup(t)
}
