package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
	"time"
)

func TestMarshalTimestampMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

	cases := &unmarshal.Group{
		Name: tType.Type().String(),
		Func: func(bytes []byte, i interface{}) error {
			return Unmarshal(tType, bytes, i)
		},
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name:         "big_data",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:         "small_data1",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:         "small_data2",
				Data:         []byte("\x00"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0), time.Time{}},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}

	cases.Gen().RunGroup(t)
}
