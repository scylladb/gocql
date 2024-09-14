package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
)

func TestMarshalDurationMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDuration}

	uCases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Ref,
		Sets: []*unmarshal.Set{
			{
				Name:        "big_data_1",
				Data:        []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "big_data_2",
				Data:        []byte("\xf0\xff\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "big_data_3",
				Data:        []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "big_data_4",
				Data:        []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe\x00"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "big_data_5",
				Data:        []byte("\xf0\xff\xff\xff\xff\xf0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x00"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "big_data_6",
				Data:        []byte("\x00\x00\x00\x00"),
				UnmarshalIn: Duration{},
				Issue:       "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:        "small_data1",
				Data:        []byte("\xf0\xff\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe"),
				UnmarshalIn: Duration{},
			},
			{
				Name:        "small_data2",
				Data:        []byte("\xf0\xff\xff\xff\xfe"),
				UnmarshalIn: Duration{},
			},
			{
				Name:        "small_data3",
				Data:        []byte("\x00"),
				UnmarshalIn: Duration{},
			},
		},
	}

	uCases.Gen().RunGroup(t)
}
