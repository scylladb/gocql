package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsDuration(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDuration}

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Name:           "string",
				Mods:           mods.Non,
				Data:           []byte("\x00\x00\xfe\x02\x93\x45\xdb\x82\x4f\xb6"),
				Value:          mods.String("99h99m99s99ms99µs99ns"),
				IssueMarshal:   "can not marshal custom string into duration",
				IssueUnmarshal: "can not unmarshal duration into string and custom string",
			},
			{
				Name:           "int64",
				Mods:           mods.Non,
				Data:           []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				Value:          mods.Int64(9223372036854775807),
				IssueMarshal:   "can not marshal custom int64 into duration",
				IssueUnmarshal: "can not unmarshal duration into int64 and custom int64",
			},
			{
				Name:  "[nil]refs",
				Mods:  mods.Non,
				Data:  nil,
				Value: tests.NilRef(Duration{}),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mods.Non,
				Funcs: funcs.ExcludeMarshal(),
				Data:  nil,
				Value: Duration{},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludeMarshal(),
				Data:  make([]byte, 0),
				Value: Duration{},
			},
			{
				Name:  "zero",
				Data:  []byte("\x00\x00\x00"),
				Value: Duration{Days: 0, Months: 0, Nanoseconds: 0},
			},
			{
				Name:  "max",
				Data:  []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				Value: Duration{Days: 2147483647, Months: 2147483647, Nanoseconds: 9223372036854775807},
			},
			{
				Name:  "min",
				Data:  []byte("\xf0\xff\xff\xff\xff\xf0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Value: Duration{Days: -2147483648, Months: -2147483648, Nanoseconds: -9223372036854775808},
			},
		},
	}

	cases.GetModified().Run(t)
}

func TestMarshalDurationMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDuration}

	cases := &tests.UnmarshalCases{
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mods.Ref,
		Cases: []*tests.MustFailUnmarshal{
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

	cases.GetModified().Run(t)
}
