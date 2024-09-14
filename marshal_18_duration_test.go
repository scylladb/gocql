package gocql

import (
	"testing"

	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
)

func TestMarshalsDuration(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDuration}

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Name:           "string",
				Mods:           mod.Non,
				Data:           []byte("\x00\x00\xfe\x02\x93\x45\xdb\x82\x4f\xb6"),
				Value:          mod.String("99h99m99s99ms99µs99ns"),
				IssueMarshal:   "can not marshal custom string into duration",
				IssueUnmarshal: "can not unmarshal duration into string and custom string",
			},
			{
				Name:           "int64",
				Mods:           mod.Non,
				Data:           []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
				Value:          mod.Int64(9223372036854775807),
				IssueMarshal:   "can not marshal custom int64 into duration",
				IssueUnmarshal: "can not unmarshal duration into int64 and custom int64",
			},
			{
				Name:  "[nil]refs",
				Mods:  mod.Non,
				Data:  nil,
				Value: mod.NilRef(Duration{}),
			},
			{
				Name:  "unmarshal nil data",
				Mods:  mod.Non,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Value: Duration{},
			},
			{
				Name:  "unmarshal zero data",
				Funcs: funcs.ExcludedMarshal(),
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

	cases.Gen().RunGroup(t)
}
