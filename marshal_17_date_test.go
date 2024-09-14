package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"testing"
	"time"
)

func TestMarshalsDate(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDate}

	zeroDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24)

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Name:           "1970-01-01_custom",
				Data:           []byte("\x80\x00\x00\x00"),
				Value:          mod.String("1970-01-01"),
				IssueMarshal:   "custom strings not supported by gocql",
				IssueUnmarshal: "custom strings not supported by gocql",
			},
			{
				Name:           "1970-01-01_int64",
				Data:           []byte("\x80\x00\x00\x00"),
				Value:          int64(0),
				IssueUnmarshal: "int64 and custom int64 not supported by gocql",
			},
			{
				Name:   "[nil]refs",
				Mods:   mod.Non,
				Data:   nil,
				Values: mod.NilRefs("", time.Time{}),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mod.Non,
				Funcs:  funcs.ExcludedMarshal(),
				Data:   nil,
				Values: []interface{}{"", time.Time{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludedMarshal(),
				Data:   make([]byte, 0),
				Values: []interface{}{"", time.Time{}},
			},
			{
				Name:         "-5877641-06-23_string",
				Data:         []byte("\x00\x00\x00\x00"),
				Value:        "-5877641-06-23",
				IssueMarshal: "error: can not marshal string into date, date layout must be '2006-01-02'",
			},
			{
				Name:   "-5877641-06-23",
				Data:   []byte("\x00\x00\x00\x00"),
				Values: []interface{}{zeroDate.AddDate(0, 0, -2147483648)},
			},
			{
				Name: "1970-01-01",
				Data: []byte("\x80\x00\x00\x00"),
				Values: []interface{}{
					"1970-01-01",
					zeroDate,
				},
			},
			{
				Name:         "5881580-07-11_string",
				Data:         []byte("\xff\xff\xff\xff"),
				Value:        "5881580-07-11",
				IssueMarshal: "error: can not marshal string into date, date layout must be '2006-01-02'",
			},
			{
				Name:   "5881580-07-11",
				Data:   []byte("\xff\xff\xff\xff"),
				Values: []interface{}{zeroDate.AddDate(0, 0, 2147483647)},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
