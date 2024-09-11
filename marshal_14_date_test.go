package gocql

import (
	"testing"
	"time"

	"github.com/gocql/gocql/marshal/tests"
	"github.com/gocql/gocql/marshal/tests/funcs"
	"github.com/gocql/gocql/marshal/tests/mods"
)

func TestMarshalsDate(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDate}

	zeroDate := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24)

	cases := tests.Serialization{
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mods.Ref,
		Cases: []*tests.Case{
			{
				Name:           "1970-01-01_custom",
				Data:           []byte("\x80\x00\x00\x00"),
				Value:          mods.String("1970-01-01"),
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
				Mods:   mods.Non,
				Data:   nil,
				Values: tests.NilRefs("", time.Time{}),
			},
			{
				Name:   "unmarshal nil data",
				Mods:   mods.Non,
				Funcs:  funcs.ExcludeMarshal(),
				Data:   nil,
				Values: []interface{}{"", time.Time{}},
			},
			{
				Name:   "unmarshal zero data",
				Funcs:  funcs.ExcludeMarshal(),
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

	cases.GetModified().Run(t)
}

func TestMarshalDateMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDate}

	cases := tests.MustFail{
		Marshal: &tests.MarshalCases{
			Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			DefaultMods: mods.Ref,
			Cases: []*tests.MustFailMarshal{
				{
					Name:      "big_vals_string1",
					MarshalIn: "5881580-07-12",
				},
				{
					Name:      "big_vals_string2",
					MarshalIn: "9223372036854775807-07-12",
				},
				{
					Name:      "big_vals_time1",
					MarshalIn: time.Date(5881580, 7, 12, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24),
					Issue:     "the error is not returned",
				},
				{
					Name:      "big_vals_time2",
					MarshalIn: time.Date(9223372036854775807, 1, 1, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24),
					Issue:     "the error is not returned",
				},
				{
					Name:      "big_vals_int64_1",
					MarshalIn: int64(2147483648),
					Issue:     "the error is not returned",
				},
				{
					Name:      "big_vals_int64_2",
					MarshalIn: int64(9223372036854775807),
					Issue:     "the error is not returned",
				},
				{
					Name:      "small_vals_string1",
					MarshalIn: "-5877641-06-24",
				},
				{
					Name:      "small_vals_string2",
					MarshalIn: "-9223372036854775808-07-12",
				},
				{
					Name:      "small_vals_time1",
					MarshalIn: time.Date(-5877641, 6, 24, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24),
					Issue:     "the error is not returned",
				},
				{
					Name:      "small_vals_time2",
					MarshalIn: time.Date(-9223372036854775808, 6, 24, 0, 0, 0, 0, time.UTC).UTC().Round(time.Hour * 24),
					Issue:     "the error is not returned",
				},
				{
					Name:      "big_vals_int64_1",
					MarshalIn: int64(-2147483649),
					Issue:     "the error is not returned",
				},
				{
					Name:      "big_vals_int64_2",
					MarshalIn: int64(-9223372036854775808),
					Issue:     "the error is not returned",
				},
				{
					Name:      "corrupt_vals",
					MarshalIn: []interface{}{"a1580-07-11", "1970-0d-11", "02-11", "1970-11"},
				},
			},
		},
		Unmarshal: &tests.UnmarshalCases{
			Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
			DefaultMods: mods.Ref,
			Cases: []*tests.MustFailUnmarshal{
				{
					Name:         "big_data_1",
					Data:         []byte("\x00\x00\x00\x00\x00"),
					UnmarshalIns: []interface{}{"", time.Time{}},
					Issue:        "https://github.com/scylladb/gocql/issues/246",
				},
				{
					Name:         "big_data_2",
					Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
					UnmarshalIns: []interface{}{"", time.Time{}},
					Issue:        "https://github.com/scylladb/gocql/issues/246",
				},
				{
					Name:         "small_data1",
					Data:         []byte("\x00\x00\x00"),
					UnmarshalIns: []interface{}{"", time.Time{}},
					Issue:        "panic",
				},
				{
					Name:         "small_data2",
					Data:         []byte("\x00"),
					UnmarshalIns: []interface{}{"", time.Time{}},
					Issue:        "panic",
				},
			},
		},
	}

	cases.GetModified().Run(t)
}
