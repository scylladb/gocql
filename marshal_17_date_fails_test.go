package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"testing"
	"time"
)

func TestMarshalDateMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeDate}

	mCases := &marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Ref,
		Sets: []*marshal.Set{
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
	}
	mCases.Gen().RunGroup(t)

	uCases := &unmarshal.Group{
		Name:        tType.Type().String(),
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Ref,
		Sets: []*unmarshal.Set{
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
	}
	uCases.Gen().RunGroup(t)
}
