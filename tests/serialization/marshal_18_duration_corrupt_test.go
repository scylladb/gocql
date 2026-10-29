//go:build unit
// +build unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/duration"
)

func TestMarshalDurationCorrupt(t *testing.T) {
	t.Parallel()

	// The suites differ in more than the entry point. gocql.Duration and
	// duration.Duration are layout-identical but distinct named types, and the
	// relationship between them is one-way: duration.Marshal takes only its
	// own, while the gocql entry point takes either, since marshalDuration
	// converts gocql.Duration and passes anything else through untouched.
	//
	// So duration.Duration alone would satisfy both suites -- and would stop
	// exercising marshalDuration's and unmarshalDuration's conversion arms, the
	// ones every caller of the gocql API goes through. Each suite keeps its own
	// type instead, and carries the constructors for it so the value sets below
	// can stay written once.
	type testSuite struct {
		name      string
		marshal   func(any) ([]byte, error)
		unmarshal func(bytes []byte, i any) error
		dur       func(gocql.Duration) any
		nilDur    any
	}

	tType := gocql.NewNativeType(4, gocql.TypeDuration)

	// Only the package API reaches the reference (**T) paths: gocql.Unmarshal
	// resolves a double pointer itself, in unmarshalNullable, before the
	// package ever sees it.
	testSuites := [2]testSuite{
		{
			name:      "serialization.duration",
			marshal:   duration.Marshal,
			unmarshal: duration.Unmarshal,
			dur:       func(d gocql.Duration) any { return duration.Duration(d) },
			nilDur:    (*duration.Duration)(nil),
		},
		{
			name: "glob",
			marshal: func(i any) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i any) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
			dur:    func(d gocql.Duration) any { return d },
			nilDur: (*gocql.Duration)(nil),
		},
	}

	for _, tSuite := range testSuites {
		dur := tSuite.dur
		marshal, unmarshal := tSuite.marshal, tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"23123113f", "sda",
					dur(gocql.Duration{Months: -1, Days: 1, Nanoseconds: 0}),
					dur(gocql.Duration{Months: -1, Days: 1, Nanoseconds: 1}),
					dur(gocql.Duration{Months: -1, Days: 1, Nanoseconds: -1}),
					dur(gocql.Duration{Months: -1, Days: -1, Nanoseconds: 1}),
					dur(gocql.Duration{Months: -1, Days: 0, Nanoseconds: 1}),
					dur(gocql.Duration{Months: 1, Days: -1, Nanoseconds: 0}),
					dur(gocql.Duration{Months: 1, Days: -1, Nanoseconds: 1}),
					dur(gocql.Duration{Months: 1, Days: -1, Nanoseconds: -1}),
					dur(gocql.Duration{Months: 1, Days: 1, Nanoseconds: -1}),
					dur(gocql.Duration{Months: 1, Days: 0, Nanoseconds: -1}),
				}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"178956971y7mo306783378w1d2562047h47m16.854775807s",
					"178956970y8mo306783378w1d2562047h47m16.854775807s",
					"178956970y7mo306783379w1d2562047h47m16.854775807s",
					"178956970y7mo306783378w2d2562047h47m16.854775807s",
					"178956970y7mo306783378w1d2562048h47m16.854775807s",
					"178956970y7mo306783378w1d2562047h48m16.854775807s",
					"178956970y7mo306783378w1d2562047h47m17.854775807s",
					"178956970y7mo306783378w1d2562047h47m16.854775808s",

					"-178956971y8mo306783378w2d2562047h47m16.854775808s",
					"-178956970y9mo306783378w2d2562047h47m16.854775808s",
					"-178956970y8mo306783379w2d2562047h47m16.854775808s",
					"-178956970y8mo306783378w3d2562047h47m16.854775808s",
					"-178956970y8mo306783378w2d2562048h47m16.854775808s",
					"-178956970y8mo306783378w2d2562047h48m16.854775808s",
					"-178956970y8mo306783378w2d2562047h47m17.854775808s",
					"-178956970y8mo306783378w2d2562047h47m16.854775809s",
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xf1\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_month1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xf1\x00\x00\x00\x01\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_month2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\xf1\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_day1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\xf1\x00\x00\x00\x01\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_day2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data_nano1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x01\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data_nano2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x01\x00\x41\xfd\xfc\x9b\xc5\xc4\x9e\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_nano3", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\xc3\x41\xfd\xfc\x9b\xc5\xc4\x9e\x00\x01"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data_nano4", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_len1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_len2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfd\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("big_data_len3", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xf0\xff\xff\xff\xfe\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len_months2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\xf0\xff\xff\xff\xfe"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len3", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len_nanos", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\xf0\xff\xff\xff\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len_days", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xf0\xff\xff\xff\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0), "", dur(gocql.Duration{}),
				}.AddVariants(mod.All...),
			}.Run("small_data_len_months", t, unmarshal)
		})
	}
}
