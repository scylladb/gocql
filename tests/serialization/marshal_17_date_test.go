//go:build unit
// +build unit

package serialization_test

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/date"
)

func TestMarshalsDate(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeDate)

	type testSuite struct {
		name      string
		marshal   func(any) ([]byte, error)
		unmarshal func(bytes []byte, i any) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.date",
			marshal:   date.Marshal,
			unmarshal: date.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i any) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i any) error {
				return gocql.Unmarshal(tType, bytes, i)
			},
		},
	}

	zeroDate := time.Date(-5877641, 06, 23, 0, 0, 0, 0, time.UTC).UTC()
	middleDate := time.UnixMilli(0).UTC()
	maxDate := time.Date(5881580, 07, 11, 0, 0, 0, 0, time.UTC).UTC()

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					(*uint32)(nil), (*int32)(nil), (*int64)(nil), (*string)(nil), "", (*time.Time)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					uint32(0), int32(0), zeroDate.UnixMilli(), "", zeroDate,
				}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					uint32(0), int32(0), zeroDate.UnixMilli(), zeroDate, "-5877641-06-23",
				}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00"),
				Values: mod.Values{
					uint32(0), int32(0), zeroDate.UnixMilli(), zeroDate, "-5877641-06-23",
				}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x01"),
				Values: mod.Values{
					uint32(1), int32(1), zeroDate.Add(time.Hour * 24).UnixMilli(), zeroDate.Add(time.Hour * 24), "-5877641-06-24",
				}.AddVariants(mod.All...),
			}.Run("1", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x80\x00\x00\x00"),
				Values: mod.Values{
					uint32(1 << 31), int32(math.MinInt32), middleDate.UnixMilli(), middleDate, "1970-01-01",
				}.AddVariants(mod.All...),
			}.Run("middle", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\xff\xff\xff\xff"),
				Values: mod.Values{
					uint32(math.MaxUint32), int32(-1), maxDate.UnixMilli(), maxDate, "5881580-07-11",
				}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)

			for _, tc := range []struct {
				name  string
				value time.Time
				data  string
			}{
				{"before_epoch", time.UnixMilli(-1), "\x7f\xff\xff\xff"},
				{"before_epoch_submillisecond", time.Unix(0, -1), "\x7f\xff\xff\xff"},
				{"before_epoch_noon", time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC), "\x7f\xff\xff\xff"},
				{"negative_midnight", time.UnixMilli(-86400000), "\x7f\xff\xff\xff"},
				{"before_negative_midnight", time.UnixMilli(-86400001), "\x7f\xff\xff\xfe"},
				{"negative_offset", time.Date(1960, 5, 1, 0, 0, 0, 0, time.FixedZone("west", -5*3600)), "\x7f\xff\xf2\x34"},
				{"positive_offset", time.Date(1969, 12, 31, 0, 0, 0, 0, time.FixedZone("east", 5*3600)), "\x7f\xff\xff\xfe"},
				{"minimum_date_nonmidnight", zeroDate.Add(time.Millisecond), "\x00\x00\x00\x00"},
				{"epoch", middleDate, "\x80\x00\x00\x00"},
				{"after_epoch_noon", time.Date(1970, 1, 1, 12, 0, 0, 0, time.UTC), "\x80\x00\x00\x00"},
			} {
				serialization.PositiveSet{
					Data: []byte(tc.data),
					Values: mod.Values{
						tc.value, tc.value.UnixMilli(),
					}.AddVariants(mod.All...),
				}.Run(tc.name, t, marshal, nil)
			}
		})
	}
}
