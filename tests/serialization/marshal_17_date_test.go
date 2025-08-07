//go:build unit
// +build unit

package serialization_test

import (
	"math"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/date"
)

func TestMarshalsDate(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeDate, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.date",
			marshal:   date.Marshal,
			unmarshal: date.Unmarshal,
		},
		{
			name: "glob",
			marshal: func(i interface{}) ([]byte, error) {
				return gocql.Marshal(tType, i)
			},
			unmarshal: func(bytes []byte, i interface{}) error {
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
		})
	}
}
