//go:build unit
// +build unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/timestamp"
)

func TestMarshalTimestampCorrupt(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeTimestamp, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.timestamp",
			marshal:   timestamp.Marshal,
			unmarshal: timestamp.Unmarshal,
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

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					time.Date(292278994, 8, 17, 7, 12, 55, 808*1000000, time.UTC),
					time.Date(292278994, 8, 17, 7, 12, 56, 807*1000000, time.UTC),
					time.Date(292278994, 8, 17, 7, 13, 55, 807*1000000, time.UTC),
					time.Date(292278994, 8, 17, 8, 12, 55, 807*1000000, time.UTC),
					time.Date(292278994, 8, 18, 7, 12, 55, 807*1000000, time.UTC),
					time.Date(292278994, 9, 17, 7, 12, 55, 807*1000000, time.UTC),
					time.Date(292278995, 8, 17, 7, 12, 55, 807*1000000, time.UTC),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					time.Date(-292275055, 5, 16, 16, 47, 4, 191*1000000, time.UTC),
					time.Date(-292275055, 5, 16, 16, 47, 3, 192*1000000, time.UTC),
					time.Date(-292275055, 5, 16, 16, 46, 4, 192*1000000, time.UTC),
					time.Date(-292275055, 5, 16, 15, 47, 4, 192*1000000, time.UTC),
					time.Date(-292275055, 5, 15, 16, 47, 4, 192*1000000, time.UTC),
					time.Date(-292275055, 4, 16, 16, 47, 4, 192*1000000, time.UTC),
					time.Date(-292275056, 5, 16, 16, 47, 4, 192*1000000, time.UTC),
				}.AddVariants(mod.All...),
			}.Run("small_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int64(0), time.Time{},
				}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
