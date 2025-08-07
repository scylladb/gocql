//go:build unit
// +build unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/date"
)

func TestMarshalDateCorrupt(t *testing.T) {
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

	for _, tSuite := range testSuites {
		marshal := tSuite.marshal
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {
			t.Parallel()

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					time.Date(5881580, 7, 12, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(5881580, 8, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(5881581, 7, 11, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(5883581, 12, 20, 0, 0, 0, 0, time.UTC).UnixMilli(),
					"5881580-07-12", "5881580-08-11", "5881581-07-11", "9223372036854775807-07-12",
					time.Date(5881580, 7, 12, 0, 0, 0, 0, time.UTC).UTC(),
					time.Date(5881580, 8, 11, 0, 0, 0, 0, time.UTC).UTC(),
					time.Date(5881581, 7, 11, 0, 0, 0, 0, time.UTC).UTC(),
					time.Date(5883581, 12, 20, 0, 0, 0, 0, time.UTC).UTC(),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					time.Date(-5877641, 06, 22, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(-5877641, 05, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(-5877642, 06, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					time.Date(-5887641, 06, 23, 0, 0, 0, 0, time.UTC).UnixMilli(),
					"-5877641-06-22", "-5877641-05-23", "-5877642-06-23", "-9223372036854775807-07-12",
					time.Date(-5877641, 06, 22, 0, 0, 0, 0, time.UTC),
					time.Date(-5877641, 05, 23, 0, 0, 0, 0, time.UTC),
					time.Date(-5877642, 06, 23, 0, 0, 0, 0, time.UTC),
					time.Date(-5887641, 06, 23, 0, 0, 0, 0, time.UTC),
				}.AddVariants(mod.All...),
			}.Run("small_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					"a1580-07-11", "1970-0d-11", "02-11", "1970-11",
				}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Time{}, "",
				}.AddVariants(mod.All...),
			}.Run("big_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Time{}, "",
				}.AddVariants(mod.All...),
			}.Run("big_data2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Time{}, "",
				}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int64(0), time.Time{}, "",
				}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
