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
	"github.com/scylladb/gocql/v2/serialization/cqltime"
)

func TestMarshalTimeCorrupt(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeTime, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.cqltime",
			marshal:   cqltime.Marshal,
			unmarshal: cqltime.Unmarshal,
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

			// marshal, unmarshal of all supported `go types` does not return an error on all type of corruption.
			//brokenTypes := serialization.GetTypes(int64(0), (*int64)(nil), mod.Int64(0), (*mod.Int64)(nil), time.Duration(0), (*time.Duration)(nil))

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					int64(86400000000000), time.Duration(86400000000000),
					int64(86500000000000), time.Duration(86500000000000),
					int64(math.MaxInt64), time.Duration(math.MaxInt64),
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeMarshalSet{
				Values: mod.Values{
					int64(-1), time.Duration(-1),
					int64(math.MinInt8), time.Duration(math.MinInt8),
					int64(math.MinInt16), time.Duration(math.MinInt16),
					int64(math.MinInt32), time.Duration(math.MinInt32),
					int64(math.MinInt64), time.Duration(math.MinInt64),
				}.AddVariants(mod.All...),
			}.Run("small_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data_len", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("small_data_len1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("small_data_len2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x4e\x94\x91\x4f\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("big_data2", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{
					int64(0), time.Duration(0),
				}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
