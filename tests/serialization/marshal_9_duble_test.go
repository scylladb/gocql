//go:build unit
// +build unit

package serialization_test

import (
	"math"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/double"
)

func TestMarshalDouble(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeDouble, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.double",
			marshal:   double.Marshal,
			unmarshal: double.Unmarshal,
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

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{(*float64)(nil)}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{float64(0)}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: mod.Values{float64(0)}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{float64(0)}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\xef\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{float64(math.MaxFloat64)}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff\xef\xff\xff\xff\xff\xff\xff"),
				Values: mod.Values{float64(-math.MaxFloat64)}.AddVariants(mod.All...),
			}.Run("min", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x00\x00\x00\x00\x01"),
				Values: mod.Values{float64(math.SmallestNonzeroFloat64)}.AddVariants(mod.All...),
			}.Run("smallest_pos", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x80\x00\x00\x00\x00\x00\x00\x01"),
				Values: mod.Values{float64(-math.SmallestNonzeroFloat64)}.AddVariants(mod.All...),
			}.Run("smallest_neg", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\xf0\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{float64(math.Inf(1))}.AddVariants(mod.All...),
			}.Run("inf+", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff\xf0\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{float64(math.Inf(-1))}.AddVariants(mod.All...),
			}.Run("inf-", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\xf8\x00\x00\x00\x00\x00\x01"),
				Values: mod.Values{float64(math.NaN())}.AddVariants(mod.All...),
			}.Run("nan", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x40\x09\x21\xfb\x53\xc8\xd4\xf1"),
				Values: mod.Values{float64(3.14159265)}.AddVariants(mod.All...),
			}.Run("pi", t, marshal, unmarshal)
		})
	}
}
