//go:build unit
// +build unit

package serialization_test

import (
	"math"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
	"github.com/scylladb/gocql/v2/serialization/float"
)

func TestMarshalFloat(t *testing.T) {
	t.Parallel()

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeFloat, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.float",
			marshal:   float.Marshal,
			unmarshal: float.Unmarshal,
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
				Values: mod.Values{(*float32)(nil)}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{float32(0)}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: mod.Values{float32(0)}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x00"),
				Values: mod.Values{float32(0)}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\x7f\xff\xff"),
				Values: mod.Values{float32(math.MaxFloat32)}.AddVariants(mod.All...),
			}.Run("max", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff\x7f\xff\xff"),
				Values: mod.Values{float32(-math.MaxFloat32)}.AddVariants(mod.All...),
			}.Run("min", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00\x00\x00\x01"),
				Values: mod.Values{float32(math.SmallestNonzeroFloat32)}.AddVariants(mod.All...),
			}.Run("smallest_pos", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x80\x00\x00\x01"),
				Values: mod.Values{float32(-math.SmallestNonzeroFloat32)}.AddVariants(mod.All...),
			}.Run("smallest_neg", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\x80\x00\x00"),
				Values: mod.Values{float32(math.Inf(1))}.AddVariants(mod.All...),
			}.Run("inf+", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff\x80\x00\x00"),
				Values: mod.Values{float32(math.Inf(-1))}.AddVariants(mod.All...),
			}.Run("inf-", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x7f\xc0\x00\x00"),
				Values: mod.Values{float32(math.NaN())}.AddVariants(mod.All...),
			}.Run("nan", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x40\x49\x0f\xdb"),
				Values: mod.Values{float32(3.14159265)}.AddVariants(mod.All...),
			}.Run("pi", t, marshal, unmarshal)
		})
	}
}
