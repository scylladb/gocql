//go:build all || unit
// +build all unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/boolean"
)

func TestMarshalBoolean(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeBoolean, "")

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.boolean",
			marshal:   boolean.Marshal,
			unmarshal: boolean.Unmarshal,
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

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{(*bool)(nil)}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{false}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: mod.Values{false}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x00"),
				Values: mod.Values{false}.AddVariants(mod.All...),
			}.Run("zeros", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\x01"),
				Values: mod.Values{true}.AddVariants(mod.All...),
			}.Run("[1]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("\xff"),
				Values: mod.Values{true}.AddVariants(mod.All...),
			}.Run("[255]", t, nil, unmarshal)
		})
	}
}
