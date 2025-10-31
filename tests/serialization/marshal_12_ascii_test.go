//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/ascii"
)

func TestMarshalAscii(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeAscii)

	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	testSuites := [2]testSuite{
		{
			name:      "serialization.int",
			marshal:   ascii.Marshal,
			unmarshal: ascii.Unmarshal,
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
				Data: nil,
				Values: mod.Values{
					([]byte)(nil),
					(*[]byte)(nil),
					(*string)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data:   nil,
				Values: mod.Values{""}.AddVariants(mod.CustomType),
			}.Run("[nil]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   make([]byte, 0),
				Values: mod.Values{make([]byte, 0), ""}.AddVariants(mod.All...),
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data:   []byte("test text string"),
				Values: mod.Values{[]byte("test text string"), "test text string"}.AddVariants(mod.All...),
			}.Run("text", t, nil, unmarshal)
		})
	}
}
