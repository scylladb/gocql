package serialization_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/double"
)

func TestMarshalDoubleCorrupt(t *testing.T) {
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
		unmarshal := tSuite.unmarshal

		t.Run(tSuite.name, func(t *testing.T) {

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80\x00\x00\x00\x00\x00\x00\x00\x00"),
				Values: mod.Values{float64(0)}.AddVariants(mod.All...),
			}.Run("big_data", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80"),
				Values: mod.Values{float64(0)}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x80\x00\x00\x00"),
				Values: mod.Values{float64(0)}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
