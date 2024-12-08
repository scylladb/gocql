package serialization_test

import (
	"gopkg.in/inf.v0"
	"testing"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
	"github.com/gocql/gocql/serialization/decimal"
)

func TestMarshalDecimalCorrupt(t *testing.T) {
	type testSuite struct {
		name      string
		marshal   func(interface{}) ([]byte, error)
		unmarshal func(bytes []byte, i interface{}) error
	}

	tType := gocql.NewNativeType(4, gocql.TypeDecimal, "")

	testSuites := [2]testSuite{
		{
			name:      "serialization.decimal",
			marshal:   decimal.Marshal,
			unmarshal: decimal.Unmarshal,
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
			serialization.NegativeMarshalSet{
				Values: mod.Values{"1s2", "1s", "-1s", ",1", "0,1"}.AddVariants(mod.All...),
			}.Run("corrupt_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x00\x00\x00\x7f"),
				Values: mod.Values{*inf.NewDec(0, 0), ""}.AddVariants(mod.All...),
			}.Run("corrupt_data+", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x00\x00\xff\x80"),
				Values: mod.Values{*inf.NewDec(0, 0), ""}.AddVariants(mod.All...),
			}.Run("corrupt_data-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00\x00\x00\x00"),
				Values: mod.Values{*inf.NewDec(0, 0), ""}.AddVariants(mod.All...),
			}.Run("small_data1", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data:   []byte("\x00"),
				Values: mod.Values{*inf.NewDec(0, 0), ""}.AddVariants(mod.All...),
			}.Run("small_data2", t, unmarshal)
		})
	}
}
