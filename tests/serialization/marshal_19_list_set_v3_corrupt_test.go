//go:build unit
// +build unit

package serialization_test

import (
	"math"
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
)

func TestMarshalSetListV3Corrupt(t *testing.T) {
	t.Parallel()

	elem := gocql.NewNativeType(3, gocql.TypeSmallInt, "")
	tTypes := []gocql.TypeInfo{
		gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeList, ""), nil, elem),
		gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeSet, ""), nil, elem),
	}

	// unmarshal data than bigger the normal data, does not return error.
	brokenBigData := serialization.GetTypes(mod.Values{
		[]int16{}, []*int16{},
		[]mod.Int16{}, []*mod.Int16{},
		[1]int16{}, [1]*int16{},
		[1]mod.Int16{}, [1]*mod.Int16{},
	}.AddVariants(mod.All...)...)

	brokenBigDataSlices := serialization.GetTypes(mod.Values{
		[]int16{}, []*int16{},
		[]mod.Int16{}, []*mod.Int16{},
	}.AddVariants(mod.All...)...)

	refInt32 := func(v int32) *int32 { return &v }
	refModInt32 := func(v mod.Int32) *mod.Int32 { return &v }

	for _, tType := range tTypes {
		marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
		unmarshal := func(bytes []byte, i interface{}) error {
			return gocql.Unmarshal(tType, bytes, i)
		}

		t.Run(tType.Type().String(), func(t *testing.T) {

			val := int32(math.MaxInt16 + 1)
			valc := mod.Int32(val)
			serialization.NegativeMarshalSet{
				Values: mod.Values{
					[]int32{val}, []*int32{refInt32(val)},
					[1]int32{val}, [1]*int32{refInt32(val)},
					[]mod.Int32{valc}, []*mod.Int32{refModInt32(valc)},
					[1]mod.Int32{valc}, [1]*mod.Int32{refModInt32(valc)},
				}.AddVariants(mod.All...),
			}.Run("big_vals", t, marshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x01"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
				BrokenTypes: brokenBigData,
			}.Run("big_data_elem1+", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\xff"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
				BrokenTypes: brokenBigData,
			}.Run("big_data_zeroElem1+", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x00\x01"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
				BrokenTypes: brokenBigDataSlices,
			}.Run("big_data_elem0+", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("small_data_elem_value-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("small_data_elem_value--", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01\x00"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("small_data_elem_len-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x01"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("small_data_elem-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("small_data_elems-", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: nil,
				Values: mod.Values{
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.CustomType),
			}.Run("nil_data_to_array", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.CustomType),
			}.Run("zero_data_to_array", t, unmarshal)

			serialization.NegativeUnmarshalSet{
				Data: []byte("\x00\x00\x00\x00"),
				Values: mod.Values{
					[1]int16{}, [1]*int16{},
					[1]mod.Int16{}, [1]*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("zero_elems_to_array", t, unmarshal)
		})
	}
}
