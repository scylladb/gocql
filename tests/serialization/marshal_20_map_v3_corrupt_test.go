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

func TestMarshalMapV3Corrupt(t *testing.T) {
	t.Parallel()

	elem := gocql.NewNativeType(3, gocql.TypeSmallInt, "")
	tType := gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeMap, ""), elem, elem)

	//unmarshal data than bigger the normal data, does not return error.
	brokenBigData := serialization.GetTypes(mod.Values{
		make(map[int16]int16), make(map[int16]*int16),
		make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
	}.AddVariants(mod.All...)...)

	refInt32 := func(v int32) *int32 { return &v }
	refModInt32 := func(v mod.Int32) *mod.Int32 { return &v }

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	val := int32(math.MaxInt16 + 1)
	valc := mod.Int32(val)
	serialization.NegativeMarshalSet{
		Values: mod.Values{
			map[int32]int32{val: val}, map[int32]int32{val: 0}, map[int32]int32{0: val},
			map[int32]*int32{val: refInt32(val)}, map[int32]*int32{val: refInt32(0)}, map[int32]*int32{0: refInt32(val)},
			map[mod.Int32]mod.Int32{valc: valc}, map[mod.Int32]mod.Int32{valc: 0}, map[mod.Int32]mod.Int32{0: valc},
			map[mod.Int32]*mod.Int32{valc: refModInt32(valc)}, map[mod.Int32]*mod.Int32{valc: refModInt32(0)}, map[mod.Int32]*mod.Int32{0: refModInt32(valc)},
		}.AddVariants(mod.All...),
	}.Run("big_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff\xff\x01"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenBigData,
	}.Run("big_data_elem1+", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\xff"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenBigData,
	}.Run("big_data_zeroElem1+", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x00\x01"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenBigData,
	}.Run("big_data_elems0+", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02\xff"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_val_value-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00\x00\x02"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_val_len", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff\x00\x00"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_val_len-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff\xff"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_val-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\xff"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_key_value-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_key_len", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_key_len-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x01"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_pair-", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("small_data_elems-", t, unmarshal)
}
