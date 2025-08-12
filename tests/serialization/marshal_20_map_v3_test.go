//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
)

func TestMarshalMapV3(t *testing.T) {
	t.Parallel()

	elem := gocql.NewNativeType(3, gocql.TypeSmallInt, "")
	tType := gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeMap, ""), elem, elem)

	refInt16 := func(v int16) *int16 { return &v }
	refModInt16 := func(v mod.Int16) *mod.Int16 { return &v }

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(map[int16]int16)(nil), (map[int16]*int16)(nil),
			(map[mod.Int16]mod.Int16)(nil), (map[mod.Int16]*mod.Int16)(nil),
			(*map[int16]int16)(nil), (*map[int16]*int16)(nil),
			(*map[mod.Int16]mod.Int16)(nil), (*map[mod.Int16]*mod.Int16)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00"),
		Values: mod.Values{
			make(map[int16]int16), make(map[int16]*int16),
			make(map[mod.Int16]mod.Int16), make(map[mod.Int16]*mod.Int16),
		}.AddVariants(mod.All...),
	}.Run("zero elems", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			map[int16]int16{0: 0}, map[int16]*int16{0: refInt16(0)},
			map[mod.Int16]mod.Int16{0: 0}, map[mod.Int16]*mod.Int16{0: refModInt16(0)},
		}.AddVariants(mod.All...),
	}.Run("[]{zero elem}unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00\x00\x00\x00\x02\x00\x00"),
		Values: mod.Values{
			map[int16]int16{0: 0}, map[int16]*int16{0: refInt16(0)},
			map[mod.Int16]mod.Int16{0: 0}, map[mod.Int16]*mod.Int16{0: refModInt16(0)},
		}.AddVariants(mod.All...),
	}.Run("[]{0:0}", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x7f\xff\x00\x00\x00\x02\x7f\xff"),
		Values: mod.Values{
			map[int16]int16{32767: 32767}, map[int16]*int16{32767: refInt16(32767)},
			map[mod.Int16]mod.Int16{32767: 32767}, map[mod.Int16]*mod.Int16{32767: refModInt16(32767)},
		}.AddVariants(mod.All...),
	}.Run("[]{max:max}", t, marshal, unmarshal)
}
