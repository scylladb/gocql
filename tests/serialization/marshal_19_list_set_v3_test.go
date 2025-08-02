//go:build unit
// +build unit

package serialization_test

import (
	"testing"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
)

func TestMarshalSetListV3(t *testing.T) {
	t.Parallel()

	elem := gocql.NewNativeType(3, gocql.TypeSmallInt, "")

	tTypes := []gocql.TypeInfo{
		gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeList, ""), nil, elem),
		gocql.NewCollectionType(gocql.NewNativeType(3, gocql.TypeSet, ""), nil, elem),
	}

	// unmarshal `zero` data return an error
	brokenZeroDataUnmarshal := serialization.GetTypes(mod.Values{
		[]int16{}, []*int16{},
		[]mod.Int16{}, []*mod.Int16{},
		&[]int16{}, &[]*int16{},
		&[]mod.Int16{}, &[]*mod.Int16{},
		(*[1]int16)(nil), (*[1]*int16)(nil),
		(*[1]mod.Int16)(nil), (*[1]*mod.Int16)(nil),
	}.AddVariants(mod.CustomType)...)

	refInt16 := func(v int16) *int16 { return &v }
	refModInt16 := func(v mod.Int16) *mod.Int16 { return &v }

	for _, tType := range tTypes {
		marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
		unmarshal := func(bytes []byte, i interface{}) error {
			return gocql.Unmarshal(tType, bytes, i)
		}

		t.Run(tType.Type().String(), func(t *testing.T) {

			serialization.PositiveSet{
				Data: nil,
				Values: mod.Values{
					([]int16)(nil), ([]*int16)(nil),
					([]mod.Int16)(nil), ([]*mod.Int16)(nil),
					(*[]int16)(nil), (*[]*int16)(nil),
					(*[]mod.Int16)(nil), (*[]*mod.Int16)(nil),
					(*[1]int16)(nil), (*[1]*int16)(nil),
					(*[1]mod.Int16)(nil), (*[1]*mod.Int16)(nil),
				}.AddVariants(mod.CustomType),
			}.Run("[nil]nullable", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: make([]byte, 0),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
					&[]int16{}, &[]*int16{},
					&[]mod.Int16{}, &[]*mod.Int16{},
					(*[1]int16)(nil), (*[1]*int16)(nil),
					(*[1]mod.Int16)(nil), (*[1]*mod.Int16)(nil),
				}.AddVariants(mod.CustomType),
				BrokenUnmarshalTypes: brokenZeroDataUnmarshal,
			}.Run("[]unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x00"),
				Values: mod.Values{
					[]int16{}, []*int16{},
					[]mod.Int16{}, []*mod.Int16{},
				}.AddVariants(mod.All...),
			}.Run("zero elems", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x00"),
				Values: mod.Values{
					[]int16{0}, []*int16{refInt16(0)},
					[]mod.Int16{0}, []*mod.Int16{refModInt16(0)},
					[1]int16{0}, [1]*int16{refInt16(0)},
					[1]mod.Int16{0}, [1]*mod.Int16{refModInt16(0)},
				}.AddVariants(mod.All...),
			}.Run("[]{0}", t, marshal, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x00"),
				Values: mod.Values{
					[]int16{0}, []*int16{refInt16(0)},
					[]mod.Int16{0}, []*mod.Int16{refModInt16(0)},
					[1]int16{0}, [1]*int16{refInt16(0)},
					[1]mod.Int16{0}, [1]*mod.Int16{refModInt16(0)},
				}.AddVariants(mod.All...),
			}.Run("[]{zero elem}unmarshal", t, nil, unmarshal)

			serialization.PositiveSet{
				Data: []byte("\x00\x00\x00\x01\x00\x00\x00\x02\x7f\xff"),
				Values: mod.Values{
					[]int16{32767}, []*int16{refInt16(32767)},
					[]mod.Int16{32767}, []*mod.Int16{refModInt16(32767)},
					[1]int16{32767}, [1]*int16{refInt16(32767)},
					[1]mod.Int16{32767}, [1]*mod.Int16{refModInt16(32767)},
				}.AddVariants(mod.All...),
			}.Run("[]{max}", t, marshal, unmarshal)
		})
	}
}
