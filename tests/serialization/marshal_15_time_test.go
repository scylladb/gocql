package serialization

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsTime(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeTime, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(*int64)(nil), (*time.Duration)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff"),
		Values: mod.Values{
			int64(86399999999999), time.Duration(86399999999999),
		}.AddVariants(mod.All...),
	}.Run("max", t, marshal, unmarshal)

}
