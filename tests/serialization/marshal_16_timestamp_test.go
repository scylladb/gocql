package serialization

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsTimestamp(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeTimestamp, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	zeroTime := time.UnixMilli(0).UTC()

	// unmarshall `nil` and `zero` data returns a negative value of the `time.Time{}`
	brokenTime := serialization.GetTypes(time.Time{}, &time.Time{})

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(*int64)(nil), (*time.Time)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			int64(0), zeroTime,
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: brokenTime,
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			int64(0), zeroTime,
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: brokenTime,
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), zeroTime,
		}.AddVariants(mod.All...),
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(math.MaxInt64), time.UnixMilli(math.MaxInt64).UTC(),
		}.AddVariants(mod.All...),
	}.Run("max", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(math.MinInt64), time.UnixMilli(math.MinInt64).UTC(),
		}.AddVariants(mod.All...),
	}.Run("min", t, marshal, unmarshal)
}
