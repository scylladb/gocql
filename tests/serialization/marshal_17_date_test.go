package serialization

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsDate(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDate, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	zeroDate := time.Date(-5877641, 06, 23, 0, 0, 0, 0, time.UTC).UTC()
	middleDate := time.UnixMilli(0).UTC()
	maxDate := time.Date(5881580, 07, 11, 0, 0, 0, 0, time.UTC).UTC()

	// marshal strings with big year like "-5877641-06-23" returns an error
	brokenBigString := serialization.GetTypes(string(""), (*string)(nil))

	// marshal `custom string` and `custom int64` unsupported
	brokenMarshal := serialization.GetTypes(mod.String(""), (*mod.String)(nil), mod.Int64(0), (*mod.Int64)(nil))

	// unmarshal `zero` data into not zero string and time.Time
	brokenZero := serialization.GetTypes(time.Time{}, &time.Time{}, string(""), (*string)(nil))

	// unmarshal `nil` data into not zero time.Time
	brokenNil := serialization.GetTypes(time.Time{})

	// unmarshal into `custom string`, `int64` and `custom int64` unsupported
	brokenUnmarshal := serialization.GetTypes(mod.String(""), (*mod.String)(nil), mod.Int64(0), (*mod.Int64)(nil), int64(0), (*int64)(nil))

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(*int64)(nil), (*time.Time)(nil), (*string)(nil),
		}.AddVariants(mod.CustomType),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			int64(0), zeroDate, "",
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: append(brokenUnmarshal, brokenNil...),
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			int64(0), zeroDate, "-5877641-06-23",
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: append(brokenUnmarshal, brokenZero...),
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00\x00"),
		Values: mod.Values{
			zeroDate.UnixMilli(), zeroDate, "-5877641-06-23",
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   append(brokenMarshal, brokenBigString...),
		BrokenUnmarshalTypes: brokenUnmarshal,
	}.Run("zeros", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x80\x00\x00\x00"),
		Values: mod.Values{
			middleDate.UnixMilli(), middleDate, "1970-01-01",
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenMarshal,
		BrokenUnmarshalTypes: brokenUnmarshal,
	}.Run("middle", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xff\xff\xff\xff"),
		Values: mod.Values{
			maxDate.UnixMilli(), maxDate, "5881580-07-11",
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   append(brokenMarshal, brokenBigString...),
		BrokenUnmarshalTypes: brokenUnmarshal,
	}.Run("max", t, marshal, unmarshal)
}
