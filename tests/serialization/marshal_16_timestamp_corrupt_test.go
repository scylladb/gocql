package serialization

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalTimestampCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeTimestamp, "")

	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// unmarshal of all supported `go types` does not return an error on all type of corruption.
	brokenTypes := serialization.GetTypes(int64(0), (*int64)(nil), mod.Int64(0), (*mod.Int64)(nil), time.Time{}, (*time.Time)(nil))

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Time{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("big_data", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Time{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00"),
		Values: mod.Values{
			int64(0), time.Time{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data2", t, unmarshal)
}
