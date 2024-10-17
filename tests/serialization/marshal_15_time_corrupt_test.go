package serialization

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalTimeCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeTime, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// marshal, unmarshal of all supported `go types` does not return an error on all type of corruption.
	brokenTypes := serialization.GetTypes(int64(0), (*int64)(nil), mod.Int64(0), (*mod.Int64)(nil), time.Duration(0), (*time.Duration)(nil))

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			int64(86300000000000), time.Duration(86300000000000),
			int64(math.MaxInt64), time.Duration(math.MaxInt64),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("big_vals", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			int64(-1), time.Duration(-1),
			int64(math.MinInt8), time.Duration(math.MinInt8),
			int64(math.MinInt64), time.Duration(math.MinInt64),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("big_data_len", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data_len1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data_len2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x4e\x94\x91\x4f\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("big_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("big_data2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x80\x00\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenTypes,
	}.Run("small_data2", t, unmarshal)
}
