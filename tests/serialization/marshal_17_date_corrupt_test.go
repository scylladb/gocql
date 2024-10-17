package serialization

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalDateCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDate, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// marshal the `int64`, `time.Time` values which out of the `cql type` range, does not return an error.
	brokenMarshalTypes := serialization.GetTypes(int64(0), (*int64)(nil), time.Time{}, &time.Time{})

	// unmarshal of `string`, `time.Time` does not return an error on all type of data corruption.
	brokenUnmarshalTypes := serialization.GetTypes(string(""), (*string)(nil), time.Time{}, &time.Time{})

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			time.Date(5881580, 7, 12, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(5881580, 8, 11, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(5881581, 7, 11, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(5883581, 12, 20, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			"5881580-07-12", "5881580-08-11", "5881581-07-11", "9223372036854775807-07-12",
			time.Date(5881580, 7, 12, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(5881580, 8, 11, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(5881581, 7, 11, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(5883581, 12, 20, 0, 0, 0, 0, time.UTC).UTC(),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenMarshalTypes,
	}.Run("big_vals", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			time.Date(-5877641, 06, 24, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(-5877641, 07, 23, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(-5877642, 06, 23, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			time.Date(-5887641, 06, 23, 0, 0, 0, 0, time.UTC).UTC().UnixMilli(),
			"5881580-07-12", "5881580-08-11", "5881581-07-11", "9223372036854775807-07-12",
			"-5877641-06-24", "-5877641-07-23", "-5877642-06-23", "-9223372036854775807-07-12",
			time.Date(-5877641, 06, 24, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(-5877641, 07, 23, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(-5877642, 06, 23, 0, 0, 0, 0, time.UTC).UTC(),
			time.Date(-5887641, 06, 23, 0, 0, 0, 0, time.UTC).UTC(),
		}.AddVariants(mod.All...),
		BrokenTypes: brokenMarshalTypes,
	}.Run("small_vals", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"a1580-07-11", "1970-0d-11", "02-11", "1970-11",
		}.AddVariants(mod.All...),
	}.Run("corrupt_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Time{}, "",
		}.AddVariants(mod.All...),
		BrokenTypes: brokenUnmarshalTypes,
	}.Run("big_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Time{}, "",
		}.AddVariants(mod.All...),
		BrokenTypes: brokenUnmarshalTypes,
	}.Run("big_data2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Time{}, "",
		}.AddVariants(mod.All...),
		BrokenTypes: brokenUnmarshalTypes,
	}.Run("small_data1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00"),
		Values: mod.Values{
			int64(0), time.Time{}, "",
		}.AddVariants(mod.All...),
		BrokenTypes: brokenUnmarshalTypes,
	}.Run("small_data2", t, unmarshal)
}
