package serialization

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalDurationCorrupt(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDuration, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	// unmarshal `gocql.Duration` with data which more cql values (int32,int32,int64) size does not return an error
	brokenDuration := serialization.GetTypes(gocql.Duration{}, &gocql.Duration{})

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"23123113", "sda",
		}.AddVariants(mod.All...),
	}.Run("corrupt_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf1\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_month1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf1\x00\x00\x00\x01\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_month2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf1\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_day1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf1\x00\x00\x00\x01\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_day2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_nano1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_nano2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_len1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_len2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfd\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenTypes: brokenDuration,
	}.Run("big_data_len3", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf0\xff\xff\xff\xfe\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf0\xff\xff\xff\xfe"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len3", t, unmarshal)
}