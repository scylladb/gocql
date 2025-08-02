//go:build unit
// +build unit

package serialization_test

import (
	"testing"
	"time"

	"github.com/scylladb/gocql/v2"
	"github.com/scylladb/gocql/v2/internal/tests/serialization"
	"github.com/scylladb/gocql/v2/internal/tests/serialization/mod"
)

func TestMarshalDurationCorrupt(t *testing.T) {
	t.Parallel()

	tType := gocql.NewNativeType(4, gocql.TypeDuration, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"23123113f", "sda",
			gocql.Duration{Months: -1, Days: 1, Nanoseconds: 0},
			gocql.Duration{Months: -1, Days: 1, Nanoseconds: 1},
			gocql.Duration{Months: -1, Days: 1, Nanoseconds: -1},
			gocql.Duration{Months: -1, Days: -1, Nanoseconds: 1},
			gocql.Duration{Months: -1, Days: 0, Nanoseconds: 1},
			gocql.Duration{Months: 1, Days: -1, Nanoseconds: 0},
			gocql.Duration{Months: 1, Days: -1, Nanoseconds: 1},
			gocql.Duration{Months: 1, Days: -1, Nanoseconds: -1},
			gocql.Duration{Months: 1, Days: 1, Nanoseconds: -1},
			gocql.Duration{Months: 1, Days: 0, Nanoseconds: -1},
		}.AddVariants(mod.All...),
	}.Run("corrupt_vals", t, marshal)

	serialization.NegativeMarshalSet{
		Values: mod.Values{
			"178956971y7mo306783378w1d2562047h47m16.854775807s",
			"178956970y8mo306783378w1d2562047h47m16.854775807s",
			"178956970y7mo306783379w1d2562047h47m16.854775807s",
			"178956970y7mo306783378w2d2562047h47m16.854775807s",
			"178956970y7mo306783378w1d2562048h47m16.854775807s",
			"178956970y7mo306783378w1d2562047h48m16.854775807s",
			"178956970y7mo306783378w1d2562047h47m17.854775807s",
			"178956970y7mo306783378w1d2562047h47m16.854775808s",

			"-178956971y8mo306783378w2d2562047h47m16.854775808s",
			"-178956970y9mo306783378w2d2562047h47m16.854775808s",
			"-178956970y8mo306783379w2d2562047h47m16.854775808s",
			"-178956970y8mo306783378w3d2562047h47m16.854775808s",
			"-178956970y8mo306783378w2d2562048h47m16.854775808s",
			"-178956970y8mo306783378w2d2562047h48m16.854775808s",
			"-178956970y8mo306783378w2d2562047h47m17.854775808s",
			"-178956970y8mo306783378w2d2562047h47m16.854775809s",
		}.AddVariants(mod.All...),
	}.Run("big_vals", t, marshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf1\x00\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_month1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf1\x00\x00\x00\x01\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_month2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf1\x00\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_day1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf1\x00\x00\x00\x01\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_day2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
	}.Run("big_data_nano1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x01\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
	}.Run("big_data_nano2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x01\x00\x41\xfd\xfc\x9b\xc5\xc4\x9e\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_nano3", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xc3\x41\xfd\xfc\x9b\xc5\xc4\x9e\x00\x01"),
		Values: mod.Values{
			int64(0), time.Duration(0),
		}.AddVariants(mod.All...),
	}.Run("big_data_nano4", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_len1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_len2", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfd\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("big_data_len3", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len1", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len2", t, unmarshal)

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

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len_nanos", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\x00\xf0\xff\xff\xff\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len_days", t, unmarshal)

	serialization.NegativeUnmarshalSet{
		Data: []byte("\xf0\xff\xff\xff\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.All...),
	}.Run("small_data_len_months", t, unmarshal)
}
