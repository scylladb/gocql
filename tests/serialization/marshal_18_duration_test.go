package serialization

import (
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/internal/tests/serialization"
	"github.com/gocql/gocql/internal/tests/serialization/mod"
)

func TestMarshalsDuration(t *testing.T) {
	tType := gocql.NewNativeType(4, gocql.TypeDuration, "")

	marshal := func(i interface{}) ([]byte, error) { return gocql.Marshal(tType, i) }
	unmarshal := func(bytes []byte, i interface{}) error {
		return gocql.Unmarshal(tType, bytes, i)
	}

	unmarshalUnsupported := serialization.GetTypes(mod.Values{int64(0), time.Duration(0), ""}.AddVariants(mod.All...)...)

	brokenCustom := serialization.GetTypes(mod.String(""), (*mod.String)(nil))

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			(*int64)(nil), (*time.Duration)(nil), (*string)(nil), (*gocql.Duration)(nil),
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: serialization.GetTypes(int64(0)),
	}.Run("[nil]nullable", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: nil,
		Values: mod.Values{
			int64(0), time.Duration(0), "", gocql.Duration{},
		}.AddVariants(mod.CustomType),
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("[nil]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: make([]byte, 0),
		Values: mod.Values{
			int64(0), time.Duration(0), "0", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("[]unmarshal", t, nil, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x00"),
		Values: mod.Values{
			int64(0), time.Duration(0), "0", gocql.Duration{},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("zeros", t, marshal, unmarshal)

	// sets for months
	serialization.PositiveSet{
		Data: []byte("\x02\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 1, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("months1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x01\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: -1, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("months-1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x80\xfe\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MaxInt8, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMaxInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x80\xff\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MinInt8, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMinInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x81\xfe\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MaxUint8, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMaxUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x81\xfd\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: -math.MaxUint8, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMinUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xc0\xff\xfe\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MaxInt16, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMaxInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xc0\xff\xff\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MinInt16, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMinInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xc1\xff\xfe\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MaxUint16, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMaxUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xc1\xff\xfd\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: -math.MaxUint16, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMinUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xf0\xff\xff\xff\xfe\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MaxInt32, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMaxInt32", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xf0\xff\xff\xff\xff\x00\x00"),
		Values: mod.Values{
			gocql.Duration{Months: math.MinInt32, Days: 0, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("monthsMinInt32", t, marshal, unmarshal)

	// sets for days
	serialization.PositiveSet{
		Data: []byte("\x00\x02\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: 1, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("days1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x01\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: -1, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("days-1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x80\xfe\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MaxInt8, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMaxInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x80\xff\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MinInt8, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMinInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x81\xfe\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MaxUint8, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMaxUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x81\xfd\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: -math.MaxUint8, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMinUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xc0\xff\xfe\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MaxInt16, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMaxInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xc0\xff\xff\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MinInt16, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMinInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xc1\xff\xfe\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MaxUint16, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMaxUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xc1\xff\xfd\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: -math.MaxUint16, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMinUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xf0\xff\xff\xff\xfe\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MaxInt32, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMaxInt32", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\xf0\xff\xff\xff\xff\x00"),
		Values: mod.Values{
			gocql.Duration{Months: 0, Days: math.MinInt32, Nanoseconds: 0},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("daysMinInt32", t, marshal, unmarshal)

	//sets for nanoseconds
	serialization.PositiveSet{
		Data: []byte("\x00\x00\x02"),
		Values: mod.Values{
			int64(1), time.Duration(1), "1ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: 1},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanos1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x01"),
		Values: mod.Values{
			int64(-1), time.Duration(-1), "-1ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: -1},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanos-1", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x80\xfe"),
		Values: mod.Values{
			int64(math.MaxInt8), time.Duration(math.MaxInt8), "127ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxInt8},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x80\xff"),
		Values: mod.Values{
			int64(math.MinInt8), time.Duration(math.MinInt8), "-128ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MinInt8},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinInt8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x81\xfe"),
		Values: mod.Values{
			int64(math.MaxUint8), time.Duration(math.MaxUint8), "255ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxUint8},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\x81\xfd"),
		Values: mod.Values{
			-int64(math.MaxUint8), -time.Duration(math.MaxUint8), "-255ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: -math.MaxUint8},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinUint8", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xc0\xff\xfe"),
		Values: mod.Values{
			int64(math.MaxInt16), time.Duration(math.MaxInt16), "32767ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxInt16},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xc0\xff\xff"),
		Values: mod.Values{
			int64(math.MinInt16), time.Duration(math.MinInt16), "-32768ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MinInt16},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinInt16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xc1\xff\xfe"),
		Values: mod.Values{
			int64(math.MaxUint16), time.Duration(math.MaxUint16), "65535ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxUint16},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xc1\xff\xfd"),
		Values: mod.Values{
			-int64(math.MaxUint16), -time.Duration(math.MaxUint16), "-65535ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: -math.MaxUint16},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinUint16", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xf0\xff\xff\xff\xfe"),
		Values: mod.Values{
			int64(math.MaxInt32), time.Duration(math.MaxInt32), "2147483647ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxInt32},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxInt32", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xf0\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(math.MinInt32), time.Duration(math.MinInt32), "-2147483648ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MinInt32},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinInt32", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
		Values: mod.Values{
			int64(math.MaxInt64), time.Duration(math.MaxInt64), "9223372036854775807ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MaxInt64},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMaxInt64", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x00\x00\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			int64(math.MinInt64), time.Duration(math.MinInt64), "-9223372036854775808ns",
			gocql.Duration{Months: 0, Days: 0, Nanoseconds: math.MinInt64},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("nanosMinInt64", t, marshal, unmarshal)

	// sets for full range
	serialization.PositiveSet{
		Data: []byte("\x02\x02\x02"),
		Values: mod.Values{
			gocql.Duration{Days: 1, Months: 1, Nanoseconds: 1},
		}.AddVariants(mod.All...),
		BrokenMarshalTypes:   brokenCustom,
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("111", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\x01\x01\x01"),
		Values: mod.Values{
			gocql.Duration{Days: -1, Months: -1, Nanoseconds: -1},
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("-111", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xf0\xff\xff\xff\xfe\xf0\xff\xff\xff\xfe\xff\xff\xff\xff\xff\xff\xff\xff\xfe"),
		Values: mod.Values{
			gocql.Duration{Days: math.MaxInt32, Months: math.MaxInt32, Nanoseconds: math.MaxInt64},
		}.AddVariants(mod.All...),
		BrokenUnmarshalTypes: unmarshalUnsupported,
	}.Run("max", t, marshal, unmarshal)

	serialization.PositiveSet{
		Data: []byte("\xf0\xff\xff\xff\xff\xf0\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"),
		Values: mod.Values{
			gocql.Duration{Days: math.MinInt32, Months: math.MinInt32, Nanoseconds: math.MinInt64},
		}.AddVariants(mod.All...),
	}.Run("min", t, marshal, unmarshal)
}
