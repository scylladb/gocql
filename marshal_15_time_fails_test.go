package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/marshal"
	"github.com/gocql/gocql/marshal/tests/gen/mustfail/unmarshal"
	"math"
	"testing"
	"time"
)

func TestMarshalTimeMustFail(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeTime}

	mCases := &marshal.Group{
		Name:        tType.Type().String(),
		Func:        func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
		DefaultMods: mod.Default,
		Sets: []*marshal.Set{
			{
				Name:       "big_vals1",
				MarshalIns: []interface{}{int64(86300000000001), time.Duration(86300000000001)},
				Issue:      "the error is not returned",
			},
			{
				Name:       "big_vals2",
				MarshalIns: []interface{}{int64(math.MaxInt64), time.Duration(math.MaxInt64)},
				Issue:      "the error is not returned",
			},
			{
				Name:       "small_vals1",
				MarshalIns: []interface{}{int64(-1), time.Duration(-1)},
				Issue:      "the error is not returned",
			},
			{
				Name:       "small_vals2",
				MarshalIns: []interface{}{int64(math.MinInt64), time.Duration(math.MinInt64)},
				Issue:      "the error is not returned",
			},
		},
	}
	mCases.Gen().RunGroup(t)

	uCases := &unmarshal.Group{
		Func:        func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		DefaultMods: mod.Default,
		Sets: []*unmarshal.Set{
			{
				Name:         "big_data",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff\xff\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/246",
			},
			{
				Name:         "small_data1",
				Data:         []byte("\x00\x00\x4e\x94\x91\x4e\xff"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
			{
				Name:         "small_data2",
				Data:         []byte("\x00"),
				UnmarshalIns: []interface{}{int64(0), time.Duration(0)},
				Issue:        "https://github.com/scylladb/gocql/issues/252",
			},
		},
	}
	uCases.Gen().RunGroup(t)
}
