package duration

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestEncStringIntegerOverflow(t *testing.T) {
	for _, value := range []string{
		"9223372036854775809",
		"18446744073709551616",
		"18446744073709551620",
		"999999999999999999999999999999",
	} {
		for _, unit := range []string{"ns", "us", "µs", "μs", "ms", "s", "m", "h", "d", "w", "mo", "y"} {
			for _, sign := range []string{"", "+", "-"} {
				input := sign + value + unit
				t.Run(input, func(t *testing.T) {
					data, err := EncString(input)
					if err == nil || data != nil {
						t.Fatalf("EncString(%q) = (%x, %v), want nil data and an error", input, data, err)
					}
				})
			}
		}
	}
}

func TestEncStringIntegerBoundaries(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  Duration
	}{
		{"9223372036854775807ns", Duration{Nanoseconds: math.MaxInt64}},
		{"+9223372036854775807ns", Duration{Nanoseconds: math.MaxInt64}},
		{"-9223372036854775808ns", Duration{Nanoseconds: math.MinInt64}},
		{"2147483647mo", Duration{Months: math.MaxInt32}},
		{"-2147483648mo", Duration{Months: math.MinInt32}},
		{"2147483647d", Duration{Days: math.MaxInt32}},
		{"-2147483648d", Duration{Days: math.MinInt32}},
	} {
		t.Run(tt.input, func(t *testing.T) {
			data, err := EncString(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			var got Duration
			if err := DecDuration(data, &got); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("decoded %q = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestEncStringCumulativeOverflow(t *testing.T) {
	for _, input := range []string{
		"9223372036854775808ns9223372036854775808ns",
		"+9223372036854775808ns9223372036854775808ns",
		"-9223372036854775808ns9223372036854775808ns",
		"9223372036854775807ns1ns",
		"-9223372036854775808ns1ns",
		"-808ns9223372036854775us1ns",
		"-775808ns9223372036854ms1ns",
		"-854775808ns9223372036s1ns",
		"-16854775808ns153722867m1ns",
		"-2836854775808ns2562047h1ns",
		"2147483647d1d",
		"-2147483648mo1mo",
		"-2147483648d1w",
		"-2147483648mo1y",
	} {
		t.Run(input, func(t *testing.T) {
			if data, err := EncString(input); err == nil || data != nil {
				t.Fatalf("EncString(%q) = (%x, %v), want nil data and an error", input, data, err)
			}
		})
	}
	for _, tt := range []struct {
		input string
		want  Duration
	}{
		{"9223372036854775806ns1ns", Duration{Nanoseconds: math.MaxInt64}},
		{"-9223372036854775807ns1ns", Duration{Nanoseconds: math.MinInt64}},
		{"-808ns9223372036854775us", Duration{Nanoseconds: math.MinInt64}},
		{"-775808ns9223372036854ms", Duration{Nanoseconds: math.MinInt64}},
		{"-854775808ns9223372036s", Duration{Nanoseconds: math.MinInt64}},
		{"-16854775808ns153722867m", Duration{Nanoseconds: math.MinInt64}},
		{"-2836854775808ns2562047h", Duration{Nanoseconds: math.MinInt64}},
		{"-2147483647d1d", Duration{Days: math.MinInt32}},
		{"-2147483647mo1mo", Duration{Months: math.MinInt32}},
		{"-2147483641d1w", Duration{Days: math.MinInt32}},
		{"-2147483636mo1y", Duration{Months: math.MinInt32}},
	} {
		t.Run(tt.input, func(t *testing.T) {
			data, err := EncString(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			var got Duration
			if err := DecDuration(data, &got); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("decoded %q = %+v, want %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestEncStringOverflowDiagnostic(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{"2147483649d", "days 2147483649 out of the range"},
		{"2147483647d2d", "days 2147483649 out of the range"},
		{"2147483649mo", "months 2147483649 out of the range"},
		{"-2147483647mo2mo", "months 2147483649 out of the range"},
		{"9223372036854775808ns1ns", "nanoseconds 9223372036854775809 out of the range"},
		{"9223372036854775808ns9223372036854775808ns", "nanoseconds 18446744073709551615 out of the range"},
	} {
		t.Run(tt.input, func(t *testing.T) {
			data, err := EncString(tt.input)
			if err == nil || data != nil {
				t.Fatalf("EncString(%q) = (%x, %v), want nil data and an error", tt.input, data, err)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("EncString(%q) error = %q, want it to contain %q", tt.input, err, tt.want)
			}
		})
	}
}

func TestEncStr(t *testing.T) {
	for n := int64(math.MaxInt64); n != 1; n = n / 2 {
		m, d := int32(n), int32(n)
		if n > math.MaxInt32 {
			m, d = math.MaxInt32, math.MaxInt32
		}
		testEncString(t, m, d, n)
		testEncString(t, 0, d, n)
		testEncString(t, m, 0, n)
		testEncString(t, m, d, 0)
	}

	for n := int64(math.MinInt64); n != -1; n = n / 2 {
		m, d := int32(n), int32(n)
		if n < math.MinInt32 {
			m, d = math.MinInt32, math.MinInt32
		}
		testEncString(t, m, d, n)
		testEncString(t, 0, d, n)
		testEncString(t, m, 0, n)
		testEncString(t, m, d, 0)
	}
}

func testEncString(t *testing.T, m, d int32, n int64) {
	t.Helper()
	testStr := getTestString(m, d, n)
	mu, du, nu, neg, err := encStringToUints(testStr)
	if err != nil {
		t.Fatalf("failed on encoding testcase value:m:%d,d:%d,n:%d\ntest string:%s\nerror:%s", m, d, n, testStr, err)
	}
	me, de, ne := int32(mu), int32(du), int64(nu)
	if neg {
		me, de, ne = -me, -de, -ne
	}
	if me != m {
		t.Fatalf("testcase:%s\nexpected and recieved months not equal expected:%d received:%d", testStr, m, me)
	}
	if de != d {
		t.Fatalf("testcase:%s\nexpected and recieved days not equal expected:%d received:%d", testStr, d, de)
	}
	if ne != n {
		t.Fatalf("testcase:%s\nexpected and recieved nonoseconds not equal expected:%d received:%d", testStr, n, ne)
	}
}

func TestMarshalLongFraction(t *testing.T) {
	type namedString string
	tests := []struct {
		input string
		want  Duration
	}{
		{"0.9999999999999999999ns", Duration{}},
		{"0.9999999999999999999us", Duration{Nanoseconds: 999}},
		{"0.9999999999999999999ms", Duration{Nanoseconds: 999999}},
		{"0.9999999999999999999s", Duration{Nanoseconds: 999999999}},
		{"0.9999999999999999999m", Duration{Nanoseconds: 59999999999}},
		{"0.9999999999999999999h", Duration{Nanoseconds: 3599999999999}},
		{"0.9999999999999999999d", Duration{}},
		{"0.9999999999999999999w", Duration{Days: 6}},
		{"0.9999999999999999999mo", Duration{}},
		{"0.9999999999999999999y", Duration{Months: 11}},
		{"9223372036854775807.0000000000000000000ns", Duration{Nanoseconds: math.MaxInt64}},
		{"9223372036854775807.9999999999999999999ns", Duration{Nanoseconds: math.MaxInt64}},
		{"1.9999999999999999999d", Duration{Days: 1}},
		{"0.9300000000000000000s", Duration{Nanoseconds: 930000000}},
		{"1.50000000000000000000s", Duration{Nanoseconds: 1500000000}},
		{"1.20000000000000000001s", Duration{Nanoseconds: 1200000000}},
		{"0.50000000000000000000ns", Duration{}},
		{"0.50000000000000000000us", Duration{Nanoseconds: 500}},
		{"0.50000000000000000000ms", Duration{Nanoseconds: 500000}},
		{"0.50000000000000000000m", Duration{Nanoseconds: 30000000000}},
		{"0.50000000000000000000h", Duration{Nanoseconds: 1800000000000}},
		{"1.50000000000000000000d", Duration{Days: 1}},
		{"0.50000000000000000000w", Duration{Days: 3}},
		{"1.50000000000000000000mo", Duration{Months: 1}},
		{"0.50000000000000000000y", Duration{Months: 6}},
		{"1.5" + strings.Repeat("0", 1000) + "s2ms", Duration{Nanoseconds: 1502000000}},
		{"0." + strings.Repeat("0", 1000) + "1h", Duration{}},
	}
	for _, tt := range tests {
		for _, prefix := range []string{"", "-"} {
			t.Run(prefix+tt.input, func(t *testing.T) {
				input, want := prefix+tt.input, tt.want
				if prefix == "-" {
					want.Months, want.Days, want.Nanoseconds = -want.Months, -want.Days, -want.Nanoseconds
				}
				expected, err := EncDuration(want)
				if err != nil {
					t.Fatal(err)
				}
				named := namedString(input)
				for _, value := range []any{input, &input, named, &named} {
					data, err := Marshal(value)
					if err != nil {
						t.Fatalf("Marshal(%T(%q)): %v", value, input, err)
					}
					if !bytes.Equal(data, expected) {
						t.Errorf("Marshal(%T(%q)) = %x, want %x (%+v)", value, input, data, expected, want)
					}
				}
			})
		}
	}
	data, err := EncString("-9223372036854775808.9999999999999999999ns")
	if err != nil {
		t.Fatal(err)
	}
	var minimum Duration
	if err := DecDuration(data, &minimum); err != nil {
		t.Fatal(err)
	}
	if minimum != (Duration{Nanoseconds: math.MinInt64}) {
		t.Errorf("minimum nanoseconds = %+v, want %d", minimum, int64(math.MinInt64))
	}
	for _, input := range []string{
		"9223372036854775808.9999999999999999999ns",
		"1.50000000000000000000bogus",
		"1.50000000000000000000",
	} {
		if _, err := EncString(input); err == nil {
			t.Errorf("EncString(%q) accepted invalid input", input)
		}
	}
}

func TestMarshalFractionTruncation(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  Duration
	}{
		{"0.999999999999999872us", Duration{Nanoseconds: 999}},
		{"0.9999999999999998720us", Duration{Nanoseconds: 999}},
		{"5.9999999999999998720us", Duration{Nanoseconds: 5999}},
		{"0.9999999999999998720µs", Duration{Nanoseconds: 999}},
		{"0.9999999999999998720μs", Duration{Nanoseconds: 999}},
		{"0.999999999999999999s", Duration{Nanoseconds: 999999999}},
		{"0.9999999999999999990s", Duration{Nanoseconds: 999999999}},
		{"0.999999999999999999d", Duration{}},
		{"0.9999999999999999990d", Duration{}},
		{"9223372036854775807.999999999999999999ns", Duration{Nanoseconds: math.MaxInt64}},
		{"9223372036854775807.9999999999999999990ns", Duration{Nanoseconds: math.MaxInt64}},
		{"0.0000000000001h", Duration{}},
		{"0.0000000000003h", Duration{Nanoseconds: 1}},
		{"0.00000000000000000001h", Duration{}},
	} {
		for _, sign := range []string{"", "-"} {
			input, want := sign+tt.input, tt.want
			if sign == "-" {
				want.Months, want.Days, want.Nanoseconds = -want.Months, -want.Days, -want.Nanoseconds
			}
			t.Run(input, func(t *testing.T) {
				data, err := EncString(input)
				if err != nil {
					t.Fatal(err)
				}
				var got Duration
				if err := DecDuration(data, &got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("decoded %q = %+v, want %+v", input, got, want)
				}
			})
		}
	}
}
