package duration

import (
	"math"
	"strconv"
	"testing"
	"time"
)

func TestDecStr(t *testing.T) {
	for n := int64(math.MaxInt64); n != 1; n = n / 2 {
		m, d := int32(n), int32(n)
		if n > math.MaxInt32 {
			m, d = math.MaxInt32, math.MaxInt32
		}
		testDecString(t, m, d, n)
		testDecString(t, 0, d, n)
		testDecString(t, m, 0, n)
		testDecString(t, m, d, 0)
	}

	for n := int64(math.MinInt64); n != -1; n = n / 2 {
		m, d := int32(n), int32(n)
		if n < math.MinInt32 {
			m, d = math.MinInt32, math.MinInt32
		}
		testDecString(t, m, d, n)
		testDecString(t, 0, d, n)
		testDecString(t, m, 0, n)
		testDecString(t, m, d, 0)
	}
}

func TestDecStringFractionalSeconds(t *testing.T) {
	for _, tc := range []struct {
		nanos int64
		want  string
	}{
		{60_500_000_000, "1m0.5s"},
		{60_050_000_000, "1m0.05s"},
		{60_000_001_000, "1m0.000001s"},
		{60_000_000_010, "1m0.00000001s"},
		{60_000_000_001, "1m0.000000001s"},
		{3_600_005_000_000, "1h0.005s"},
		{3_600_000_000_001, "1h0.000000001s"},
		{3_660_500_000_000, "1h1m0.5s"},
		{61_500_000_000, "1m1.5s"},
		{65_000_000_000, "1m5s"},
		{60_000_000_000, "1m"},
		{1_500_000, "1.5ms"},
	} {
		for _, sign := range []int64{1, -1} {
			nanos, want := sign*tc.nanos, tc.want
			if sign < 0 {
				want = "-" + want
			}
			t.Run(want, func(t *testing.T) {
				data, err := EncInt64(nanos)
				if err != nil {
					t.Fatal(err)
				}
				var got string
				if err := DecString(data, &got); err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("DecString(%d) = %q, want %q", nanos, got, want)
				}
				data, err = EncString(got)
				if err != nil {
					t.Fatal(err)
				}
				var roundTrip int64
				if err := DecInt64(data, &roundTrip); err != nil {
					t.Fatal(err)
				}
				if roundTrip != nanos {
					t.Fatalf("round trip = %d, want %d", roundTrip, nanos)
				}
			})
		}
	}
}

func TestDecStrLargeMonths(t *testing.T) {
	// Cover every month remainder near the int32 limits, including the
	// two-digit month remainders that produce the longest duration strings.
	for offset := int32(0); offset < 24; offset++ {
		testDecString(t, math.MinInt32+offset, math.MinInt32, math.MinInt64)
		testDecString(t, math.MaxInt32-offset, math.MaxInt32, math.MaxInt64)
	}
}

func testDecString(t *testing.T, m, d int32, n int64) {
	t.Helper()
	expected := getTestString(m, d, n)
	received := decString(m, d, n)

	if expected != received {
		t.Fatalf("expected and recieved strings not equal\nvalue:m:%d,d:%d,n:%d\nexpected:%s\nreceived:%s", m, d, n, expected, received)
	}
}

func getTestString(m, d int32, n int64) string {
	out := ""
	if m < 0 || d < 0 || n < 0 {
		out += "-"
	}
	if m != 0 {
		out += getStringMonths(m)
	}
	if d != 0 {
		out += getStringDays(d)
	}
	if n != 0 {
		out += getStringNanos(n)
	}
	if out == "" {
		return zeroDuration
	}
	return out
}

func getStringMonths(m int32) string {
	out := ""
	mu := uint64(m)
	if m < 0 {
		mu = -mu
	}
	y := mu / 12
	if mu = mu % 12; mu == 0 {
		y--
		mu = 12
	}
	if y != 0 {
		out += strconv.FormatUint(y, 10) + "y"
	}
	out += strconv.FormatUint(mu, 10) + "mo"
	return out
}

func getStringDays(d int32) string {
	out := ""
	du := uint64(d)
	if d < 0 {
		du = -du
	}
	w := du / 7
	if du = du % 7; du == 0 {
		w--
		du = 7
	}
	if w != 0 {
		out += strconv.FormatUint(w, 10) + "w"
	}
	out += strconv.FormatUint(du, 10) + "d"
	return out
}

func getStringNanos(d int64) string {
	out := time.Duration(d).String()
	if d < 0 {
		out = out[1:]
	}
	return out
}
