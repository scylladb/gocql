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
