package duration

import (
	"math"
	"testing"
)

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
