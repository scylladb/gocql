package duration

import (
	"math"
	"testing"
)

func TestDecVint32(t *testing.T) {
	for i := int32(math.MaxInt32); i != 1; i = i / 2 {
		testDec32(t, i)
		testDec32(t, -i-1)
	}
}

func TestDecVint64(t *testing.T) {
	for i := int64(math.MaxInt64); i != 1; i = i / 2 {
		testDec64(t, i)
		testDec64(t, -i-1)
	}
}

func testDec32(t *testing.T, expected int32) {
	t.Helper()
	// appending one byte is necessary because the `decVint32` function looks at the length of the data for the next vint len read.
	data := append(genVintData(int64(expected)), 0)

	vint, read := decVint32(data, 0)
	if read == 0 {
		t.Fatalf("decVint32 function can`t read vint data: value %d, data %b", expected, data)
	}

	received := decZigZag32(vint)
	if expected != received {
		t.Fatalf("\nexpected:%d\nreceived:%d\ndata:%b", expected, received, data)
	}
}

func testDec64(t *testing.T, expected int64) {
	t.Helper()
	data := genVintData(int64(expected))

	vint, read := decVint64(data, 0)
	if read == 0 {
		t.Fatalf("decVint64 function can`t read vint data: value %d, data %b", expected, data)
	}

	received := decZigZag64(vint)
	if expected != received {
		t.Fatalf("\nexpected:%d\nreceived:%d\ndata:%b", expected, received, data)
	}
}
