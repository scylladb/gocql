package duration

import (
	"bytes"
	"math"
	"math/bits"
	"testing"
)

func TestEncVint32(t *testing.T) {
	for i := int32(math.MaxInt32); i != 1; i = i / 2 {
		testEnc32(t, i)
		testEnc32(t, -i-1)
	}
}

func TestEncVint64(t *testing.T) {
	for i := int64(math.MaxInt64); i != 1; i = i / 2 {
		testEnc64(t, i)
		testEnc64(t, -i-1)
	}
}

func testEnc32(t *testing.T, v int32) {
	t.Helper()
	expected := genVintData(int64(v))
	received := encVint32(encIntZigZag32(v))

	if !bytes.Equal(expected, received) {
		t.Fatalf("expected and recieved data not equal\nvalue:%d\ndata expected:%b\ndata received:%b", v, expected, received)
	}
}

func testEnc64(t *testing.T, v int64) {
	t.Helper()
	expected := genVintData(v)
	received := encVint64(encIntZigZag64(v))

	if !bytes.Equal(expected, received) {
		t.Fatalf("expected and recieved data not equal\nvalue:%d\ndata expected:%b\ndata received:%b", v, expected, received)
	}
}

func genVintData(v int64) []byte {
	vEnc := encIntZigZag64(v)
	lead0 := bits.LeadingZeros64(vEnc)
	numBytes := (639 - lead0*9) >> 6

	// It can be 1 or 0 is v ==0
	if numBytes <= 1 {
		return []byte{byte(vEnc)}
	}
	extraBytes := numBytes - 1
	var buf = make([]byte, numBytes)
	for i := extraBytes; i >= 0; i-- {
		buf[i] = byte(vEnc)
		vEnc >>= 8
	}
	buf[0] |= byte(^(0xff >> uint(extraBytes)))
	return buf
}
