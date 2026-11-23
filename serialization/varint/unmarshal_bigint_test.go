package varint

import (
	"math"
	"math/big"
	"math/rand"
	"testing"
)

func TestDec2BigInt(t *testing.T) {
	t.Parallel()

	genData := func(v int64) []byte {
		data := []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
		out := make([]byte, 0)
		add := false
		for i, b := range data {
			if !add {
				if v < 0 {
					if b != 255 || b == 255 && data[i+1] < 128 {
						add = true
					} else {
						continue
					}
				} else {
					if b != 0 || b == 0 && data[i+1] > 127 {
						add = true
					} else {
						continue
					}
				}
			}
			out = append(out, b)
		}

		return out
	}

	t.Run("positive", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for i := int64(math.MaxInt16); i < 1<<23; i = i + int64(rnd.Int31n(300)) {
			data := genData(i)
			expected := big.NewInt(i)

			received := Dec2BigInt(data)
			if expected.Cmp(received) != 0 {
				t.Fatalf("%d\nexpected:%s\nreceived:%s", i, expected, received)
			}

			_ = DecBigInt(data, received)
			if expected.Cmp(received) != 0 {
				t.Fatalf("%d\nexpected:%s\nreceived:%s", i, expected, received)
			}
		}
	})

	t.Run("negative", func(t *testing.T) {
		rnd := rand.New(rand.NewSource(rand.Int63()))
		for i := int64(math.MinInt16); i > -1<<23; i = i - int64(rnd.Int31n(300)) {
			data := genData(i)
			expected := big.NewInt(i)

			received := Dec2BigInt(data)
			if expected.Cmp(received) != 0 {
				t.Fatalf("%d\nexpected:%s\nreceived:%s", i, expected, received)
			}

			_ = DecBigInt(data, received)
			if expected.Cmp(received) != 0 {
				t.Fatalf("%d\nexpected:%s\nreceived:%s", i, expected, received)
			}
		}
	})
}

func TestUnmarshalBigIntNilDestination(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{name: "null"},
		{name: "empty", data: []byte{}},
		{name: "positive", data: []byte{1}},
		{name: "negative", data: []byte{255}},
		{name: "large", data: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var v *big.Int
			var vr **big.Int
			for _, decode := range []struct {
				name string
				call func() error
				want error
			}{
				{name: "DecBigInt", call: func() error { return DecBigInt(tt.data, v) }, want: errNilReference(v)},
				{name: "DecBigIntR", call: func() error { return DecBigIntR(tt.data, vr) }, want: errNilReference(vr)},
				{name: "UnmarshalBigInt", call: func() error { return Unmarshal(tt.data, v) }, want: errNilReference(v)},
				{name: "UnmarshalBigIntR", call: func() error { return Unmarshal(tt.data, vr) }, want: errNilReference(vr)},
			} {
				t.Run(decode.name, func(t *testing.T) {
					if err := decode.call(); err == nil || err.Error() != decode.want.Error() {
						t.Fatalf("expected %v, got %v", decode.want, err)
					}
				})
			}
		})
	}
}
