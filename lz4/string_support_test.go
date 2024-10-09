package lz4

import (
	"bytes"
	"strconv"
	"testing"
)

func TestBinaryToASII(t *testing.T) {
	tcases := []struct {
		in       []byte
		expected []byte
	}{
		{
			in:       []byte{0xaa},
			expected: []byte{0x2a, 0x1},
		},
		{
			in:       []byte{0xaa, 0xaa, 0xaa},
			expected: []byte{0x2a, 0x55, 0x2a, 0x5},
		},
		{
			in:       []byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa},
			expected: []byte{0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x55},
		},
		{
			in:       []byte{0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa},
			expected: []byte{0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x55, 0x2a, 0x5},
		},
	}
	
	t.Run("convertBinToASCII", func(t *testing.T) {
		for id, tcase := range tcases {
			t.Run(strconv.Itoa(id), func(t *testing.T) {
				got := convertBinToASCII(tcase.in)
				if !bytes.Equal(tcase.expected, got) {
					t.Errorf("expected %v, got %v", tcase.expected, got)
				}
			})
		}
	})

	t.Run("convertASCIIToBin", func(t *testing.T) {
		for id, tcase := range tcases {
			t.Run(strconv.Itoa(id), func(t *testing.T) {
				got := convertASCIIToBin(tcase.expected)
				if !bytes.Equal(tcase.in, got) {
					t.Errorf("expected %v, got %v", tcase.expected, got)
				}
			})
		}
	})
}
