//go:build all || unit

package cqlint

import (
	"testing"
	"unsafe"
)

// TestArchitectureSpecificDecInt tests that decInt works correctly on both 32-bit and 64-bit architectures
func TestArchitectureSpecificDecInt(t *testing.T) {
	t.Parallel()

	intSize := int(unsafe.Sizeof(int(0)))

	t.Run("int size detection", func(t *testing.T) {
		if intSize != 4 && intSize != 8 {
			t.Fatalf("unexpected int size: %d bytes", intSize)
		}
		t.Logf("Running on %d-bit architecture (int size: %d bytes)", intSize*8, intSize)
	})

	t.Run("decInt with positive values", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []byte
			expected int
		}{
			{"zero", []byte{0, 0, 0, 0}, 0},
			{"one", []byte{0, 0, 0, 1}, 1},
			{"max int8", []byte{0, 0, 0, 127}, 127},
			{"max int16", []byte{0, 0, 127, 255}, 32767},
			{"max int32", []byte{127, 255, 255, 255}, 2147483647},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := decInt(tc.input)
				if result != tc.expected {
					t.Errorf("decInt(%v) = %d, expected %d", tc.input, result, tc.expected)
				}
			})
		}
	})

	t.Run("decInt with negative values", func(t *testing.T) {
		testCases := []struct {
			name     string
			input    []byte
			expected int
		}{
			{"minus one", []byte{255, 255, 255, 255}, -1},
			{"min int8", []byte{255, 255, 255, 128}, -128},
			{"min int16", []byte{255, 255, 128, 0}, -32768},
			{"min int32", []byte{128, 0, 0, 0}, -2147483648},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := decInt(tc.input)
				if result != tc.expected {
					t.Errorf("decInt(%v) = %d, expected %d", tc.input, result, tc.expected)
				}
			})
		}
	})

	t.Run("round trip with DecInt and EncInt", func(t *testing.T) {
		testValues := []int{
			0, 1, -1, 127, -128, 32767, -32768,
			2147483647, -2147483648,
		}

		for _, value := range testValues {
			encoded, err := EncInt(value)
			if err != nil {
				t.Errorf("EncInt(%d) failed: %v", value, err)
				continue
			}

			var decoded int
			err = DecInt(encoded, &decoded)
			if err != nil {
				t.Errorf("DecInt failed for value %d: %v", value, err)
				continue
			}

			if decoded != value {
				t.Errorf("Round trip failed for value %d: got %d", value, decoded)
			}
		}
	})
}

// TestDecIntConsistency verifies that decInt produces the same results as decInt32 cast to int
func TestDecIntConsistency(t *testing.T) {
	t.Parallel()

	testCases := [][]byte{
		{0, 0, 0, 0},
		{0, 0, 0, 1},
		{127, 255, 255, 255},
		{255, 255, 255, 255},
		{128, 0, 0, 0},
		{255, 255, 255, 128},
	}

	for _, input := range testCases {
		resultInt := decInt(input)
		resultInt32 := decInt32(input)
		expected := int(resultInt32)

		if resultInt != expected {
			t.Errorf("decInt(%v) = %d, but int(decInt32(%v)) = %d", input, resultInt, input, expected)
		}
	}
}
