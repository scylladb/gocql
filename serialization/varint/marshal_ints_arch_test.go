//go:build all || unit

package varint

import (
	"testing"
	"unsafe"
)

// TestArchitectureSpecificInt tests that int operations work correctly on both 32-bit and 64-bit architectures
func TestArchitectureSpecificInt(t *testing.T) {
	t.Parallel()

	intSize := int(unsafe.Sizeof(int(0)))

	t.Run("int size detection", func(t *testing.T) {
		if intSize != 4 && intSize != 8 {
			t.Fatalf("unexpected int size: %d bytes", intSize)
		}
		t.Logf("Running on %d-bit architecture (int size: %d bytes)", intSize*8, intSize)
	})

	t.Run("marshal and unmarshal int max values", func(t *testing.T) {
		if intSize == 4 {
			// 32-bit architecture: test up to 4 bytes (int32 range)
			testCases := []struct {
				name  string
				value int
				bytes int
			}{
				{"max int8", 127, 1},
				{"min int8", -128, 1},
				{"max int16", 32767, 2},
				{"min int16", -32768, 2},
				{"max int24", 8388607, 3},
				{"min int24", -8388608, 3},
				{"max int32", 2147483647, 4},
				{"min int32", -2147483648, 4},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					// Test marshal
					encoded := encInt(tc.value)
					if len(encoded) != tc.bytes {
						t.Errorf("encInt(%d) returned %d bytes, expected %d", tc.value, len(encoded), tc.bytes)
					}

					// Test unmarshal
					var decoded int
					err := DecInt(encoded, &decoded)
					if err != nil {
						t.Errorf("DecInt failed: %v", err)
					}
					if decoded != tc.value {
						t.Errorf("DecInt(%x) = %d, expected %d", encoded, decoded, tc.value)
					}

					// Test unmarshal with pointer
					var decodedPtr *int
					err = DecIntR(encoded, &decodedPtr)
					if err != nil {
						t.Errorf("DecIntR failed: %v", err)
					}
					if decodedPtr == nil || *decodedPtr != tc.value {
						t.Errorf("DecIntR(%x) = %v, expected %d", encoded, decodedPtr, tc.value)
					}
				})
			}

			// Test that 5+ byte data is rejected on 32-bit
			t.Run("reject 5-byte data on 32-bit", func(t *testing.T) {
				// 5 bytes of data
				data := []byte{0x01, 0x00, 0x00, 0x00, 0x00}
				var decoded int
				err := DecInt(data, &decoded)
				if err == nil {
					t.Error("DecInt should reject 5-byte data on 32-bit architecture")
				}
			})

		} else {
			// 64-bit architecture: test using the 64-bit specific tests
			// (defined in marshal_ints_arch_64bit_test.go)
			t.Skip("64-bit tests defined in separate file")
		}
	})

	t.Run("round trip test", func(t *testing.T) {
		// Test various int values for round-trip encoding/decoding
		testValues := []int{
			0, 1, -1, 127, -128, 255, -256,
			32767, -32768, 65535, -65536,
			2147483647, -2147483648,
		}

		for _, value := range testValues {
			encoded := encInt(value)
			var decoded int
			err := DecInt(encoded, &decoded)
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

// TestInt64Operations ensures int64 operations work on both 32-bit and 64-bit
func TestInt64Operations(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name  string
		value int64
		bytes int
	}{
		{"max int8", 127, 1},
		{"min int8", -128, 1},
		{"max int16", 32767, 2},
		{"min int16", -32768, 2},
		{"max int32", 2147483647, 4},
		{"min int32", -2147483648, 4},
		{"max int40", 549755813887, 5},
		{"min int40", -549755813888, 5},
		{"max int48", 140737488355327, 6},
		{"min int48", -140737488355328, 6},
		{"max int56", 36028797018963967, 7},
		{"min int56", -36028797018963968, 7},
		{"max int64", 9223372036854775807, 8},
		{"min int64", -9223372036854775808, 8},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test marshal
			encoded := EncInt64Ext(tc.value)
			if len(encoded) != tc.bytes {
				t.Errorf("EncInt64Ext(%d) returned %d bytes, expected %d", tc.value, len(encoded), tc.bytes)
			}

			// Test unmarshal
			var decoded int64
			err := DecInt64(encoded, &decoded)
			if err != nil {
				t.Errorf("DecInt64 failed: %v", err)
			}
			if decoded != tc.value {
				t.Errorf("DecInt64(%x) = %d, expected %d", encoded, decoded, tc.value)
			}
		})
	}
}
