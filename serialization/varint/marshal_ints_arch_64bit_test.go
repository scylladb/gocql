//go:build ((all || unit) && (amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || s390x || riscv64 || loong64)) && !gocql_32bit

package varint

import (
	"testing"
)

// Test64BitIntOperations tests int operations that only work on 64-bit architectures
func Test64BitIntOperations(t *testing.T) {
	t.Parallel()

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
		{"max int40", 549755813887, 5},
		{"min int40", -549755813888, 5},
		{"max int48", 140737488355327, 6},
		{"min int48", -140737488355328, 6},
		{"max int56", 36028797018963967, 7},
		{"min int56", -36028797018963968, 7},
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

	// Round trip test with 64-bit values
	t.Run("round trip with large values", func(t *testing.T) {
		testValues := []int{
			549755813887, -549755813888,
			140737488355327, -140737488355328,
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
