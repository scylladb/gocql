//go:build (amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || s390x || riscv64 || loong64) && !gocql_32bit

package varint

// Constants for int40, int48, and int56 range checks
// These are safe on 64-bit architectures where int is 64 bits
const (
	maxInt40 = 1<<39 - 1
	maxInt48 = 1<<47 - 1
	maxInt56 = 1<<55 - 1

	minInt40 = -1 << 39
	minInt48 = -1 << 47
	minInt56 = -1 << 55
)
