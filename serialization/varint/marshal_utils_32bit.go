//go:build (386 || arm || mips || mipsle) || gocql_32bit

package varint

// On 32-bit architectures, constants for int40, int48, and int56 would overflow
// when used with int type. However, they are still needed for int64 operations.
// We define them as int64 here so they can be used in EncInt64Ext.
const (
	maxInt40 = int64(1<<39 - 1)
	maxInt48 = int64(1<<47 - 1)
	maxInt56 = int64(1<<55 - 1)

	minInt40 = int64(-1 << 39)
	minInt48 = int64(-1 << 47)
	minInt56 = int64(-1 << 55)
)
