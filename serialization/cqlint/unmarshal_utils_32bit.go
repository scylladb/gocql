//go:build (386 || arm || mips || mipsle) || gocql_32bit

package cqlint

// On 32-bit architectures, int is 32 bits, so we use int32 for sign extension
func decInt(p []byte) int {
	// Use int32 for proper sign extension on 32-bit systems
	return int(int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3]))
}
