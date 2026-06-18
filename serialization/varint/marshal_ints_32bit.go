//go:build (386 || arm || mips || mipsle) || gocql_32bit

package varint

// On 32-bit architectures, int is 32 bits maximum, so we only support up to 4 bytes
func encInt(v int) []byte {
	if v <= maxInt8 && v >= minInt8 {
		return []byte{byte(v)}
	}
	if v <= maxInt16 && v >= minInt16 {
		return []byte{byte(v >> 8), byte(v)}
	}
	if v <= maxInt24 && v >= minInt24 {
		return []byte{byte(v >> 16), byte(v >> 8), byte(v)}
	}
	// On 32-bit, int max is int32, so this is the final case
	return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
