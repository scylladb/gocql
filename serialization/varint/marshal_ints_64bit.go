//go:build (amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || s390x || riscv64 || loong64) && !gocql_32bit

package varint

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
	if v <= maxInt32 && v >= minInt32 {
		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
	if v <= maxInt40 && v >= minInt40 {
		return []byte{byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
	if v <= maxInt48 && v >= minInt48 {
		return []byte{byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
	if v <= maxInt56 && v >= minInt56 {
		return []byte{byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	}
	return []byte{byte(v >> 56), byte(v >> 48), byte(v >> 40), byte(v >> 32), byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}
