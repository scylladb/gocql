//go:build (amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || s390x || riscv64 || loong64) && !gocql_32bit

package cqlint

import "math"

const (
	negInt = int(-1) << 32
)

func decInt(p []byte) int {
	if p[0] > math.MaxInt8 {
		return negInt | int(p[0])<<24 | int(p[1])<<16 | int(p[2])<<8 | int(p[3])
	}
	return int(p[0])<<24 | int(p[1])<<16 | int(p[2])<<8 | int(p[3])
}
