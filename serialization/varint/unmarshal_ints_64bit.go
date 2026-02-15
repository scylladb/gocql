//go:build (amd64 || arm64 || ppc64 || ppc64le || mips64 || mips64le || s390x || riscv64 || loong64) && !gocql_32bit

package varint

import (
	"fmt"
)

const (
	negIntS32 = int(-1) << 32
	negIntS40 = int(-1) << 40
	negIntS48 = int(-1) << 48
	negIntS56 = int(-1) << 56
)

func DecInt(p []byte, v *int) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		*v = 0
		return nil
	case 1:
		*v = dec1toInt(p)
		return nil
	case 2:
		*v = dec2toInt(p)
	case 3:
		*v = dec3toInt(p)
	case 4:
		*v = dec4toInt(p)
	case 5:
		*v = dec5toInt(p)
	case 6:
		*v = dec6toInt(p)
	case 7:
		*v = dec7toInt(p)
	case 8:
		*v = dec8toInt(p)
	default:
		return fmt.Errorf("failed to unmarshal varint: to unmarshal into int, the data value should be in the int range")
	}
	return errBrokenData(p)
}

func DecIntR(p []byte, v **int) error {
	if v == nil {
		return errNilReference(v)
	}
	switch len(p) {
	case 0:
		if p == nil {
			*v = nil
		} else {
			*v = new(int)
		}
		return nil
	case 1:
		val := dec1toInt(p)
		*v = &val
		return nil
	case 2:
		val := dec2toInt(p)
		*v = &val
	case 3:
		val := dec3toInt(p)
		*v = &val
	case 4:
		val := dec4toInt(p)
		*v = &val
	case 5:
		val := dec5toInt(p)
		*v = &val
	case 6:
		val := dec6toInt(p)
		*v = &val
	case 7:
		val := dec7toInt(p)
		*v = &val
	case 8:
		val := dec8toInt(p)
		*v = &val
	default:
		return fmt.Errorf("failed to unmarshal varint: to unmarshal into int, the data value should be in the int range")
	}
	return errBrokenData(p)
}

func dec5toInt(p []byte) int {
	if p[0] > 127 {
		return negIntS40 | int(p[0])<<32 | int(p[1])<<24 | int(p[2])<<16 | int(p[3])<<8 | int(p[4])
	}
	return int(p[0])<<32 | int(p[1])<<24 | int(p[2])<<16 | int(p[3])<<8 | int(p[4])
}

func dec6toInt(p []byte) int {
	if p[0] > 127 {
		return negIntS48 | int(p[0])<<40 | int(p[1])<<32 | int(p[2])<<24 | int(p[3])<<16 | int(p[4])<<8 | int(p[5])
	}
	return int(p[0])<<40 | int(p[1])<<32 | int(p[2])<<24 | int(p[3])<<16 | int(p[4])<<8 | int(p[5])
}

func dec7toInt(p []byte) int {
	if p[0] > 127 {
		return negIntS56 | int(p[0])<<48 | int(p[1])<<40 | int(p[2])<<32 | int(p[3])<<24 | int(p[4])<<16 | int(p[5])<<8 | int(p[6])
	}
	return int(p[0])<<48 | int(p[1])<<40 | int(p[2])<<32 | int(p[3])<<24 | int(p[4])<<16 | int(p[5])<<8 | int(p[6])
}

func dec8toInt(p []byte) int {
	return int(p[0])<<56 | int(p[1])<<48 | int(p[2])<<40 | int(p[3])<<32 | int(p[4])<<24 | int(p[5])<<16 | int(p[6])<<8 | int(p[7])
}
