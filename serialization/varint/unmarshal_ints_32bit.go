//go:build (386 || arm || mips || mipsle) || gocql_32bit

package varint

import (
	"fmt"
)

// On 32-bit architectures, int is 32 bits maximum, so we don't define
// negIntS32, negIntS40, negIntS48, negIntS56 as they would overflow.
// DecInt and DecIntR are also redefined to only support up to 4 bytes.

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
	default:
		return fmt.Errorf("failed to unmarshal varint: to unmarshal into int, the data value should be in the int range (max 32-bit on this platform)")
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
	default:
		return fmt.Errorf("failed to unmarshal varint: to unmarshal into int, the data value should be in the int range (max 32-bit on this platform)")
	}
	return errBrokenData(p)
}
