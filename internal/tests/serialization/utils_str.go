package serialization

import (
	"fmt"
	"math/big"
	"net"
	"reflect"
	"time"

	"gopkg.in/inf.v0"
)

const printLimit = 100

// stringValue returns (value_type)(value) in the human-readable format.
func stringValue(in any) string {
	valStr := stringVal(in)
	if len(valStr) > printLimit {
		return fmt.Sprintf("(%T)", in)
	}
	return fmt.Sprintf("(%T)(%s)", in, valStr)
}

func stringData(p []byte) string {
	if len(p) > printLimit {
		p = p[:printLimit]
	}
	if p == nil {
		return "[nil]"
	}
	return fmt.Sprintf("[%x]", p)
}

func stringVal(in any) string {
	switch i := in.(type) {
	case string:
		return i
	case inf.Dec:
		return fmt.Sprintf("%v", i.String())
	case big.Int:
		return fmt.Sprintf("%v", i.String())
	case net.IP:
		return fmt.Sprintf("%v", []byte(i))
	case time.Time:
		return fmt.Sprintf("%v", i.UnixMilli())
	case nil:
		return "nil"
	}

	rv := reflect.ValueOf(in)
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			return "*nil"
		}
		return fmt.Sprintf("*%s", stringVal(rv.Elem().Interface()))
	case reflect.Slice:
		if rv.IsNil() {
			return "[nil]"
		}
		return fmt.Sprintf("%v", rv.Interface())
	default:
		return fmt.Sprintf("%v", in)
	}
}
