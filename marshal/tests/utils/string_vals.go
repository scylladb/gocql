package utils

import (
	"fmt"
	"math/big"
	"net"
	"reflect"
	"time"
)

func ValueNames(vals []interface{}) []string {
	names := make([]string, 0, len(vals))
	if len(vals) == 1 {
		names = append(names, fmt.Sprintf("(%T)", vals[0]))
	} else {
		for i := range vals {
			names = append(names, valStrLim(100, vals[i]))
		}
	}
	return names
}

func StringValue(in interface{}) string {
	return fmt.Sprintf("(%T)(%s)", in, stringValue(in))
}

func valStrLim(lim int, in interface{}) string {
	out := StringValue(in)
	if len(out) > lim {
		return out[:lim]
	}
	return out
}

func stringValue(in interface{}) string {
	switch i := in.(type) {
	case string:
		return i
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
		return fmt.Sprintf("*%s", stringValue(rv.Elem().Interface()))
	case reflect.Slice:
		if rv.IsNil() {
			return "[nil]"
		}
		return fmt.Sprintf("%v", rv.Interface())
	default:
		return fmt.Sprintf("%v", in)
	}
}
