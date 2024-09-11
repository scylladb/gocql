package tests

import (
	"fmt"
	"math/big"
	"net"
	"reflect"
	"time"
)

const (
	printLimit = 1000
	soloRunMsg = "SoloRun is true, please remove it after finished tuning"
)

func stringData(p []byte) string {
	if p == nil {
		return "[nil]"
	}
	return fmt.Sprintf("[%x]", p)
}

func stringValLimited(lim int, in interface{}) string {
	out := stringVal(in)
	if len(out) > lim {
		return out[:lim]
	}
	return out
}

func stringVal(in interface{}) string {
	out := fmt.Sprintf("(%T)(%s)", in, stringValue(in))
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
