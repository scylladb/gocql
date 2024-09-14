package utils

import (
	"fmt"
	"reflect"
)

const SoloRunMsg = "SoloRun is true, please remove it after finished tuning"
const PrintLimit = 100

func StringPointer(i interface{}) string {
	rv := reflect.ValueOf(i)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return ""
		}
		if rv.Elem().Kind() == reflect.Ptr {
			return StringPointer(rv.Elem().Interface())
		}
		return fmt.Sprintf("%d", rv.Pointer())
	}
	return ""
}

func StringData(p []byte) string {
	if p == nil {
		return "[nil]"
	}
	return fmt.Sprintf("[%x]", p)
}
