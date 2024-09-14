package funcs

import (
	"fmt"
	"reflect"
)

var (
	excludedMarshal   = func(interface{}) ([]byte, error) { return nil, fmt.Errorf("run on excludedMarshal func") }
	excludedUnmarshal = func([]byte, interface{}) error { return fmt.Errorf("run on excludedUnmarshal func") }
)

func IsExcludedMarshal(f func(interface{}) ([]byte, error)) bool {
	return reflect.ValueOf(f).Pointer() == reflect.ValueOf(excludedMarshal).Pointer()
}

func IsExcludedUnmarshal(f func([]byte, interface{}) error) bool {
	return reflect.ValueOf(f).Pointer() == reflect.ValueOf(excludedUnmarshal).Pointer()
}
