package serialization

import (
	"reflect"
)

func GetTypes(values ...any) []reflect.Type {
	types := make([]reflect.Type, len(values))
	for i, value := range values {
		types[i] = reflect.TypeOf(value)
	}
	return types
}

func isTypeOf(value any, types []reflect.Type) bool {
	valueType := reflect.TypeOf(value)
	for i := range types {
		if types[i] == valueType {
			return true
		}
	}
	return false
}

func deReference(in any) any {
	return reflect.Indirect(reflect.ValueOf(in)).Interface()
}
