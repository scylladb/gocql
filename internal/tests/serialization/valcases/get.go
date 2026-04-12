package valcases

import (
	"reflect"
)

type SimpleTypes []SimpleTypeCases

type SimpleTypeCases struct {
	CQLName string
	Cases   []SimpleTypeCase
	CQLType int
}

type SimpleTypeCase struct {
	Name      string
	Data      []byte
	LangCases []LangCase
}

type LangCase struct {
	Value     any
	LangType  string
	ErrInsert bool
	ErrSelect bool
}

var nilBytes = ([]byte)(nil)

func GetSimple() SimpleTypes {
	return simpleTypesCases
}

func nilRef(in any) any {
	out := reflect.NewAt(reflect.TypeOf(in), nil).Interface()
	return out
}
