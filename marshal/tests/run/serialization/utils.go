package serialization

import (
	"github.com/gocql/gocql/marshal/tests/utils"
)

func deRef(in interface{}) interface{} {
	return utils.DeReference(in)
}

func dataStr(p []byte) string {
	return utils.StringData(p)
}

func valStr(in interface{}) string {
	out := utils.StringValue(in)
	if len(out) > utils.PrintLimit {
		return out[:utils.PrintLimit]
	}
	return out
}

func ptrStr(in interface{}) string { return utils.StringPointer(in) }
