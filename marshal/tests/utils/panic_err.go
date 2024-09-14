package utils

import (
	"fmt"
)

type PanicErr struct {
	Err   error
	Stack []byte
}

func (e PanicErr) Error() string {
	return fmt.Sprintf("%v\n%s", e.Err, e.Stack)
}
