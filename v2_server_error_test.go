package gocql

import (
	"errors"
	"fmt"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestV2ServerErrorsExposeTheirCodeThroughWrappers(t *testing.T) {
	for _, err := range []error{
		frm.ErrorFrame{Code: ErrCodeSyntax, Message: "syntax"},
		&RequestErrUnavailable{ErrorFrame: frm.ErrorFrame{Code: ErrCodeUnavailable, Message: "unavailable"}},
		&RequestErrWriteTimeout{ErrorFrame: frm.ErrorFrame{Code: ErrCodeWriteTimeout, Message: "write timeout"}},
		&RequestErrUnprepared{ErrorFrame: frm.ErrorFrame{Code: ErrCodeUnprepared, Message: "unprepared"}},
	} {
		var direct RequestError
		if !errors.As(err, &direct) {
			t.Fatalf("%T does not satisfy server error contract", err)
		}
		var wrapped RequestError
		if !errors.As(fmt.Errorf("outer: %w", err), &wrapped) || wrapped.GetCode() != direct.GetCode() || wrapped.GetMessage() != direct.GetMessage() {
			t.Fatalf("wrapped %T lost server details", err)
		}
	}
}
