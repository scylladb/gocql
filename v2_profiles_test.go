package gocql

import (
	"context"
	"errors"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func TestV2RuntimeSafeRetryPreservesServerErrors(t *testing.T) {
	policy := SafeRetryPolicy{MaxAttempts: 3}
	for _, test := range []struct {
		code       int
		idempotent bool
		want       RetryAction
	}{{ErrCodeOverloaded, true, RetryNext}, {ErrCodeBootstrapping, true, RetryNext}, {ErrCodeOverloaded, false, StopRetry}, {ErrCodeSyntax, true, StopRetry}, {ErrCodeInvalid, true, StopRetry}, {ErrCodeProtocol, true, StopRetry}} {
		server := &frm.ErrorFrame{Code: test.code, Message: "test error"}
		err := &QueryError{err: server, potentiallyExecuted: true}
		decision := policy.Decide(AttemptError{Err: err, Attempt: 1, Idempotent: test.idempotent, BytesWritten: true})
		if decision.Action != test.want {
			t.Fatalf("code %x idempotent %t got %d want %d", test.code, test.idempotent, decision.Action, test.want)
		}
		var extracted RequestError
		if !errors.As(err, &extracted) || extracted.GetCode() != test.code {
			t.Fatal("server identity lost")
		}
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, &ProtocolError{Stage: "response", Err: errors.New("invalid frame")}, &ValidationError{Field: "page size", Err: errors.New("invalid")}} {
		if got := policy.Decide(AttemptError{Err: err, Attempt: 1, Idempotent: true}); got.Action != StopRetry {
			t.Fatalf("retry deterministic error %v", err)
		}
	}
}
func TestV2RuntimeStatementKeywords(t *testing.T) {
	for _, cql := range []string{"USE ks", " -- comment\n use ks", "/* comment */ USE ks"} {
		if _, err := BindUnpreparedBorrowed(Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{}); !errors.Is(err, ErrUseStmt) {
			t.Fatalf("USE accepted %q: %v", cql, err)
		}
	}
	if conditionalCQL("INSERT INTO t(a) VALUES ('IF')") || !conditionalCQL("INSERT INTO t(a) VALUES ('x') IF NOT EXISTS") {
		t.Fatal("conditional classification")
	}
}
