//go:build integration
// +build integration

package gocql

import "testing"

func isSelectWithoutFromUnsupported(err error) bool {
	requestErr, ok := err.(RequestError)
	if !ok {
		return false
	}
	return requestErr.Code() == ErrCodeSyntax || requestErr.Code() == ErrCodeInvalid
}

func TestSelectWithoutFrom(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	var one int
	if err := unpreparedQuery(session, "SELECT 1").Scan(&one); err != nil {
		if isSelectWithoutFromUnsupported(err) {
			t.Skip("server does not support SELECT without FROM")
		}
		t.Fatalf("SELECT 1 failed: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected SELECT 1 to return 1, got %d", one)
	}

	var now UUID
	if err := unpreparedQuery(session, "SELECT now()").Scan(&now); err != nil {
		t.Fatalf("SELECT now() failed: %v", err)
	}

	var preparedOne int
	if err := session.Query("SELECT 1").Bind().Scan(&preparedOne); err != nil {
		t.Fatalf("prepared SELECT 1 failed: %v", err)
	}
	if preparedOne != 1 {
		t.Fatalf("expected prepared SELECT 1 to return 1, got %d", preparedOne)
	}

	var preparedNow UUID
	if err := session.Query("SELECT now()").Bind().Scan(&preparedNow); err != nil {
		t.Fatalf("prepared SELECT now() failed: %v", err)
	}
}

func unpreparedQuery(session *Session, stmt string) *Query {
	query := session.Query(stmt)
	query.skipPrepare = true
	return query
}
