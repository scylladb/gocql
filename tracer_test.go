//go:build integration
// +build integration

package gocql

import (
	"fmt"
	"testing"
)

func TestTracingNewAPI(t *testing.T) {
	t.Parallel()

	session := createSession(t)
	defer session.Close()

	table := testTableName(t)

	if err := createTable(session, fmt.Sprintf(`CREATE TABLE gocql_test.%s (id int primary key)`, table)); err != nil {
		t.Fatal("create:", err)
	}

	trace := NewTracer(session)
	if err := session.Query(fmt.Sprintf(`INSERT INTO %s (id) VALUES (?)`, table), 42).Trace(trace).Exec(); err != nil {
		t.Fatal("insert:", err)
	}

	var value int
	if err := session.Query(fmt.Sprintf(`SELECT id FROM %s WHERE id = ?`, table), 42).Trace(trace).Scan(&value); err != nil {
		t.Fatal("select:", err)
	} else if value != 42 {
		t.Fatalf("value: expected %d, got %d", 42, value)
	}

	for _, traceID := range trace.AllTraceIDs() {
		var (
			isReady bool
			err     error
		)
		for !isReady {
			isReady, err = trace.IsReady(traceID)
			if err != nil {
				t.Fatal("Error: ", err)
			}
		}
		activities, err := trace.GetActivities(traceID)
		if err != nil {
			t.Fatal(err)
		}
		coordinator, _, err := trace.GetCoordinatorTime(traceID)
		if err != nil {
			t.Fatal(err)
		}
		if len(activities) == 0 {
			t.Fatal("Failed to obtain any tracing for tradeID: ", traceID)
		} else if coordinator == "" {
			t.Fatal("Failed to obtain coordinator for traceID: ", traceID)
		}
	}
}
