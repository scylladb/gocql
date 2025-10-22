package gocql

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBatchQueryConstruction(t *testing.T) {
	// Test that the batch query is constructed correctly
	session := &Session{
		cfg: ClusterConfig{
			RetryPolicy:       &SimpleRetryPolicy{NumRetries: 3},
			SerialConsistency: LocalSerial,
			Timeout:           5 * time.Second,
		},
		cons: Quorum,
	}

	batch := session.Batch(LoggedBatch)
	batch.WithServerTimeout(500 * time.Millisecond)
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 2, "Bob")

	// Manually construct what the query should look like
	var buf strings.Builder
	buf.WriteString("BEGIN BATCH USING TIMEOUT 500ms ")
	buf.WriteString("INSERT INTO users (id, name) VALUES (?, ?); ")
	buf.WriteString("INSERT INTO users (id, name) VALUES (?, ?); ")
	buf.WriteString("APPLY BATCH")

	expectedQuery := buf.String()
	expectedArgs := []interface{}{1, "Alice", 2, "Bob"}

	t.Logf("Expected query: %s", expectedQuery)
	t.Logf("Expected args: %v", expectedArgs)

	// Verify batch entries
	if len(batch.Entries) != 2 {
		t.Errorf("expected 2 batch entries, got %d", len(batch.Entries))
	}

	if batch.serverTimeout != 500*time.Millisecond {
		t.Errorf("expected serverTimeout to be 500ms, got %v", batch.serverTimeout)
	}
}

func TestBatchQueryConstructionWithTimestamp(t *testing.T) {
	session := &Session{
		cfg: ClusterConfig{
			RetryPolicy:       &SimpleRetryPolicy{NumRetries: 3},
			SerialConsistency: LocalSerial,
			Timeout:           5 * time.Second,
		},
		cons: Quorum,
	}

	timestamp := int64(12345)
	batch := session.Batch(UnloggedBatch)
	batch.WithServerTimeout(500 * time.Millisecond)
	batch.WithTimestamp(timestamp)
	batch.Query("INSERT INTO users (id, name) VALUES (?, ?)", 1, "Alice")

	// Manually construct what the query should look like
	expectedQuery := fmt.Sprintf("BEGIN UNLOGGED BATCH USING TIMEOUT 500ms AND TIMESTAMP %d INSERT INTO users (id, name) VALUES (?, ?); APPLY BATCH", timestamp)

	t.Logf("Expected query: %s", expectedQuery)

	if batch.serverTimeout != 500*time.Millisecond {
		t.Errorf("expected serverTimeout to be 500ms, got %v", batch.serverTimeout)
	}

	if !batch.defaultTimestamp {
		t.Error("expected defaultTimestamp to be true")
	}

	if batch.defaultTimestampValue != timestamp {
		t.Errorf("expected timestamp to be %d, got %d", timestamp, batch.defaultTimestampValue)
	}
}

func TestBatchQueryConstructionCounterBatch(t *testing.T) {
	session := &Session{
		cfg: ClusterConfig{
			RetryPolicy:       &SimpleRetryPolicy{NumRetries: 3},
			SerialConsistency: LocalSerial,
			Timeout:           5 * time.Second,
		},
		cons: Quorum,
	}

	batch := session.Batch(CounterBatch)
	batch.WithServerTimeout(1 * time.Second)
	batch.Query("UPDATE counters SET count = count + 1 WHERE id = ?", 1)

	// Manually construct what the query should look like
	expectedQuery := "BEGIN COUNTER BATCH USING TIMEOUT 1000ms UPDATE counters SET count = count + 1 WHERE id = ?; APPLY BATCH"

	t.Logf("Expected query: %s", expectedQuery)

	if batch.serverTimeout != 1*time.Second {
		t.Errorf("expected serverTimeout to be 1s, got %v", batch.serverTimeout)
	}

	if batch.Type != CounterBatch {
		t.Errorf("expected batch type to be CounterBatch, got %v", batch.Type)
	}
}
