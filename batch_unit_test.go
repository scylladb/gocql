/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"testing"
	"time"
)

func TestBatch_WithServerTimeout_Unit(t *testing.T) {
	// Test that WithServerTimeout sets the serverTimeout field correctly
	cluster := NewCluster("localhost")
	cluster.Keyspace = "test"

	// Create a mock session (we're just testing the API, not execution)
	session := &Session{
		cfg: ClusterConfig{
			RetryPolicy:       &SimpleRetryPolicy{NumRetries: 3},
			SerialConsistency: LocalSerial,
			Timeout:           5 * time.Second,
		},
		cons: Quorum,
	}

	batch := session.Batch(LoggedBatch)

	// Verify serverTimeout is initially zero
	if batch.serverTimeout != 0 {
		t.Errorf("expected initial serverTimeout to be 0, got %v", batch.serverTimeout)
	}

	// Set server timeout
	timeout := 500 * time.Millisecond
	batch.WithServerTimeout(timeout)

	// Verify serverTimeout was set
	if batch.serverTimeout != timeout {
		t.Errorf("expected serverTimeout to be %v, got %v", timeout, batch.serverTimeout)
	}
}

func TestBatch_WithServerTimeout_Chaining(t *testing.T) {
	// Test that WithServerTimeout can be chained with other methods
	session := &Session{
		cfg: ClusterConfig{
			RetryPolicy:       &SimpleRetryPolicy{NumRetries: 3},
			SerialConsistency: LocalSerial,
			Timeout:           5 * time.Second,
		},
		cons: Quorum,
	}

	batch := session.Batch(UnloggedBatch).
		WithServerTimeout(500 * time.Millisecond).
		WithTimestamp(12345).
		SerialConsistency(LocalSerial)

	if batch.serverTimeout != 500*time.Millisecond {
		t.Errorf("expected serverTimeout to be 500ms, got %v", batch.serverTimeout)
	}

	if batch.defaultTimestampValue != 12345 {
		t.Errorf("expected timestamp to be 12345, got %v", batch.defaultTimestampValue)
	}

	if batch.serialCons != LocalSerial {
		t.Errorf("expected serial consistency to be LocalSerial, got %v", batch.serialCons)
	}

	if batch.Type != UnloggedBatch {
		t.Errorf("expected batch type to be UnloggedBatch, got %v", batch.Type)
	}
}
