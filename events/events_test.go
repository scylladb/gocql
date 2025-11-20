//go:build unit
// +build unit

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

package events

import (
	"net"
	"testing"
)

func TestTopologyChangeEvent(t *testing.T) {
	event := &TopologyChangeEvent{
		Change: "NEW_NODE",
		Host:   net.ParseIP("192.168.1.1"),
		Port:   9042,
	}

	if event.Type() != ClusterEventTypeTopologyChange {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeTopologyChange)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("TopologyChangeEvent.String() = %s", str)
}

func TestStatusChangeEvent(t *testing.T) {
	event := &StatusChangeEvent{
		Change: "UP",
		Host:   net.ParseIP("192.168.1.2"),
		Port:   9042,
	}

	if event.Type() != ClusterEventTypeStatusChange {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeStatusChange)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("StatusChangeEvent.String() = %s", str)
}

func TestSchemaChangeKeyspaceEvent(t *testing.T) {
	event := &SchemaChangeKeyspaceEvent{
		Change:   "CREATED",
		Keyspace: "test_keyspace",
	}

	if event.Type() != ClusterEventTypeSchemaChangeKeyspace {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeSchemaChangeKeyspace)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("SchemaChangeKeyspaceEvent.String() = %s", str)
}

func TestSchemaChangeTableEvent(t *testing.T) {
	event := &SchemaChangeTableEvent{
		Change:   "UPDATED",
		Keyspace: "test_keyspace",
		Table:    "test_table",
	}

	if event.Type() != ClusterEventTypeSchemaChangeTable {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeSchemaChangeTable)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("SchemaChangeTableEvent.String() = %s", str)
}

func TestSchemaChangeTypeEvent(t *testing.T) {
	event := &SchemaChangeTypeEvent{
		Change:   "DROPPED",
		Keyspace: "test_keyspace",
		TypeName: "test_type",
	}

	if event.Type() != ClusterEventTypeSchemaChangeType {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeSchemaChangeType)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("SchemaChangeTypeEvent.String() = %s", str)
}

func TestSchemaChangeFunctionEvent(t *testing.T) {
	event := &SchemaChangeFunctionEvent{
		Change:    "CREATED",
		Keyspace:  "test_keyspace",
		Function:  "test_function",
		Arguments: []string{"int", "text"},
	}

	if event.Type() != ClusterEventTypeSchemaChangeFunction {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeSchemaChangeFunction)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("SchemaChangeFunctionEvent.String() = %s", str)
}

func TestSchemaChangeAggregateEvent(t *testing.T) {
	event := &SchemaChangeAggregateEvent{
		Change:    "UPDATED",
		Keyspace:  "test_keyspace",
		Aggregate: "test_aggregate",
		Arguments: []string{"int"},
	}

	if event.Type() != ClusterEventTypeSchemaChangeAggregate {
		t.Errorf("Type() = %v, want %v", event.Type(), ClusterEventTypeSchemaChangeAggregate)
	}

	str := event.String()
	if str == "" {
		t.Error("String() returned empty string")
	}
	t.Logf("SchemaChangeAggregateEvent.String() = %s", str)
}

func TestEventInterface(t *testing.T) {
	events := []Event{
		&TopologyChangeEvent{Change: "NEW_NODE", Host: net.ParseIP("127.0.0.1"), Port: 9042},
		&StatusChangeEvent{Change: "UP", Host: net.ParseIP("127.0.0.2"), Port: 9042},
		&SchemaChangeKeyspaceEvent{Change: "CREATED", Keyspace: "ks"},
		&SchemaChangeTableEvent{Change: "UPDATED", Keyspace: "ks", Table: "tbl"},
		&SchemaChangeTypeEvent{Change: "DROPPED", Keyspace: "ks", TypeName: "typ"},
		&SchemaChangeFunctionEvent{Change: "CREATED", Keyspace: "ks", Function: "fn", Arguments: []string{}},
		&SchemaChangeAggregateEvent{Change: "UPDATED", Keyspace: "ks", Aggregate: "agg", Arguments: []string{}},
	}

	for _, event := range events {
		if event.Type() < ClusterEventTypeTopologyChange || event.Type() > ClusterEventTypeSchemaChangeAggregate {
			t.Errorf("Invalid event type: %v", event.Type())
		}
		if event.String() == "" {
			t.Errorf("Event.String() returned empty string for %T", event)
		}
	}
}
