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

package events_test

import (
	"net"
	"testing"

	"github.com/gocql/gocql/events"
	frm "github.com/gocql/gocql/internal/frame"
)

func TestFrameToEvent_TopologyChange(t *testing.T) {
	frame := &frm.TopologyChangeEventFrame{
		Change: "NEW_NODE",
		Host:   net.ParseIP("192.168.1.1"),
		Port:   9042,
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	topologyEvent, ok := event.(*events.TopologyChangeEvent)
	if !ok {
		t.Fatalf("Expected *TopologyChangeEvent, got %T", event)
	}

	if topologyEvent.Change != "NEW_NODE" {
		t.Errorf("Change = %v, want NEW_NODE", topologyEvent.Change)
	}
	if !topologyEvent.Host.Equal(net.ParseIP("192.168.1.1")) {
		t.Errorf("Host = %v, want 192.168.1.1", topologyEvent.Host)
	}
	if topologyEvent.Port != 9042 {
		t.Errorf("Port = %v, want 9042", topologyEvent.Port)
	}
	if topologyEvent.Type() != events.ClusterEventTypeTopologyChange {
		t.Errorf("Type() = %v, want EventTypeTopologyChange", topologyEvent.Type())
	}
}

func TestFrameToEvent_StatusChange(t *testing.T) {
	frame := &frm.StatusChangeEventFrame{
		Change: "UP",
		Host:   net.ParseIP("192.168.1.2"),
		Port:   9042,
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	statusEvent, ok := event.(*events.StatusChangeEvent)
	if !ok {
		t.Fatalf("Expected *StatusChangeEvent, got %T", event)
	}

	if statusEvent.Change != "UP" {
		t.Errorf("Change = %v, want UP", statusEvent.Change)
	}
	if !statusEvent.Host.Equal(net.ParseIP("192.168.1.2")) {
		t.Errorf("Host = %v, want 192.168.1.2", statusEvent.Host)
	}
	if statusEvent.Port != 9042 {
		t.Errorf("Port = %v, want 9042", statusEvent.Port)
	}
	if statusEvent.Type() != events.ClusterEventTypeStatusChange {
		t.Errorf("Type() = %v, want EventTypeStatusChange", statusEvent.Type())
	}
}

func TestFrameToEvent_SchemaChangeKeyspace(t *testing.T) {
	frame := &frm.SchemaChangeKeyspace{
		Change:   "CREATED",
		Keyspace: "test_keyspace",
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	schemaEvent, ok := event.(*events.SchemaChangeKeyspaceEvent)
	if !ok {
		t.Fatalf("Expected *SchemaChangeKeyspaceEvent, got %T", event)
	}

	if schemaEvent.Change != "CREATED" {
		t.Errorf("Change = %v, want CREATED", schemaEvent.Change)
	}
	if schemaEvent.Keyspace != "test_keyspace" {
		t.Errorf("Keyspace = %v, want test_keyspace", schemaEvent.Keyspace)
	}
	if schemaEvent.Type() != events.ClusterEventTypeSchemaChangeKeyspace {
		t.Errorf("Type() = %v, want EventTypeSchemaChangeKeyspace", schemaEvent.Type())
	}
}

func TestFrameToEvent_SchemaChangeTable(t *testing.T) {
	frame := &frm.SchemaChangeTable{
		Change:   "UPDATED",
		Keyspace: "test_keyspace",
		Object:   "test_table",
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	schemaEvent, ok := event.(*events.SchemaChangeTableEvent)
	if !ok {
		t.Fatalf("Expected *SchemaChangeTableEvent, got %T", event)
	}

	if schemaEvent.Change != "UPDATED" {
		t.Errorf("Change = %v, want UPDATED", schemaEvent.Change)
	}
	if schemaEvent.Keyspace != "test_keyspace" {
		t.Errorf("Keyspace = %v, want test_keyspace", schemaEvent.Keyspace)
	}
	if schemaEvent.Table != "test_table" {
		t.Errorf("Table = %v, want test_table", schemaEvent.Table)
	}
	if schemaEvent.Type() != events.ClusterEventTypeSchemaChangeTable {
		t.Errorf("Type() = %v, want EventTypeSchemaChangeTable", schemaEvent.Type())
	}
}

func TestFrameToEvent_SchemaChangeType(t *testing.T) {
	frame := &frm.SchemaChangeType{
		Change:   "DROPPED",
		Keyspace: "test_keyspace",
		Object:   "test_type",
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	schemaEvent, ok := event.(*events.SchemaChangeTypeEvent)
	if !ok {
		t.Fatalf("Expected *SchemaChangeTypeEvent, got %T", event)
	}

	if schemaEvent.Change != "DROPPED" {
		t.Errorf("Change = %v, want DROPPED", schemaEvent.Change)
	}
	if schemaEvent.Keyspace != "test_keyspace" {
		t.Errorf("Keyspace = %v, want test_keyspace", schemaEvent.Keyspace)
	}
	if schemaEvent.TypeName != "test_type" {
		t.Errorf("TypeName = %v, want test_type", schemaEvent.TypeName)
	}
	if schemaEvent.Type() != events.ClusterEventTypeSchemaChangeType {
		t.Errorf("Type() = %v, want EventTypeSchemaChangeType", schemaEvent.Type())
	}
}

func TestFrameToEvent_SchemaChangeFunction(t *testing.T) {
	frame := &frm.SchemaChangeFunction{
		Change:   "CREATED",
		Keyspace: "test_keyspace",
		Name:     "test_function",
		Args:     []string{"int", "text"},
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	schemaEvent, ok := event.(*events.SchemaChangeFunctionEvent)
	if !ok {
		t.Fatalf("Expected *SchemaChangeFunctionEvent, got %T", event)
	}

	if schemaEvent.Change != "CREATED" {
		t.Errorf("Change = %v, want CREATED", schemaEvent.Change)
	}
	if schemaEvent.Keyspace != "test_keyspace" {
		t.Errorf("Keyspace = %v, want test_keyspace", schemaEvent.Keyspace)
	}
	if schemaEvent.Function != "test_function" {
		t.Errorf("Function = %v, want test_function", schemaEvent.Function)
	}
	if len(schemaEvent.Arguments) != 2 {
		t.Errorf("len(Arguments) = %v, want 2", len(schemaEvent.Arguments))
	}
	if schemaEvent.Type() != events.ClusterEventTypeSchemaChangeFunction {
		t.Errorf("Type() = %v, want EventTypeSchemaChangeFunction", schemaEvent.Type())
	}
}

func TestFrameToEvent_SchemaChangeAggregate(t *testing.T) {
	frame := &frm.SchemaChangeAggregate{
		Change:   "UPDATED",
		Keyspace: "test_keyspace",
		Name:     "test_aggregate",
		Args:     []string{"int"},
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	schemaEvent, ok := event.(*events.SchemaChangeAggregateEvent)
	if !ok {
		t.Fatalf("Expected *SchemaChangeAggregateEvent, got %T", event)
	}

	if schemaEvent.Change != "UPDATED" {
		t.Errorf("Change = %v, want UPDATED", schemaEvent.Change)
	}
	if schemaEvent.Keyspace != "test_keyspace" {
		t.Errorf("Keyspace = %v, want test_keyspace", schemaEvent.Keyspace)
	}
	if schemaEvent.Aggregate != "test_aggregate" {
		t.Errorf("Aggregate = %v, want test_aggregate", schemaEvent.Aggregate)
	}
	if len(schemaEvent.Arguments) != 1 {
		t.Errorf("len(Arguments) = %v, want 1", len(schemaEvent.Arguments))
	}
	if schemaEvent.Type() != events.ClusterEventTypeSchemaChangeAggregate {
		t.Errorf("Type() = %v, want EventTypeSchemaChangeAggregate", schemaEvent.Type())
	}
}

func TestFrameToEvent_Nil(t *testing.T) {
	event := events.FrameToEvent(nil)
	if event != nil {
		t.Errorf("FrameToEvent(nil) = %v, want nil", event)
	}
}

func TestFrameToEvent_NonEventFrame(t *testing.T) {
	// Test with a non-event frame type
	frame := &frm.ErrorFrame{}
	event := events.FrameToEvent(frame)
	if event != nil {
		t.Errorf("FrameToEvent(non-event) = %v, want nil", event)
	}
}

func TestFrameToEvent_ClientRoutesChanged(t *testing.T) {
	frame := &frm.ClientRoutesChanged{
		ChangeType:    "UPDATED",
		ConnectionIDs: []string{"c1", ""},
		HostIDs:       []string{},
	}

	event := events.FrameToEvent(frame)
	if event == nil {
		t.Fatal("FrameToEvent returned nil")
	}

	clientEvent, ok := event.(*events.ClientRoutesChangedEvent)
	if !ok {
		t.Fatalf("Expected *ClientRoutesChangedEvent, got %T", event)
	}

	if clientEvent.ChangeType != "UPDATED" {
		t.Errorf("ChangeType = %v, want UPDATED", clientEvent.ChangeType)
	}
	if len(clientEvent.ConnectionIDs) != 2 || clientEvent.ConnectionIDs[1] != "" {
		t.Errorf("ConnectionIDs = %v, want [c1 \"\"]", clientEvent.ConnectionIDs)
	}
	if len(clientEvent.HostIDs) != 0 {
		t.Errorf("HostIDs = %v, want empty", clientEvent.HostIDs)
	}
	if clientEvent.Type() != events.ClusterEventTypeClientRoutesChanged {
		t.Errorf("Type() = %v, want ClusterEventTypeClientRoutesChanged", clientEvent.Type())
	}
}
