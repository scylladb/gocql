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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cqlproto

import (
	"encoding/binary"
	"testing"
)

func TestReaderReadBigInt(t *testing.T) {
	// [4-byte len=8][8-byte value]
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:], 8)
	binary.BigEndian.PutUint64(buf[4:], uint64(0x7FFFFFFFFFFFFFFF))

	r := NewReader(buf)
	got, ok := r.ReadBigInt()
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if !ok {
		t.Fatal("expected non-null")
	}
	if got != 0x7FFFFFFFFFFFFFFF {
		t.Fatalf("expected MaxInt64, got %d", got)
	}
}

func TestReaderReadInt(t *testing.T) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:], 4)
	binary.BigEndian.PutUint32(buf[4:], 42)

	r := NewReader(buf)
	got, ok := r.ReadInt()
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if !ok {
		t.Fatal("expected non-null")
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestReaderReadUUID(t *testing.T) {
	buf := make([]byte, 20)
	binary.BigEndian.PutUint32(buf[0:], 16)
	for i := 0; i < 16; i++ {
		buf[4+i] = byte(i + 1)
	}

	r := NewReader(buf)
	got, ok := r.ReadUUID()
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if !ok {
		t.Fatal("expected non-null")
	}
	for i := 0; i < 16; i++ {
		if got[i] != byte(i+1) {
			t.Fatalf("byte %d: expected %d, got %d", i, i+1, got[i])
		}
	}
}

func TestReaderErrorPropagation(t *testing.T) {
	// Empty buffer should error on first read.
	r := NewReader(nil)
	_ = r.ReadRawInt()
	if r.Err() == nil {
		t.Fatal("expected error on empty read")
	}

	// Subsequent reads should be no-ops.
	_, _ = r.ReadBigInt()
	if r.Err() == nil {
		t.Fatal("expected error to persist")
	}
}

func TestReaderReadBytes_Null(t *testing.T) {
	// Length = -1 means null in CQL.
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf[0:], 0xFFFFFFFF) // -1 as uint32
	r := NewReader(buf)
	got := r.ReadBytes()
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if got != nil {
		t.Fatalf("expected nil for null, got %v", got)
	}
}

func TestReaderCollectionCount(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf[0:], 7)
	r := NewReader(buf)
	got := r.ReadCollectionCount()
	if r.Err() != nil {
		t.Fatal(r.Err())
	}
	if got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
}
