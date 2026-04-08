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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	frm "github.com/gocql/gocql/internal/frame"
)

func TestEventDebounce(t *testing.T) {
	t.Parallel()

	const eventCount = 150
	var eventsSeen atomic.Int64
	done := make(chan struct{}, 1)

	debouncer := newEventDebouncer("testDebouncer", func(events []frame) {
		if eventsSeen.Add(int64(len(events))) >= eventCount {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}, &defaultLogger{})
	defer debouncer.stop()

	for i := 0; i < eventCount; i++ {
		debouncer.debounce(&frm.StatusChangeEventFrame{
			Change: "UP",
			Host:   net.IPv4(127, 0, 0, 1),
			Port:   9042,
		})
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for events: saw %d of %d", eventsSeen.Load(), eventCount)
	}
	if n := eventsSeen.Load(); n != eventCount {
		t.Fatalf("expected to see %d events but got %d", eventCount, n)
	}
}

// TestEventDebounceMultipleFlushes verifies that the debouncer correctly
// accumulates events across multiple flush cycles without panicking.
// This is a regression test for a race where the callback could fire
// more than once (due to timer re-fires), causing a negative WaitGroup
// counter panic in the original test.
func TestEventDebounceMultipleFlushes(t *testing.T) {
	t.Parallel()

	const eventCount = 50
	var eventsSeen atomic.Int64
	var flushCount atomic.Int64
	firstFlushDone := make(chan struct{}, 1)
	done := make(chan struct{}, 1)

	debouncer := newEventDebouncer("testDebouncerMulti", func(events []frame) {
		if flushCount.Add(1) == 1 {
			select {
			case firstFlushDone <- struct{}{}:
			default:
			}
		}
		if eventsSeen.Add(int64(len(events))) >= eventCount {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}, &defaultLogger{})
	defer debouncer.stop()

	// Send events in two batches separated by more than eventDebounceTime
	// to force at least two separate flush cycles.
	for i := 0; i < eventCount/2; i++ {
		debouncer.debounce(&frm.StatusChangeEventFrame{
			Change: "UP",
			Host:   net.IPv4(127, 0, 0, 1),
			Port:   9042,
		})
	}

	select {
	case <-firstFlushDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for first flush: saw %d events across %d flushes", eventsSeen.Load(), flushCount.Load())
	}

	for i := 0; i < eventCount/2; i++ {
		debouncer.debounce(&frm.StatusChangeEventFrame{
			Change: "UP",
			Host:   net.IPv4(127, 0, 0, 1),
			Port:   9042,
		})
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for events: saw %d of %d", eventsSeen.Load(), eventCount)
	}
	if n := eventsSeen.Load(); n != eventCount {
		t.Fatalf("expected to see %d events but got %d", eventCount, n)
	}
	if f := flushCount.Load(); f < 2 {
		t.Fatalf("expected at least 2 flush cycles but got %d", f)
	}
}
