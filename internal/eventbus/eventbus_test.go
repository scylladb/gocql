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

package eventbus

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	if eb == nil {
		t.Fatal("New returned nil")
	}
	if eb.input == nil {
		t.Error("input channel is nil")
	}
	if len(eb.subscribers) != 0 {
		t.Error("subscribers list is not empty")
	}
	if eb.status != statusInitialized {
		t.Error("EventBus should not be status initially")
	}
}

func TestStartStop(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)

	// Test starting
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Test double start
	err = eb.Start()
	if err != ErrAlreadyStarted {
		t.Errorf("Expected ErrAlreadyStarted, got: %v", err)
	}

	// Test stopping
	err = eb.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Test double stop
	err = eb.Stop()
	if err != ErrAlreadyStopped {
		t.Errorf("Expected ErrAlreadyStopped, got: %v", err)
	}
}

func TestStopWithoutStart(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Stop()
	if err != ErrNotStarted {
		t.Errorf("Expected ErrNotStarted, got: %v", err)
	}
}

func TestEventDistribution(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer eb.Stop()

	sub1 := eb.Subscribe("sub1", 10, nil)
	sub2 := eb.Subscribe("sub2", 10, nil)

	// Send events
	eb.PublishEvent(1)
	eb.PublishEvent(2)
	eb.PublishEvent(3)

	// Verify both subscribers receive all events
	for i := 1; i <= 3; i++ {
		select {
		case val := <-sub1.Events():
			if val != i {
				t.Errorf("sub1: expected %d, got %d", i, val)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("sub1: timeout waiting for event")
		}

		select {
		case val := <-sub2.Events():
			if val != i {
				t.Errorf("sub2: expected %d, got %d", i, val)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("sub2: timeout waiting for event")
		}
	}
}

func TestEventFiltering(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer eb.Stop()

	// Subscriber 1: no filter (receives all)
	sub1 := eb.Subscribe("all", 10, nil)

	// Subscriber 2: only even numbers
	evenFilter := func(n int) bool { return n%2 == 0 }
	sub2 := eb.Subscribe("even", 10, evenFilter)

	// Subscriber 3: only odd numbers
	oddFilter := func(n int) bool { return n%2 != 0 }
	sub3 := eb.Subscribe("odd", 10, oddFilter)

	// Send events
	for i := 1; i <= 6; i++ {
		eb.PublishEvent(i)
	}

	// Verify subscriber 1 gets all events
	received1 := make([]int, 0, 6)
	for i := 0; i < 6; i++ {
		select {
		case val := <-sub1.Events():
			received1 = append(received1, val)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for event on ch1")
		}
	}
	if len(received1) != 6 {
		t.Errorf("ch1: expected 6 events, got %d", len(received1))
	}

	// Verify subscriber 2 gets only even numbers
	received2 := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case val := <-sub2.Events():
			if val%2 != 0 {
				t.Errorf("ch2: received odd number %d", val)
			}
			received2 = append(received2, val)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for event on ch2")
		}
	}

	// Verify subscriber 3 gets only odd numbers
	received3 := make([]int, 0, 3)
	for i := 0; i < 3; i++ {
		select {
		case val := <-sub3.Events():
			if val%2 == 0 {
				t.Errorf("ch3: received even number %d", val)
			}
			received3 = append(received3, val)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for event on ch3")
		}
	}
}

func TestSubscriberCount(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)

	if eb.SubscriberCount() != 0 {
		t.Errorf("Expected 0 subscribers, got %d", eb.SubscriberCount())
	}

	sub1 := eb.Subscribe("sub1", 5, nil)
	if eb.SubscriberCount() != 1 {
		t.Errorf("Expected 1 subscriber, got %d", eb.SubscriberCount())
	}

	eb.Subscribe("sub2", 5, nil)
	if eb.SubscriberCount() != 2 {
		t.Errorf("Expected 2 subscribers, got %d", eb.SubscriberCount())
	}

	err := sub1.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
	if eb.SubscriberCount() != 1 {
		t.Errorf("Expected 1 subscriber, got %d", eb.SubscriberCount())
	}
}

func TestConcurrentSubscribers(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer eb.Stop()

	numSubscribers := 10
	eventsPerSubscriber := 100

	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	// Create multiple subscribers
	for i := 0; i < numSubscribers; i++ {
		sub := eb.Subscribe(string(rune('A'+i)), 100, nil)

		go func(sub *Subscriber[int], subName string) {
			defer wg.Done()
			count := 0
			for range sub.Events() {
				count++
				if count == eventsPerSubscriber {
					return
				}
			}
		}(sub, string(rune('A'+i)))
	}

	// Send events
	go func() {
		for i := 0; i < eventsPerSubscriber; i++ {
			eb.PublishEventBlocking(i)
		}
	}()

	// Wait for all subscribers to receive their events
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All subscribers received events
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for subscribers")
	}
}

func TestSubscribeWithContext(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer eb.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	sub := eb.SubscribeWithContext(ctx, "test", 10, nil)
	if err != nil {
		t.Fatalf("SubscribeWithContext failed: %v", err)
	}

	// Send an event
	eb.PublishEvent(42)

	// Verify event is received
	select {
	case val := <-sub.Events():
		if val != 42 {
			t.Errorf("Expected 42, got %d", val)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Cancel context
	cancel()

	// Give it time to be removed
	time.Sleep(100 * time.Millisecond)

	// Verify subscriber was removed
	if eb.SubscriberCount() != 0 {
		t.Errorf("Expected 0 subscribers after context cancel, got %d", eb.SubscriberCount())
	}
}

func TestChannelClosedOnStop(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	sub := eb.Subscribe("test", 10, nil)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = eb.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify channel is closed
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("Channel should be closed")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for channel close")
	}
}

func TestSubscriberStopAfterEventBusStop(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	sub := eb.Subscribe("test", 10, nil)

	err = eb.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Stop should not panic after eventbus Stop: %v", r)
		}
	}()

	err = sub.Stop()
	if err != ErrSubscriberNotFound {
		t.Fatalf("Expected ErrSubscriberNotFound, got: %v", err)
	}
}

func TestChannelClosedOnUnsubscribe(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)

	sub := eb.Subscribe("test", 10, nil)

	err := sub.Stop()
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	// Verify channel is closed
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("Channel should be closed")
		}
	default:
		// Channel might not be immediately readable, try with timeout
		time.Sleep(10 * time.Millisecond)
		select {
		case _, ok := <-sub.Events():
			if ok {
				t.Error("Channel should be closed")
			}
		default:
			t.Error("Channel should be closed but is not readable")
		}
	}
}

func TestString(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	str := eb.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	eb.Subscribe("test", 5, nil)
	str = eb.String()
	if str == "" {
		t.Error("String() returned empty string after subscription")
	}
}

func TestSlowSubscriberDoesNotBlockBus(t *testing.T) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	err := eb.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer eb.Stop()

	// Fast subscriber with buffer
	fastSub := eb.Subscribe("fast", 100, nil)

	// Slow subscriber with small buffer
	slowSub := eb.Subscribe("slow", 1, nil)

	// Send many events quickly
	for i := 0; i < 50; i++ {
		eb.PublishEventBlocking(i)
	}

	// Fast subscriber should receive most/all events
	fastCount := 0
	timeout := time.After(1 * time.Second)
drainFast:
	for {
		select {
		case <-fastSub.Events():
			fastCount++
			if fastCount == 50 {
				break drainFast
			}
		case <-timeout:
			break drainFast
		}
	}

	if fastCount < 50 { // Should receive most events
		t.Errorf("Fast subscriber only received %d events, expected 50", fastCount)
	}

	// Slow subscriber may have dropped events (buffer overflow)
	slowCount := 0
drainSlow:
	for {
		select {
		case <-slowSub.Events():
			slowCount++
		default:
			break drainSlow
		}
	}

	// Slow subscriber should have received some events but likely not all
	t.Logf("Slow subscriber received %d events (some may have been dropped)", slowCount)
}

func BenchmarkEventDistribution(b *testing.B) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 1000,
		}, nil)
	eb.Start()
	defer eb.Stop()

	// Create 10 subscribers
	for i := 0; i < 10; i++ {
		eb.Subscribe(string(rune('A'+i)), 1000, nil)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.PublishEvent(i)
	}
}

func BenchmarkEventDistributionWithFilter(b *testing.B) {
	eb := New[int](
		EventBusConfig{
			InputEventsQueueSize: 1000,
		}, nil)
	eb.Start()
	defer eb.Stop()

	filter := func(n int) bool { return n%2 == 0 }

	// Create 10 subscribers with filters
	for i := 0; i < 10; i++ {
		eb.Subscribe(string(rune('A'+i)), 1000, filter)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eb.PublishEvent(i)
	}
}
