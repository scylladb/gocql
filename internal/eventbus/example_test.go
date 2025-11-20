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

package eventbus_test

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql/internal/eventbus"
)

// Example demonstrates basic usage of EventBus
func Example() {
	// Create a new EventBus for integer events with input buffer of 10
	eb := eventbus.New[int](eventbus.EventBusConfig{
		InputEventsQueueSize: 10,
	}, nil)

	// Start the event bus
	if err := eb.Start(); err != nil {
		panic(err)
	}
	defer eb.Stop()

	// Subscribe to all events
	allSub := eb.Subscribe("all-subscriber", 10, nil)

	// Subscribe to even numbers only
	evenFilter := func(n int) bool { return n%2 == 0 }
	evenSub := eb.Subscribe("even-subscriber", 10, evenFilter)

	// Send some events
	go func() {
		for i := 1; i <= 5; i++ {
			eb.PublishEvent(i)
		}
	}()

	// Receive events
	time.Sleep(100 * time.Millisecond) // Give time for events to be distributed

	// Drain all events from allEvents
	for {
		select {
		case val := <-allSub.Events():
			fmt.Printf("All subscriber received: %d\n", val)
		default:
			goto evenLoop
		}
	}

evenLoop:
	// Drain even events
	for {
		select {
		case val := <-evenSub.Events():
			fmt.Printf("Even subscriber received: %d\n", val)
		default:
			return
		}
	}

	// Output:
	// All subscriber received: 1
	// All subscriber received: 2
	// All subscriber received: 3
	// All subscriber received: 4
	// All subscriber received: 5
	// Even subscriber received: 2
	// Even subscriber received: 4
}

// Example_withContext demonstrates using context-based subscriptions
func Example_withContext() {
	eb := eventbus.New[string](
		eventbus.EventBusConfig{
			InputEventsQueueSize: 10,
		}, nil)
	eb.Start()
	defer eb.Stop()

	// Create a context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Subscribe with context - will auto-remove when context is cancelled
	sub := eb.SubscribeWithContext(ctx, "temp-subscriber", 10, nil)

	// Send some events
	go func() {
		for i := 0; i < 3; i++ {
			eb.PublishEvent(fmt.Sprintf("event-%d", i))
			time.Sleep(30 * time.Millisecond)
		}
	}()

	// Receive events until context is cancelled
	for {
		select {
		case event := <-sub.Events():
			fmt.Println("Received:", event)
		case <-ctx.Done():
			fmt.Println("Context cancelled, subscription ended")
			return
		}
	}

	// Output:
	// Received: event-0
	// Received: event-1
	// Received: event-2
	// Context cancelled, subscription ended
}

// Example_multipleSubscribers demonstrates multiple subscribers with different filters
func Example_multipleSubscribers() {
	type LogEvent struct {
		Level   string
		Message string
	}

	eb := eventbus.New[LogEvent](eventbus.EventBusConfig{
		InputEventsQueueSize: 10,
	}, nil)
	eb.Start()
	defer eb.Stop()

	// Subscribe to error logs only
	errorFilter := func(e LogEvent) bool { return e.Level == "ERROR" }
	errorSub := eb.Subscribe("error-logger", 5, errorFilter)

	// Subscribe to all logs
	allSub := eb.Subscribe("all-logger", 10, nil)

	// Send various log events
	go func() {
		logs := []LogEvent{
			{Level: "INFO", Message: "Application status"},
			{Level: "ERROR", Message: "Connection failed"},
			{Level: "INFO", Message: "Retrying connection"},
			{Level: "ERROR", Message: "Max retries exceeded"},
		}
		for _, log := range logs {
			eb.PublishEvent(log)
		}
	}()

	time.Sleep(100 * time.Millisecond)

	fmt.Println("Error logger received:")
	for {
		select {
		case event := <-errorSub.Events():
			fmt.Printf("  [%s] %s\n", event.Level, event.Message)
		default:
			goto allLogs
		}
	}

allLogs:
	fmt.Println("All logger received:")
	for {
		select {
		case event := <-allSub.Events():
			fmt.Printf("  [%s] %s\n", event.Level, event.Message)
		default:
			return
		}
	}

	// Output:
	// Error logger received:
	//   [ERROR] Connection failed
	//   [ERROR] Max retries exceeded
	// All logger received:
	//   [INFO] Application status
	//   [ERROR] Connection failed
	//   [INFO] Retrying connection
	//   [ERROR] Max retries exceeded
}

// Example_unsubscribe demonstrates dynamic subscription management
func Example_unsubscribe() {
	eb := eventbus.New[int](eventbus.EventBusConfig{
		InputEventsQueueSize: 10,
	}, nil)

	eb.Start()
	defer eb.Stop()

	// Subscribe
	sub := eb.Subscribe("temporary", 10, nil)

	// Send first batch of events
	eb.PublishEvent(1)
	eb.PublishEvent(2)

	// Receive first batch
	fmt.Println("Before remove:")
	for i := 0; i < 2; i++ {
		fmt.Println("Received:", <-sub.Events())
	}

	// Unsubscribe using Stop method
	err := sub.Stop()
	if err != nil {
		panic(err)
	}

	// Send more events (won't be received)
	eb.PublishEvent(3)
	eb.PublishEvent(4)

	// Channel is now closed
	if _, ok := <-sub.Events(); !ok {
		fmt.Println("Channel closed after remove")
	}

	// Output:
	// Before remove:
	// Received: 1
	// Received: 2
	// Channel closed after remove
}
