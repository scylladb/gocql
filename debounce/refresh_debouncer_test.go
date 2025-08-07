//go:build unit
// +build unit

package debounce

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This test sends debounce requests and waits until the refresh function is called (which should happen when the timer elapses).
func TestRefreshDebouncer_MultipleEvents(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := NewRefreshDebouncer(2*time.Second, fn)
	defer d.Stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Debounce()
		}()
	}
	wg.Wait()
	timeoutCh := time.After(2500 * time.Millisecond) // extra time to avoid flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1.5 seconds instead of 2 seconds to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~2 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait another 2 seconds and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1.5 seconds after test starts
//
// The end result should be 1 refresh function call when refreshNow() is called.
func TestRefreshDebouncer_RefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 1 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	eventsWg := sync.WaitGroup{}
	d := NewRefreshDebouncer(2*time.Second, fn)
	defer d.Stop()
	for i := 0; i < numberOfEvents; i++ {
		eventsWg.Add(1)
		go func() {
			defer eventsWg.Done()
			d.Debounce()
		}()
	}

	refreshNowWg := sync.WaitGroup{}
	refreshNowWg.Add(1)
	go func() {
		defer refreshNowWg.Done()
		time.Sleep(1500 * time.Millisecond)
		d.RefreshNow()
	}()

	eventsWg.Wait()
	select {
	case <-channel:
		t.Fatalf("function was called before the expected time")
	default:
	}

	refreshNowWg.Wait()

	timeoutCh := time.After(200 * time.Millisecond) // allow for 200ms of delay to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called")
	}
	afterFunctionCall := time.Now()

	// use 1 second instead of 1.5s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 1000*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1.5 seconds", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	// wait some time and check if function was called again
	time.Sleep(2500 * time.Millisecond)
	if len(channel) > 0 {
		t.Fatalf("function was called more than once")
	}
}

// This test:
//
//	1 - Sends debounce requests when test starts
//	2 - Calls refreshNow() before the timer elapsed (which stops the timer) about 1 second after test starts
//	3 - Sends more debounce requests (which resets the timer with a 3-second interval) about 2 seconds after test starts
//
// The end result should be 2 refresh function calls:
//
//	1 - When refreshNow() is called (1 second after the test starts)
//	2 - When the timer elapses after the second "wave" of debounce requests (5 seconds after the test starts)
func TestRefreshDebouncer_EventsAfterRefreshNow(t *testing.T) {
	const numberOfEvents = 10
	channel := make(chan int, numberOfEvents) // should never use more than 2 but allow for more to possibly detect bugs
	fn := func() error {
		channel <- 0
		return nil
	}
	beforeEvents := time.Now()
	wg := sync.WaitGroup{}
	d := NewRefreshDebouncer(3*time.Second, fn)
	defer d.Stop()
	for i := 0; i < numberOfEvents; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.Debounce()
			time.Sleep(2000 * time.Millisecond)
			d.Debounce()
		}()
	}

	go func() {
		time.Sleep(1 * time.Second)
		d.RefreshNow()
	}()

	wg.Wait()
	timeoutCh := time.After(1500 * time.Millisecond) // extra 500ms to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after refreshNow()")
	}
	afterFunctionCall := time.Now()

	// use 500ms instead of 1s to avoid timer precision issues
	if afterFunctionCall.Sub(beforeEvents) < 500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~1 second", afterFunctionCall.Sub(beforeEvents).Milliseconds())
	}

	timeoutCh = time.After(4 * time.Second) // extra 1s to prevent flakiness
	select {
	case <-channel:
	case <-timeoutCh:
		t.Fatalf("timeout elapsed without flush function being called after debounce requests")
	}
	afterSecondFunctionCall := time.Now()

	// use 2.5s instead of 3s to avoid timer precision issues
	if afterSecondFunctionCall.Sub(afterFunctionCall) < 2500*time.Millisecond {
		t.Fatalf("function was called after %v ms instead of ~3 seconds", afterSecondFunctionCall.Sub(afterFunctionCall).Milliseconds())
	}

	if len(channel) > 0 {
		t.Fatalf("function was called more than twice")
	}
}

func TestErrorBroadcaster_MultipleListeners(t *testing.T) {
	b := newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	err := errors.New("expected error")
	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			receivedErr, ok := <-currentListener
			if !ok {
				result.Store(errors.New("listener was closed"))
			} else if receivedErr != err {
				result.Store(errors.New("expected received error to be the same as the one that was broadcasted"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.broadcast(err)
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Error(loadedVal.(error).Error())
	}
}

func TestErrorBroadcaster_StopWithoutBroadcast(t *testing.T) {
	var b = newErrorBroadcaster()
	defer b.stop()
	const numberOfListeners = 10
	var listeners []<-chan error
	for i := 0; i < numberOfListeners; i++ {
		listeners = append(listeners, b.newListener())
	}

	wg := sync.WaitGroup{}
	result := atomic.Value{}
	for _, listener := range listeners {
		currentListener := listener
		wg.Add(1)
		go func() {
			defer wg.Done()
			// broadcaster stopped, expect listener to be closed
			_, ok := <-currentListener
			if ok {
				result.Store(errors.New("expected listener to be closed"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// call stop without broadcasting anything to current listeners
		b.stop()
	}()
	wg.Wait()
	if loadedVal := result.Load(); loadedVal != nil {
		t.Error(loadedVal.(error).Error())
	}
}
