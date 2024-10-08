package debounce

import (
	"runtime"
	"sync/atomic"
	"testing"
)

// TestDebouncer tests that the debouncer allows only one function to execute at a time
func TestSimpleDebouncer(t *testing.T) {
	d := NewSimpleDebouncer()
	var executions int32
	startedCh := make(chan struct{}, 1)
	doneCh := make(chan struct{}, 1)
	// Function to increment executions
	fn := func() {
		<-startedCh // Simulate work
		atomic.AddInt32(&executions, 1)
		<-doneCh // Simulate work
	}
	t.Run("Case 1", func(t *testing.T) {
		// Case 1: Normal single execution
		startedCh <- struct{}{}
		doneCh <- struct{}{}
		d.Debounce(fn)
		// We expect that the function has only executed once due to debouncing
		if atomic.LoadInt32(&executions) != 1 {
			t.Errorf("Expected function to be executed only once, but got %d executions", executions)
		}
	})
	atomic.StoreInt32(&executions, 0)
	t.Run("Case 2", func(t *testing.T) {
		// Case 2: Debounce the function multiple times at row when body is started
		go d.Debounce(fn)
		startedCh <- struct{}{}
		go d.Debounce(fn)
		go d.Debounce(fn)
		doneCh <- struct{}{}
		startedCh <- struct{}{}
		doneCh <- struct{}{}
		waitTillChannelIsEmpty(doneCh)
		// We expect that the function has only executed once due to debouncing
		if atomic.LoadInt32(&executions) != 2 {
			t.Errorf("Expected function to be executed twice, but got %d executions", executions)
		}
	})
}
func waitTillChannelIsEmpty(ch chan struct{}) {
	for {
		if len(ch) == 0 {
			return
		}
		runtime.Gosched()
	}
}
