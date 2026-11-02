package main

import (
	"context"
	"testing"
	"time"
)

type fakeWorker struct{ delay time.Duration }

func (f fakeWorker) Write(context.Context, int32, int32, []byte) error { return nil }
func (f fakeWorker) Read(ctx context.Context, k int32, c consumer) error {
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return c(&row{K: k, P: payload(false, k, 0)})
}
func TestOwnedValidation(t *testing.T) {
	initialize()
	k := int32(73)
	v := row{K: k, P: append([]byte(nil), payload(false, k, 0)...)}
	if e := validator(false, k)(&v); e != nil {
		t.Fatal(e)
	}
	v.P[987] ^= 1
	if e := validator(false, k)(&v); e == nil {
		t.Fatal("accepted corrupt full payload")
	}
	v.P = payload(false, k, 0)
	v.R = 1
	if e := validator(false, k)(&v); e == nil {
		t.Fatal("accepted wrong row ID")
	}
}
func TestWindowAndDrain(t *testing.T) {
	initialize()
	s, e := phase([]worker{fakeWorker{8 * time.Millisecond}}, "read", 30*time.Millisecond, 0)
	if e != nil {
		t.Fatal(e)
	}
	if s.Completed != s.Admitted || s.Errors != 0 || s.InWindow+s.Drained != s.Completed || s.Drained < 1 {
		t.Fatalf("bad completion accounting: %+v", s)
	}
	if s.Latency.TotalCount() != s.Completed {
		t.Fatal("histogram omitted operations")
	}
}
func TestBoundedScheduledQueue(t *testing.T) {
	oldQueueSize := *queueSize
	*queueSize = 4
	t.Cleanup(func() { *queueSize = oldQueueSize })
	initialize()
	s, e := phase([]worker{fakeWorker{4 * time.Millisecond}}, "read", 40*time.Millisecond, 2000)
	if e != nil {
		t.Fatal(e)
	}
	if s.Rejected == 0 || s.PeakQueue > 4 || s.Completed != s.Admitted || s.Offered != s.Admitted+s.Rejected {
		t.Fatalf("bad queue accounting: %+v", s)
	}
	if s.Latency.ValueAtQuantile(90) <= s.Dispatch.ValueAtQuantile(90) {
		t.Fatal("scheduled latency omitted queueing")
	}
}
