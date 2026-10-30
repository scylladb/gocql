//go:build race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"fmt"
	"time"
)

// Race builds on loaded CI can delay runnable goroutines long enough to trip
// short deadlock guards even when cleanup is progressing.
const callReqDoneTimeout = 10 * time.Second

func InternalWaitCallReqDone(call *InternalCallReq, where string) {
	done := make(chan struct{})
	go func() {
		call.InternalDone.Wait()
		close(done)
	}()

	timer := time.NewTimer(callReqDoneTimeout)
	defer timer.Stop()

	select {
	case <-done:
	case <-timer.C:
		panic(fmt.Sprintf("gocql: timed out waiting for exec cleanup in %s (stream=%d)", where, call.InternalStreamID))
	}
}
