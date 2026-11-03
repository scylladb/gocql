// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"
	"time"
)

func TestStreamCancellationHookUsesEarliestScope(t *testing.T) {
	background := context.Background()
	caller, cancelCaller := context.WithCancel(background)
	defer cancelCaller()
	callerDeadline, cancelDeadline := context.WithTimeout(background, time.Hour)
	defer cancelDeadline()
	unbounded, cancelUnbounded := context.WithCancel(background)
	defer cancelUnbounded()
	derived, cancelDerived := context.WithCancel(caller)
	defer cancelDerived()
	sameDeadline, cancelSame := context.WithCancel(callerDeadline)
	defer cancelSame()
	earlier, cancelEarlier := context.WithTimeout(callerDeadline, time.Minute)
	defer cancelEarlier()
	ownDeadline, cancelOwn := context.WithTimeout(background, time.Minute)
	defer cancelOwn()
	for _, test := range []struct {
		name                    string
		parent, execution, want context.Context
	}{
		{"explicit shutdown only", background, unbounded, nil},
		{"caller cancellation", caller, derived, caller},
		{"same deadline", callerDeadline, sameDeadline, callerDeadline},
		{"earlier driver deadline", callerDeadline, earlier, earlier},
		{"driver deadline without caller cancellation", background, ownDeadline, ownDeadline},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := streamCancellationHook(test.parent, test.execution); got != test.want {
				t.Fatal("wrong cancellation scope")
			}
		})
	}
}
