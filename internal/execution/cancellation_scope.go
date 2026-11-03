// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import "context"

// The explicit query/session shutdown path already closes stream cursors. If
// execution adds no earlier deadline, one callback on the caller's scope also
// handles all external cancellation without another child map on execution.
func streamCancellationHook(parent, execution context.Context) context.Context {
	executionDeadline, executionBounded := execution.Deadline()
	if parent.Done() == nil {
		if !executionBounded {
			return nil
		}
		return execution
	}
	parentDeadline, parentBounded := parent.Deadline()
	if !executionBounded || parentBounded && !executionDeadline.Before(parentDeadline) {
		return parent
	}
	return execution
}
