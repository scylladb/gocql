//go:build !race

// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

func InternalWaitCallReqDone(call *InternalCallReq, where string) {
	call.InternalDone.Wait()
}
