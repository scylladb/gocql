// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	"context"
	"testing"
)

// This isolates admission/capture from transport, decoding, and application
// hashing. Canonical comparisons remain necessary for throughput decisions.
func BenchmarkReusableQueryAdmission(b *testing.B) {
	q := testOwnedQuery(b)
	ctx := context.Background()
	value := []byte("owned parameter")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := q.capture(ctx, value); err != nil {
			b.Fatal(err)
		}
		_, cancel, request, err := InternalAdmitExecution(ctx, q.state.runtime.Session, &q.bound, Options{})
		if err != nil {
			b.Fatal(err)
		}
		cancel()
		request.InternalLogical.InternalFinish(Result{}, nil)
		request.InternalLogical.InternalReleaseInput()
		q.state.owner.InternalRelease()
	}
}
