// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution_test

import . "github.com/scylladb/gocql/v2"

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2RuntimeInputAdmissionAndWriterRetirement(t *testing.T) {
	owner := newInputOwner()
	if err := owner.InternalWait(context.Background()); err == nil {
		t.Fatal("Wait before Close succeeded")
	}
	if err := owner.InternalAdmit(); err != nil {
		t.Fatal(err)
	}
	lease := owner.InternalWriterLease(wire.Chain{})
	if !lease.Retain() {
		t.Fatal("writer retain")
	}
	owner.InternalClose()
	if !errors.Is(owner.InternalAdmit(), ErrClosed) {
		t.Fatal("closed owner admitted")
	}
	owner.InternalRelease()
	lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if !errors.Is(owner.InternalWait(ctx), context.DeadlineExceeded) {
		t.Fatal("Wait retired an active writer")
	}
	lease.Release()
	if err := owner.InternalWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner.InternalClose()
}
func TestV2RuntimeCloseAdmissionRace(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		owner := newInputOwner()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if owner.InternalAdmit() == nil {
					owner.InternalRelease()
				}
			}()
		}
		owner.InternalClose()
		wg.Wait()
		if err := owner.InternalWait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
