package gocql

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
	if err := owner.wait(context.Background()); err == nil {
		t.Fatal("Wait before Close succeeded")
	}
	if err := owner.admit(); err != nil {
		t.Fatal(err)
	}
	lease := owner.writerLease(wire.Chain{})
	if !lease.Retain() {
		t.Fatal("writer retain")
	}
	owner.close()
	if !errors.Is(owner.admit(), ErrClosed) {
		t.Fatal("closed owner admitted")
	}
	owner.release()
	lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if !errors.Is(owner.wait(ctx), context.DeadlineExceeded) {
		t.Fatal("Wait retired an active writer")
	}
	lease.Release()
	if err := owner.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	owner.close()
}
func TestV2RuntimeCloseAdmissionRace(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		owner := newInputOwner()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if owner.admit() == nil {
					owner.release()
				}
			}()
		}
		owner.close()
		wg.Wait()
		if err := owner.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}
func TestV2RuntimePageStates(t *testing.T) {
	start := StartPage()
	empty := RestorePageState(nil)
	complete := completePageState()
	if start.More() || start.Complete() || !empty.More() || empty.Complete() || !complete.Complete() {
		t.Fatal("page state distinctions lost")
	}
	if empty.Bytes() == nil {
		t.Fatal("empty continuation became absent")
	}
	token := []byte{1}
	state := RestorePageState(token)
	token[0] = 2
	copy := state.Bytes()
	copy[0] = 3
	if state.Bytes()[0] != 1 {
		t.Fatal("state token aliased")
	}
}
