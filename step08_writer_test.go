//go:build unit

package gocql

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gocql/v2/internal/wire"
)

type step08DeadlineFailure struct {
	net.Conn
	err   error
	wrote bool
}

func (w *step08DeadlineFailure) SetWriteDeadline(time.Time) error { return w.err }
func (w *step08DeadlineFailure) Write(p []byte) (int, error)      { w.wrote = true; return len(p), nil }

func TestStep08DisabledWriteTimeoutClearsStartupDeadline(t *testing.T) {
	for _, mode := range []string{"shared", "span", "coalesced"} {
		t.Run(mode, func(t *testing.T) {
			client, peer := net.Pipe()
			defer client.Close()
			defer peer.Close()
			if err := client.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				p := make([]byte, 3)
				_, err := io.ReadFull(peer, p)
				if string(p) != "abc" && err == nil {
					err = errors.New("wrong wire data")
				}
				done <- err
			}()
			writer := &deadlineContextWriter{InternalW: client, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			writer.InternalSetWriteTimeout(0)
			var err error
			switch mode {
			case "shared":
				_, err = writer.InternalWriteContext(context.Background(), []byte("abc"))
			case "span":
				chain, _ := wire.NewChain(wire.Borrow([]byte("abc")))
				_, err = writer.InternalWriteSpans(context.Background(), chain)
			case "coalesced":
				result := make(chan writeResult, 1)
				coalescer := &writeCoalescer{InternalC: client}
				coalescer.InternalSetWriteTimeout(0)
				coalescer.InternalFlushSizes([]chan<- writeResult{result}, net.Buffers{[]byte("abc")}, []int64{3})
				err = (<-result).InternalErr
			}
			if err != nil {
				t.Fatal("disabled timeout retained expired startup deadline", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStep08ZeroWriteDeadlineSettingError(t *testing.T) {
	for _, mode := range []string{"shared", "span", "coalesced"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("cannot clear deadline")
			socket := &step08DeadlineFailure{err: cause}
			writer := &deadlineContextWriter{InternalW: socket, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
			var err error
			switch mode {
			case "shared":
				_, err = writer.InternalWriteContext(context.Background(), []byte("abc"))
			case "span":
				chain, _ := wire.NewChain(wire.Borrow([]byte("abc")))
				_, err = writer.InternalWriteSpans(context.Background(), chain)
			case "coalesced":
				result := make(chan writeResult, 1)
				coalescer := &writeCoalescer{InternalC: socket}
				coalescer.InternalFlushSizes([]chan<- writeResult{result}, net.Buffers{[]byte("abc")}, []int64{3})
				err = (<-result).InternalErr
			}
			if !errors.Is(err, cause) || socket.wrote {
				t.Fatal("failed deadline clear did not reject write", err, socket.wrote)
			}
		})
	}
}
