package gocql

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func foundationChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return chain
}
func foundationPanicReleased(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		v := recover()
		err, ok := v.(error)
		if !ok || !errors.Is(err, ErrReleased) {
			t.Fatalf("expected ErrReleased panic, got %v", v)
		}
	}()
	fn()
}
func TestV2FoundationBorrowedViews(t *testing.T) {
	first, second := []byte("ab"), []byte("cdef")
	chain := foundationChain(t, first, second)
	released := 0
	lease := wire.NewLease(chain, func() { released++ })
	view := Bytes{chain: chain, lease: lease}
	slice, err := view.Slice(1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if view.Len() != 6 || slice.Len() != 4 {
		t.Fatal("wrong view length")
	}
	var spans [][]byte
	if err := slice.each(func(s wire.Span) error { spans = append(spans, s.Bytes()); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(spans) != 2 || &spans[0][0] != &first[1] || &spans[1][0] != &second[0] {
		t.Fatal("slice copied payload")
	}
	var out bytes.Buffer
	if n, err := slice.WriteTo(&out); err != nil || n != 4 || out.String() != "bcde" {
		t.Fatalf("WriteTo: %d %v %q", n, err, out.String())
	}
	short := make([]byte, 2)
	if n, err := slice.CopyTo(short); n != 2 || !errors.Is(err, io.ErrShortBuffer) || string(short) != "bc" {
		t.Fatalf("short copy: %d %v %q", n, err, short)
	}
	cloned := slice.Clone()
	if !lease.Retain() {
		t.Fatal("retain failed")
	}
	lease.Release()
	if b, err := slice.At(0); err != nil || b != 'b' {
		t.Fatalf("retained view: %c %v", b, err)
	}
	lease.Release()
	if released != 1 {
		t.Fatalf("release count %d", released)
	}
	if slice.Len() != 4 {
		t.Fatal("released length changed")
	}
	if _, err := slice.At(0); !errors.Is(err, ErrReleased) {
		t.Fatalf("At after release: %v", err)
	}
	if _, err := slice.Slice(0, 0); !errors.Is(err, ErrReleased) {
		t.Fatalf("Slice after release: %v", err)
	}
	if _, err := slice.CopyTo(nil); !errors.Is(err, ErrReleased) {
		t.Fatalf("CopyTo after release: %v", err)
	}
	if _, err := slice.WriteTo(io.Discard); !errors.Is(err, ErrReleased) {
		t.Fatalf("WriteTo after release: %v", err)
	}
	foundationPanicReleased(t, func() { slice.Clone() })
	foundationPanicReleased(t, func() { slice.OwnedBytes() })
	if got := string(cloned.OwnedBytes()); got != "bcde" {
		t.Fatalf("clone lost ownership: %q", got)
	}
}
func TestV2FoundationTextChunks(t *testing.T) {
	data := []byte("a€界z")
	chain := foundationChain(t, data[:2], data[2:4], data[4:6], data[6:])
	lease := wire.NewLease(chain, nil)
	text := Text{bytes: Bytes{chain: chain, lease: lease}}
	if got := text.String(); got != string(data) {
		t.Fatalf("String: %q", got)
	}
	clone := text.Clone()
	lease.Release()
	foundationPanicReleased(t, func() { text.String() })
	foundationPanicReleased(t, func() { text.Clone() })
	if clone.String() != string(data) {
		t.Fatal("text clone did not detach")
	}
	borrowed := BorrowText("source")
	if borrowed.String() != "source" {
		t.Fatal("external string view failed")
	}
	if BorrowBytes(nil).Len() != 0 || BorrowBytes([]byte{}).Len() != 0 {
		t.Fatal("empty borrowed bytes")
	}
}

func TestV2FoundationBorrowTextValidation(t *testing.T) {
	defer func() {
		recovered := recover()
		err, ok := recovered.(error)
		var validation *ValidationError
		if !ok || !errors.As(err, &validation) {
			t.Fatalf("invalid UTF-8 panic: %v", recovered)
		}
	}()
	BorrowText(string([]byte{0xff}))
}
func TestV2FoundationConcurrentRetainedViews(t *testing.T) {
	data := []byte("immutable retained page")
	chain := foundationChain(t, data)
	lease := wire.NewLease(chain, nil)
	view := Bytes{chain: chain, lease: lease}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		if !lease.Retain() {
			t.Fatal("retain failed")
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer lease.Release()
			for j := 0; j < 500; j++ {
				v, err := view.At(2)
				if err != nil || v != 'm' {
					t.Errorf("retained read %c: %v", v, err)
					return
				}
			}
		}()
	}
	lease.Release()
	wg.Wait()
	if _, err := view.At(0); !errors.Is(err, ErrReleased) {
		t.Fatalf("final release: %v", err)
	}
}
