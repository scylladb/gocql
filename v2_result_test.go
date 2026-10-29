package gocql

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/scylladb/gocql/v2/internal/wire"
)

func TestV2RuntimePageLeaseAndClone(t *testing.T) {
	typ, _ := ScalarType(TypeBlob)
	columns := NewColumns(ColumnSpec{Name: "v", Type: typ})
	body := append(binary.BigEndian.AppendUint32(nil, 5), []byte("value")...)
	chain, _ := wire.NewChain(wire.Borrow(body))
	released := false
	owner := wire.NewLease(chain, func() { released = true })
	response := &responsePage{columns: columns, rows: chain, rowCount: 1, owner: owner, state: RestorePageState([]byte{1})}
	page, err := newPage(response, OneColumn(BlobCodec), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !page.Next() {
		t.Fatal(page.Err())
	}
	view := page.Row()
	clone, err := page.Clone()
	if err != nil {
		t.Fatal(err)
	}
	lease, err := page.Retain()
	if err != nil {
		t.Fatal(err)
	}
	page.Close()
	if released {
		t.Fatal("page recycled despite lease")
	}
	if b, err := view.At(0); err != nil || b != 'v' {
		t.Fatal(b, err)
	}
	lease.Close()
	if !released {
		t.Fatal("page not released")
	}
	if _, err = view.At(0); !errors.Is(err, ErrReleased) {
		t.Fatal(err)
	}
	if string(clone.OwnedBytes()) != "value" {
		t.Fatal("clone changed")
	}
	if page.State().Bytes()[0] != 1 {
		t.Fatal("page state lost")
	}
}
func TestV2RuntimeCASMetadataAndEmptyPageValidation(t *testing.T) {
	boolean, _ := ScalarType(TypeBoolean)
	integer, _ := ScalarType(TypeInt)
	codec := CASRows(OneColumn(Int32))
	columns := NewColumns(ColumnSpec{Name: "[applied]", Type: boolean})
	plan, err := codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	chain, _ := wire.NewChain(wire.Borrow([]byte{0, 0, 0, 1, 1}))
	reader := newRowReader(columns, chain, nil)
	result, err := plan.Decode(reader)
	if err != nil || !result.Applied || result.Previous.Set {
		t.Fatal(result, err)
	}
	columns = NewColumns(ColumnSpec{Name: "[applied]", Type: boolean}, ColumnSpec{Name: "previous", Type: integer})
	plan, err = codec.Compile(columns)
	if err != nil {
		t.Fatal(err)
	}
	chain, _ = wire.NewChain(wire.Borrow([]byte{0, 0, 0, 1, 0, 0, 0, 0, 4, 0, 0, 0, 7}))
	result, err = plan.Decode(newRowReader(columns, chain, nil))
	if err != nil || result.Applied || !result.Previous.Set || result.Previous.Value != 7 {
		t.Fatal(result, err)
	}
	bad, _ := ScalarType(TypeBlob)
	response := &responsePage{columns: NewColumns(ColumnSpec{Name: "bad", Type: bad}), owner: wire.NewLease(wire.Chain{}, nil)}
	if _, err := newPage(response, OneColumn(Int32), nil); err == nil {
		t.Fatal("empty page skipped metadata validation")
	}
}
func TestV2RuntimeFutureCloseRetiresLatePage(t *testing.T) {
	future := &pageFuture{done: make(chan struct{})}
	future.close()
	released := false
	future.complete(&responsePage{owner: wire.NewLease(wire.Chain{}, func() { released = true })}, nil)
	if !released {
		t.Fatal("late page leaked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	future = &pageFuture{done: make(chan struct{})}
	if _, err := future.take(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
