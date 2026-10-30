package gocql

import (
	"context"
	"encoding/binary"
	"github.com/scylladb/gocql/v2/internal/wire"
	"io"
	"testing"
)

func codecTestMust[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
func foundationScalar(t testing.TB, kind Type) DataType {
	t.Helper()
	d, err := ScalarType(kind)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
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
func scalarReader(t testing.TB, kind Type, state CellState, chunks ...[]byte) ValueReader {
	t.Helper()
	spans := make([]wire.Span, len(chunks))
	for i, b := range chunks {
		spans[i] = wire.Borrow(b)
	}
	chain, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return newValueReader(foundationScalar(t, kind), state, chain, nil)
}
func streamCell(body []byte) []byte {
	b := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(b, uint32(len(body)))
	copy(b[4:], body)
	return b
}
func streamDecodeFixture[T any](t testing.TB, typ DataType, decoder StreamDecoder[T], data []byte, dst *T, step int) error {
	t.Helper()
	columns := NewColumns(ColumnSpec{Name: "v", Type: typ})
	r, scope := streamReaderFixture(t, columns, &streamChunkSource{data: data, step: step}, len(data))
	plan, err := StreamOneColumn(decoder).Compile(columns)
	if err != nil {
		return err
	}
	plan.Reset(dst)
	if err := plan.DecodeInto(r, dst); err != nil {
		return err
	}
	if err := r.RequireEnd(); err != nil {
		return err
	}
	return scope.RequireEnd()
}

type streamChunkSource struct {
	data                []byte
	offset, step, reads int
	record              func([]byte, int)
}

func (s *streamChunkSource) Read(p []byte) (int, error) {
	if s.offset == len(s.data) {
		return 0, io.EOF
	}
	n := min(len(p), len(s.data)-s.offset)
	if s.step > 0 {
		n = min(n, s.step)
	}
	if s.record != nil {
		s.record(p[:n], s.offset)
	}
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	s.reads++
	return n, nil
}

func streamReaderFixture(t testing.TB, columns Columns, source io.Reader, size int) (*StreamRowReader, *wire.Stream) {
	t.Helper()
	scope, err := wire.NewStream(context.Background(), source, int64(size), wire.StreamLimits{MaxDepth: 130, DiscardBytes: 127})
	if err != nil {
		t.Fatal(err)
	}
	r, err := newStreamRowReader(columns, scope)
	if err != nil {
		t.Fatal(err)
	}
	return r, scope
}
