//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

// receivedBatchStmt is one statement of a BATCH as the server decoded it:
// either an inline query string or a prepared id, plus the bound values.
type receivedBatchStmt struct {
	query  string
	id     uint64
	values [][]byte
}

type receivedBatch struct {
	typ           BatchType
	stmts         []receivedBatchStmt
	cons          Consistency
	serialCons    Consistency
	hasSerialCons bool
}

// readBatchRequest decodes a BATCH body written by framer.writeBatchFrame. It
// only handles what these tests send: protocol v4, no custom payload, no
// default timestamp.
func readBatchRequest(f *framer) receivedBatch {
	b := receivedBatch{typ: BatchType(f.readByte())}
	n := int(f.readShort())
	for i := 0; i < n; i++ {
		var st receivedBatchStmt
		if f.readByte() == 0 {
			st.query = f.readLongString()
		} else {
			st.id = binary.BigEndian.Uint64(f.readShortBytesCopy())
		}
		nv := int(f.readShort())
		for j := 0; j < nv; j++ {
			st.values = append(st.values, f.readBytesCopy())
		}
		b.stmts = append(b.stmts, st)
	}
	b.cons = f.readConsistency()
	flags := uint32(f.readByte())
	b.hasSerialCons = flags&frm.FlagWithSerialConsistency != 0
	if b.hasSerialCons {
		b.serialCons = f.readConsistency()
	}
	return b
}

type testRowsCol struct {
	name string
	typ  Type
}

// writeRowsResult writes a RESULT/Rows reply. Each row holds one encoded value
// per column.
func writeRowsResult(resp *framer, stream int, cols []testRowsCol, rows [][][]byte) {
	resp.writeHeader(0, frm.OpResult, stream)
	resp.writeInt(frm.ResultKindRows)
	resp.writeInt(int32(frm.FlagGlobalTableSpec))
	resp.writeInt(int32(len(cols)))
	resp.writeString("ks1")
	resp.writeString("tbl1")
	for _, c := range cols {
		resp.writeString(c.name)
		resp.writeShort(uint16(c.typ))
	}
	resp.writeInt(int32(len(rows)))
	for _, row := range rows {
		for _, v := range row {
			resp.writeBytes(v)
		}
	}
}

func writeVoidResult(resp *framer, stream int) {
	resp.writeHeader(0, frm.OpResult, stream)
	resp.writeInt(frm.ResultKindVoid)
}

func writeErrorResult(resp *framer, stream int, code int, msg string) {
	resp.writeHeader(0, frm.OpError, stream)
	resp.writeInt(int32(code))
	resp.writeString(msg)
}

// batchReply answers the n-th BATCH (0-based) the server received.
type batchReply func(n int, b receivedBatch, stream int, resp *framer)

// batchTestServer extends routingTestServer with BATCH handling: it records
// every batch it receives and answers with reply.
type batchTestServer struct {
	*routingTestServer
	mu      sync.Mutex
	batches []receivedBatch
}

func newBatchTestServer(t *testing.T, prepared map[string]testPrepared, supported map[string][]string, reply batchReply) *batchTestServer {
	t.Helper()
	srv := &batchTestServer{
		routingTestServer: newRoutingTestServerWithSupported(t, protoVersion4, prepared, supported),
	}
	if reply == nil {
		reply = func(_ int, _ receivedBatch, stream int, resp *framer) { writeVoidResult(resp, stream) }
	}
	srv.setBatchHook(func(req *framer, stream int, resp *framer) {
		b := readBatchRequest(req)
		srv.mu.Lock()
		n := len(srv.batches)
		srv.batches = append(srv.batches, b)
		srv.mu.Unlock()
		reply(n, b, stream, resp)
	})
	return srv
}

func (srv *batchTestServer) received() []receivedBatch {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	return append([]receivedBatch(nil), srv.batches...)
}

const (
	batchInsert       = "INSERT INTO ks1.tbl1 (a, b) VALUES (?, ?)"
	batchInsertLWT    = "INSERT INTO ks1.tbl1 (a, b) VALUES (?, ?) IF NOT EXISTS"
	batchInsertInline = "INSERT INTO ks1.tbl1 (a, b) VALUES (1, 'x')"
	batchInsertBroken = "INSERT INTO ks1.broken (a) VALUES (?)"

	batchInsertID    = 30
	batchInsertLWTID = 31

	testLWTBit  = 1 << 31
	testLWTMask = "LWT_OPTIMIZATION_META_BIT_MASK=2147483648"
)

var batchPrepared = map[string]testPrepared{
	batchInsert: {
		id:       batchInsertID,
		keyspace: "ks1", table: "tbl1",
		cols:      []testPreparedCol{{name: "a", typ: TypeInt}, {name: "b", typ: TypeVarchar}},
		pkIndexes: []uint16{0},
	},
	batchInsertLWT: {
		id:       batchInsertLWTID,
		flags:    testLWTBit,
		keyspace: "ks1", table: "tbl1",
		cols:      []testPreparedCol{{name: "a", typ: TypeInt}, {name: "b", typ: TypeVarchar}},
		pkIndexes: []uint16{0},
	},
}

var testLWTSupported = map[string][]string{lwtAddMetadataMarkKey: {testLWTMask}}

// A batch mixes inline statements, prepared statements with arguments and
// bound statements; each must reach the server in its own form, with the
// batch's type and consistencies.
func TestBatchExecuteWireFormat(t *testing.T) {
	t.Parallel()

	srv := newBatchTestServer(t, batchPrepared, nil, nil)
	s := srv.newSession(t)

	var bindInfo *QueryInfo
	b := s.Batch(UnloggedBatch)
	b.SetConsistency(Quorum)
	b.SerialConsistency(LocalSerial)
	b.Query(batchInsert, int32(1), "x")
	b.Query(batchInsertInline)
	b.Bind(batchInsert, func(q *QueryInfo) ([]any, error) {
		bindInfo = q
		return []any{int32(2), "y"}, nil
	})

	if err := b.Exec(); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	got := srv.received()
	if len(got) != 1 {
		t.Fatalf("server received %d batches, want 1", len(got))
	}
	rb := got[0]
	if rb.typ != UnloggedBatch || rb.cons != Quorum || rb.serialCons != LocalSerial {
		t.Fatalf("batch type/cons/serial = %v/%v/%v, want %v/%v/%v",
			rb.typ, rb.cons, rb.serialCons, UnloggedBatch, Quorum, LocalSerial)
	}

	want := []receivedBatchStmt{
		{id: batchInsertID, values: [][]byte{int32Bytes(1), []byte("x")}},
		{query: batchInsertInline},
		{id: batchInsertID, values: [][]byte{int32Bytes(2), []byte("y")}},
	}
	if len(rb.stmts) != len(want) {
		t.Fatalf("batch has %d statements, want %d", len(rb.stmts), len(want))
	}
	for i, w := range want {
		g := rb.stmts[i]
		if g.query != w.query || g.id != w.id || len(g.values) != len(w.values) {
			t.Fatalf("statement %d = %+v, want %+v", i, g, w)
		}
		for j := range w.values {
			if !bytes.Equal(g.values[j], w.values[j]) {
				t.Fatalf("statement %d value %d = %x, want %x", i, j, g.values[j], w.values[j])
			}
		}
	}

	if bindInfo == nil || binary.BigEndian.Uint64(bindInfo.Id) != batchInsertID || len(bindInfo.Args) != 2 {
		t.Fatalf("binding got QueryInfo %+v, want the prepared id and two args", bindInfo)
	}
	if c := srv.prepareCount(batchInsert); c != 1 {
		t.Fatalf("statement prepared %d times, want 1 (shared by both entries)", c)
	}
}

// Failures while building the batch must be returned without sending
// anything to the server.
func TestBatchExecuteFailsBeforeSending(t *testing.T) {
	t.Parallel()

	errBind := errors.New("bind failed")

	tests := []struct {
		name    string
		build   func(b *Batch)
		wantErr func(error) bool
	}{
		{
			name:  "ArgCountMismatch",
			build: func(b *Batch) { b.Query(batchInsert, int32(1)) },
			wantErr: func(err error) bool {
				return strings.Contains(err.Error(), "batch statement 0 expected 2 values send got 1")
			},
		},
		{
			name:    "MarshalError",
			build:   func(b *Batch) { b.Query(batchInsert, "not an int", "x") },
			wantErr: func(err error) bool { return err != nil },
		},
		{
			name: "BindingError",
			build: func(b *Batch) {
				b.Bind(batchInsert, func(*QueryInfo) ([]any, error) { return nil, errBind })
			},
			wantErr: func(err error) bool { return errors.Is(err, errBind) },
		},
		{
			name: "BindingValueCountMismatch",
			build: func(b *Batch) {
				b.Bind(batchInsert, func(*QueryInfo) ([]any, error) { return []any{int32(1)}, nil })
			},
			wantErr: func(err error) bool {
				return strings.Contains(err.Error(), "expected 2 values send got 1")
			},
		},
		{
			// The failing statement comes second: the first one's pooled
			// values must be released too, and still nothing is sent.
			name: "PrepareError",
			build: func(b *Batch) {
				b.Query(batchInsert, int32(1), "x")
				b.Query(batchInsertBroken, int32(1))
			},
			wantErr: func(err error) bool { return strings.Contains(err.Error(), "unknown statement") },
		},
		{
			name: "TooManyStatements",
			build: func(b *Batch) {
				for i := 0; i <= BatchSizeMaximum; i++ {
					b.Query(batchInsertInline)
				}
			},
			wantErr: func(err error) bool { return errors.Is(err, ErrTooManyStmts) },
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newBatchTestServer(t, batchPrepared, nil, nil)
			s := srv.newSession(t)

			b := s.Batch(LoggedBatch)
			tc.build(b)
			err := b.Exec()
			if err == nil || !tc.wantErr(err) {
				t.Fatalf("Exec error = %v", err)
			}
			if n := len(srv.received()); n != 0 {
				t.Fatalf("server received %d batches, want 0", n)
			}
		})
	}
}

func TestBatchExecuteClosedSession(t *testing.T) {
	t.Parallel()

	srv := newBatchTestServer(t, batchPrepared, nil, nil)
	s := srv.newSession(t)
	b := s.Batch(LoggedBatch)
	b.Query(batchInsertInline)
	s.Close()

	if err := b.Exec(); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Exec error = %v, want ErrSessionClosed", err)
	}
}

func TestBatchExecuteServerError(t *testing.T) {
	t.Parallel()

	srv := newBatchTestServer(t, batchPrepared, nil, func(_ int, _ receivedBatch, stream int, resp *framer) {
		writeErrorResult(resp, stream, ErrCodeInvalid, "bad batch")
	})
	s := srv.newSession(t)

	b := s.Batch(LoggedBatch)
	b.Query(batchInsert, int32(1), "x")
	err := s.ExecuteBatch(b)
	var errFrame frm.ErrorFrame
	if !errors.As(err, &errFrame) || errFrame.Code != ErrCodeInvalid || errFrame.Message != "bad batch" {
		t.Fatalf("Exec error = %v, want the server's invalid-request error", err)
	}
}

// A server that has evicted a statement answers UNPREPARED; the driver must
// drop its cached id, prepare again and resend the whole batch.
func TestBatchExecuteReprepareOnUnprepared(t *testing.T) {
	t.Parallel()

	srv := newBatchTestServer(t, batchPrepared, nil, func(n int, b receivedBatch, stream int, resp *framer) {
		if n > 0 {
			writeVoidResult(resp, stream)
			return
		}
		writeErrorResult(resp, stream, ErrCodeUnprepared, "unprepared")
		resp.writeShortBytes(binary.BigEndian.AppendUint64(nil, b.stmts[0].id))
	})
	s := srv.newSession(t)

	b := s.Batch(LoggedBatch)
	b.Query(batchInsert, int32(1), "x")
	if err := b.Exec(); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n := len(srv.received()); n != 2 {
		t.Fatalf("server received %d batches, want 2 (original and resend)", n)
	}
	if c := srv.prepareCount(batchInsert); c != 2 {
		t.Fatalf("statement prepared %d times, want 2 (the evicted id must be re-prepared)", c)
	}
}

// A batch is LWT when any of its prepared statements is.
func TestBatchExecuteLWT(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		stmts []string
		want  bool
	}{
		{"NoConditional", []string{batchInsert, batchInsert}, false},
		{"OneConditional", []string{batchInsert, batchInsertLWT}, true},
		{"InlineIsNeverMarked", []string{batchInsertInline}, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newBatchTestServer(t, batchPrepared, testLWTSupported, nil)
			s := srv.newSession(t)

			b := s.Batch(LoggedBatch)
			for _, stmt := range tc.stmts {
				if stmt == batchInsertInline {
					b.Query(stmt)
				} else {
					b.Query(stmt, int32(1), "x")
				}
			}
			if err := b.Exec(); err != nil {
				t.Fatalf("Exec: %v", err)
			}
			if got := b.IsLWT(); got != tc.want {
				t.Fatalf("IsLWT() = %v, want %v", got, tc.want)
			}
		})
	}
}

var (
	casAppliedCol = testRowsCol{name: "[applied]", typ: TypeBoolean}
	casACol       = testRowsCol{name: "a", typ: TypeInt}
	casBCol       = testRowsCol{name: "b", typ: TypeVarchar}
)

func casReply(cols []testRowsCol, row [][]byte) batchReply {
	return func(_ int, _ receivedBatch, stream int, resp *framer) {
		writeRowsResult(resp, stream, cols, [][][]byte{row})
	}
}

func TestExecuteBatchCAS(t *testing.T) {
	t.Parallel()

	t.Run("NotAppliedReturnsExistingRow", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, casReply(
			[]testRowsCol{casAppliedCol, casACol, casBCol},
			[][]byte{{0}, int32Bytes(7), []byte("old")},
		))
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		var a int
		var bv string
		applied, iter, err := s.ExecuteBatchCAS(b, &a, &bv)
		if err != nil {
			t.Fatalf("ExecuteBatchCAS: %v", err)
		}
		defer iter.Close()
		if applied || a != 7 || bv != "old" {
			t.Fatalf("applied, a, b = %v, %d, %q; want false, 7, \"old\"", applied, a, bv)
		}
	})

	t.Run("AppliedOnlyColumn", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, casReply(
			[]testRowsCol{casAppliedCol},
			[][]byte{{1}},
		))
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		applied, iter, err := s.ExecuteBatchCAS(b)
		if err != nil {
			t.Fatalf("ExecuteBatchCAS: %v", err)
		}
		defer iter.Close()
		if !applied {
			t.Fatal("applied = false, want true")
		}
	})

	t.Run("ServerError", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, func(_ int, _ receivedBatch, stream int, resp *framer) {
			writeErrorResult(resp, stream, ErrCodeInvalid, "bad batch")
		})
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		applied, iter, err := s.ExecuteBatchCAS(b)
		if err == nil || applied || iter != nil {
			t.Fatalf("ExecuteBatchCAS = %v, %v, %v; want false, nil, error", applied, iter, err)
		}
	})
}

func TestMapExecuteBatchCAS(t *testing.T) {
	t.Parallel()

	t.Run("NotAppliedReturnsExistingRow", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, casReply(
			[]testRowsCol{casAppliedCol, casACol, casBCol},
			[][]byte{{0}, int32Bytes(7), []byte("old")},
		))
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		dest := map[string]any{}
		applied, iter, err := s.MapExecuteBatchCAS(b, dest)
		if err != nil {
			t.Fatalf("MapExecuteBatchCAS: %v", err)
		}
		defer iter.Close()
		if applied {
			t.Fatal("applied = true, want false")
		}
		if _, ok := dest["[applied]"]; ok {
			t.Fatal("[applied] left in dest")
		}
		if dest["a"] != 7 || dest["b"] != "old" {
			t.Fatalf("dest = %v, want a=7 b=old", dest)
		}
	})

	t.Run("Applied", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, casReply(
			[]testRowsCol{casAppliedCol},
			[][]byte{{1}},
		))
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		dest := map[string]any{}
		applied, iter, err := s.MapExecuteBatchCAS(b, dest)
		if err != nil {
			t.Fatalf("MapExecuteBatchCAS: %v", err)
		}
		defer iter.Close()
		if !applied || len(dest) != 0 {
			t.Fatalf("applied, dest = %v, %v; want true, empty", applied, dest)
		}
	})

	// Rows without an [applied] column (not a conditional batch) are scanned
	// into dest and reported as not applied, with no error.
	t.Run("NoAppliedColumn", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, casReply(
			[]testRowsCol{casACol},
			[][]byte{int32Bytes(3)},
		))
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsert, int32(3), "x")
		dest := map[string]any{}
		applied, iter, err := s.MapExecuteBatchCAS(b, dest)
		if err != nil {
			t.Fatalf("MapExecuteBatchCAS: %v", err)
		}
		defer iter.Close()
		if applied || dest["a"] != 3 {
			t.Fatalf("applied, dest = %v, %v; want false, a=3", applied, dest)
		}
	})

	t.Run("ServerError", func(t *testing.T) {
		t.Parallel()
		srv := newBatchTestServer(t, batchPrepared, nil, func(_ int, _ receivedBatch, stream int, resp *framer) {
			writeErrorResult(resp, stream, ErrCodeInvalid, "bad batch")
		})
		s := srv.newSession(t)

		b := s.Batch(LoggedBatch)
		b.Query(batchInsertLWT, int32(7), "new")
		applied, iter, err := s.MapExecuteBatchCAS(b, map[string]any{})
		if err == nil || applied || iter != nil {
			t.Fatalf("MapExecuteBatchCAS = %v, %v, %v; want false, nil, error", applied, iter, err)
		}
	})
}
