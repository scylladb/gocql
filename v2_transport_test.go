package gocql

import (
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gocql/lz4"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/streams"
	"github.com/scylladb/gocql/v2/internal/wire"
)

func typedTestChain(t *testing.T, parts ...[]byte) wire.Chain {
	t.Helper()
	spans := make([]wire.Span, len(parts))
	for i, p := range parts {
		spans[i] = wire.Borrow(p)
	}
	c, err := wire.NewChain(spans...)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func typedTestBytes(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	p := make([]byte, c.Len())
	if _, err := c.CopyTo(p); err != nil {
		t.Fatal(err)
	}
	return p
}
func typedTestConn(t testing.TB, proto byte, compressor Compressor) (*Conn, net.Conn) {
	t.Helper()
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{version: proto, ctx: ctx, cancel: cancel, r: &connReader{conn: client}, calls: make(map[int]*callReq), streams: streams.New(), logger: nopLogger{}, compressor: compressor, errorHandler: connErrorHandlerFn(func(*Conn, error, bool) {})}
	c.w = &deadlineContextWriter{w: client, semaphore: make(chan struct{}, 1), quit: make(chan struct{})}
	if proto == 5 && compressor != nil {
		c.segCompressor = compressor.(segment.Compressor)
	}
	c.initFramerCache()
	t.Cleanup(func() { c.Close(); server.Close() })
	return c, server
}
func typedTestResponse(t testing.TB, proto byte, stream int, body []byte) []byte {
	t.Helper()
	out := []byte{proto | 0x80, 0, byte(stream >> 8), byte(stream), byte(frm.OpResult)}
	out = binary.BigEndian.AppendUint32(out, uint32(len(body)))
	return append(out, body...)
}
func typedTestRowsBody(value []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, 2)
	out = binary.BigEndian.AppendUint32(out, 1) // global table
	out = binary.BigEndian.AppendUint32(out, 1)
	for _, s := range []string{"ks", "tbl", "value"} {
		out = binary.BigEndian.AppendUint16(out, uint16(len(s)))
		out = append(out, s...)
	}
	out = binary.BigEndian.AppendUint16(out, uint16(TypeBlob))
	out = binary.BigEndian.AppendUint32(out, 1)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}
func typedTestReadRequest(t testing.TB, c *Conn, server net.Conn) int {
	t.Helper()
	if c.version == 5 {
		h, err := segment.ReadHeader(server, c.segCompressor != nil)
		if err != nil {
			t.Error(err)
			return 0
		}
		owner, err := segment.ReadOwnedPayload(server, h, c.segCompressor)
		if err != nil {
			t.Error(err)
			return 0
		}
		defer owner.Release()
		cursor := wire.NewCursor(owner.Chain())
		cursor.Skip(2)
		stream, _ := cursor.Uint16()
		return int(stream)
	}
	var header [9]byte
	if _, err := io.ReadFull(server, header[:]); err != nil {
		t.Error(err)
		return 0
	}
	if _, err := io.CopyN(io.Discard, server, int64(binary.BigEndian.Uint32(header[5:]))); err != nil {
		t.Error(err)
	}
	return int(binary.BigEndian.Uint16(header[2:4]))
}

func TestTypedRequestMatchesLegacyWireAndKeepsPayload(t *testing.T) {
	payload := bytes.Repeat([]byte("original-borrowed-payload"), 7000)
	length := binary.BigEndian.AppendUint32(nil, uint32(len(payload)))
	values := typedTestChain(t, length, payload)
	for _, proto := range []byte{3, 4, 5} {
		c := &Conn{version: proto}
		c.initFramerCache()
		request := &attemptRequest{Statement: "INSERT INTO ks.tbl (value) VALUES (?)", Values: values, ValueCount: 1, Consistency: Quorum, SerialConsistency: LocalSerial, PageSize: 83, PagingState: []byte{1, 2, 3}, Timestamp: 78321, DefaultTimestamp: true, Trace: true}
		if proto == 5 {
			request.Keyspace = "ks"
			now := int32(321)
			request.NowInSeconds = &now
		}
		got, err := c.buildTypedRequest(request, 7)
		if err != nil {
			t.Fatal(err)
		}
		f := c.getWriteFramer()
		f.Trace()
		params := fixtureQueryParams{values: []fixtureQueryValue{{value: payload}}, consistency: Quorum, serialConsistency: LocalSerial, pageSize: 83, pagingState: request.PagingState, defaultTimestamp: true, defaultTimestampValue: 78321, keyspace: request.Keyspace}
		if request.NowInSeconds != nil {
			now := int(*request.NowInSeconds)
			params.nowInSeconds = &now
		}
		if err := fixtureWriteQuery(f, 7, request.Statement, &params, nil); err != nil {
			t.Fatal(err)
		}
		expected := append([]byte(nil), f.Buf...)
		if proto == 5 {
			var decoded []byte
			r := bytes.NewReader(typedTestBytes(t, got))
			for r.Len() > 0 {
				h, err := segment.ReadHeader(r, false)
				if err != nil {
					t.Fatal(err)
				}
				if h.IsSelfContained {
					t.Fatal("large split frame marked self contained")
				}
				owner, err := segment.ReadOwnedPayload(r, h, nil)
				if err != nil {
					t.Fatal(err)
				}
				decoded = append(decoded, typedTestBytes(t, owner.Chain())...)
				owner.Release()
			}
			if !bytes.Equal(decoded, expected) {
				t.Fatal("v5 wire differs from independent contiguous oracle")
			}
		} else if !bytes.Equal(typedTestBytes(t, got), expected) {
			t.Fatalf("v%d wire differs", proto)
		}
		// Request segments retain original payload backing even when boundaries cut it.
		retained := 0
		_ = got.Each(func(s wire.Span) error {
			b := s.Bytes()
			if len(b) > 1000 && bytes.Contains(payload, b) {
				found := false
				for i := 0; i+len(b) <= len(payload); i++ {
					if &b[0] == &payload[i] {
						found = true
						break
					}
				}
				if !found {
					t.Fatal("request payload gathered")
				}
				retained += len(b)
			}
			return nil
		})
		if retained != len(payload) {
			t.Fatalf("only %d/%d original bytes retained", retained, len(payload))
		}
		c.releaseWriteFramer(f)
	}
}

func TestTypedReceiveLargeFragmentedRows(t *testing.T) {
	payload := make([]byte, 500123)
	rand.New(rand.NewSource(42)).Read(payload)
	for _, pattern := range []string{"random", "repetitive"} {
		payload := payload
		if pattern == "repetitive" {
			payload = bytes.Repeat([]byte("compressible-row-cell"), 25007)
		}
		for _, proto := range []byte{4, 5} {
			for _, compressed := range []bool{false, true} {
				t.Run(pattern+"/"+string(rune('0'+proto))+map[bool]string{true: "LZ4", false: "raw"}[compressed], func(t *testing.T) {
					var comp Compressor
					if compressed {
						comp = lz4.LZ4Compressor{}
					}
					c, server := typedTestConn(t, proto, comp)
					go c.serve(c.ctx)
					peerDone := make(chan struct{})
					go func() {
						defer close(peerDone)
						stream := typedTestReadRequest(t, c, server)
						body := typedTestRowsBody(payload)
						if proto == 4 {
							response := typedTestResponse(t, proto, stream, body)
							if compressed {
								encoded, err := comp.Encode(body)
								if err != nil {
									t.Error(err)
									return
								}
								response = typedTestResponse(t, proto, stream, encoded)
								response[1] |= frm.FlagCompress
							}
							if _, err := server.Write(response); err != nil {
								t.Error(err)
							}
							return
						}
						frame := typedTestResponse(t, proto, stream, body)
						// Deliberately split inside the CQL header, value length, and payload.
						for offset := 0; offset < len(frame); {
							n := min(70001, len(frame)-offset)
							if offset == 0 {
								n = 3
							}
							source := typedTestChain(t, frame[offset:offset+n])
							packet, err := segment.EncodeSpans(source, false, compressed)
							if err != nil {
								t.Error(err)
								return
							}
							if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
								t.Error(err)
								return
							}
							offset += n
						}
					}()
					page, err := c.executeTyped(context.Background(), &attemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: One, Timeout: 5 * time.Second})
					if err != nil {
						t.Fatal(err)
					}
					<-peerDone
					c.Close() // Delivered owners survive connection shutdown.
					cursor := wire.NewCursor(page.Rows)
					n, err := cursor.Uint32()
					if err != nil {
						t.Fatal(err)
					}
					chain, err := cursor.Take(int64(n))
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(typedTestBytes(t, chain), payload) {
						t.Fatal("received row changed")
					}
					if proto == 5 {
						count := 0
						_ = chain.Each(func(wire.Span) error { count++; return nil })
						if count < 2 {
							t.Fatal("split row was reassembled")
						}
					}
					page.Owner.Release()
				})
			}
		}
	}
}

type typedShortWriter struct {
	limit int
	calls int
	mu    sync.Mutex
}

func (w *typedShortWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *typedShortWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	n := min(w.limit, len(p))
	w.limit -= n
	return n, nil
}
func TestTypedWritersPartialCompletion(t *testing.T) {
	for _, limit := range []int{0, 3, 6, 8, 10} {
		sink := &typedShortWriter{limit: limit}
		coalescer := &writeCoalescer{c: sink}
		a, b := make(chan writeResult, 1), make(chan writeResult, 1)
		coalescer.flushSizes([]chan<- writeResult{a, b}, net.Buffers{[]byte("abc"), []byte("def"), []byte("ghi"), []byte("j")}, []int64{6, 4})
		ar, br := <-a, <-b
		if ar.n != min(limit, 6) || br.n != max(0, limit-6) {
			t.Fatalf("limit %d wrong completion: %v %v", limit, ar, br)
		}
		if limit < 6 && !errors.Is(ar.err, io.ErrShortWrite) {
			t.Fatal("missing first short write")
		}
		if limit < 10 && !errors.Is(br.err, io.ErrShortWrite) {
			t.Fatal("missing second short write")
		}
	}
}

func TestTypedCancelKeepsStreamUntilOrphanArrives(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	go c.serve(c.ctx)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() {
		_, err := c.executeTyped(ctx, &attemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: One})
		out <- err
	}()
	stream := typedTestReadRequest(t, c, server)
	cancel()
	if err := <-out; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if c.streams.InUse() != 1 {
		t.Fatal("cancel reused stream before orphan arrived")
	}
	if _, err := server.Write(typedTestResponse(t, 4, stream, typedTestRowsBody([]byte("orphan")))); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for c.streams.InUse() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.streams.InUse() != 0 {
		t.Fatal("orphan did not release stream")
	}
}

func TestTypedSharedSegmentPagesHaveIndependentOwnership(t *testing.T) {
	c, server := typedTestConn(t, 5, nil)
	received := make(chan error, 1)
	go func() { received <- c.recvLeasedSegment(c.ctx) }()
	type result struct {
		page *responsePage
		err  error
	}
	out := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			page, err := c.executeTyped(context.Background(), &attemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: One, Timeout: time.Second})
			out <- result{page, err}
		}()
	}
	a, b := typedTestReadRequest(t, c, server), typedTestReadRequest(t, c, server)
	first := typedTestResponse(t, 5, a, typedTestRowsBody([]byte("first")))
	second := typedTestResponse(t, 5, b, typedTestRowsBody([]byte("second")))
	payload := typedTestChain(t, first, second)
	packet, err := segment.EncodeSpans(payload, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.Write(typedTestBytes(t, packet)); err != nil {
		t.Fatal(err)
	}
	x, y := <-out, <-out
	// Both request callbacks can run before the segment dispatch returns. Join
	// that dispatch before testing that the receive loop released its share.
	if err := <-received; err != nil {
		t.Fatal(err)
	}
	if x.err != nil || y.err != nil {
		t.Fatalf("errors %v %v", x.err, y.err)
	}
	if x.page.Owner != y.page.Owner {
		t.Fatal("self-contained frames did not share segment lease")
	}
	x.page.Owner.Release()
	if !y.page.Owner.Alive() {
		t.Fatal("releasing first page invalidated second")
	}
	if len(typedTestBytes(t, y.page.Rows)) == 0 {
		t.Fatal("second page lost bytes")
	}
	y.page.Owner.Release()
	if y.page.Owner.Alive() {
		t.Fatal("receive loop retained completed segment")
	}
}

type typedBlockedWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	spans   [][]byte
}

func (w *typedBlockedWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *typedBlockedWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered); <-w.release })
	w.spans = append(w.spans, p)
	return len(p), nil
}
func TestTypedWriterCancellationJoinsAcceptedSpans(t *testing.T) {
	sink := &typedBlockedWriter{entered: make(chan struct{}), release: make(chan struct{})}
	w := &deadlineContextWriter{w: sink, semaphore: make(chan struct{}, 1), quit: make(chan struct{})}
	a, b := []byte("first"), []byte("second")
	input := typedTestChain(t, a, b)
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan error, 1)
	go func() { _, err := w.writeSpans(ctx, input); out <- err }()
	<-sink.entered
	cancel()
	select {
	case <-out:
		t.Fatal("cancellation returned while accepted writer accesses input")
	default:
	}
	close(sink.release)
	if err := <-out; err != nil {
		t.Fatal(err)
	}
	if len(sink.spans) != 2 || &sink.spans[0][0] != &a[0] || &sink.spans[1][0] != &b[0] {
		t.Fatal("writer gathered spans")
	}
}

func TestTypedCoalescerShutdownCompletesAcceptedRequest(t *testing.T) {
	quit := make(chan struct{})
	sink := &typedShortWriter{limit: 100}
	w := newWriteCoalescer(sink, 0, time.Hour, quit)
	admitted := make(chan struct{})
	w.testEnqueuedHook = func() { close(admitted) }
	out := make(chan error, 1)
	go func() {
		_, err := w.writeSpans(context.Background(), typedTestChain(t, []byte("one"), []byte("two")))
		out <- err
	}()
	<-admitted
	close(quit)
	select {
	case err := <-out:
		if err == nil {
			t.Fatal("shutdown unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("accepted request never completed")
	}
}

func TestTypedFatalCloseInterruptsBlockedWriter(t *testing.T) {
	c, _ := typedTestConn(t, 4, nil)
	out := make(chan error, 1)
	payload := bytes.Repeat([]byte("blocked"), 40000)
	values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload)
	released := make(chan struct{})
	owner := wire.NewLease(values, func() { close(released) })
	go func() {
		_, err := c.executeTyped(context.Background(), &attemptRequest{Statement: "INSERT INTO ks.tbl (value) VALUES (?)", Values: values, ValueCount: 1, Lease: owner, Consistency: One})
		out <- err
	}()
	deadline := time.Now().Add(time.Second)
	for c.streams.InUse() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{})
	go func() { c.closeWithError(io.ErrUnexpectedEOF); close(closed) }()
	select {
	case err := <-out:
		if err == nil {
			t.Fatal("blocked write succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("fatal close deadlocked writer")
	}
	owner.Release()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("write ownership leaked")
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("fatal close deadlocked drain")
	}
}

func TestTypedServerErrorKeepsMetadataAndUnwraps(t *testing.T) {
	c, server := typedTestConn(t, 4, nil)
	go c.serve(c.ctx)
	go func() {
		stream := typedTestReadRequest(t, c, server)
		body := bytes.Repeat([]byte{9}, 16)
		body = binary.BigEndian.AppendUint16(body, 1)
		body = binary.BigEndian.AppendUint16(body, 4)
		body = append(body, "warn"...)
		body = binary.BigEndian.AppendUint16(body, 1)
		body = binary.BigEndian.AppendUint16(body, 3)
		body = append(body, "key"...)
		body = binary.BigEndian.AppendUint32(body, 3)
		body = append(body, "val"...)
		body = binary.BigEndian.AppendUint32(body, ErrCodeUnprepared)
		body = binary.BigEndian.AppendUint16(body, 7)
		body = append(body, "missing"...)
		body = binary.BigEndian.AppendUint16(body, 2)
		body = append(body, 1, 2)
		response := typedTestResponse(t, 4, stream, body)
		response[1] = frm.FlagTracing | frm.FlagWarning | frm.FlagCustomPayload
		response[4] = byte(frm.OpError)
		if _, err := server.Write(response); err != nil {
			t.Error(err)
		}
	}()
	page, err := c.executeTyped(context.Background(), &attemptRequest{Statement: "SELECT value FROM ks.tbl", Consistency: One, Timeout: time.Second})
	var unprepared *RequestErrUnprepared
	if !errors.As(err, &unprepared) || !bytes.Equal(unprepared.StatementId, []byte{1, 2}) {
		t.Fatalf("wrong error %v", err)
	}
	if page == nil || len(page.Warnings) != 1 || len(page.TraceID) != 16 || len(page.Payload) != 1 {
		t.Fatal("error metadata lost")
	}
	if got := string(page.Payload[0].Value.Value.OwnedBytes()); got != "val" {
		t.Fatalf("wrong payload %q", got)
	}
	page.Owner.Release()
	if _, err := page.Payload[0].Value.Value.At(0); !errors.Is(err, ErrReleased) {
		t.Fatal("error payload owner did not expire")
	}
}

func TestTypedCompressedRequestsIndependentDecode(t *testing.T) {
	payload := bytes.Repeat([]byte("useful-compression-across-spans-"), 2000)
	var pieces [][]byte
	pieces = append(pieces, binary.BigEndian.AppendUint32(nil, uint32(len(payload))))
	for i := 0; i < len(payload); {
		n := min(43, len(payload)-i)
		pieces = append(pieces, payload[i:i+n])
		i += n
	}
	values := typedTestChain(t, pieces...)
	for _, compressor := range []Compressor{lz4.LZ4Compressor{}, SnappyCompressor{}} {
		c := &Conn{version: 4, compressor: compressor}
		c.initFramerCache()
		request := &attemptRequest{Statement: "INSERT INTO tbl (v) VALUES (?)", Values: values, ValueCount: 1, Consistency: One}
		packet, err := c.buildTypedRequest(request, 8)
		if err != nil {
			t.Fatal(err)
		}
		encoded := typedTestBytes(t, packet)
		if encoded[1]&frm.FlagCompress == 0 {
			t.Fatal("compression flag missing")
		}
		if len(encoded) > len(payload)/2 {
			t.Fatal("span compressor failed to compress repetitive data")
		}
		decoded, err := compressor.Decode(encoded[9:])
		if err != nil {
			t.Fatal(err)
		}
		plain := &Conn{version: 4}
		plain.initFramerCache()
		oracle, err := plain.buildTypedRequest(request, 8)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(decoded, typedTestBytes(t, oracle)[9:]) {
			t.Fatal("independent decompressor observed wrong request bytes")
		}
	}
}

func TestTypedWritevBeyondPlatformVectorLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		peer <- conn
	}()
	client, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-peer
	defer server.Close()
	var spans []wire.Span
	var expected []byte
	for i := 0; i < 2300; i++ {
		part := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		spans = append(spans, wire.Borrow(part))
		expected = append(expected, part...)
	}
	chain, _ := wire.NewChain(spans...)
	writer := &deadlineContextWriter{w: client, semaphore: make(chan struct{}, 1), quit: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		n, err := writer.writeSpans(context.Background(), chain)
		if err == nil && n != chain.Len() {
			err = io.ErrShortWrite
		}
		done <- err
	}()
	received := make([]byte, len(expected))
	if _, err := io.ReadFull(server, received); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, received) {
		t.Fatal("writev truncated or reordered spans beyond vector limit")
	}
}

func TestTypedExecutePrepareBatchWireOracle(t *testing.T) {
	for _, proto := range []byte{3, 4, 5} {
		payload := []byte("borrowed-batch-value")
		values := typedTestChain(t, binary.BigEndian.AppendUint32(nil, uint32(len(payload))), payload)
		for _, kind := range []string{"prepare", "execute", "batch"} {
			t.Run(string(rune('0'+proto))+kind, func(t *testing.T) {
				c := &Conn{version: proto}
				c.initFramerCache()
				r := &attemptRequest{Statement: "INSERT INTO ks.tbl (v) VALUES (?)", Consistency: Quorum, SerialConsistency: LocalSerial, Timestamp: 123456, DefaultTimestamp: true}
				if proto >= 5 {
					r.Keyspace = "ks"
					now := int32(123)
					r.NowInSeconds = &now
				}
				f := c.getWriteFramer()
				defer c.releaseWriteFramer(f)
				params := fixtureQueryParams{consistency: Quorum, serialConsistency: LocalSerial, defaultTimestamp: true, defaultTimestampValue: 123456, keyspace: r.Keyspace}
				if r.NowInSeconds != nil {
					now := int(*r.NowInSeconds)
					params.nowInSeconds = &now
				}
				var err error
				switch kind {
				case "prepare":
					r.Prepare = true
					r.NowInSeconds = nil
					err = (&writePrepareFrame{Statement: r.Statement, Keyspace: r.Keyspace}).BuildFrame(f, 23)
				case "execute":
					r.PreparedID = []byte{1, 2, 3}
					r.ResultMetadataID = []byte{4, 5, 6}
					r.Values = values
					r.ValueCount = 1
					params.values = []fixtureQueryValue{{value: payload}}
					empty := map[string][]byte(nil)
					err = fixtureWriteExecute(f, 23, r.PreparedID, r.ResultMetadataID, &params, &empty)
				case "batch":
					r.Batch = &typedBatchRequest{Kind: LoggedBatch, Items: []typedBatchItem{{Statement: r.Statement, Values: values, ValueCount: 1}, {PreparedID: []byte{1, 2}, Values: values, ValueCount: 1}}}
					err = (&fixtureBatchFrame{typ: LoggedBatch, statements: []fixtureBatchStatement{{statement: r.Statement, values: []fixtureQueryValue{{value: payload}}}, {preparedID: []byte{1, 2}, values: []fixtureQueryValue{{value: payload}}}}, consistency: Quorum, serialConsistency: LocalSerial, defaultTimestamp: true, defaultTimestampValue: 123456, keyspace: r.Keyspace, nowInSeconds: params.nowInSeconds}).BuildFrame(f, 23)
				}
				if err != nil {
					t.Fatal(err)
				}
				actual, err := c.buildTypedRequest(r, 23)
				if err != nil {
					t.Fatal(err)
				}
				encoded := typedTestBytes(t, actual)
				if proto >= 5 {
					reader := bytes.NewReader(encoded)
					h, err := segment.ReadHeader(reader, false)
					if err != nil {
						t.Fatal(err)
					}
					owner, err := segment.ReadOwnedPayload(reader, h, nil)
					if err != nil {
						t.Fatal(err)
					}
					encoded = typedTestBytes(t, owner.Chain())
					owner.Release()
				}
				if !bytes.Equal(encoded, f.Buf) {
					t.Fatalf("%s differs from legacy wire oracle\ngot %x\nwant %x", kind, encoded, f.Buf)
				}
			})
		}
	}
}

func TestTypedTabletHintAcrossEveryByte(t *testing.T) {
	e := newTypedEnvelope()
	e.U32(8)
	e.U64(11)
	e.U32(8)
	e.U64(22)
	e.U32(36)
	e.U32(1)
	e.U32(28)
	e.U32(16)
	e.U64(2)
	e.U64(3)
	e.U32(4)
	e.U32(5)
	chain, err := e.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	body := typedTestBytes(t, chain)
	for split := 0; split <= len(body); split++ {
		chain := typedTestChain(t, body[:split], body[split:])
		owner := wire.NewLease(chain, nil)
		_, err := parseTypedTabletHint(cqltype.BytesView(chain, owner), "ks", "tbl")
		owner.Release()
		if err != nil {
			t.Fatalf("split %d: %v", split, err)
		}
	}
	bad := append(append([]byte(nil), body...), 0)
	if _, err := parseTypedTabletHint(BorrowBytes(bad), "ks", "tbl"); err == nil {
		t.Fatal("accepted trailing tablet bytes")
	}
}

type typedTLSRecorder struct {
	conn     *tls.Conn
	expected [][]byte
	next     int
}

func (w *typedTLSRecorder) SetWriteDeadline(t time.Time) error { return w.conn.SetWriteDeadline(t) }
func (w *typedTLSRecorder) Write(p []byte) (int, error) {
	if w.next >= len(w.expected) || len(p) != len(w.expected[w.next]) || &p[0] != &w.expected[w.next][0] {
		return 0, fmt.Errorf("TLS plaintext was gathered before tls.Conn.Write")
	}
	w.next++
	return w.conn.Write(p)
}
func TestTypedTLSReceivesOriginalPlaintextSpans(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"span.test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(cryptorand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	rawClient, rawServer := net.Pipe()
	defer rawClient.Close()
	defer rawServer.Close()
	server := tls.Server(rawServer, &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13})
	client := tls.Client(rawClient, &tls.Config{RootCAs: roots, ServerName: "span.test", MinVersion: tls.VersionTLS13})
	parts := [][]byte{[]byte("header"), bytes.Repeat([]byte("borrowed plaintext "), 30000), []byte("trailer")}
	var expected []byte
	for _, part := range parts {
		expected = append(expected, part...)
	}
	readResult := make(chan error, 1)
	go func() {
		received := make([]byte, len(expected))
		_, err := io.ReadFull(server, received)
		if err == nil && !bytes.Equal(received, expected) {
			err = fmt.Errorf("TLS reordered payload")
		}
		readResult <- err
	}()
	recorder := &typedTLSRecorder{conn: client, expected: parts}
	writer := &deadlineContextWriter{w: recorder, semaphore: make(chan struct{}, 1), quit: make(chan struct{})}
	writer.setWriteTimeout(5 * time.Second)
	if _, err := writer.writeSpans(context.Background(), typedTestChain(t, parts...)); err != nil {
		t.Fatal(err)
	}
	if err := <-readResult; err != nil {
		t.Fatal(err)
	}
	if recorder.next != len(parts) {
		t.Fatal("TLS missed an input span")
	}
}

func TestTypedProtocolLimits(t *testing.T) {
	t.Run("request", func(t *testing.T) {
		c := &Conn{version: 4, cfg: &ConnConfig{TypedLimits: ProtocolLimits{MaxFrameBytes: 8}}}
		if _, err := c.buildTypedRequest(&attemptRequest{Statement: "SELECT value FROM table", Consistency: One}, 1); !errors.Is(err, ErrFrameTooBig) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("metadata depth", func(t *testing.T) {
		e := newTypedEnvelope()
		e.U32(frm.ResultKindRows)
		e.U32(uint32(frm.FlagGlobalTableSpec))
		e.U32(1)
		e.Str("ks", false)
		e.Str("tbl", false)
		e.Str("v", false)
		e.U16(uint16(TypeList))
		e.U16(uint16(TypeList))
		e.U16(uint16(TypeInt))
		e.U32(0)
		body, _ := e.Freeze()
		owner := wire.NewLease(body, nil)
		defer owner.Release()
		c := &Conn{version: 4, cfg: &ConnConfig{TypedLimits: ProtocolLimits{MaxMetadataDepth: 1}}}
		if _, err := c.parseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner); err == nil {
			t.Fatal("metadata depth budget ignored")
		}
		c.cfg.TypedLimits.MaxMetadataDepth = 2
		if _, err := c.parseTypedResponse(frm.FrameHeader{Version: 0x84, Op: frm.OpResult}, body, owner); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("v4 expansion before allocation", func(t *testing.T) {
		c, server := typedTestConn(t, 4, lz4.LZ4Compressor{})
		c.cfg = &ConnConfig{TypedLimits: ProtocolLimits{MaxDecompressedBytes: 64}}
		go c.serve(c.ctx)
		go func() {
			stream := typedTestReadRequest(t, c, server)
			response := typedTestResponse(t, 4, stream, binary.BigEndian.AppendUint32(nil, 1000))
			response[1] = frm.FlagCompress
			server.Write(response)
		}()
		page, err := c.executeTyped(context.Background(), &attemptRequest{Statement: "q", Consistency: One, Timeout: time.Second})
		if page != nil {
			page.Owner.Release()
		}
		if !errors.Is(err, ErrFrameTooBig) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("v5 expansion before payload read", func(t *testing.T) {
		c := &Conn{cfg: &ConnConfig{TypedLimits: ProtocolLimits{MaxDecompressedBytes: 64}}}
		if _, err := c.readTypedSegmentPayload(segment.Header{PayloadLen: 1, UncompressedLen: 65}); !errors.Is(err, ErrFrameTooBig) {
			t.Fatalf("got %v", err)
		}
	})
}
