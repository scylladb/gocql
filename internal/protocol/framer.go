/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package protocol

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"
	cqltype "github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/wirecompress"

	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"

	"sync/atomic"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
)

const (
	ProtoDirectionMask = 0x80
	ProtoVersionMask   = 0x7F
	protoVersion1      = 0x01
	ProtoVersion2      = 0x02
	ProtoVersion3      = 0x03
	ProtoVersion4      = 0x04
	// protoVersion5 is the final (non-beta) native protocol v5: the envelope and
	// transport-segment layout implemented by segment.go.
	//
	// Requests are deliberately never marked with frm.FlagBetaProtocol. That flag
	// opts into whatever v5 dialect the server is currently developing, which for
	// Cassandra 3.11 is the *beta* v5 dialect — it does not use the final envelope
	// layout, so setting the flag makes such a server accept the handshake and
	// then fail every subsequent frame with a protocol error, instead of cleanly
	// rejecting the version. Apache removed the same automatic opt-in in
	// CASSGO-88 (https://issues.apache.org/jira/browse/CASSGO-88). Supporting a
	// beta dialect would need an explicit opt-in bound to that specific dialect.
	ProtoVersion5 = 0x05
)

var (
	ErrReadHeaderTimeout = errors.New("unable to read frame header")
)

func ReadInt(p []byte) int32 {
	return int32(binary.BigEndian.Uint32(p[:4]))
}

const DefaultBufSize = 128

// framerInterface represents a frame reader/writer for the CQL protocol.
//
// Framers are pooled and reused. Any byte slices returned from frame parsing
// methods may be backed by pooled buffers that are reused after Release() is
// called. If data must outlive the framer, use readBytesCopy() instead of
// readBytes() when implementing parseFrame(), or copy returned byte slices
// before calling Release().
//
// After Release() is called, the framer and any slices derived from its
// buffers must not be accessed.
type FramerInterface interface {
	ReadBytesInternal() ([]byte, error)
	GetCustomPayload() map[string][]byte
	GetHeaderWarnings() []string
	// Release returns the framer to its pool (if pooled).
	// Must be called when the framer is no longer needed.
	// Safe to call multiple times; subsequent calls are no-ops.
	Release()
}

const HeadSize = 9

// a framer is responsible for reading, writing and parsing frames on a single stream
type Framer struct {
	Compressor    Compressor
	Header        *frm.FrameHeader
	CustomPayload map[string][]byte
	ReleaseHook   func()
	TraceID       []byte
	ReadBuffer    []byte
	Buf           []byte
	// wireBuf is the framer's second reusable byte buffer. prepareModernLayout
	// encodes the v5 transport segments into it and then swaps it with buf, so a
	// framer reused for consecutive v5 requests keeps both the raw-frame buffer
	// and the wire buffer alive instead of allocating a new one per request.
	WireBuf               []byte
	MaxFrameBytes         int64
	MaxDecompressedBytes  int64
	FlagLWT               int
	RateLimitingErrorCode int
	Flags                 byte
	Proto                 byte
	TabletsRoutingV1      bool
	tabletsRoutingV2      bool
	ScyllaUseMetadataID   bool
	Released              atomic.Bool
}

// defaultFramerFlags computes the default header flags a framer carries for the
// given compressor and negotiated protocol version. It is the single source of
// truth shared by newFramer and initCache so the startup/fallback path and the
// pooled framers cannot drift.
//
// Only FlagCompress is derived, and only below proto v5 (v5+ compresses at the
// segment layer, not via a frame-header flag). It depends on the negotiated
// compressor, so it must only be applied after startup: the server may reject the
// requested compression, which clears c.compressor. No version-derived flag is
// added — in particular FlagBetaProtocol is never set, see protoVersion5.
func DefaultFramerFlags(compressor Compressor, version byte) byte {
	// Mask off the direction/reserved high bit: newFramer is called with the
	// unmasked version byte, and an unmasked byte must not defeat the v5 check and
	// re-enable frame-header compression on v5.
	if compressor != nil && version&ProtoVersionMask < ProtoVersion5 {
		return frm.FlagCompress
	}
	return 0
}

func NewFramer(compressor Compressor, version byte) *Framer {
	buf := make([]byte, DefaultBufSize)
	f := &Framer{
		Buf:        buf[:0],
		ReadBuffer: buf,
	}
	flags := DefaultFramerFlags(compressor, version)

	version &= ProtoVersionMask
	f.Compressor = compressor
	f.Proto = version
	f.Flags = flags
	f.Header = nil
	f.TraceID = nil

	f.TabletsRoutingV1 = false
	f.tabletsRoutingV2 = false
	f.ScyllaUseMetadataID = false

	return f
}

// Release returns the framer to its pool. If the framer was not obtained
// from a pool (release is nil), this is a no-op.
//
// Conn.releaseFramer owns the released-state guard, so this method delegates
// directly to the release closure.
func (f *Framer) Release() {
	if f.ReleaseHook != nil {
		f.ReleaseHook()
	}
}

type Frame interface {
	Header() frm.FrameHeader
}

func ReadHeader(r io.Reader, p []byte) (head frm.FrameHeader, err error) {
	n, err := io.ReadFull(r, p[:HeadSize])
	if err != nil {
		// A timeout that consumed nothing is the benign idle wait for the next
		// frame, which serve() recovers from by simply reading again. A timeout that
		// consumed part of a header is not: the stream position is now unknown, so
		// resuming would mis-frame everything after it. Only the former is
		// normalised to ErrReadHeaderTimeout; the latter stays a plain error and
		// takes the connection down.
		//
		// errors.As rather than a bare type assertion, matching
		// Conn.readFirstSegmentHeader: the two timeout checks must stay in
		// agreement even if a caller in between starts wrapping the error.
		var netErr net.Error
		if n == 0 && errors.As(err, &netErr) && netErr.Timeout() {
			return frm.FrameHeader{}, fmt.Errorf("%w: %w", ErrReadHeaderTimeout, err)
		}
		return frm.FrameHeader{}, err
	}

	return frm.DecodeHeader(p[:HeadSize])
}

// explicitly enables tracing for the framers outgoing requests
func (f *Framer) Trace() {
	f.Flags |= frm.FlagTracing
}

// explicitly enables the custom payload flag
func (f *Framer) Payload() {
	f.Flags |= frm.FlagCustomPayload
}

// reads a frame form the wire into the framers buffer
func (f *Framer) ReadFrame(r io.Reader, head *frm.FrameHeader) error {
	// A negative length is rejected by readHeader, which is where it has to be
	// caught: there the error is fatal to the connection, whereas an error from
	// here is delivered to the waiting caller. Kept as a precondition check for
	// callers that synthesise a header.
	if head.Length < 0 {
		return fmt.Errorf("frame body length can not be less than 0: %d", head.Length)
	} else if int64(head.Length) > f.frameLimit() || head.Flags&frm.FlagCompress == 0 && int64(head.Length) > f.decompressedLimit() {
		// need to free up the connection to be used again
		_, err := io.CopyN(io.Discard, r, int64(head.Length))
		if err != nil {
			// %w, not %v: a failed discard leaves the undiscarded remainder of the
			// body on the wire, so the caller has to be able to recognise the read
			// failure here (Conn.bodyReadDesyncedConn) and close the connection rather
			// than read the leftover bytes as the next frame header.
			return fmt.Errorf("error whilst trying to discard frame with invalid length: %w", err)
		}
		return ErrFrameTooBig
	}

	if cap(f.ReadBuffer) >= head.Length {
		f.Buf = f.ReadBuffer[:head.Length]
	} else {
		f.ReadBuffer = make([]byte, head.Length)
		f.Buf = f.ReadBuffer
	}

	n, err := io.ReadFull(r, f.Buf)
	if err != nil {
		// %w, not %v: a partially read body leaves the rest of it on the wire, so
		// the connection is desynced and must be closed. Conn.bodyReadDesyncedConn
		// decides that by inspecting the wrapped error, which a %v-formatted error
		// would defeat — the connection would be reused and every later frame
		// mis-framed.
		return fmt.Errorf("unable to read frame body: read %d/%d bytes: %w", n, head.Length, err)
	}

	if f.Proto < ProtoVersion5 && head.Flags&frm.FlagCompress == frm.FlagCompress {
		if f.Compressor == nil {
			return NewErrProtocol("no compressor available with compressed frame body")
		}

		if decoder, ok := wirecompress.FrameDecoderWithLimit(f.Compressor, f.decompressedLimit()); ok {
			if _, err = decoder.DecodedLen(f.Buf); err == nil {
				f.Buf, err = decoder.DecodeTo(nil, f.Buf)
			}
		} else {
			f.Buf, err = f.Compressor.Decode(f.Buf)
		}
		if err != nil {
			return err
		}
	}
	if int64(len(f.Buf)) > f.decompressedLimit() {
		return ErrFrameTooBig
	}

	f.Header = head
	return nil
}

// adoptFrameBody takes ownership of a frame body that is already fully in memory,
// instead of reading and copying it as readFrame does. It is used for a v5 frame
// reassembled from several transport segments (Conn.recvSplitFrame): that buffer
// is already exactly frame-sized, so copying it would mean holding the frame twice
// — 512 MiB for a frm.MaxFrameSize response.
//
// f.readBuffer is deliberately left pointing at the pooled buffer, so releasing
// the framer drops the adopted body instead of retaining an outsized buffer in the
// framer pool. Segment-level decompression has already happened, so unlike
// readFrame there is no frame-header compression flag to honour (it only exists
// below v5).
func (f *Framer) AdoptFrameBody(body []byte, head *frm.FrameHeader) error {
	if int64(len(body)) > min(f.frameLimit(), f.decompressedLimit()) {
		return ErrFrameTooBig
	}
	if head.Length != len(body) {
		return fmt.Errorf("gocql: frame body length %d does not match the %d bytes reassembled", head.Length, len(body))
	}
	f.Buf = body
	f.Header = head
	return nil
}

// malformedFrameError carries a read-path failure out of the helpers through
// panic/recover. It is the only panic value parseFrame converts into an error:
// anything else unwinding through it comes from a driver bug, and returning that as
// an error would file the bug against the peer.
type MalformedFrameError struct{ Err error }

func (e MalformedFrameError) Error() string { return e.Err.Error() }

func (e MalformedFrameError) Unwrap() error { return e.Err }

// malformedFramef builds the value the read helpers panic with. Every panic under
// parseFrame goes through it; a bare panic(fmt.Errorf(...)) is not converted.
func MalformedFramef(format string, a ...any) MalformedFrameError {
	return MalformedFrameError{Err: fmt.Errorf(format, a...)}
}

// recoverMalformedFrame turns a read-path panic into a returned error and re-panics
// everything else -- a runtime.Error, a panic("..."), or a plain error from code that
// does not speak this contract. See the read-helper rules above readByte.
func RecoverMalformedFrame(r any, err *error) {
	if r == nil {
		return
	}
	mf, ok := r.(MalformedFrameError)
	if !ok {
		panic(r)
	}
	*err = mf.Unwrap()
}

func (f *Framer) ParseFrame() (frame Frame, err error) {
	// The read helpers panic with a malformedFrameError instead of returning one (see
	// readByte); this recover is what makes a malformed frame a protocol error.
	defer func() { RecoverMalformedFrame(recover(), &err) }()

	if f.Header.Version.Request() {
		return nil, NewErrProtocol("got a request frame from server: %v", f.Header.Version)
	}

	if f.Header.Flags&frm.FlagTracing == frm.FlagTracing {
		f.readTrace()
	}

	if f.Header.Flags&frm.FlagWarning == frm.FlagWarning {
		f.Header.Warnings = f.ReadStringList()
	}

	if f.Header.Flags&frm.FlagCustomPayload == frm.FlagCustomPayload {
		f.CustomPayload = f.ReadBytesMap()
	}

	// assumes that the frame body has been read into rbuf
	switch f.Header.Op {
	case frm.OpError:
		frame = f.parseErrorFrame()
	case frm.OpReady:
		frame = f.parseReadyFrame()
	case frm.OpResult:
		frame, err = f.parseResultFrame()
	case frm.OpSupported:
		frame = f.parseSupportedFrame()
	case frm.OpAuthenticate:
		frame = f.parseAuthenticateFrame()
	case frm.OpAuthChallenge:
		frame = f.parseAuthChallengeFrame()
	case frm.OpAuthSuccess:
		frame = f.parseAuthSuccessFrame()
	case frm.OpEvent:
		frame = f.ParseEventFrame()
	default:
		return nil, NewErrProtocol("unknown op in frame header: %s", f.Header.Op)
	}

	return
}

func (f *Framer) parseErrorFrame() Frame {
	code := f.ReadInt()
	msg := f.ReadString()

	errD := frm.ErrorFrame{
		FrameHeader: *f.Header,
		Code:        code,
		Message:     msg,
	}

	switch code {
	case ErrCodeUnavailable:
		cl := f.ReadConsistency()
		required := f.ReadInt()
		alive := f.ReadInt()
		return &RequestErrUnavailable{
			ErrorFrame:  errD,
			Consistency: cl,
			Required:    required,
			Alive:       alive,
		}
	case ErrCodeWriteTimeout:
		cl := f.ReadConsistency()
		received := f.ReadInt()
		blockfor := f.ReadInt()
		writeType := f.ReadString()
		return &RequestErrWriteTimeout{
			ErrorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			WriteType:   writeType,
		}
	case ErrCodeReadTimeout:
		cl := f.ReadConsistency()
		received := f.ReadInt()
		blockfor := f.ReadInt()
		dataPresent := f.ReadByteValue()
		return &RequestErrReadTimeout{
			ErrorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			DataPresent: dataPresent,
		}
	case ErrCodeAlreadyExists:
		ks := f.ReadString()
		table := f.ReadString()
		return &RequestErrAlreadyExists{
			ErrorFrame: errD,
			Keyspace:   ks,
			Table:      table,
		}
	case ErrCodeUnprepared:
		return &RequestErrUnprepared{
			ErrorFrame:  errD,
			StatementId: f.ReadShortBytesCopy(),
		}
	case ErrCodeReadFailure:
		res := &RequestErrReadFailure{
			ErrorFrame: errD,
		}
		res.Consistency = f.ReadConsistency()
		res.Received = f.ReadInt()
		res.BlockFor = f.ReadInt()
		if f.Proto > ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.ReadInt()
		}
		res.DataPresent = f.ReadByteValue() != 0

		return res
	case ErrCodeWriteFailure:
		res := &RequestErrWriteFailure{
			ErrorFrame: errD,
		}
		res.Consistency = f.ReadConsistency()
		res.Received = f.ReadInt()
		res.BlockFor = f.ReadInt()
		if f.Proto > ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.ReadInt()
		}
		res.WriteType = f.ReadString()
		return res
	case ErrCodeFunctionFailure:
		res := &RequestErrFunctionFailure{
			ErrorFrame: errD,
		}
		res.Keyspace = f.ReadString()
		res.Function = f.ReadString()
		res.ArgTypes = f.ReadStringList()
		return res

	case ErrCodeCDCWriteFailure:
		res := &RequestErrCDCWriteFailure{
			ErrorFrame: errD,
		}
		return res
	case ErrCodeCASWriteUnknown:
		res := &RequestErrCASWriteUnknown{
			ErrorFrame: errD,
		}
		res.Consistency = f.ReadConsistency()
		res.Received = f.ReadInt()
		res.BlockFor = f.ReadInt()
		return res
	case ErrCodeInvalid, ErrCodeBootstrapping, ErrCodeConfig, ErrCodeCredentials, ErrCodeOverloaded,
		ErrCodeProtocol, ErrCodeServer, ErrCodeSyntax, ErrCodeTruncate, ErrCodeUnauthorized:
		// TODO(zariel): we should have some distinct types for these errors
		return errD
	default:
		if f.RateLimitingErrorCode != 0 && code == f.RateLimitingErrorCode {
			res := &RequestErrRateLimitReached{
				ErrorFrame: errD,
			}
			res.OpType = OpType(f.ReadByteValue())
			res.RejectedByCoordinator = f.ReadByteValue() != 0
			return res
		} else {
			return &UnknownServerError{
				ErrorFrame: errD,
			}
		}
	}
}

func (f *Framer) readErrorMap() (errMap ErrorMap) {
	errMap = make(ErrorMap)
	numErrs := f.ReadInt()
	for i := 0; i < numErrs; i++ {
		ip := f.readInetAdressOnly().String()
		errMap[ip] = f.ReadShort()
	}
	return
}

func (f *Framer) WriteHeader(flags byte, op frm.Op, stream int) {
	f.Buf = append(f.Buf[:0],
		f.Proto, flags, byte(stream>>8), byte(stream),
		// pad out length
		byte(op), 0, 0, 0, 0,
	)
}

func (f *Framer) setLength(length int) {
	f.Buf[5] = byte(length >> 24)
	f.Buf[6] = byte(length >> 16)
	f.Buf[7] = byte(length >> 8)
	f.Buf[8] = byte(length)
}

func (f *Framer) Finish() error {
	bufLen := len(f.Buf)
	if int64(bufLen-HeadSize) > f.frameLimit() {
		// huge app frame, lets remove it so it doesn't bloat the heap.
		//
		// readBuffer is reset with it. On a write framer it is never read -- only
		// releaseRead realigns buf to it -- but the pool keeps the two aliased to one
		// array, and replacing buf alone breaks that: the framer would go back to the
		// pool holding this 128-byte buf plus the previous, EWMA-sized readBuffer that
		// nothing can reach. Two arrays where the pool intends one.
		f.Buf = make([]byte, DefaultBufSize)
		f.ReadBuffer = f.Buf
		return ErrFrameTooBig
	}

	if f.Proto < ProtoVersion5 && f.Buf[1]&frm.FlagCompress == frm.FlagCompress {
		if f.Compressor == nil {
			panic("compress flag set with no compressor")
		}

		// TODO: only compress frames which are big enough
		compressed, err := f.Compressor.Encode(f.Buf[HeadSize:])
		if err != nil {
			return err
		}

		// The check at the top of this function measured the uncompressed body, which
		// is the wrong side of Encode to bound. Snappy's block format has no stored-raw
		// mode, so an incompressible body comes back larger than it went in: a body that
		// fit the limit can exceed it once compressed. setLength below writes the
		// compressed length, and that is the number this driver's own reader and the
		// server both bound, so without this the frame goes out declaring a length its
		// peer will refuse -- a remote failure, or a killed connection, in place of a
		// local error the caller can act on.
		//
		// Checked before the append so an oversized result is never copied into the
		// frame, and f.buf is dropped -- with readBuffer, for the reasons the
		// pre-check gives -- because at this size it would otherwise sit in the pool.
		if int64(len(compressed)) > f.frameLimit() {
			f.Buf = make([]byte, DefaultBufSize)
			f.ReadBuffer = f.Buf
			return ErrFrameTooBig
		}

		f.Buf = append(f.Buf[:HeadSize], compressed...)
		bufLen = len(f.Buf)
	}
	length := bufLen - HeadSize
	f.setLength(length)

	return nil
}

func (f *Framer) frameLimit() int64 {
	if f.MaxFrameBytes > 0 {
		return f.MaxFrameBytes
	}
	return frm.MaxFrameSize
}

func (f *Framer) decompressedLimit() int64 {
	if f.MaxDecompressedBytes > 0 {
		return f.MaxDecompressedBytes
	}
	return frm.MaxFrameSize
}

func (f *Framer) WriteFrameTo(w io.Writer) error {
	_, err := w.Write(f.Buf)
	return err
}

func (f *Framer) readTrace() {
	if len(f.Buf) < 16 {
		panic(MalformedFramef("not enough bytes in buffer to read trace uuid require 16 got: %d", len(f.Buf)))
	}
	if len(f.TraceID) != 16 {
		f.TraceID = make([]byte, 16)
	}
	copy(f.TraceID, f.Buf[:16])
	f.Buf = f.Buf[16:]
}

func (f *Framer) parseReadyFrame() Frame {
	return &frm.ReadyFrame{
		FrameHeader: *f.Header,
	}
}

// TODO: if we move the body buffer onto the frameHeader then we only need a single
// framer, and can move the methods onto the header.
func (f *Framer) parseSupportedFrame() Frame {
	return &frm.SupportedFrame{
		FrameHeader: *f.Header,

		Supported: f.readStringMultiMap(),
	}
}

type WriteStartupFrame struct {
	Opts map[string]string
}

func (w WriteStartupFrame) String() string {
	return fmt.Sprintf("[startup opts=%+v]", w.Opts)
}

func (w *WriteStartupFrame) BuildFrame(f *Framer, streamID int) error {
	f.WriteHeader(f.Flags&^frm.FlagCompress, frm.OpStartup, streamID)
	f.writeStringMap(w.Opts)

	return f.Finish()
}

func (f *Framer) ReadTypeInfo() TypeInfo {
	// TODO: factor this out so the same code paths can be used to parse custom
	// types and other types, as much of the logic will be duplicated.
	id := f.ReadShort()

	simple := cqltype.NewCustomType(f.Proto,
		Type(id), "")

	// Fast path for simple native types (through TypeDuration).
	if id > 0 && id <= uint16(TypeDuration) {
		return simple
	}

	if cqltype.NativeKind(simple) == TypeCustom {
		simple = NewCustomType(simple.Version(), simple.Type(), f.ReadString())
		if cassType := cqltype.GetApacheCassandraType(cqltype.NativeCustom(simple)); cassType != TypeCustom {
			simple = NewCustomType(simple.Version(), cassType, simple.Custom())
		}
	}

	switch cqltype.NativeKind(simple) {
	case TypeTuple:
		n := f.ReadShort()
		tuple := TupleTypeInfo{
			NativeType: simple,
			Elems:      make([]TypeInfo, n),
		}

		for i := 0; i < int(n); i++ {
			tuple.Elems[i] = f.ReadTypeInfo()
		}

		return tuple

	case TypeUDT:
		udt := UDTTypeInfo{
			NativeType: simple,
		}
		udt.KeySpace = f.ReadString()
		udt.Name = f.ReadString()

		n := f.ReadShort()
		udt.Elements = make([]UDTField, n)
		for i := 0; i < int(n); i++ {
			field := &udt.Elements[i]
			field.Name = f.ReadString()
			field.Type = f.ReadTypeInfo()
		}

		return udt
	case TypeMap, TypeList, TypeSet:
		collection := CollectionType{
			NativeType: simple,
		}

		if cqltype.NativeKind(simple) == TypeMap {
			collection.Key = f.ReadTypeInfo()
		}

		collection.Elem = f.ReadTypeInfo()

		return collection
	case TypeCustom:
		vectorTypePrefix := ApacheCassandraTypePrefix + "VectorType"
		if strings.HasPrefix(cqltype.NativeCustom(simple), vectorTypePrefix) {
			return f.readVectorTypeInfo(simple, vectorTypePrefix)
		}
	}

	return simple
}

// readVectorTypeInfo resolves a custom type named "<prefix>(<subtype>, <dimensions>)".
// Every part is checked before it is sliced: an unchecked index raises a
// runtime.Error, which parseFrame's recover re-panics by design. A name that only
// starts with the prefix is not a vector and degrades to the plain custom type.
func (f *Framer) readVectorTypeInfo(simple NativeType, vectorTypePrefix string) TypeInfo {
	rest := cqltype.NativeCustom(simple)[len(vectorTypePrefix):]
	switch {
	case rest == "":
		panic(MalformedFramef("invalid vector type %q: expected %s(<type>, <dimensions>)", cqltype.NativeCustom(simple), vectorTypePrefix))
	case rest[0] != '(':
		// Not a vector: a different type that shares the prefix.
		return simple
	case rest[len(rest)-1] != ')':
		panic(MalformedFramef("invalid vector type %q: unterminated argument list", cqltype.NativeCustom(simple)))
	}

	// The dimensions are the last argument, so the last comma separates them from
	// a subtype that may itself be parenthesised and hold commas of its own.
	spec := rest[1 : len(rest)-1]
	idx := strings.LastIndex(spec, ",")
	if idx < 0 {
		panic(MalformedFramef("invalid vector type %q: missing dimensions", cqltype.NativeCustom(simple)))
	}

	typeStr := strings.TrimSpace(spec[:idx])
	if typeStr == "" {
		panic(MalformedFramef("invalid vector type %q: missing element type", cqltype.NativeCustom(simple)))
	}

	// Cassandra requires a positive dimension, and a negative one reaches
	// reflect.MakeSlice in unmarshalVector, outside any recover.
	dim, err := strconv.Atoi(strings.TrimSpace(spec[idx+1:]))
	if err != nil || dim < 1 {
		panic(MalformedFramef("invalid vector type %q: dimensions must be a positive integer", cqltype.NativeCustom(simple)))
	}

	return VectorType{
		NativeType: simple,
		SubType:    cqltype.GetCassandraLongType(typeStr, f.Proto, cqltype.NopTypeLogger{}),
		Dimensions: dim,
	}
}

type PreparedMetadata struct {
	keyspace string
	table    string
	// proto v4+
	PkeyColumns []int
	ResultMetadata
	// LWT query detected
	lwt bool
}

func (r PreparedMetadata) String() string {
	return fmt.Sprintf("[prepared flags=0x%x pkey=%v paging_state=% X columns=%v col_count=%d actual_col_count=%d lwt=%t]",
		r.Flags, r.PkeyColumns, r.PagingState, r.Columns, r.ColCount, r.ActualColCount, r.lwt)
}

func (f *Framer) ParsePreparedMetadata() PreparedMetadata {
	// TODO: deduplicate this from parseMetadata
	meta := PreparedMetadata{}

	meta.Flags = f.ReadInt()
	meta.ColCount = f.ReadInt()
	if meta.ColCount < 0 {
		panic(MalformedFramef("received negative column count: %d", meta.ColCount))
	}
	meta.ActualColCount = meta.ColCount

	if f.Proto >= ProtoVersion4 {
		pkeyCount := f.ReadInt()
		// Like the colCount guard above, reject a negative count: make would panic
		// with a runtime error, which parseFrame's recover re-panics rather than
		// converting to an error. Unlike colCount — whose huge-value case is handled
		// by the make/append split further down — also reject a count larger than the
		// remaining buffer could supply: each pkey index is a short (2 bytes), so a
		// valid frame always has pkeyCount <= len(f.buf)/2. That bounds make() to the
		// actual frame size instead of a peer-declared count, so a small malformed
		// frame cannot force a large allocation.
		if pkeyCount < 0 || pkeyCount > len(f.Buf)/2 {
			panic(MalformedFramef("invalid partition key count %d (remaining %d bytes)", pkeyCount, len(f.Buf)))
		}
		pkeys := make([]int, pkeyCount)
		for i := 0; i < pkeyCount; i++ {
			pkeys[i] = int(f.ReadShort())
		}
		meta.PkeyColumns = pkeys
	}

	// flagLWT is 0 unless the server negotiated SCYLLA_LWT_ADD_METADATA_MARK, and
	// flags&0 == 0 always holds, so without the guard every statement would be LWT.
	meta.lwt = f.FlagLWT != 0 && meta.Flags&f.FlagLWT == f.FlagLWT

	if meta.Flags&frm.FlagHasMorePages == frm.FlagHasMorePages {
		meta.PagingState = f.ReadBytesCopy()
	}

	if meta.Flags&frm.FlagNoMetaData == frm.FlagNoMetaData {
		return meta
	}

	globalSpec := meta.Flags&frm.FlagGlobalTableSpec == frm.FlagGlobalTableSpec
	if globalSpec {
		meta.keyspace = f.ReadString()
		meta.table = f.ReadString()
	}

	var cols []ColumnInfo
	readPerColumnSpec := !globalSpec
	var tracker keyspaceTableTracker
	if meta.ColCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.ColCount)
		for i := 0; i < meta.ColCount; i++ {
			col := &cols[i]
			keyspace, table := f.readColWithSpec(col, &meta.ResultMetadata, globalSpec, meta.keyspace, meta.table, i, readPerColumnSpec)
			if readPerColumnSpec {
				tracker.track(i, keyspace, table)
			}
		}
	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.ColCount; i++ {
			var col ColumnInfo
			keyspace, table := f.readColWithSpec(&col, &meta.ResultMetadata, globalSpec, meta.keyspace, meta.table, i, readPerColumnSpec)
			if readPerColumnSpec {
				tracker.track(i, keyspace, table)
			}
			cols = append(cols, col)
		}
	}

	if !globalSpec && meta.ColCount > 0 && tracker.allSame {
		meta.keyspace = tracker.keyspace
		meta.table = tracker.table
	}

	meta.Columns = cols

	return meta
}

type ResultMetadata struct {
	PagingState []byte
	// this is a count of the total number of columns which can be scanned,
	// it is at minimum len(columns) but may be larger, for instance when a column
	// is a UDT or tuple.
	Columns        []ColumnInfo
	NewMetadataID  []byte
	Flags          int
	ColCount       int
	ActualColCount int
}

func (r *ResultMetadata) morePages() bool {
	return r.Flags&frm.FlagHasMorePages == frm.FlagHasMorePages
}

func (r *ResultMetadata) noMetaData() bool {
	return r.Flags&frm.FlagNoMetaData == frm.FlagNoMetaData
}

func (r ResultMetadata) String() string {
	return fmt.Sprintf("[metadata flags=0x%x paging_state=% X columns=%v new_metadata_id=% X]", r.Flags, r.PagingState, r.Columns, r.NewMetadataID)
}

// keyspaceTableTracker tracks whether all columns share the same keyspace/table.
type keyspaceTableTracker struct {
	keyspace string
	table    string
	allSame  bool
}

func (t *keyspaceTableTracker) track(colIndex int, keyspace, table string) {
	if colIndex == 0 {
		t.keyspace = keyspace
		t.table = table
		t.allSame = true
	} else if t.allSame && (keyspace != t.keyspace || table != t.table) {
		t.allSame = false
	}
}

func (f *Framer) readColWithSpec(col *ColumnInfo, meta *ResultMetadata, globalSpec bool, keyspace, table string, colIndex int, readPerColumnSpec bool) (string, string) {
	if readPerColumnSpec {
		// Per-column table spec encoding: read keyspace/table for this column.
		col.Keyspace = f.ReadString()
		col.Table = f.ReadString()
	} else {
		if !globalSpec && colIndex != 0 {
			// Skip per-column keyspace/table already read from column 0.
			f.skipString()
			f.skipString()
		}
		col.Keyspace = keyspace
		col.Table = table
	}

	col.Name = f.ReadString()
	col.TypeInfo = f.ReadTypeInfo()
	if tuple, ok := col.TypeInfo.(TupleTypeInfo); ok {
		// -1 because we already included the tuple column
		meta.ActualColCount += len(tuple.Elems) - 1
	}

	return col.Keyspace, col.Table
}

func (f *Framer) ParseResultMetadata() ResultMetadata {
	var meta ResultMetadata

	meta.Flags = f.ReadInt()
	meta.ColCount = f.ReadInt()
	if meta.ColCount < 0 {
		panic(MalformedFramef("received negative column count: %d", meta.ColCount))
	}
	meta.ActualColCount = meta.ColCount

	if meta.Flags&frm.FlagHasMorePages == frm.FlagHasMorePages {
		meta.PagingState = f.ReadBytesCopy()
	}

	// The re-issue of a result metadata ID, reached from a RESULT/Rows whose
	// EXECUTE carried an ID the server found stale. It supersedes the one
	// RESULT/Prepared issued; see parseResultPrepared for the other half.
	//
	// Read after the paging state, matching Cassandra's encoder
	// (ResultSet$ResultMetadata$Codec.encode) and the v5 spec. See
	// TestParseResultMetadata_PagingStateBeforeNewMetadataID, which is the only
	// test that can distinguish the two orderings.
	if (f.Proto > ProtoVersion4 || f.ScyllaUseMetadataID) && meta.Flags&frm.FlagMetaDataChanged == frm.FlagMetaDataChanged {
		meta.NewMetadataID = f.ReadShortBytesCopy()
	}

	if meta.noMetaData() {
		return meta
	}

	globalSpec := meta.Flags&frm.FlagGlobalTableSpec == frm.FlagGlobalTableSpec

	// Read keyspace/table once and reuse for all columns. ROWS results are
	// always single-table; when !globalSpec this consumes column 0's wire
	// values and readColWithSpec skips the rest via skipString().
	var keyspace, table string
	if globalSpec || meta.ColCount > 0 {
		keyspace = f.ReadString()
		table = f.ReadString()
	}

	var cols []ColumnInfo
	if meta.ColCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.ColCount)
		for i := 0; i < meta.ColCount; i++ {
			f.readColWithSpec(&cols[i], &meta, globalSpec, keyspace, table, i, false)
		}

	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.ColCount; i++ {
			var col ColumnInfo
			f.readColWithSpec(&col, &meta, globalSpec, keyspace, table, i, false)
			cols = append(cols, col)
		}
	}

	meta.Columns = cols

	return meta
}

type resultVoidFrame struct {
	frm.FrameHeader
}

func (f *resultVoidFrame) String() string {
	return "[result_void]"
}

func (f *Framer) parseResultFrame() (Frame, error) {
	kind := f.ReadInt()

	switch kind {
	case frm.ResultKindVoid:
		return &resultVoidFrame{FrameHeader: *f.Header}, nil
	case frm.ResultKindRows:
		return f.parseResultRows(), nil
	case frm.ResultKindKeyspace:
		return f.parseResultSetKeyspace(), nil
	case frm.ResultKindPrepared:
		return f.parseResultPrepared(), nil
	case frm.ResultKindSchemaChanged:
		return f.parseResultSchemaChange(), nil
	}

	return nil, NewErrProtocol("unknown result kind: %x", kind)
}

type ResultRowsFrame struct {
	frm.FrameHeader

	Meta ResultMetadata
	// dont parse the rows here as we only need to do it once
	numRows int
}

func (f *ResultRowsFrame) String() string {
	return fmt.Sprintf("[result_rows meta=%v]", f.Meta)
}

func (f *Framer) parseResultRows() Frame {
	result := &ResultRowsFrame{}
	result.Meta = f.ParseResultMetadata()

	result.numRows = f.ReadInt()
	if result.numRows < 0 {
		panic(MalformedFramef("invalid row_count in result frame: %d", result.numRows))
	}

	return result
}

type ResultKeyspaceFrame struct {
	keyspace string
	frm.FrameHeader
}

func (r *ResultKeyspaceFrame) String() string {
	return fmt.Sprintf("[result_keyspace keyspace=%s]", r.keyspace)
}

func (f *Framer) parseResultSetKeyspace() Frame {
	return &ResultKeyspaceFrame{
		FrameHeader: *f.Header,
		keyspace:    f.ReadString(),
	}
}

type ResultPreparedFrame struct {
	preparedID       []byte
	ResultMetadataID []byte
	respMeta         ResultMetadata
	frm.FrameHeader
	ReqMeta PreparedMetadata
}

// parseResultPrepared parses a RESULT/Prepared body:
//
//	<id>                 [short bytes]  prepared statement ID
//	<result_metadata_id> [short bytes]  v5, or v4 with SCYLLA_USE_METADATA_ID
//	<metadata>           bind variables and partition key indexes (request side)
//	<result_metadata>    the columns rows will carry (response side)
//
// The two metadata blocks describe opposite directions and are unrelated; only
// the second one has an ID, because only it can go stale without the driver
// noticing.
//
// The ID read here is the first one for this statement, issued alongside the
// metadata it identifies and echoed back by every later EXECUTE. A superseding
// ID arrives by a different route — newMetadataID inside a RESULT/Rows, behind
// METADATA_CHANGED — so that the server can repair a stale ID in the response it
// was already sending instead of making the driver re-prepare. Both land in
// preparedStatment.ResultMetadataID.
//
// parseResultMetadata below is the same codec RESULT/Rows uses, as it is in
// Cassandra (ResultSet$ResultMetadata$Codec), so it reads newMetadataID whenever
// METADATA_CHANGED is set. Nothing sets it here: that flag says a previously
// issued ID is stale, which cannot apply to the response issuing the first one.
func (f *Framer) parseResultPrepared() Frame {
	frame := &ResultPreparedFrame{
		FrameHeader: *f.Header,
		preparedID:  f.ReadShortBytesCopy(),
	}

	if f.Proto > ProtoVersion4 || f.ScyllaUseMetadataID {
		frame.ResultMetadataID = f.ReadShortBytesCopy()
	}

	frame.ReqMeta = f.ParsePreparedMetadata()
	frame.respMeta = f.ParseResultMetadata()

	return frame
}

func (f *Framer) parseResultSchemaChange() Frame {
	change := f.ReadString()
	target := f.ReadString()

	// TODO: could just use a separate type for each target
	switch target {
	case "KEYSPACE":
		return &frm.SchemaChangeKeyspace{
			FrameHeader: *f.Header,
			Change:      change,
			Keyspace:    f.ReadString(),
		}
	case "TABLE":
		return &frm.SchemaChangeTable{
			FrameHeader: *f.Header,
			Change:      change,
			Keyspace:    f.ReadString(),
			Object:      f.ReadString(),
		}
	case "TYPE":
		return &frm.SchemaChangeType{
			FrameHeader: *f.Header,
			Change:      change,
			Keyspace:    f.ReadString(),
			Object:      f.ReadString(),
		}
	case "FUNCTION":
		return &frm.SchemaChangeFunction{
			FrameHeader: *f.Header,
			Change:      change,
			Keyspace:    f.ReadString(),
			Name:        f.ReadString(),
			Args:        f.ReadStringList(),
		}
	case "AGGREGATE":
		return &frm.SchemaChangeAggregate{
			FrameHeader: *f.Header,
			Change:      change,
			Keyspace:    f.ReadString(),
			Name:        f.ReadString(),
			Args:        f.ReadStringList(),
		}
	default:
		panic(MalformedFramef("gocql: unknown SCHEMA_CHANGE target: %q change: %q", target, change))
	}
}

func (f *Framer) parseAuthenticateFrame() Frame {
	return &frm.AuthenticateFrame{
		FrameHeader: *f.Header,
		Class:       f.ReadString(),
	}
}

func (f *Framer) parseAuthSuccessFrame() Frame {
	return &frm.AuthSuccessFrame{
		FrameHeader: *f.Header,
		Data:        f.ReadBytesCopy(),
	}
}

func (f *Framer) parseAuthChallengeFrame() Frame {
	return &frm.AuthChallengeFrame{
		FrameHeader: *f.Header,
		Data:        f.ReadBytesCopy(),
	}
}

func (f *Framer) ParseEventFrame() Frame {
	eventType := f.ReadString()

	switch eventType {
	case "TOPOLOGY_CHANGE":
		frame := &frm.TopologyChangeEventFrame{FrameHeader: *f.Header}
		frame.Change = f.ReadString()
		frame.Host, frame.Port = f.readInet()

		return frame
	case "STATUS_CHANGE":
		frame := &frm.StatusChangeEventFrame{FrameHeader: *f.Header}
		frame.Change = f.ReadString()
		frame.Host, frame.Port = f.readInet()

		return frame
	case "SCHEMA_CHANGE":
		// this should work for all versions
		return f.parseResultSchemaChange()
	case "CLIENT_ROUTES_CHANGE":
		return &frm.ClientRoutesChanged{
			FrameHeader:   *f.Header,
			ChangeType:    f.ReadString(),
			ConnectionIDs: f.ReadStringList(),
			HostIDs:       f.ReadStringList(),
		}
	default:
		panic(MalformedFramef("gocql: unknown event type: %q", eventType))
	}

}

type WriteAuthResponseFrame struct {
	Data []byte
}

func (a *WriteAuthResponseFrame) String() string {
	return fmt.Sprintf("[auth_response data=%q]", a.Data)
}

func (a *WriteAuthResponseFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.writeAuthResponseFrame(streamID, a.Data)
}

func (f *Framer) writeAuthResponseFrame(streamID int, data []byte) error {
	f.WriteHeader(f.Flags, frm.OpAuthResponse, streamID)
	f.WriteBytes(data)
	return f.Finish()
}

// validateV5Options rejects the request options that only exist from protocol v5
// onwards, and the one v5 option whose value has to fit the wire type. Callers
// pass nil for nowInSeconds when their frame has no such field.
//
// It is the single source of truth for these checks, shared by every writer that
// accepts them (QUERY/EXECUTE via writeQueryParams, BATCH, PREPARE), so the three
// cannot drift apart in what they reject or in what they say. Each BuildFrame also
// calls it before writing any byte, so a rejected option cannot leave a partially
// serialised frame behind.
func (f *Framer) ValidateV5Options(keyspace string, nowInSeconds *int) error {
	if keyspace != "" && f.Proto < ProtoVersion5 {
		return fmt.Errorf("gocql: keyspace override can only be set with protocol v5 or higher, current protocol: %d", f.Proto)
	}
	if nowInSeconds != nil {
		if f.Proto < ProtoVersion5 {
			return fmt.Errorf("gocql: now_in_seconds can only be set with protocol v5 or higher, current protocol: %d", f.Proto)
		}
		if v := *nowInSeconds; v < math.MinInt32 || v > math.MaxInt32 {
			return fmt.Errorf("gocql: nowInSeconds value %d overflows int32", v)
		}
	}
	return nil
}

// validateShortBytes rejects a value too long for the [short bytes] wire type, whose
// length field is two bytes. Both values written with it -- a prepared id and a v5
// result metadata id -- are opaque server-issued tokens far below the limit, so this
// is about what a mis-sized one would do rather than about a value anyone expects to
// see: writeShortBytes would truncate the length silently and emit a frame whose
// header disagrees with its payload, desynchronising the connection.
//
// The single source of truth for the check, like validateV5Options above, so the
// callers that hoist it and writeShortBytes itself cannot drift apart.
func ValidateShortBytes(field string, p []byte) error {
	if len(p) > math.MaxUint16 {
		return fmt.Errorf("gocql: %s is %d bytes, which overflows the 2-byte length of a [short bytes] value", field, len(p))
	}
	return nil
}

type FrameBuilder interface {
	BuildFrame(framer *Framer, streamID int) error
}

type FrameWriterFunc func(framer *Framer, streamID int) error

func (f FrameWriterFunc) BuildFrame(framer *Framer, streamID int) error {
	return f(framer, streamID)
}

type WriteOptionsFrame struct{}

func (w *WriteOptionsFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.writeOptionsFrame(streamID, w)
}

func (f *Framer) writeOptionsFrame(stream int, _ *WriteOptionsFrame) error {
	f.WriteHeader(f.Flags&^frm.FlagCompress, frm.OpOptions, stream)
	return f.Finish()
}

type WriteRegisterFrame struct {
	Events []string
}

func (w *WriteRegisterFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.writeRegisterFrame(streamID, w)
}

func (f *Framer) writeRegisterFrame(streamID int, w *WriteRegisterFrame) error {
	f.WriteHeader(f.Flags, frm.OpRegister, streamID)
	f.WriteStringList(w.Events)

	return f.Finish()
}

// The read helpers below bounds-check and then panic instead of returning an error;
// parseFrame's recover converts it. Two rules keep that working:
//   - panic with malformedFramef, never a bare fmt.Errorf and never a string:
//     recoverMalformedFrame converts that one type and re-panics everything else;
//   - never raise a runtime.Error: bound every index against len(f.buf) and reject
//     a negative length first -- a negative is not below any length. parseFrame
//     re-panics a runtime.Error on purpose.
func (f *Framer) ReadByteValue() byte {
	if len(f.Buf) < 1 {
		panic(MalformedFramef("not enough bytes in buffer to read byte require 1 got: %d", len(f.Buf)))
	}

	b := f.Buf[0]
	f.Buf = f.Buf[1:]
	return b
}

func (f *Framer) ReadInt() (n int) {
	if len(f.Buf) < 4 {
		panic(MalformedFramef("not enough bytes in buffer to read int require 4 got: %d", len(f.Buf)))
	}

	n = int(int32(binary.BigEndian.Uint32(f.Buf[:4])))
	f.Buf = f.Buf[4:]
	return
}

func (f *Framer) ReadShort() (n uint16) {
	if len(f.Buf) < 2 {
		panic(MalformedFramef("not enough bytes in buffer to read short require 2 got: %d", len(f.Buf)))
	}
	n = binary.BigEndian.Uint16(f.Buf[:2])
	f.Buf = f.Buf[2:]
	return
}

func (f *Framer) ReadString() (s string) {
	size := f.ReadShort()

	if len(f.Buf) < int(size) {
		panic(MalformedFramef("not enough bytes in buffer to read string require %d got: %d", size, len(f.Buf)))
	}

	s = string(f.Buf[:size])
	f.Buf = f.Buf[size:]
	return
}

// skipString advances past a string without allocating.
func (f *Framer) skipString() {
	size := f.ReadShort()

	if len(f.Buf) < int(size) {
		panic(MalformedFramef("not enough bytes in buffer to skip string, requires %d got %d", size, len(f.Buf)))
	}

	f.Buf = f.Buf[size:]
}

func (f *Framer) ReadLongString() (s string) {
	size := f.ReadInt()

	// A [long string]'s length is signed, and unlike [bytes] a negative is malformed,
	// not null. len(f.buf) is never below a negative, so without this f.buf[:size]
	// raises a runtime.Error that parseFrame re-panics.
	if size < 0 {
		panic(MalformedFramef("invalid long string length: %d", size))
	}

	if len(f.Buf) < size {
		panic(MalformedFramef("not enough bytes in buffer to read long string require %d got: %d", size, len(f.Buf)))
	}

	s = string(f.Buf[:size])
	f.Buf = f.Buf[size:]
	return
}

func (f *Framer) ReadStringList() []string {
	size := f.ReadShort()

	l := make([]string, size)
	for i := 0; i < int(size); i++ {
		l[i] = f.ReadString()
	}

	return l
}

func (f *Framer) ReadBytesInternal() ([]byte, error) {
	size := f.ReadInt()
	if size < 0 {
		return nil, nil
	}

	if len(f.Buf) < size {
		return nil, fmt.Errorf("not enough bytes in buffer to read bytes require %d got: %d", size, len(f.Buf))
	}

	l := f.Buf[:size]
	f.Buf = f.Buf[size:]

	return l, nil
}

func (f *Framer) ReadBytesCopy() []byte {
	size := f.ReadInt()
	if size < 0 {
		return nil
	}

	if len(f.Buf) < size {
		panic(MalformedFramef("not enough bytes in buffer to read bytes require %d got: %d", size, len(f.Buf)))
	}

	out := make([]byte, size)
	copy(out, f.Buf[:size])
	f.Buf = f.Buf[size:]
	return out
}

func (f *Framer) ReadShortBytesCopy() []byte {
	size := f.ReadShort()
	if len(f.Buf) < int(size) {
		panic(MalformedFramef("not enough bytes in buffer to read short bytes: require %d got %d", size, len(f.Buf)))
	}

	out := make([]byte, size)
	copy(out, f.Buf[:size])
	f.Buf = f.Buf[size:]

	return out
}

func (f *Framer) readInetAdressOnly() net.IP {
	if len(f.Buf) < 1 {
		panic(MalformedFramef("not enough bytes in buffer to read inet size require %d got: %d", 1, len(f.Buf)))
	}

	size := f.Buf[0]
	f.Buf = f.Buf[1:]

	if !(size == 4 || size == 16) {
		panic(MalformedFramef("invalid IP size: %d", size))
	}

	if len(f.Buf) < int(size) {
		panic(MalformedFramef("not enough bytes in buffer to read inet require %d got: %d", size, len(f.Buf)))
	}

	ip := make(net.IP, size)
	copy(ip, f.Buf[:size])
	f.Buf = f.Buf[size:]
	return ip
}

func (f *Framer) readInet() (net.IP, int) {
	return f.readInetAdressOnly(), f.ReadInt()
}

func (f *Framer) ReadConsistency() Consistency {
	return Consistency(f.ReadShort())
}

func (f *Framer) ReadBytesMap() map[string][]byte {
	size := f.ReadShort()
	m := make(map[string][]byte, size)

	for i := 0; i < int(size); i++ {
		m[f.ReadString()] = f.ReadBytesCopy()
	}

	return m
}

func (f *Framer) readStringMultiMap() map[string][]string {
	size := f.ReadShort()
	m := make(map[string][]string, size)

	for i := 0; i < int(size); i++ {
		k := f.ReadString()
		v := f.ReadStringList()
		m[k] = v
	}

	return m
}

func (f *Framer) WriteByteValue(b byte) {
	f.Buf = append(f.Buf, b)
}

func appendBytes(p []byte, d []byte) []byte {
	if d == nil {
		return appendIntNeg1(p)
	}
	p = appendInt(p, int32(len(d)))
	p = append(p, d...)
	return p
}

func appendShort(p []byte, n uint16) []byte {
	return append(p,
		byte(n>>8),
		byte(n),
	)
}

func appendInt(p []byte, n int32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func appendIntNeg1(p []byte) []byte {
	return append(p, 255, 255, 255, 255)
}

func appendUint(p []byte, n uint32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func appendLong(p []byte, n int64) []byte {
	return append(p,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func (f *Framer) WriteCustomPayload(customPayload *map[string][]byte) {
	if len(*customPayload) > 0 {
		if f.Proto < ProtoVersion4 {
			panic("Custom payload is not supported with version V3 or less")
		}
		f.writeBytesMap(*customPayload)
	}
}

func (f *Framer) GetCustomPayload() map[string][]byte {
	return f.CustomPayload
}

func (f *Framer) GetHeaderWarnings() []string {
	return f.Header.Warnings
}

// these are protocol level binary types
func (f *Framer) WriteInt(n int32) {
	f.Buf = appendInt(f.Buf, n)
}

func (f *Framer) writeIntNeg1() {
	f.Buf = appendIntNeg1(f.Buf)
}

func (f *Framer) writeIntNeg2() {
	f.Buf = append(f.Buf, 255, 255, 255, 254)
}

func (f *Framer) WriteUint(n uint32) {
	f.Buf = appendUint(f.Buf, n)
}

func (f *Framer) WriteShort(n uint16) {
	f.Buf = appendShort(f.Buf, n)
}

func (f *Framer) WriteLong(n int64) {
	f.Buf = appendLong(f.Buf, n)
}

func (f *Framer) WriteString(s string) {
	f.WriteShort(uint16(len(s)))
	f.Buf = append(f.Buf, s...)
}

func (f *Framer) WriteLongString(s string) {
	f.WriteInt(int32(len(s)))
	f.Buf = append(f.Buf, s...)
}

func (f *Framer) WriteStringList(l []string) {
	f.WriteShort(uint16(len(l)))
	for _, s := range l {
		f.WriteString(s)
	}
}

func (f *Framer) WriteUnset() {
	// Protocol version 4 specifies that bind variables do not require having a
	// value when executing a statement.   Bind variables without a value are
	// called 'unset'. The 'unset' bind variable is serialized as the int
	// value '-2' without following bytes.
	f.writeIntNeg2()
}

func (f *Framer) WriteBytes(p []byte) {
	// TODO: handle null case correctly,
	//     [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
	//					  no byte should follow and the value represented is `null`.
	if p == nil {
		f.writeIntNeg1()
	} else {
		f.WriteInt(int32(len(p)))
		f.Buf = append(f.Buf, p...)
	}
}

// writeShortBytes writes a [short bytes] value. Validated again here, not only in the
// callers that hoist it before writing any byte: this is package-internal and nothing
// else would enforce the precondition, and uint16(len(p)) would otherwise truncate.
func (f *Framer) WriteShortBytes(p []byte) error {
	if err := ValidateShortBytes("value", p); err != nil {
		return err
	}
	f.WriteShort(uint16(len(p)))
	f.Buf = append(f.Buf, p...)
	return nil
}

func (f *Framer) WriteConsistency(cons Consistency) {
	f.WriteShort(uint16(cons))
}

func (f *Framer) writeStringMap(m map[string]string) {
	f.WriteShort(uint16(len(m)))
	for k, v := range m {
		f.WriteString(k)
		f.WriteString(v)
	}
}

func (f *Framer) WriteStringMultiMap(m map[string][]string) {
	f.WriteShort(uint16(len(m)))
	for k, v := range m {
		f.WriteString(k)
		f.WriteStringList(v)
	}
}

func (f *Framer) writeBytesMap(m map[string][]byte) {
	f.WriteShort(uint16(len(m)))
	for k, v := range m {
		f.WriteString(k)
		f.WriteBytes(v)
	}
}

// prepareModernLayout rewrites the framer's buffer from a bare CQL frame into the
// v5 wire format: one transport segment if the frame fits in one, otherwise a
// chain of non-self-contained segments (see segment.go).
//
// Segment headers, payloads and CRCs are encoded straight into f.wireBuf, sized
// up front so appending never has to grow it, and nothing per-segment is
// allocated on the way. f.buf and f.wireBuf are then swapped, which both hands
// the caller the wire bytes in f.buf and keeps the raw-frame buffer alive as
// f.wireBuf for the next request on this framer.
func (f *Framer) PrepareModernLayout() error {
	// Ensure protocol version is V5 or higher
	if f.Proto < ProtoVersion5 {
		return fmt.Errorf("gocql: modern layout is not supported with protocol version %d (requires v5+)", f.Proto)
	}

	// Narrow the compressor once per frame rather than once per segment: a frame past
	// segment.MaxPayloadSize is segmented by the loop below, and every one of those
	// segments takes the same layout from the same compressor, which is fixed for the
	// life of the framer.
	//
	// The narrowing is defensive — clusterSettings.Validate rejects a compressor without
	// segment support on v5 before a connection is dialed, and
	// Conn.resolveSegmentCompressor re-checks it at the handshake — and is reported
	// rather than ignored, because falling back to the uncompressed layout would
	// produce segments the peer decodes with the wrong one.
	segComp, err := AsSegmentCompressor(f.Compressor)
	if err != nil {
		return err
	}

	// Segment the frame via a local cursor rather than mutating f.buf as we go,
	// and only swap the buffers once the whole frame has been segmented
	// successfully, so that an error partway through leaves f.buf byte-for-byte
	// intact.
	src := f.Buf
	wire := f.growWireBuf(segment.EncodedSize(len(src), segComp != nil))

	selfContained := true

	// Process the buffer in chunks if it exceeds the max payload size
	for len(src) > segment.MaxPayloadSize {
		wire, err = segment.Append(wire, src[:segment.MaxPayloadSize], false, segComp)
		if err != nil {
			return err
		}

		src = src[segment.MaxPayloadSize:]
		selfContained = false
	}

	// Process the remaining buffer
	if wire, err = segment.Append(wire, src, selfContained, segComp); err != nil {
		return err
	}

	f.WireBuf, f.Buf = f.Buf, wire

	return nil
}

// growWireBuf returns f.wireBuf emptied and with room for at least n bytes,
// reallocating only when what it already holds is too small.
func (f *Framer) growWireBuf(n int) []byte {
	if cap(f.WireBuf) < n {
		f.WireBuf = make([]byte, 0, n)
	}
	return f.WireBuf[:0]
}

// useKeyspaceFrame is the parameter-free QUERY needed while opening a pooled
// connection on protocols3/4. Application requests use the canonical span writer.
type UseKeyspaceFrame struct{ Statement string }

func (w *UseKeyspaceFrame) BuildFrame(f *Framer, streamID int) error {
	f.WriteHeader(f.Flags, frm.OpQuery, streamID)
	f.WriteLongString(w.Statement)
	f.WriteConsistency(One)
	if f.Proto >= ProtoVersion5 {
		f.WriteUint(0)
	} else {
		f.WriteByteValue(0)
	}
	return f.Finish()
}
