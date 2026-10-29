/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package segment

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/scylladb/gocql/v2/internal/crc"
)

var (
	ErrInvalidHeader  = errors.New("gocql: invalid segment lengths or compression")
	ErrUnitLimit      = errors.New("gocql: segment transform storage limit exceeded")
	ErrUnitIntegrity  = errors.New("gocql: segment payload checksum mismatch")
	ErrDecoderStorage = errors.New("gocql: segment decoder did not use bounded output storage")
)

func validatePayloadHeader(h Header, compressed bool) error {
	if h.PayloadLen < 0 || h.PayloadLen > MaxPayloadSize || h.UncompressedLen < 0 || h.UncompressedLen > MaxPayloadSize || (!compressed && h.UncompressedLen != 0) {
		return ErrInvalidHeader
	}
	return nil
}

const unitFramingBytes = 16

// DefaultUnitStorageBytes covers two maximum units and fixed framing scratch.
const DefaultUnitStorageBytes = 2*MaxPayloadSize + unitFramingBytes

// UnitLimits limits simultaneous transform storage and the declared output of
// one segment. The native 17-bit ceilings always apply. Zero selects defaults.
type UnitLimits struct {
	MaxStorageBytes int64
	MaxDecodedBytes int
}

// UnitReader validates one integrity unit before exposing any payload. Next
// reuses one fixed bounded allocation; its result is valid only until the next
// Next or Close. No list of previous units or complete frame is retained. This
// is intentionally distinct from independently retained ReadOwnedPayload leases.
// The exclusive transport owns framing across units, context interruption of
// blocking I/O, and release/return-to-pool behavior; this type starts no worker.
type UnitReader struct {
	ctx     context.Context
	source  io.Reader
	comp    Compressor
	limits  UnitLimits
	storage []byte
	framing [unitFramingBytes]byte
	err     error
	closed  bool
}

func NewUnitReader(ctx context.Context, source io.Reader, comp Compressor, limits UnitLimits) (*UnitReader, error) {
	if ctx == nil || source == nil || limits.MaxStorageBytes < 0 || limits.MaxDecodedBytes < 0 {
		return nil, ErrUnitLimit
	}
	if limits.MaxStorageBytes == 0 {
		limits.MaxStorageBytes = DefaultUnitStorageBytes
	}
	if limits.MaxDecodedBytes == 0 {
		limits.MaxDecodedBytes = MaxPayloadSize
	}
	if limits.MaxStorageBytes < unitFramingBytes {
		return nil, ErrUnitLimit
	}
	return &UnitReader{ctx: ctx, source: source, comp: comp, limits: limits}, nil
}

// StorageBytes reports reserved transform/framing bytes, separately from the
// small reader object. It cannot grow with response size or segment count.
func (r *UnitReader) StorageBytes() int64 { return int64(cap(r.storage)) + unitFramingBytes }

func (r *UnitReader) fail(err error) (Header, []byte, error) {
	if r.err == nil {
		r.err = err
	}
	return Header{}, nil, r.err
}

func (r *UnitReader) Next() (Header, []byte, error) {
	if r.err != nil {
		return Header{}, nil, r.err
	}
	if r.closed {
		return r.fail(io.ErrClosedPipe)
	}
	if err := r.ctx.Err(); err != nil {
		return r.fail(err)
	}
	headerSize := HeaderSize(r.comp != nil)
	if _, err := io.ReadFull(r.source, r.framing[:headerSize]); err != nil {
		return r.fail(err)
	}
	h, err := DecodeHeader(r.framing[:headerSize], r.comp != nil)
	if err != nil {
		return r.fail(err)
	}
	if err := validatePayloadHeader(h, r.comp != nil); err != nil {
		return r.fail(err)
	}
	decoded := h.UncompressedLen
	if decoded == 0 {
		decoded = h.PayloadLen
	}
	if decoded > r.limits.MaxDecodedBytes || int64(h.PayloadLen+h.UncompressedLen)+unitFramingBytes > r.limits.MaxStorageBytes {
		return r.fail(ErrUnitLimit)
	}
	if err := r.ctx.Err(); err != nil {
		return r.fail(err)
	}
	if r.storage == nil {
		maxPayload := MaxPayloadSize
		if r.comp != nil {
			maxPayload *= 2
		}
		size := min(int64(maxPayload), r.limits.MaxStorageBytes-unitFramingBytes)
		r.storage = make([]byte, int(size))
	}
	payload := r.storage[:h.PayloadLen:h.PayloadLen]
	if _, err := io.ReadFull(r.source, payload); err != nil {
		return r.fail(err)
	}
	trailer := r.framing[CompressedHeaderSize : CompressedHeaderSize+Crc32Size]
	if _, err := io.ReadFull(r.source, trailer); err != nil {
		return r.fail(err)
	}
	if crc.Crc32(payload) != binary.LittleEndian.Uint32(trailer) {
		return r.fail(ErrUnitIntegrity)
	}
	if h.UncompressedLen != 0 {
		out := r.storage[h.PayloadLen : h.PayloadLen : h.PayloadLen+h.UncompressedLen]
		out, err = r.comp.AppendDecompressed(out, payload, uint32(h.UncompressedLen))
		if err != nil {
			return r.fail(err)
		}
		if len(out) != h.UncompressedLen || &out[0] != &r.storage[h.PayloadLen] {
			return r.fail(ErrDecoderStorage)
		}
		payload = out
	}
	if err := r.ctx.Err(); err != nil {
		return r.fail(err)
	}
	return h, payload, nil
}

// Close drops scratch, not the transport. It must be called only after the
// owner has stopped Next; concurrent I/O interruption belongs to that owner.
func (r *UnitReader) Close() { r.closed = true; r.storage = nil }
