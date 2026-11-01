// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

import (
	"github.com/scylladb/gocql/v2/internal/model"

	"encoding/binary"

	"fmt"
	"math"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

// typedEnvelope owns only newly encoded Protocol fields. Borrowed values never
// enter the slab, and Freeze cannot relocate any published bytes.
// A TypedEnvelope must not be copied after its first use.
type TypedEnvelope struct {
	b   wire.Builder
	err error
}

func NewTypedEnvelope() *TypedEnvelope {
	// Most requests need only short scalar framing; strings and payloads are
	// borrowed spans. Additional slabs handle larger envelopes without moving
	// bytes already published to the builder.
	e := new(TypedEnvelope)
	e.b.SetSlabSize(256)
	return e
}
func (e *TypedEnvelope) check(err error) {
	if e.err == nil {
		e.err = err
	}
}
func (e *TypedEnvelope) U8(v byte)    { e.check(e.b.Uint8(v)) }
func (e *TypedEnvelope) U16(v uint16) { e.check(e.b.Uint16(v)) }
func (e *TypedEnvelope) U32(v uint32) { e.check(e.b.Uint32(v)) }
func (e *TypedEnvelope) U64(v uint64) { e.check(e.b.Uint64(v)) }
func (e *TypedEnvelope) Chain(c wire.Chain) {
	e.check(c.Each(func(s wire.Span) error { return e.b.Borrow(s) }))
}
func (e *TypedEnvelope) Str(s string, long bool) {
	if (!long && len(s) > math.MaxUint16) || int64(len(s)) > math.MaxInt32 {
		e.check(fmt.Errorf("gocql: oversized Protocol string"))
		return
	}
	if long {
		e.U32(uint32(len(s)))
	} else {
		e.U16(uint16(len(s)))
	}
	e.check(e.b.Borrow(wire.BorrowString(s)))
}
func (e *TypedEnvelope) Bytes(p []byte, short bool) {
	if short {
		if len(p) > math.MaxUint16 {
			e.check(fmt.Errorf("gocql: oversized short bytes"))
			return
		}
		e.U16(uint16(len(p)))
	} else {
		if int64(len(p)) > math.MaxInt32 {
			e.check(ErrFrameTooBig)
			return
		}
		e.U32(uint32(len(p)))
	}
	e.check(e.b.Borrow(wire.Borrow(p)))
}
func (e *TypedEnvelope) Freeze() (wire.Chain, error) {
	if e.err != nil {
		return wire.Chain{}, e.err
	}
	return e.b.Freeze()
}

func validateTypedValues(c wire.Chain, count int, proto byte) error {
	if count < 0 || count > math.MaxUint16 {
		return fmt.Errorf("gocql: invalid bound value count %d", count)
	}
	cursor := wire.NewCursor(c)
	for i := 0; i < count; i++ {
		n, err := cursor.Uint32()
		if err != nil {
			return err
		}
		switch int32(n) {
		case -1:
		case -2:
			if proto < 4 {
				return fmt.Errorf("gocql: unset requires Protocol v4")
			}
		default:
			if int32(n) < 0 {
				return fmt.Errorf("gocql: invalid bound length %d", int32(n))
			}
			if err := cursor.Skip(int64(n)); err != nil {
				return err
			}
		}
	}
	return cursor.RequireEnd()
}

func (c Settings) BuildTypedRequest(req *model.AttemptRequest, stream int) (wire.Chain, error) {
	proto := c.Protocol
	if proto < 3 || proto > 5 {
		return wire.Chain{}, fmt.Errorf("gocql: unsupported typed Protocol %d", proto)
	}
	if (req.Keyspace != "" || req.NowInSeconds != nil) && proto < 5 {
		return wire.Chain{}, fmt.Errorf("gocql: keyspace and now_in_seconds overrides require Protocol v5")
	}
	if req.PageSize < 0 || int64(req.PageSize) > math.MaxInt32 {
		return wire.Chain{}, fmt.Errorf("gocql: invalid page size")
	}
	if err := validateTypedValues(req.Values, req.ValueCount, proto); err != nil {
		return wire.Chain{}, err
	}
	e := NewTypedEnvelope()
	Flags := c.Flags & frm.FlagBetaProtocol
	if req.Trace {
		Flags |= frm.FlagTracing
	}
	if len(req.Payload) > 0 {
		Flags |= frm.FlagCustomPayload
	}
	op := frm.OpQuery
	switch {
	case req.Prepare:
		op = frm.OpPrepare
	case req.Batch != nil:
		op = frm.OpBatch
	case len(req.PreparedID) > 0:
		op = frm.OpExecute
	}
	// Uncompressed native frames can share one scalar slab and immutable span
	// index with their body. Write the known header now and patch only its owned
	// length field before Freeze; v3/v4 compressors still receive the body alone.
	inlineHeader := proto >= 5 || c.Compression == ""
	var bodyLength wire.Mark
	if inlineHeader {
		e.U8(proto)
		e.U8(Flags)
		e.U16(uint16(stream))
		e.U8(byte(op))
		bodyLength = e.b.Mark()
		e.U32(0)
	}
	if len(req.Payload) > 0 {
		if proto < 4 {
			return wire.Chain{}, fmt.Errorf("gocql: custom payload requires Protocol v4")
		}
		if len(req.Payload) > math.MaxUint16 {
			return wire.Chain{}, fmt.Errorf("gocql: too many payload entries")
		}
		e.U16(uint16(len(req.Payload)))
		for _, entry := range req.Payload {
			e.Str(entry.Key, false)
			if !entry.Value.Valid {
				e.U32(math.MaxUint32)
				continue
			}
			value := entry.Value.Value
			if err := cqltype.CheckBytesAlive(value); err != nil {
				return wire.Chain{}, err
			}
			if value.Len() > math.MaxInt32 {
				return wire.Chain{}, ErrFrameTooBig
			}
			e.U32(uint32(value.Len()))
			e.Chain(cqltype.BytesChain(value))
		}
	}
	switch {
	case req.Prepare:
		e.Str(req.Statement, true)
		if proto >= 5 {
			var f uint32
			if req.Keyspace != "" {
				f = frm.FlagWithPreparedKeyspace
			}
			e.U32(f)
		}
		if req.Keyspace != "" {
			e.Str(req.Keyspace, false)
		}
	case req.Batch != nil:
		if len(req.Batch.Items) > math.MaxUint16 {
			return wire.Chain{}, fmt.Errorf("gocql: too many batch entries")
		}
		e.U8(byte(req.Batch.Kind))
		e.U16(uint16(len(req.Batch.Items)))
		for _, item := range req.Batch.Items {
			if err := validateTypedValues(item.Values, item.ValueCount, proto); err != nil {
				return wire.Chain{}, err
			}
			if len(item.PreparedID) > 0 {
				e.U8(1)
				e.Bytes(item.PreparedID, true)
			} else {
				e.U8(0)
				e.Str(item.Statement, true)
			}
			e.U16(uint16(item.ValueCount))
			e.Chain(item.Values)
		}
		c.WriteTypedParams(e, req, true)
	default:
		if len(req.PreparedID) > 0 {
			e.Bytes(req.PreparedID, true)
			if proto >= 5 || c.MetadataID {
				e.Bytes(req.ResultMetadataID, true)
			}
		} else {
			e.Str(req.Statement, true)
		}
		c.WriteTypedParams(e, req, false)
		if op == frm.OpExecute && c.TabletsRoutingV2 {
			e.U8(req.TabletVersionBlock)
		}
	}
	var frame wire.Chain
	var err error
	if inlineHeader {
		if e.err != nil {
			return wire.Chain{}, e.err
		}
		size := e.b.Len() - HeadSize
		if size > c.MaxFrameBytes {
			return wire.Chain{}, ErrFrameTooBig
		}
		e.check(e.b.PatchUint32(bodyLength, uint32(size)))
		frame, err = e.Freeze()
	} else {
		body, freezeErr := e.Freeze()
		if freezeErr != nil {
			return wire.Chain{}, freezeErr
		}
		if body.Len() > c.MaxFrameBytes {
			return wire.Chain{}, ErrFrameTooBig
		}
		var out []byte
		switch c.Compression {
		case "lz4":
			out = make([]byte, 4)
			binary.BigEndian.PutUint32(out, uint32(body.Len()))
			out, err = wirecompress.AppendLZ4(out, body)
		case "snappy":
			out, err = wirecompress.AppendSnappy(nil, body)
		default:
			err = fmt.Errorf("gocql: span encoder unavailable for compressor %q", c.Compression)
		}
		if err != nil {
			return wire.Chain{}, err
		}
		body, _ = wire.NewChain(wire.Borrow(out))
		Flags |= frm.FlagCompress
		if body.Len() > c.MaxFrameBytes {
			return wire.Chain{}, ErrFrameTooBig
		}
		header := NewTypedEnvelope()
		header.U8(proto)
		header.U8(Flags)
		header.U16(uint16(stream))
		header.U8(byte(op))
		header.U32(uint32(body.Len()))
		header.Chain(body)
		frame, err = header.Freeze()
	}
	if err != nil || proto < 5 {
		return frame, err
	}
	if c.SegmentCompression && c.Compression != "lz4" {
		return wire.Chain{}, fmt.Errorf("gocql: v5 spans require LZ4 segment Compression")
	}
	var spans []wire.Span
	selfContained := frame.Len() <= segment.MaxPayloadSize
	for offset := int64(0); offset < frame.Len(); {
		size := min(int64(segment.MaxPayloadSize), frame.Len()-offset)
		part, _ := frame.Slice(offset, size)
		encoded, err := segment.EncodeSpans(part, selfContained, c.SegmentCompression)
		if err != nil {
			return wire.Chain{}, err
		}
		_ = encoded.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
		offset += size
	}
	return wire.NewChain(spans...)
}
func (c Settings) WriteTypedParams(e *TypedEnvelope, r *model.AttemptRequest, batch bool) {
	e.U16(uint16(r.Consistency))
	var Flags uint32
	if !batch {
		if r.ValueCount > 0 {
			Flags |= frm.FlagValues
		}
		if r.SkipMetadata {
			Flags |= frm.FlagSkipMetaData
		}
		if r.PageSize > 0 {
			Flags |= frm.FlagPageSize
		}
		if r.PagingState != nil {
			Flags |= frm.FlagWithPagingState
		}
	}
	if r.SerialConsistency != 0 {
		Flags |= frm.FlagWithSerialConsistency
	}
	if r.DefaultTimestamp {
		Flags |= frm.FlagDefaultTimestamp
	}
	if r.Keyspace != "" {
		Flags |= frm.FlagWithKeyspace
	}
	if r.NowInSeconds != nil {
		Flags |= frm.FlagWithNowInSeconds
	}
	if c.Protocol >= 5 {
		e.U32(Flags)
	} else {
		e.U8(byte(Flags))
	}
	if !batch {
		if r.ValueCount > 0 {
			e.U16(uint16(r.ValueCount))
			e.Chain(r.Values)
		}
		if r.PageSize > 0 {
			e.U32(uint32(r.PageSize))
		}
		if r.PagingState != nil {
			e.Bytes(r.PagingState, false)
		}
	}
	if r.SerialConsistency != 0 {
		e.U16(uint16(r.SerialConsistency))
	}
	if r.DefaultTimestamp {
		e.U64(uint64(r.Timestamp))
	}
	if r.Keyspace != "" {
		e.Str(r.Keyspace, false)
	}
	if r.NowInSeconds != nil {
		e.U32(uint32(*r.NowInSeconds))
	}
}

// Settings snapshots only negotiated, immutable wire settings.
type Settings struct {
	Protocol                            byte
	Flags                               byte
	MetadataID, TabletsRoutingV2        bool
	MaxFrameBytes, MaxDecompressedBytes int64
	MaxMetadataDepth                    int
	FlagLWT, RateLimitingErrorCode      int
	Compression                         string
	SegmentCompression                  bool
}
