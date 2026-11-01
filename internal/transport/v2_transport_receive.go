// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"context"
	"encoding/binary"
	"fmt"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
	"io"
	"time"
)

func (c *Conn) receiveTypedCall(ctx context.Context, call *InternalCallReq, src frameSource, head frm.FrameHeader) error {
	var owner *wire.Lease
	var body wire.Chain
	var err error
	fatal := false
	limits := c.typedProtocolLimits()
	if int64(head.Length) > limits.MaxFrameBytes {
		err = ErrFrameTooBig
		if src.InternalOwner == nil {
			if discardErr := c.discardFrame(src.InternalR, head); discardErr != nil {
				err = discardErr
				fatal = true
			}
		}
	} else if src.InternalOwner != nil {
		if !src.InternalOwner.Retain() {
			err = ErrReleased
		} else {
			owner = src.InternalOwner
			body = src.InternalChain
		}
	} else {
		var cache *wire.ReceiveCache
		if c.InternalCfg != nil {
			cache = c.InternalCfg.InternalReceiveCache
		}
		// An unextended custom decoder may alias its input. Leave that original
		// ownership unpooled; built-ins support checked final-arena decoding.
		decoder, pooledDecode := wirecompress.FrameDecoderWithLimit(c.InternalCompressor, limits.MaxDecompressedBytes)
		if head.Flags&frm.FlagCompress != 0 && !pooledDecode {
			cache = nil
		}
		data, epoch := cache.Acquire(head.Length)
		cacheInput := data
		returned := false
		var output []byte
		var outputEpoch uint64
		outputReturned := false
		defer func() {
			if output != nil && !outputReturned {
				cache.Release(output, outputEpoch)
			}
			if !returned {
				cache.Release(cacheInput, epoch)
			}
		}()
		if _, err = io.ReadFull(src.InternalR, data); err != nil {
			fatal = true
		} else {
			if c.InternalVersion < 5 && head.Flags&frm.FlagCompress != 0 {
				if c.InternalCompressor == nil {
					err = fmt.Errorf("gocql: compressed frame without compressor")
				} else {
					var size uint64
					switch c.InternalCompressor.Name() {
					case "lz4":
						if len(data) < 4 {
							err = io.ErrUnexpectedEOF
						} else {
							size = uint64(binary.BigEndian.Uint32(data[:4]))
						}
					case "snappy":
						var n int
						size, n = binary.Uvarint(data)
						if n <= 0 {
							err = fmt.Errorf("gocql: invalid Snappy length")
						}
					default:
						err = fmt.Errorf("gocql: unsupported typed compressor %q", c.InternalCompressor.Name())
					}
					if err == nil && size > uint64(limits.MaxDecompressedBytes) {
						err = ErrFrameTooBig
					}
					if err == nil {
						if pooledDecode {
							checkedSize, checkErr := decoder.DecodedLen(data)
							if checkErr != nil {
								err = checkErr
							} else if uint64(checkedSize) != size {
								err = fmt.Errorf("gocql: decoded frame length mismatch")
							} else {
								output, outputEpoch = cache.Acquire(checkedSize)
								data, err = decoder.DecodeTo(output[:0], data)
							}
							if err == nil && len(data) > 0 && (len(output) == 0 || &data[0] != &output[0]) {
								err = segment.ErrDecoderStorage
							}
						} else {
							data, err = c.InternalCompressor.Decode(data)
						}
						if err == nil && uint64(len(data)) != size {
							err = fmt.Errorf("gocql: decoded frame length mismatch")
						}
					}
				}
			}
			if err == nil && int64(len(data)) > limits.MaxDecompressedBytes {
				err = ErrFrameTooBig
			}
			if err == nil {
				body, _ = wire.NewChain(wire.Borrow(data))
				if c.InternalVersion < 5 && head.Flags&frm.FlagCompress == 0 {
					returned = true
					owner = wire.NewLease(body, func() { cache.Release(data, epoch) })
				} else if pooledDecode && head.Flags&frm.FlagCompress != 0 {
					outputReturned = true
					owner = wire.NewLease(body, func() { cache.Release(output, outputEpoch) })
				} else {
					owner = wire.NewLease(body, nil)
				}
			}
		}
	}
	var page *InternalResponsePage
	if err == nil {
		if headerErr := frm.CheckResponseHeader(head, c.InternalVersion, c.typedResponseExtensionFlags()); headerErr != nil {
			err = headerErr
		} else {
			page, err = c.InternalParseTypedResponse(head, body, owner)
		}
	}
	if err != nil && owner != nil && page == nil {
		owner.Release()
		owner = nil
	}
	select {
	case call.InternalResp <- InternalCallResp{InternalPage: page, InternalErr: err, removedFromCalls: true}:
	case <-call.InternalTimeout:
		if page != nil {
			page.Owner.Release()
		}
		c.releaseStream(call)
		call.waitExecDone("receiveTypedCall")
		InternalPutCallReq(call)
	case <-ctx.Done():
		if page != nil {
			page.Owner.Release()
		}
		c.releaseStream(call)
		call.waitExecDone("receiveTypedCall")
		InternalPutCallReq(call)
	}
	if fatal {
		return err
	}
	return nil
}

func (c *Conn) InternalRecvLeasedSegment(ctx context.Context) error {
	start := c.observedNow()
	hdr, err := c.InternalReadFirstSegmentHeader()
	if err != nil {
		return err
	}
	first, err := c.InternalReadTypedSegmentPayload(hdr)
	if err != nil {
		return err
	}
	end := c.observedNow()
	if hdr.IsSelfContained {
		defer first.Release()
		cursor := wire.NewCursor(first.Chain())
		for cursor.Remaining() > 0 {
			frameHeader, err := cursor.Take(headSize)
			if err != nil {
				return fmt.Errorf("gocql: incomplete frame header in self-contained segment: %w", err)
			}
			head, err := readHeader(newChainReader(frameHeader), c.headerBuf[:])
			if err != nil {
				return err
			}
			body, err := cursor.Take(int64(head.Length))
			if err != nil {
				return fmt.Errorf("gocql: frame header declares a %d byte body but only %d bytes remain in the self-contained segment", head.Length, cursor.Remaining())
			}
			if err := c.processOwnedFrame(ctx, frameHeader, body, first, start, end); err != nil {
				return err
			}
		}
		return nil
	}
	owners := []*wire.Lease{first}
	defer func() {
		for _, owner := range owners {
			owner.Release()
		}
	}()
	var spans []wire.Span
	_ = first.Chain().Each(func(s wire.Span) error { spans = append(spans, s); return nil })
	total := first.Chain().Len()
	readNext := func() error {
		nextHdr, err := segment.ReadHeader(c.InternalR, c.InternalSegCompressor != nil)
		if err != nil {
			return fmt.Errorf("gocql: failed to read continuation segment header: %w", err)
		}
		if nextHdr.IsSelfContained {
			return fmt.Errorf("gocql: received self-contained segment, but expected a continuation")
		}
		next, err := c.InternalReadTypedSegmentPayload(nextHdr)
		if err != nil {
			return fmt.Errorf("gocql: failed to read continuation segment payload: %w", err)
		}
		if next.Chain().Len() == 0 {
			next.Release()
			return fmt.Errorf("gocql: continuation segment made no progress (empty payload)")
		}
		owners = append(owners, next)
		_ = next.Chain().Each(func(s wire.Span) error { spans = append(spans, s); return nil })
		total += next.Chain().Len()
		return nil
	}
	for total < headSize {
		if err := readNext(); err != nil {
			return err
		}
		end = c.observedNow()
	}
	chain, _ := wire.NewChain(spans...)
	header, _ := chain.Slice(0, headSize)
	head, err := readHeader(newChainReader(header), c.headerBuf[:])
	if err != nil {
		return err
	}
	limits := c.typedProtocolLimits()
	if int64(head.Length) > min(limits.MaxFrameBytes, limits.MaxDecompressedBytes) {
		return fmt.Errorf("gocql: invalid frame body length in segmented frame: %d", head.Length)
	}
	expected := int64(head.Length) + headSize
	for total < expected {
		if err := readNext(); err != nil {
			return err
		}
	}
	if total != expected {
		return fmt.Errorf("gocql: segmented frame exceeds its declared length %d", expected)
	}
	chain, _ = wire.NewChain(spans...)
	body, _ := chain.Slice(headSize, int64(head.Length))

	retained := append([]*wire.Lease(nil), owners...)
	for _, owner := range retained {
		owner.Retain()
	}
	owner := wire.NewLease(chain, func() {
		for _, part := range retained {
			part.Release()
		}
	})
	defer owner.Release()
	return c.processOwnedFrame(ctx, header, body, owner, start, end)
}

func (c *Conn) processOwnedFrame(ctx context.Context, header, body wire.Chain, owner *wire.Lease, start, end time.Time) error {
	var spans []wire.Span
	_ = header.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
	_ = body.Each(func(s wire.Span) error { spans = append(spans, s); return nil })
	chain, _ := wire.NewChain(spans...)
	return c.InternalProcessFrameSource(ctx, frameSource{InternalR: newChainReader(chain), InternalOwner: owner, InternalChain: body, InternalNetStart: start, InternalNetEnd: end})
}

func (c *Conn) InternalReadTypedSegmentPayload(hdr segment.Header) (*wire.Lease, error) {
	if int64(hdr.UncompressedLen) > c.typedProtocolLimits().MaxDecompressedBytes {
		return nil, ErrFrameTooBig
	}
	return segment.ReadOwnedPayload(c.InternalR, hdr, c.InternalSegCompressor)
}

// Scylla negotiates tablet feedback even on native v3. Permit its custom payload
// envelope only for that advertised extension; ordinary v3 remains strict.
func (c *Conn) typedResponseExtensionFlags() byte {
	if c.InternalVersion == 3 && (c.InternalFramers.InternalDefaults.InternalTabletsRoutingV1 || c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2) {
		return frm.FlagCustomPayload
	}
	return 0
}

func (c *Conn) InternalParseTypedResponse(head frm.FrameHeader, body wire.Chain, owner *wire.Lease) (*InternalResponsePage, error) {
	page := new(InternalResponsePage)
	data, err := c.nativeProtocol().ParseTypedResponseInto(head, body, owner, &page.InternalResponseData)
	if data == nil {
		return nil, err
	}
	return page, err
}

// chainReader is used only for small frame headers and the migration adapter
// serving legacy callers. Typed response payloads pass through checked cursors.
type InternalChainReader struct{ cursor *wire.Cursor }

func InternalNewChainReader(c wire.Chain) *InternalChainReader {
	return &InternalChainReader{cursor: wire.NewCursor(c)}
}

func (r *InternalChainReader) Read(p []byte) (int, error) {
	if r.cursor.Remaining() == 0 {
		return 0, io.EOF
	}
	n := min(int64(len(p)), r.cursor.Remaining())
	part, err := r.cursor.Take(n)
	if err != nil {
		return 0, err
	}
	return part.CopyTo(p[:int(n)])
}
