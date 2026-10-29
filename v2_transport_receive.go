package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/segment"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// chainReader is used only for small frame headers and the migration adapter
// serving legacy callers. Typed response payloads pass through checked cursors.
type chainReader struct{ cursor *wire.Cursor }

func newChainReader(c wire.Chain) *chainReader { return &chainReader{cursor: wire.NewCursor(c)} }
func (r *chainReader) Read(p []byte) (int, error) {
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

func (c *Conn) receiveTypedCall(ctx context.Context, call *callReq, src frameSource, head frm.FrameHeader) error {
	var owner *wire.Lease
	var body wire.Chain
	var err error
	fatal := false
	limits := c.typedProtocolLimits()
	if int64(head.Length) > limits.MaxFrameBytes {
		err = ErrFrameTooBig
		if src.owner == nil {
			if discardErr := c.discardFrame(src.r, head); discardErr != nil {
				err = discardErr
				fatal = true
			}
		}
	} else if src.owner != nil {
		if !src.owner.Retain() {
			err = ErrReleased
		} else {
			owner = src.owner
			body = src.chain
		}
	} else {
		data := make([]byte, head.Length)
		if _, err = io.ReadFull(src.r, data); err != nil {
			fatal = true
		} else {
			if c.version < 5 && head.Flags&frm.FlagCompress != 0 {
				if c.compressor == nil {
					err = fmt.Errorf("gocql: compressed frame without compressor")
				} else {
					var size uint64
					switch c.compressor.Name() {
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
						err = fmt.Errorf("gocql: unsupported typed compressor %q", c.compressor.Name())
					}
					if err == nil && size > uint64(limits.MaxDecompressedBytes) {
						err = ErrFrameTooBig
					}
					if err == nil {
						data, err = c.compressor.Decode(data)
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
				owner = wire.NewLease(body, nil)
			}
		}
	}
	var page *responsePage
	if err == nil {
		if head.Version.Version() != c.version || head.Version.Request() {
			err = NewErrProtocol("unexpected response version %d", head.Version)
		} else {
			page, err = c.parseTypedResponse(head, body, owner)
		}
	}
	if err != nil && owner != nil && page == nil {
		owner.Release()
		owner = nil
	}
	select {
	case call.resp <- callResp{page: page, err: err, removedFromCalls: true}:
	case <-call.timeout:
		if page != nil {
			page.owner.Release()
		}
		c.releaseStream(call)
		call.waitExecDone("receiveTypedCall")
		putCallReq(call)
	case <-ctx.Done():
		if page != nil {
			page.owner.Release()
		}
		c.releaseStream(call)
		call.waitExecDone("receiveTypedCall")
		putCallReq(call)
	}
	if fatal {
		return err
	}
	return nil
}

func (c *Conn) recvLeasedSegment(ctx context.Context) error {
	start := c.observedNow()
	hdr, err := c.readFirstSegmentHeader()
	if err != nil {
		return err
	}
	first, err := c.readTypedSegmentPayload(hdr)
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
		nextHdr, err := segment.ReadHeader(c.r, c.segCompressor != nil)
		if err != nil {
			return fmt.Errorf("gocql: failed to read continuation segment header: %w", err)
		}
		if nextHdr.IsSelfContained {
			return fmt.Errorf("gocql: received self-contained segment, but expected a continuation")
		}
		next, err := c.readTypedSegmentPayload(nextHdr)
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
	expected := int64(head.Length + headSize)
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
	// A page owner groups all segment leases. It owns exactly one share in each;
	// the receive-loop shares below are released only after dispatch completes.
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
	return c.processFrameSource(ctx, frameSource{r: newChainReader(chain), owner: owner, chain: body, netStart: start, netEnd: end})
}

func (c *Conn) readTypedSegmentPayload(hdr segment.Header) (*wire.Lease, error) {
	if int64(hdr.UncompressedLen) > c.typedProtocolLimits().MaxDecompressedBytes {
		return nil, ErrFrameTooBig
	}
	return segment.ReadOwnedPayload(c.r, hdr, c.segCompressor)
}
