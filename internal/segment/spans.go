package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/scylladb/gocql/v2/internal/crc"
	"github.com/scylladb/gocql/v2/internal/wire"
	"github.com/scylladb/gocql/v2/internal/wirecompress"
)

// EncodeSpans wraps immutable spans in a segment without gathering the payload.
// Compression writes its final output directly; raw fallback retains the input.
func EncodeSpans(payload wire.Chain, selfContained, compressed bool) (wire.Chain, error) {
	if payload.Len() > MaxPayloadSize {
		return wire.Chain{}, fmt.Errorf("gocql: segment payload exceeds %d", MaxPayloadSize)
	}
	originalLen := payload.Len()
	uncompressedLen := int64(0)
	if compressed {
		out, err := wirecompress.AppendLZ4(nil, payload)
		if err != nil {
			return wire.Chain{}, err
		}
		if len(out) > 0 && int64(len(out)) < originalLen {
			payload, _ = wire.NewChain(wire.Borrow(out))
			uncompressedLen = originalLen
		}
	}
	headerSize := HeaderSize(compressed)
	header := make([]byte, headerSize)
	fields := headerSize - Crc24Size
	packed := uint64(payload.Len())
	if compressed {
		packed |= uint64(uncompressedLen) << 17
		if selfContained {
			packed |= 1 << 34
		}
	} else if selfContained {
		packed |= 1 << 17
	}
	for i := 0; i < fields; i++ {
		header[i] = byte(packed >> (8 * i))
	}
	checksum := crc.Crc24(header[:fields])
	for i := 0; i < 3; i++ {
		header[fields+i] = byte(checksum >> (8 * i))
	}
	sum := crc32.ChecksumIEEE([]byte{0xfa, 0x2d, 0x55, 0xca})
	spans := []wire.Span{wire.Borrow(header)}
	_ = payload.Each(func(s wire.Span) error {
		sum = crc32.Update(sum, crc32.IEEETable, s.Bytes())
		spans = append(spans, s)
		return nil
	})
	trailer := make([]byte, 4)
	binary.LittleEndian.PutUint32(trailer, sum)
	spans = append(spans, wire.Borrow(trailer))
	return wire.NewChain(spans...)
}

// ReadOwnedPayload receives directly into final immutable storage. The returned
// lease can outlive both the connection and reads of subsequent segments.
func ReadOwnedPayload(r io.Reader, h Header, comp Compressor) (*wire.Lease, error) {
	if err := validatePayloadHeader(h, comp != nil); err != nil {
		return nil, err
	}
	payload := make([]byte, h.PayloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("gocql: failed reading segment payload: %w", err)
	}
	var trailer [4]byte
	if _, err := io.ReadFull(r, trailer[:]); err != nil {
		return nil, fmt.Errorf("gocql: failed reading segment CRC: %w", err)
	}
	if crc.Crc32(payload) != binary.LittleEndian.Uint32(trailer[:]) {
		return nil, fmt.Errorf("gocql: crc32 mismatch in segment payload")
	}
	if h.UncompressedLen != 0 {
		if comp == nil {
			return nil, fmt.Errorf("gocql: compressed segment without compressor")
		}
		decoded, err := comp.AppendDecompressed(make([]byte, 0, h.UncompressedLen), payload, uint32(h.UncompressedLen))
		if err != nil {
			return nil, err
		}
		if len(decoded) != h.UncompressedLen {
			return nil, fmt.Errorf("gocql: segment decompressed to %d bytes, expected %d", len(decoded), h.UncompressedLen)
		}
		payload = decoded
	}
	chain, _ := wire.NewChain(wire.Borrow(payload))
	return wire.NewLease(chain, nil), nil
}
