package segment

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"testing"

	lz4 "github.com/pierrec/lz4/v4"
	"github.com/scylladb/gocql/v2/internal/wire"
)

type independentLZ4 struct{}

func (independentLZ4) AppendCompressed(dst, src []byte) ([]byte, error) { panic("unused") }
func (independentLZ4) AppendDecompressed(dst, src []byte, n uint32) ([]byte, error) {
	start := len(dst)
	dst = append(dst, make([]byte, n)...)
	decoded, err := lz4.UncompressBlock(src, dst[start:])
	if err != nil {
		return nil, err
	}
	return dst[:start+decoded], nil
}
func flattenTest(t *testing.T, c wire.Chain) []byte {
	t.Helper()
	out := make([]byte, c.Len())
	if _, err := c.CopyTo(out); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestEncodeSpansOracleAndBacking(t *testing.T) {
	random := make([]byte, 70000)
	rand.New(rand.NewSource(62)).Read(random)
	for _, input := range [][]byte{nil, []byte("abcd"), bytes.Repeat([]byte("span-compression-pattern-"), 4000), random} {
		for _, compressed := range []bool{false, true} {
			var spans []wire.Span
			for i := 0; i < len(input); {
				n := min(len(input)-i, 1+(i%113))
				spans = append(spans, wire.Borrow(input[i:i+n]))
				i += n
			}
			source, _ := wire.NewChain(spans...)
			result, err := EncodeSpans(source, true, compressed)
			if err != nil {
				t.Fatal(err)
			}
			encoded := flattenTest(t, result)
			r := bytes.NewReader(encoded)
			header, err := ReadHeader(r, compressed)
			if err != nil {
				t.Fatal(err)
			}
			if !header.IsSelfContained {
				t.Fatal("self contained bit lost")
			}
			var comp Compressor
			if compressed {
				comp = independentLZ4{}
			}
			owner, err := ReadOwnedPayload(r, header, comp)
			if err != nil {
				t.Fatal(err)
			}
			if got := flattenTest(t, owner.Chain()); !bytes.Equal(got, input) {
				t.Fatal("independent LZ4 roundtrip mismatch")
			}
			owner.Release()
			start := HeaderSize(compressed)
			payload := encoded[start : len(encoded)-4]
			crcInput := append([]byte{0xfa, 0x2d, 0x55, 0xca}, payload...)
			if binary.LittleEndian.Uint32(encoded[len(encoded)-4:]) != crc32.ChecksumIEEE(crcInput) {
				t.Fatal("incremental CRC mismatch")
			}
			if !compressed || header.UncompressedLen == 0 {
				raw, _ := result.Slice(int64(start), source.Len())
				i := 0
				_ = raw.Each(func(s wire.Span) error {
					if &s.Bytes()[0] != &spans[i].Bytes()[0] {
						t.Fatal("raw payload copied")
					}
					i++
					return nil
				})
				if i != len(spans) {
					t.Fatal("unexpected raw descriptor count")
				}
			}
			if compressed && len(input) > 90000 && header.UncompressedLen == 0 {
				t.Fatal("compressible input stored raw")
			}
		}
	}
}
func TestReadOwnedSegmentSurvivesLaterRead(t *testing.T) {
	a, _ := wire.NewChain(wire.Borrow([]byte("first segment")))
	b, _ := wire.NewChain(wire.Borrow([]byte("second segment")))
	ac, _ := EncodeSpans(a, true, false)
	bc, _ := EncodeSpans(b, true, false)
	r := bytes.NewReader(append(flattenTest(t, ac), flattenTest(t, bc)...))
	h, _ := ReadHeader(r, false)
	first, err := ReadOwnedPayload(r, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	h, _ = ReadHeader(r, false)
	second, err := ReadOwnedPayload(r, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	second.Release()
	if got := string(flattenTest(t, first.Chain())); got != "first segment" {
		t.Fatalf("retained payload overwritten: %q", got)
	}
	first.Release()
}
