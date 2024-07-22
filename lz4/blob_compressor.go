package lz4

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4/v4"
)

type BlobCompressor struct {
	prefix                    []byte
	prefixPlusLen             int
	lowerSizeLimit            int
	defaultBlobRateEvaluator  RateEvaluator
	defaultASCIIRateEvaluator RateEvaluator
}

type Option func(*BlobCompressor) error

func CompressorSizeLimit(limit int) Option {
	return func(c *BlobCompressor) error {
		c.lowerSizeLimit = limit
		return nil
	}
}

func CompressedPrefix(prefix []byte) Option {
	return func(c *BlobCompressor) error {
		if len(prefix) == 0 {
			return fmt.Errorf("prefix should not be empty")
		}
		c.prefix = prefix
		c.prefixPlusLen = len(prefix)
		return nil
	}
}

func DefaultASCIIRateEvaluator(re RateEvaluator) Option {
	return func(c *BlobCompressor) error {
		c.defaultASCIIRateEvaluator = re
		return nil
	}
}

func DefaultBlobRateEvaluator(re RateEvaluator) Option {
	return func(c *BlobCompressor) error {
		c.defaultBlobRateEvaluator = re
		return nil
	}
}

func NewBlobCompressor(prefix []byte, options ...Option) (*BlobCompressor, error) {
	if len(prefix) == 0 {
		prefix = []byte("lz4:")
	}
	res := &BlobCompressor{
		prefix:         prefix,
		prefixPlusLen:  len(prefix) + 4,
		lowerSizeLimit: 1024,
	}

	err := res.ApplyOptions(options...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func NewBlobCompressorMust(prefix []byte, options ...Option) *BlobCompressor {
	bc, err := NewBlobCompressor(prefix, options...)
	if err != nil {
		panic(err)
	}
	return bc
}

func (c *BlobCompressor) ApplyOptions(opts ...Option) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}
	return nil
}

func (c *BlobCompressor) CompressBinary(data []byte) ([]byte, error) {
	if len(data) < c.lowerSizeLimit {
		return data, nil
	}

	buf := make([]byte, c.prefixPlusLen+lz4.CompressBlockBound(len(data)))
	copy(buf, c.prefix)

	var compressor lz4.Compressor

	n, err := compressor.CompressBlock(data, buf[c.prefixPlusLen:])
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf[len(c.prefix):], uint32(len(data)))

	return buf[:c.prefixPlusLen+n], nil
}

func (c *BlobCompressor) DecompressBinary(data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, c.prefix) {
		return data, nil
	}
	uncompressedLength := binary.BigEndian.Uint32(data[len(c.prefix):])
	if uncompressedLength == 0 {
		return nil, nil
	}
	buf := make([]byte, uncompressedLength)
	n, err := lz4.UncompressBlock(data[c.prefixPlusLen:], buf)
	return buf[:n], err
}

// CompressASCII compresses the given string data into a ascii-compatible byte slice.
func (c *BlobCompressor) CompressASCII(data string) ([]byte, error) {
	if len(data) < c.lowerSizeLimit {
		return []byte(data), nil
	}
	b, err := c.CompressBinary([]byte(data))
	return convertBinToASCII(b), err
}

// DecompressASCII decompresses the given ascii-compatible byte slice to a string.
func (c *BlobCompressor) DecompressASCII(data []byte) (string, error) {
	b := convertASCIIToBin(data[:1+len(c.prefix)*8/7])
	if !bytes.HasPrefix(b, c.prefix) {
		return string(data), nil
	}
	b, err := c.DecompressBinary(convertASCIIToBin(data))
	return string(b), err
}

func (c *BlobCompressor) IsDataCompressed(i interface{}) bool {
	switch data := i.(type) {
	case string:
		b := convertASCIIToBin([]byte(data[:1+len(c.prefix)*8/7]))
		return bytes.HasPrefix(b, c.prefix)
	case []byte:
		return bytes.HasPrefix(data, c.prefix)
	default:
		return false
	}
}

func (c *BlobCompressor) Blob(val []byte) *CompressedBlob {
	return &CompressedBlob{
		c:           c,
		value:       val,
		rationStats: c.defaultBlobRateEvaluator,
	}
}

func (c *BlobCompressor) String(val string) *CompressedString {
	return &CompressedString{
		c:           c,
		value:       val,
		rationStats: c.defaultBlobRateEvaluator,
	}
}
