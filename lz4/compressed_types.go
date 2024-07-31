package lz4

import (
	"github.com/gocql/gocql"
)

type Decision int

const (
	CompressionDecisionNone = Decision(iota)
	CompressionDecisionCompress
	CompressionDecisionDontCompress
)

type RateEvaluator interface {
	WorthOfCompressingRate(size int, compressed int) bool
	WorthOfCompressing(size int) Decision
}

// CompressedBlob is a compressed string type that can be used with gocql.
// Suitable for to accept data only from `blob` column type
// On other colum types, such as `ascii`, `text` and `varchar` generated data is going to hit server side utf-8/ascii validation
// and will be rejected.
// To accommodate server side validation, use CompressedString instead.
type CompressedBlob struct {
	value       []byte
	c           *BlobCompressor
	rationStats RateEvaluator
}

func (c *CompressedBlob) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	switch c.rationStats.WorthOfCompressing(len(c.value)) {
	case CompressionDecisionCompress:
		return c.c.CompressBinary(c.value)
	case CompressionDecisionDontCompress:
		return c.value, nil
	default:
		compressed, err := c.c.CompressBinary(c.value)
		if err != nil {
			return nil, err
		}
		if c.rationStats.WorthOfCompressingRate(len(c.value), len(compressed)) {
			return compressed, nil
		}
		return c.value, nil
	}
}

func (c *CompressedBlob) UnmarshalCQL(info gocql.TypeInfo, data []byte) (err error) {
	c.value, err = c.c.DecompressBinary(data)
	return err
}

func (c *CompressedBlob) Value() []byte {
	return c.value
}

func (c *CompressedBlob) SetValue(val []byte) *CompressedBlob {
	c.value = val
	return c
}

func (c *CompressedBlob) SetRatioStats(val RateEvaluator) *CompressedBlob {
	c.rationStats = val
	return c
}

// CompressedString is a compressed string type that can be used with gocql.
// Suitable for to accept data from `varchar`, `text` and `ascii` column types.
type CompressedString struct {
	value       string
	c           *BlobCompressor
	rationStats RateEvaluator
}

func (c *CompressedString) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	if c.rationStats == nil {
		return c.c.CompressASCII(c.value)
	}
	switch c.rationStats.WorthOfCompressing(len(c.value)) {
	case CompressionDecisionCompress:
		return c.c.CompressASCII(c.value)
	case CompressionDecisionDontCompress:
		return []byte(c.value), nil
	default:
		compressed, err := c.c.CompressASCII(c.value)
		if err != nil {
			return nil, err
		}
		if c.rationStats.WorthOfCompressingRate(len(c.value), len(compressed)) {
			return compressed, nil
		}
		return []byte(c.value), nil
	}
}

func (c *CompressedString) UnmarshalCQL(info gocql.TypeInfo, data []byte) (err error) {
	c.value, err = c.c.DecompressASCII(data)
	return err
}

func (c *CompressedString) Value() string {
	return c.value
}

func (c *CompressedString) SetValue(val string) {
	c.value = val
}

func (c *CompressedString) SetRatioStats(val RateEvaluator) *CompressedString {
	c.rationStats = val
	return c
}
