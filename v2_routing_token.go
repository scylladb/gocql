package gocql

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/scylladb/gocql/v2/internal/murmur"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// routingToken hashes precisely the encoded components later sent to the server.
// It never invokes an application encoder or materializes a composite key.
func routingToken(cells []encodedCell, indexes []int) (Option[int64], error) {
	if len(indexes) == 0 {
		return Option[int64]{}, nil
	}
	missing := false
	for _, index := range indexes {
		if index < 0 || index >= len(cells) {
			return Option[int64]{}, fmt.Errorf("gocql: partition key index %d outside %d bound values", index, len(cells))
		}
		cell := cells[index]
		if cell.State == Null || cell.State == Unset {
			missing = true
		}
		if len(indexes) > 1 && cell.Body.Len() > math.MaxUint16 {
			return Option[int64]{}, fmt.Errorf("gocql: composite partition component %d exceeds 65535 bytes", index)
		}
	}
	if missing {
		return Option[int64]{}, nil
	}
	var hash murmur.Stream
	var prefix [2]byte
	var zero [1]byte
	for _, index := range indexes {
		cell := cells[index]
		if len(indexes) > 1 {
			binary.BigEndian.PutUint16(prefix[:], uint16(cell.Body.Len()))
			hash.Write(prefix[:])
		}
		if err := cell.Body.Each(func(span wire.Span) error { _, err := hash.Write(span.Bytes()); return err }); err != nil {
			return Option[int64]{}, err
		}
		if len(indexes) > 1 {
			hash.Write(zero[:])
		}
	}
	return Option[int64]{Value: hash.Sum64(), Set: true}, nil
}
