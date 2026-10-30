package gocql

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/lz4"
	"github.com/scylladb/gocql/v2/internal/wire"
)

var typedTransportBenchmarkSink wire.Chain

// The pointer assertions in TestTypedRequestMatchesLegacyWireAndKeepsPayload
// establish the copy boundary. These counters measure envelope/descriptor work;
// compression output is a separate representation change, not payload gathering.
func BenchmarkTypedBuildRequest(b *testing.B) {
	for _, size := range []int{0, 32, 4096, 500 * 1024} {
		for _, proto := range []byte{4, 5} {
			for _, compressed := range []bool{false, true} {
				b.Run(fmt.Sprintf("v%d/%d/compressed=%t", proto, size, compressed), func(b *testing.B) {
					payload := bytes.Repeat([]byte{'v'}, size)
					values, _ := wire.NewChain(wire.Borrow(binary.BigEndian.AppendUint32(nil, uint32(size))), wire.Borrow(payload))
					c := &driverConn{InternalVersion: proto}
					if compressed {
						c.InternalCompressor = lz4.LZ4Compressor{}
						if proto == 5 {
							c.InternalSegCompressor = lz4.LZ4Compressor{}
						}
					}
					c.InternalInitFramerCache()
					req := &attemptRequest{Statement: "INSERT INTO tbl (v) VALUES (?)", Values: values, ValueCount: 1, Consistency: One}
					b.SetBytes(int64(size))
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						out, err := c.InternalBuildTypedRequest(req, 3)
						if err != nil {
							b.Fatal(err)
						}
						typedTransportBenchmarkSink = out
					}
					b.StopTimer()
					b.ReportMetric(float64(typedTransportBenchmarkSink.Len()), "wire-bytes/op")
				})
			}
		}
	}
}
