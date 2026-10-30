package gocql

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/scylladb/gocql/v2/internal/testkit"
)

// Fixture construction and seeded final caller storage are outside measurement.
// This measures public API setup/receive against the independent wire oracle,
// including peer scheduling. It does not measure native database throughput.
func BenchmarkStep08IndependentStreamOracle(b *testing.B) {
	for _, mode := range step08Modes {
		for _, shape := range []struct{ rows, cell int }{{1, 1024}, {32, 1024}, {256, 1024}, {1, 400 << 10}, {1, 8 << 20}} {
			b.Run(fmt.Sprintf("%s/rows=%d/cell=%d", mode.name, shape.rows, shape.cell), func(b *testing.B) {
				s, c, peer := step08StreamSession(b, mode)
				values := make([][]byte, shape.rows)
				for i := range values {
					values[i] = bytes.Repeat([]byte{byte(i)}, shape.cell)
				}
				frame := testkit.BlobRows(mode.version, 0, values...)
				// Payload is built once; only stream/checksum fields change.
				packet := step08Packet(mode, frame)
				done := make(chan struct{})
				go func() {
					defer close(done)
					for i := 0; i < b.N; i++ {
						stream := typedTestReadRequest(b, c, peer)
						testkit.BindStream(packet, mode.version, mode.compression != nil, uint16(stream))
						if _, err := peer.Write(packet); err != nil {
							b.Error(err)
							return
						}
					}
				}()
				bound := runtimeQuery(b)
				defer bound.Close()
				dst := make([]byte, 0, shape.cell)
				b.ReportAllocs()
				b.SetBytes(int64(shape.rows * shape.cell))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					cursor, err := RunStream(context.Background(), s, bound, StreamOneColumn(StreamBlob), StreamOptions{})
					if err != nil {
						b.Fatal(err)
					}
					rows := 0
					for cursor.NextInto(&dst) {
						rows++
					}
					if err := cursor.Err(); err != nil {
						b.Fatal(err)
					}
					cursor.Close()
					if rows != shape.rows {
						b.Fatal(rows)
					}
				}
				b.StopTimer()
				<-done
			})
		}
	}
}
