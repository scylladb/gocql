package gocql

import (
	implTransport "github.com/scylladb/gocql/v2/internal/transport"
	"github.com/scylladb/gocql/v2/internal/wire"
	"io"
	"net"
	"time"
)

type chainReader = implTransport.InternalChainReader
type connReadSource = implTransport.InternalConnReadSource
type connReader = implTransport.InternalConnReader
type contextWriter = implTransport.InternalContextWriter
type deadlineContextWriter = implTransport.InternalDeadlineContextWriter
type deadlineDisarmer = implTransport.InternalDeadlineDisarmer
type deadlineWriter = implTransport.InternalDeadlineWriter

func effectiveWriteDeadline(a0 int64) time.Time {
	return implTransport.InternalEffectiveWriteDeadline(a0)
}

var errArmReadDeadline = implTransport.InternalErrArmReadDeadline

type frameSource = implTransport.InternalFrameSource
type headerReader = implTransport.InternalHeaderReader

const maxReadAttempts = implTransport.InternalMaxReadAttempts

func newChainReader(a0 wire.Chain) *chainReader { return implTransport.InternalNewChainReader(a0) }
func newWriteCoalescer(a0 deadlineWriter, a1 time.Duration, a2 time.Duration, a3 <-chan struct{}) *writeCoalescer {
	return implTransport.InternalNewWriteCoalescer(a0, a1, a2, a3)
}

type spanContextWriter = implTransport.InternalSpanContextWriter
type writeCoalescer = implTransport.InternalWriteCoalescer
type writeRequest = implTransport.InternalWriteRequest
type writeResult = implTransport.InternalWriteResult

var writeResultChanPool = &implTransport.InternalWriteResultChanPool

func writeSpanBuffers(a0 io.Writer, a1 net.Buffers) (int64, error) {
	return implTransport.InternalWriteSpanBuffers(a0, a1)
}
