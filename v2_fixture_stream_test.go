package gocql

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	lz4 "github.com/scylladb/gocql/lz4"
	"github.com/scylladb/gocql/v2/internal/testkit"
)

type step08Mode struct {
	name        string
	version     byte
	compression Compressor
	tls         bool
}

var step08Modes = []step08Mode{{"v3", 3, nil, false}, {"v4", 4, nil, false}, {"v4tls", 4, nil, true}, {"v5raw", 5, nil, false}, {"v5lz4", 5, lz4.LZ4Compressor{}, false}}

func step08StreamSession(t testing.TB, mode step08Mode, spy ...bool) (*Session, *driverConn, net.Conn) {
	t.Helper()
	s, _, _ := runtimeTestSession(t, 0)
	c, peer := typedTestConn(t, mode.version, mode.compression)
	if mode.tls {
		cert, config := step04TLS(t)
		rawClient := c.InternalR.(*connReader).InternalConn
		client := tls.Client(rawClient, config)
		server := tls.Server(peer, &tls.Config{Certificates: []tls.Certificate{cert}})
		done := make(chan error, 1)
		go func() { done <- server.Handshake() }()
		if err := client.Handshake(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		clientConn := &step08TLSConn{Conn: client, raw: rawClient}
		c.InternalR.(*connReader).InternalConn = clientConn
		c.InternalW = &deadlineContextWriter{InternalW: clientConn, InternalSemaphore: make(chan struct{}, 1), InternalQuit: make(chan struct{})}
		peer = &step08TLSConn{Conn: server, raw: peer}
	}
	if len(spy) > 0 && spy[0] {
		c.InternalR.(*connReader).InternalConn = &step08ReadTargets{Conn: c.InternalR.(*connReader).InternalConn}
	}
	c.InternalHost = func() *HostInfo {
		opaqueStorageValue := new(HostInfo)
		implEndpoint.HostInfoData(opaqueStorageValue).InternalHostId = UUID{15: 1}
		implEndpoint.HostInfoData(opaqueStorageValue).InternalState = NodeUp
		return opaqueStorageValue
	}()
	c.InternalHooks = newConnectionHooks(s)
	implCluster.SessionData(s).InternalHostSource.InternalHosts[c.InternalHost.HostID()] = c.InternalHost
	picker := newDefaultConnPicker(1)
	picker.Put(c)
	implCluster.SessionData(s).InternalPool.InternalHostConnPools[implEndpoint.InternalHostInfoHostUUID(c.InternalHost)] = &hostConnPool{InternalHost: c.InternalHost, InternalConnPicker: picker, InternalRepairCtx: c.InternalCtx}
	t.Cleanup(func() { peer.Close(); s.Close() })
	go c.InternalServe(c.InternalCtx)
	return s, c, peer
}

func step08Packet(mode step08Mode, frame []byte) []byte {
	if mode.version < 5 {
		return frame
	}
	var packet []byte
	for offset := 0; offset < len(frame); {
		end := min(offset+130000, len(frame))
		packet = append(packet, testkit.Unit(frame[offset:end], mode.compression != nil, len(frame) <= 130000)...)
		offset = end
	}
	return packet
}

func TestStep08StreamIndependentEverySplitAndTruncation(t *testing.T) {
	for _, mode := range step08Modes {
		t.Run(mode.name, func(t *testing.T) {
			sample := step08Packet(mode, testkit.BlobRows(mode.version, 0, []byte("first"), []byte("last")))
			for _, truncate := range []bool{false, true} {
				t.Run(fmt.Sprintf("truncate=%v", truncate), func(t *testing.T) {
					for split := 0; split < len(sample); split++ {
						s, c, peer := step08StreamSession(t, mode)
						done := make(chan struct{})
						finish := make(chan struct{})
						var finishOnce sync.Once
						finishRead := func() { finishOnce.Do(func() { close(finish) }) }
						t.Cleanup(finishRead)
						go func() {
							defer close(done)
							stream := typedTestReadRequest(t, c, peer)
							packet := step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("last")))
							_, _ = peer.Write(packet[:split])
							if !truncate {
								_, _ = peer.Write(packet[split:])
								// net.Pipe rejects deadline setters after peer close,
								// unlike a TCP socket with a received FIN. Keep complete
								// TLS records readable until the consumer acknowledges.
								<-finish
							}
							peer.Close()
						}()
						b := runtimeQuery(t)
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
						if err == nil {
							dst := make([]byte, 0, 10)
							var rows [][]byte
							for cursor.NextInto(&dst) {
								rows = append(rows, append([]byte(nil), dst...))
							}
							err = cursor.Err()
							cursor.Close()
							if !truncate && (len(rows) != 2 || string(rows[0]) != "first" || string(rows[1]) != "last") {
								t.Fatalf("split %d rows %q error %v", split, rows, err)
							}
						}
						if truncate && err == nil {
							t.Fatalf("accepted truncated prefix %d/%d", split, len(sample))
						}
						if !truncate && err != nil {
							t.Fatalf("split %d: %v", split, err)
						}
						finishRead()
						cancel()
						b.Close()
						peer.Close()
						s.Close()
						<-done
					}
				})
			}
		})
	}
}

func TestStep08StreamIndependentFirstRowGate(t *testing.T) {
	for _, mode := range step08Modes {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := step08StreamSession(t, mode)
			release := make(chan struct{})
			done := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				frame := testkit.BlobRows(mode.version, uint16(stream), []byte("first"), []byte("second"))
				cut := len(frame) - 10
				first, last := frame[:cut], frame[cut:]
				if mode.version == 5 {
					first = testkit.Unit(first, mode.compression != nil, false)
					last = testkit.Unit(last, mode.compression != nil, false)
				}
				_, _ = peer.Write(first)
				<-release
				_, _ = peer.Write(last)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, 12)
			if !cursor.NextInto(&dst) || string(dst) != "first" {
				t.Fatal("first row waited beyond gate", cursor.Err())
			}
			close(release)
			if !cursor.NextInto(&dst) || string(dst) != "second" {
				t.Fatal(cursor.Err())
			}
			if cursor.NextInto(&dst) || cursor.Err() != nil {
				t.Fatal(cursor.Err())
			}
			cursor.Close()
			<-done
		})
	}
}

func TestStep08StreamIndependentCorruptChecksums(t *testing.T) {
	for _, mode := range step08Modes[3:] {
		t.Run(mode.name, func(t *testing.T) {
			// Corrupt every byte of each header and payload checksum, independently.
			headerSize := 6
			if mode.compression != nil {
				headerSize = 8
			}
			for _, checksum := range []string{"header", "payload"} {
				for i := 0; i < map[string]int{"header": 3, "payload": 4}[checksum]; i++ {
					s, c, peer := step08StreamSession(t, mode)
					done := make(chan struct{})
					finish := make(chan struct{})
					var finishOnce sync.Once
					finishRead := func() { finishOnce.Do(func() { close(finish) }) }
					t.Cleanup(finishRead)
					go func() {
						defer close(done)
						stream := typedTestReadRequest(t, c, peer)
						packet := step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("forbidden")))
						offset := headerSize - 3 + i
						if checksum == "payload" {
							offset = len(packet) - 4 + i
						}
						packet[offset] ^= 0x80
						_, _ = peer.Write(packet)
						peer.Close()
					}()
					b := runtimeQuery(t)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{})
					if err == nil {
						dst := make([]byte, 0, 20)
						if cursor.NextInto(&dst) {
							t.Fatal("corrupt unit delivered caller output")
						}
						err = cursor.Err()
						cursor.Close()
					}
					if err == nil {
						t.Fatal("corrupt checksum accepted")
					}
					finishRead()
					cancel()
					b.Close()
					peer.Close()
					s.Close()
					<-done
				}
			}
		})
	}
}

type step08FinalAccess struct {
	entered, leave chan struct{}
	writes         atomic.Int32
}

func (d *step08FinalAccess) Compile(Columns) (StreamRowPlan[[]byte], error) { return d, nil }
func (d *step08FinalAccess) Reset(dst *[]byte)                              { *dst = (*dst)[:0] }
func (d *step08FinalAccess) DecodeInto(r *StreamRowReader, dst *[]byte) error {
	plan, err := StreamBlob.Compile(codecTestMust(ScalarType(TypeBlob)))
	if err != nil {
		return err
	}
	if err = ReadStreamColumn(r, plan, dst); err != nil {
		return err
	}
	close(d.entered)
	<-d.leave
	(*dst)[0] = 'F'
	d.writes.Add(1)
	return nil
}
func TestStep08StreamCloseDuringFinalCallerAccess(t *testing.T) {
	for _, mode := range step08Modes {
		for _, action := range []string{"close", "cancel", "session"} {
			t.Run(mode.name+"-"+action, func(t *testing.T) {
				s, c, peer := step08StreamSession(t, mode)
				go func() {
					stream := typedTestReadRequest(t, c, peer)
					_, _ = peer.Write(step08Packet(mode, testkit.BlobRows(mode.version, uint16(stream), []byte("final"))))
				}()
				b := runtimeQuery(t)
				defer b.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				decoder := &step08FinalAccess{entered: make(chan struct{}), leave: make(chan struct{})}
				cursor, err := RunStream(ctx, s, b, decoder, StreamOptions{})
				if err != nil {
					t.Fatal(err)
				}
				dst := make([]byte, 0, 10)
				next := make(chan struct{})
				go func() { cursor.NextInto(&dst); close(next) }()
				<-decoder.entered
				closing := make(chan struct{})
				closed := make(chan struct{})
				go func() {
					close(closing)
					switch action {
					case "cancel":
						cancel()
						cursor.Close()
					case "session":
						s.Close()
					default:
						cursor.Close()
					}
					close(closed)
				}()
				<-closing
				for i := 0; i < 100; i++ {
					runtime.Gosched()
				}
				select {
				case <-closed:
					t.Fatal("close returned during final storage access")
				default:
				}
				close(decoder.leave)
				<-closed
				<-next
				if decoder.writes.Load() != 1 || !bytes.Equal(dst, []byte("Final")) {
					t.Fatal("missing joined final write", dst)
				}
				snapshot := append([]byte(nil), dst...)
				cursor.Close()
				s.Close()
				if !bytes.Equal(snapshot, dst) {
					t.Fatal("late write")
				}
			})
		}
	}
}

func TestStep08LoggerConcurrentCapture(t *testing.T) {
	logger := &testLogger{}
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				logger.Print("a")
				logger.Printf("%s", "b")
				logger.Println("c")
				_ = logger.String()
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if len(logger.String()) != 8*100*4 {
		t.Fatal("lost concurrent log data")
	}
}

// A failed stream write must poison its connection and release canonical input,
// whether no byte or a request prefix reached the peer.
type step08InterruptedWriter struct {
	net.Conn
	accepted int
	calls    atomic.Int32
}

func (w *step08InterruptedWriter) Write(p []byte) (int, error) {
	w.calls.Add(1)
	return min(w.accepted, len(p)), net.ErrClosed
}
func TestStep08StreamInterruptedRequestWrite(t *testing.T) {
	for _, mode := range step08Modes {
		for _, accepted := range []int{0, 1, 8} {
			t.Run(mode.name+fmt.Sprint(accepted), func(t *testing.T) {
				s, c, peer := step08StreamSession(t, mode)
				defer peer.Close()
				writer := &step08InterruptedWriter{Conn: c.InternalR.(*connReader).InternalConn, accepted: accepted}
				c.InternalW.(*deadlineContextWriter).InternalW = writer
				b := runtimeQuery(t)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				cursor, err := RunStream(ctx, s, b, StreamOneColumn(StreamBlob), StreamOptions{Options: Options{Retry: SafeRetryPolicy{MaxAttempts: 1}}})
				if cursor != nil {
					cursor.Close()
				}
				if err == nil {
					t.Fatal("interrupted write succeeded")
				}
				b.Close()
				if err := b.Wait(ctx); err != nil {
					t.Fatal("write failure retained input", err)
				}
				if !c.Closed() || writer.calls.Load() != 1 {
					t.Fatal("failed request connection not poisoned", writer.calls.Load())
				}
			})
		}
	}
}

func TestStep08IndependentRawDirectRead(t *testing.T) {
	for _, mode := range step08Modes[:3] {
		t.Run(mode.name, func(t *testing.T) {
			s, c, peer := step08StreamSession(t, mode, true)
			payload := bytes.Repeat([]byte{0x71}, 400<<10)
			done := make(chan struct{})
			go func() {
				defer close(done)
				stream := typedTestReadRequest(t, c, peer)
				_, _ = peer.Write(testkit.BlobRows(mode.version, uint16(stream), payload))
			}()
			b := runtimeQuery(t)
			defer b.Close()
			cursor, err := RunStream(context.Background(), s, b, StreamOneColumn(StreamBlob), StreamOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dst := make([]byte, 0, len(payload))
			backing := &dst[:cap(dst)][0]
			if !cursor.NextInto(&dst) || !bytes.Equal(dst, payload) || &dst[0] != backing {
				t.Fatal("lost final storage", cursor.Err())
			}
			spy := c.InternalR.(*connReader).InternalConn.(*step08ReadTargets)
			spy.mu.Lock()
			direct := 0
			otherBulk := 0
			start := uintptr(unsafe.Pointer(backing))
			end := start + uintptr(len(dst))
			for _, target := range spy.targets {
				if target.n == 0 {
					continue
				}
				address := uintptr(unsafe.Pointer(&target.p[0]))
				if address >= start && address+uintptr(target.n) <= end {
					direct += target.n
				} else if target.n >= 4096 {
					otherBulk += target.n
				}
			}
			spy.mu.Unlock()
			if direct != len(payload) || otherBulk != 0 {
				t.Fatal("unexpected raw payload staging", direct, otherBulk)
			}
			cursor.NextInto(&dst)
			cursor.Close()
			<-done
		})
	}
}

// Fault fixtures interrupt TLS through the socket, including blocked close alerts.
type step08TLSConn struct {
	*tls.Conn
	raw net.Conn
}

func (c *step08TLSConn) Close() error { return c.raw.Close() }

type step08ReadTargets struct {
	net.Conn
	mu      sync.Mutex
	targets []struct {
		p []byte
		n int
	}
}

func (c *step08ReadTargets) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.mu.Lock()
	c.targets = append(c.targets, struct {
		p []byte
		n int
	}{p, n})
	c.mu.Unlock()
	return n, err
}
