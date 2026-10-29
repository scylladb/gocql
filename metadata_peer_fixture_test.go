//go:build integration

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// incompletePeerDialer appends an incomplete peer to real database responses.
// It exercises discovery through the transport without modifying system tables.
// This fixture uses uncompressed protocol v4 against Cassandra so its framing
// is independent of Scylla extensions and v5 transport segments.
type incompletePeerDialer struct {
	net.Dialer
	injected atomic.Int64
}

func (d *incompletePeerDialer) DialContext(ctx context.Context, target DialTarget) (net.Conn, error) {
	network, address := target.Network, target.Address
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &incompletePeerConn{
		Conn: conn, injected: &d.injected,
		requests: make(map[uint16]peerFixtureRequest), prepared: make(map[string]string),
	}, nil
}

type peerFixtureRequest struct {
	op    frm.Op
	query string
}

type incompletePeerConn struct {
	net.Conn
	injected *atomic.Int64
	writeMu  sync.Mutex
	readMu   sync.Mutex
	mu       sync.Mutex
	requests map[uint16]peerFixtureRequest
	prepared map[string]string
	written  []byte
	pending  []byte
}

func (c *incompletePeerConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.written = append(c.written, p...)
	for len(c.written) >= headSize {
		length := int(binary.BigEndian.Uint32(c.written[5:9]))
		if length > frm.MaxFrameSize {
			return 0, fmt.Errorf("peer fixture: oversized request frame")
		}
		if len(c.written) < headSize+length {
			break
		}
		header, body := c.written[:headSize], c.written[headSize:headSize+length]
		request := peerFixtureRequest{op: frm.Op(header[4])}
		c.mu.Lock()
		switch request.op {
		case frm.OpQuery, frm.OpPrepare:
			if len(body) >= 4 {
				n := int(binary.BigEndian.Uint32(body[:4]))
				if n <= len(body)-4 {
					request.query = strings.ToLower(string(body[4 : 4+n]))
				}
			}
		case frm.OpExecute:
			if len(body) >= 2 {
				n := int(binary.BigEndian.Uint16(body[:2]))
				if n <= len(body)-2 {
					request.query = c.prepared[string(body[2:2+n])]
				}
			}
		}
		if strings.Contains(request.query, "system.peers") {
			c.requests[binary.BigEndian.Uint16(header[2:4])] = request
		}
		c.mu.Unlock()
		c.written = c.written[headSize+length:]
	}
	return c.Conn.Write(p)
}

func (c *incompletePeerConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if len(c.pending) == 0 {
		var header [headSize]byte
		if _, err := io.ReadFull(c.Conn, header[:]); err != nil {
			return 0, err
		}
		length := int(binary.BigEndian.Uint32(header[5:9]))
		if length > frm.MaxFrameSize {
			return 0, fmt.Errorf("peer fixture: oversized response frame")
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(c.Conn, body); err != nil {
			return 0, err
		}
		stream := binary.BigEndian.Uint16(header[2:4])
		c.mu.Lock()
		request, tracked := c.requests[stream]
		delete(c.requests, stream)
		c.mu.Unlock()
		if tracked && frm.Op(header[4]) == frm.OpResult {
			reader := newFramer(nil, protoVersion4)
			reader.buf = body
			reader.header = &frm.FrameHeader{
				Version: frm.ProtoVersion(header[0]), Flags: header[1],
				Op: frm.OpResult, Stream: int(stream), Length: length,
			}
			response, err := reader.parseFrame()
			if err != nil {
				return 0, fmt.Errorf("peer fixture: decode response: %w", err)
			}
			switch response := response.(type) {
			case *resultPreparedFrame:
				c.mu.Lock()
				c.prepared[string(response.preparedID)] = request.query
				c.mu.Unlock()
			case *resultRowsFrame:
				if response.meta.noMetaData() || len(response.meta.columns) == 0 {
					return 0, fmt.Errorf("peer fixture requires result metadata")
				}
				countOffset := len(body) - len(reader.buf) - 4
				binary.BigEndian.PutUint32(body[countOffset:countOffset+4], uint32(response.numRows+1))
				peerID, err := ParseUUID("35c0ec48-5109-40fd-9281-9e9d4add2f1e")
				if err != nil {
					return 0, err
				}
				for _, column := range response.meta.columns {
					var value any
					switch column.Name {
					case "peer", "rpc_address", "native_address":
						value = net.IPv4(127, 0, 21, 254)
					case "host_id":
						value = peerID
					case "data_center":
						value = "datacenter1"
					}
					encoded, err := Marshal(column.TypeInfo, value)
					if err != nil {
						return 0, fmt.Errorf("peer fixture column %s: %w", column.Name, err)
					}
					body = appendBytes(body, encoded)
				}
				c.injected.Add(1)
			}
		}
		binary.BigEndian.PutUint32(header[5:9], uint32(len(body)))
		c.pending = append(header[:], body...)
	}
	n := copy(p, c.pending)
	c.pending = c.pending[n:]
	return n, nil
}

func (c *incompletePeerConn) SetNoDelay(v bool) error   { return c.Conn.(*net.TCPConn).SetNoDelay(v) }
func (c *incompletePeerConn) SetKeepAlive(v bool) error { return c.Conn.(*net.TCPConn).SetKeepAlive(v) }
func (c *incompletePeerConn) SetKeepAlivePeriod(v time.Duration) error {
	return c.Conn.(*net.TCPConn).SetKeepAlivePeriod(v)
}
func (c *incompletePeerConn) SetReadBuffer(v int) error {
	return c.Conn.(*net.TCPConn).SetReadBuffer(v)
}
func (c *incompletePeerConn) SetWriteBuffer(v int) error {
	return c.Conn.(*net.TCPConn).SetWriteBuffer(v)
}
func (c *incompletePeerConn) SetLinger(v int) error { return c.Conn.(*net.TCPConn).SetLinger(v) }
