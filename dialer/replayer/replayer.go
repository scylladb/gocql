package replayer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"time"

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/dialer"
)

func NewReplayDialer(dir string) *ReplayDialer {
	return &ReplayDialer{
		dir: dir,
	}
}

type ReplayDialer struct {
	dir string
	net.Dialer
}

func (d *ReplayDialer) DialContext(ctx context.Context, network, addr string) (conn net.Conn, err error) {
	sourcePort := gocql.ScyllaGetSourcePort(ctx)
	return NewConnectionReplayer(path.Join(d.dir, fmt.Sprintf("%s-%d", addr, sourcePort)))
}

func NewConnectionReplayer(fname string) (net.Conn, error) {
	frames, err := loadResponseFramesFromFiles(fname+"Reads", fname+"Writes")
	if err != nil {
		return nil, err
	}
	return &ConnectionReplayer{frames: frames, frameIdsToReplay: []int{}, streamIdsToReplay: []int{}, frameIdx: 0, frameResponsePosition: 0, gotRequest: make(chan struct{}, 1)}, nil
}

type ConnectionReplayer struct {
	gotRequest            chan struct{}
	frames                []*FrameRecorded
	frameIdsToReplay      []int
	streamIdsToReplay     []int
	frameIdx              int
	frameResponsePosition int
	closed                bool
}

func (c *ConnectionReplayer) frameStreamID() int {
	return c.streamIdsToReplay[c.frameIdx]
}

func (c *ConnectionReplayer) getPendingFrame() *FrameRecorded {
	if c.frameIdx < 0 || c.frameIdx >= len(c.frameIdsToReplay) {
		return nil
	}
	frameId := c.frameIdsToReplay[c.frameIdx]
	if frameId < 0 || frameId >= len(c.frames) {
		return nil
	}
	return c.frames[frameId]
}

func (c *ConnectionReplayer) pushStreamIDToReplay(b []byte, idx int) {
	if b[0] > 0x02 {
		c.streamIdsToReplay = append(c.streamIdsToReplay, int(b[2])<<8|int(b[3]))
	} else {
		c.streamIdsToReplay = append(c.streamIdsToReplay, int(b[2]))
	}
	c.frameIdsToReplay = append(c.frameIdsToReplay, idx)

	select {
	case c.gotRequest <- struct{}{}:
	default:
	}
}

func replaceFrameStreamID(b []byte, stream int) {
	if b[0] > 0x02 {
		b[2] = byte(stream >> 8)
		b[3] = byte(stream)
	} else {
		b[2] = byte(stream)
	}
}

func (c *ConnectionReplayer) Read(b []byte) (n int, err error) {
	frame := c.getPendingFrame()
	for frame == nil {
		<-c.gotRequest
		frame = c.getPendingFrame()
	}
	if c.Closed() {
		return 0, io.EOF
	}
	response := frame.Response[c.frameResponsePosition:]

	if len(b) < len(response) {
		copy(b, response[:len(b)])
		c.frameResponsePosition = c.frameResponsePosition + len(b)
		return len(b), err
	}

	copy(b, response)
	if c.frameResponsePosition == 0 {
		replaceFrameStreamID(b, c.frameStreamID())
	}

	c.frameIdx = c.frameIdx + 1
	c.frameResponsePosition = 0
	return len(response), err
}

func (c *ConnectionReplayer) Write(b []byte) (n int, err error) {
	writeHash := dialer.GetFrameHash(b)

	for i, q := range c.frames {
		if q.Hash == writeHash {
			c.pushStreamIDToReplay(b, i)
			return len(b), nil
		}
	}
	panic(fmt.Errorf("unable to find a response to replay"))
}

func (c *ConnectionReplayer) Close() error {
	close(c.gotRequest)
	c.closed = true
	return nil
}

func (c *ConnectionReplayer) Closed() bool {
	return c.closed
}

type MockAddr struct {
	network string
	address string
}

func (m *MockAddr) Network() string {
	return m.network
}

func (m *MockAddr) String() string {
	return m.address
}

func (c ConnectionReplayer) LocalAddr() net.Addr {
	return &MockAddr{
		network: "tcp",
		address: "10.0.0.1:54321",
	}
}

func (c ConnectionReplayer) RemoteAddr() net.Addr {
	return &MockAddr{
		network: "tcp",
		address: "192.168.1.100:12345",
	}
}

func (c ConnectionReplayer) SetDeadline(t time.Time) error {
	return nil
}

func (c ConnectionReplayer) SetReadDeadline(t time.Time) error {
	return nil
}

func (c ConnectionReplayer) SetWriteDeadline(t time.Time) error {
	return nil
}

func loadFramesFromFile(filename string) (map[int]dialer.Record, error) {
	records := make(map[int]dialer.Record)

	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record dialer.Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			fmt.Printf("Error decoding JSON in %s: %s\n", filename, err)
			continue
		}
		records[record.StreamID] = record
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file %s: %w", filename, err)
	}
	return records, nil
}

func loadResponseFramesFromFiles(read_file, write_file string) ([]*FrameRecorded, error) {
	read_records, err := loadFramesFromFile(read_file)
	if err != nil {
		return nil, err
	}
	write_records, err := loadFramesFromFile(write_file)
	if err != nil {
		return nil, err
	}

	var frames = []*FrameRecorded{}
	for streamID, record1 := range read_records {
		if record2, exists := write_records[streamID]; exists {
			frames = append(frames, &FrameRecorded{Response: record1.Data, Hash: dialer.GetFrameHash(record2.Data)})
		}
	}
	return frames, nil
}

type FrameRecorded struct {
	Response []byte
	Hash     int64
}
