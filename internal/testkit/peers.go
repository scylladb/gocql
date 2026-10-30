package testkit

import (
	"encoding/binary"
	"fmt"
)

type peerCursor struct {
	data   []byte
	offset int
	err    error
}

func (p *peerCursor) take(n int) []byte {
	if n < 0 || n > len(p.data)-p.offset {
		p.err = fmt.Errorf("truncated peer fixture metadata")
		return nil
	}
	b := p.data[p.offset : p.offset+n]
	p.offset += n
	return b
}
func (p *peerCursor) u16() int {
	b := p.take(2)
	if len(b) != 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(b))
}
func (p *peerCursor) i32() int {
	b := p.take(4)
	if len(b) != 4 {
		return 0
	}
	return int(int32(binary.BigEndian.Uint32(b)))
}
func (p *peerCursor) str() string { return string(p.take(p.u16())) }
func (p *peerCursor) typ(depth int) {
	if depth > 16 {
		p.err = fmt.Errorf("peer fixture type depth")
		return
	}
	switch p.u16() {
	case 0:
		p.str()
	case 0x20, 0x22:
		p.typ(depth + 1)
	case 0x21:
		p.typ(depth + 1)
		p.typ(depth + 1)
	case 0x30:
		p.str()
		p.str()
		n := p.u16()
		for i := 0; i < n && p.err == nil; i++ {
			p.str()
			p.typ(depth + 1)
		}
	case 0x31:
		n := p.u16()
		for i := 0; i < n && p.err == nil; i++ {
			p.typ(depth + 1)
		}
	}
}

// AppendIncompletePeer preserves a real uncompressed v4 RESULT/ROWS body and
// appends one peer with address/UUID/DC but null rack and tokens. It parses only
// native metadata framing, independently of the driver's parser and serializers.
func AppendIncompletePeer(body []byte) ([]byte, error) {
	p := peerCursor{data: body}
	if p.i32() != 2 {
		return nil, fmt.Errorf("peer fixture requires ROWS")
	}
	flags, count := p.i32(), p.i32()
	if flags & ^3 != 0 || count < 1 || count > 1024 {
		return nil, fmt.Errorf("unsupported peer fixture metadata")
	}
	if flags&2 != 0 {
		n := p.i32()
		p.take(n)
	}
	if flags&1 != 0 {
		p.str()
		p.str()
	}
	names := make([]string, count)
	for i := range names {
		if flags&1 == 0 {
			p.str()
			p.str()
		}
		names[i] = p.str()
		p.typ(0)
	}
	offset := p.offset
	rows := p.i32()
	if rows < 0 || rows > 100000 {
		return nil, fmt.Errorf("invalid peer row count")
	}
	for i := 0; i < rows && p.err == nil; i++ {
		for range names {
			n := p.i32()
			if n != -1 {
				p.take(n)
			}
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	if p.offset != len(body) {
		return nil, fmt.Errorf("trailing peer fixture data")
	}
	result := append([]byte(nil), body...)
	binary.BigEndian.PutUint32(result[offset:offset+4], uint32(rows+1))
	for _, name := range names {
		var value []byte
		switch name {
		case "peer", "rpc_address", "native_address":
			value = []byte{127, 0, 21, 254}
		case "host_id":
			value = []byte{0x35, 0xc0, 0xec, 0x48, 0x51, 0x09, 0x40, 0xfd, 0x92, 0x81, 0x9e, 0x9d, 0x4a, 0xdd, 0x2f, 0x1e}
		case "data_center":
			value = []byte("datacenter1")
		}
		if value == nil {
			result = binary.BigEndian.AppendUint32(result, ^uint32(0))
		} else {
			result = binary.BigEndian.AppendUint32(result, uint32(len(value)))
			result = append(result, value...)
		}
	}
	return result, nil
}
