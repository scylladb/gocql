package testkit

import (
	"encoding/binary"
	"testing"
)

func TestIncompletePeerIndependentMetadata(t *testing.T) {
	body := binary.BigEndian.AppendUint32(nil, 2)
	body = binary.BigEndian.AppendUint32(body, 1)
	body = binary.BigEndian.AppendUint32(body, 3)
	str := func(v string) { body = binary.BigEndian.AppendUint16(body, uint16(len(v))); body = append(body, v...) }
	str("system")
	str("peers")
	for i, name := range []string{"peer", "host_id", "rack"} {
		str(name)
		body = binary.BigEndian.AppendUint16(body, []uint16{0x10, 0x0c, 0x0d}[i])
	}
	offset := len(body)
	body = binary.BigEndian.AppendUint32(body, 0)
	for end := 0; end < len(body); end++ {
		if _, err := AppendIncompletePeer(body[:end]); err == nil {
			t.Fatalf("accepted truncated metadata %d", end)
		}
	}
	result, err := AppendIncompletePeer(body)
	if err != nil {
		t.Fatal(err)
	}
	if binary.BigEndian.Uint32(result[offset:offset+4]) != 1 {
		t.Fatal("peer not appended")
	}
	// 4-byte inet, 16-byte UUID, null rack; each has a four-byte length.
	if len(result) != len(body)+4+4+4+16+4 || binary.BigEndian.Uint32(result[len(result)-4:]) != ^uint32(0) {
		t.Fatal("invalid independent row", result)
	}
	if binary.BigEndian.Uint32(body[offset:]) != 0 {
		t.Fatal("source was mutated")
	}
}
