package murmur

import "encoding/binary"

// Stream computes Cassandra's Murmur3 token incrementally. Only a partial
// 16-byte algorithm block is buffered; inputs are never concatenated.
// Sum64 does not consume the state, so callers may inspect and continue writing.
type Stream struct {
	h1, h2 int64
	length uint64
	tail   [16]byte
	used   int
}

func (s *Stream) block(data []byte) {
	k1 := int64(binary.LittleEndian.Uint64(data[:8]))
	k2 := int64(binary.LittleEndian.Uint64(data[8:16]))
	k1 *= c1
	k1 = rotl(k1, 31)
	k1 *= c2
	s.h1 ^= k1
	s.h1 = rotl(s.h1, 27)
	s.h1 += s.h2
	s.h1 = s.h1*5 + 0x52dce729
	k2 *= c2
	k2 = rotl(k2, 33)
	k2 *= c1
	s.h2 ^= k2
	s.h2 = rotl(s.h2, 31)
	s.h2 += s.h1
	s.h2 = s.h2*5 + 0x38495ab5
}

func (s *Stream) Write(data []byte) (int, error) {
	n := len(data)
	s.length += uint64(n)
	if s.used > 0 {
		copied := copy(s.tail[s.used:], data)
		s.used += copied
		data = data[copied:]
		if s.used == 16 {
			s.block(s.tail[:])
			s.used = 0
		}
	}
	for len(data) >= 16 {
		s.block(data[:16])
		data = data[16:]
	}
	if len(data) > 0 {
		s.used = copy(s.tail[:], data)
	}
	return n, nil
}

func (s *Stream) Sum64() int64 {
	h1, h2 := s.h1, s.h2
	var k1, k2 int64
	// Preserve Cassandra's signed-byte tail arithmetic, including high bytes.
	for i := s.used - 1; i >= 8; i-- {
		k2 ^= block(s.tail[i]) << uint((i-8)*8)
	}
	if s.used > 8 {
		k2 *= c2
		k2 = rotl(k2, 33)
		k2 *= c1
		h2 ^= k2
	}
	for i := min(s.used, 8) - 1; i >= 0; i-- {
		k1 ^= block(s.tail[i]) << uint(i*8)
	}
	if s.used > 0 {
		k1 *= c1
		k1 = rotl(k1, 31)
		k1 *= c2
		h1 ^= k1
	}
	h1 ^= int64(s.length)
	h2 ^= int64(s.length)
	h1 += h2
	h2 += h1
	h1 = fmix(h1)
	h2 = fmix(h2)
	return h1 + h2
}
