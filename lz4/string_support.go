package lz4

const only7Bits = uint16(0x7f)

// convertBinToASCII converts a slice of bytes to a slice of ascii-compatible bytes with only 7 bits populated.
func convertBinToASCII(p []byte) []byte {
	b := make([]byte, 0, 1+len(p)*8/7)
	buff := uint16(0)
	k := byte(0)
	for _, v := range p {
		if k == 7 {
			b = append(b, byte(buff))
			k = 0
			buff = 0
		}
		buff |= uint16(v) << k
		b = append(b, byte(buff&only7Bits))
		buff >>= 7
		k++
	}
	if k > 0 {
		b = append(b, byte(buff))
	}
	return b
}

// convertBinToASCII converts a slice of ascii-compatible bytes with only 7 bits populated to regular bytes.
func convertASCIIToBin(p []byte) []byte {
	b := make([]byte, 0, len(p)*7/8)
	buff := uint16(0)
	k := byte(0)
	for _, v := range p {
		buff |= uint16(v) << k
		k += 7
		if k >= 8 {
			k -= 8
			b = append(b, byte(buff))
			buff >>= 8
		}
	}
	return b
}
