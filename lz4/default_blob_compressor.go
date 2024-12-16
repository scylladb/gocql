package lz4

var DefaultCompressor = NewBlobCompressorMust([]byte("lz4:"))

func SetDefaultCompressorLimit(limit int) {
	DefaultCompressor.lowerSizeLimit = limit
}

func SetDefaultCompressorPrefix(prefix []byte) {
	DefaultCompressor.prefix = prefix
}

func Blob(val []byte) *CompressedBlob {
	return DefaultCompressor.Blob(val)
}

func String(val string) *CompressedString {
	return DefaultCompressor.String(val)
}
