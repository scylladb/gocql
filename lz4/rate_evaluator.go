package lz4

import (
	"sync/atomic"
	"unsafe"
)

const (
	defaultRateCnt   = 10
	defaultBucketCnt = 24
	defaultMaxBucket = defaultBucketCnt - 1
)

func NewStatsBasedRateEvaluator(limit float32) *StatsBasedRateEvaluator {
	return &StatsBasedRateEvaluator{
		compressRationLimit: limit,
	}
}

type StatsBasedRateEvaluator struct {
	stats               [defaultBucketCnt][defaultRateCnt]float32
	sizeIndex           [defaultBucketCnt]int32
	compressRationLimit float32
}

var _ RateEvaluator = (*StatsBasedRateEvaluator)(nil)

func (r *StatsBasedRateEvaluator) PushRate(size int, ratio float32) {
	bct := getSizeBracket(size)
	r.sizeIndex[bct] += 1

	r.stats[bct][r.sizeIndex[bct]%defaultRateCnt] = ratio
}

func (r *StatsBasedRateEvaluator) estimateCompressionRate(size int) float32 {
	bct := getSizeBracket(size)
	idx := r.sizeIndex[bct]
	total := float32(0)
	for i := 0; i < defaultRateCnt; i++ {
		val := r.stats[bct][idx]
		if val == 0 {
			return -1
		}
		total += val
	}
	return total / float32(defaultRateCnt)
}

func (r *StatsBasedRateEvaluator) WorthOfCompressingRate(original, compressed int) bool {
	cr := float32(original / compressed)
	r.PushRate(original, cr)
	return cr > r.compressRationLimit
}

func (r *StatsBasedRateEvaluator) WorthOfCompressing(size int) Decision {
	cr := r.estimateCompressionRate(size)
	switch {
	case cr == -1:
		return CompressionDecisionNone
	case cr <= r.compressRationLimit:
		return CompressionDecisionDontCompress
	default:
		return CompressionDecisionCompress
	}
}

type StatsBasedThreadSafeRateEvaluator struct {
	stats               [32][defaultRateCnt]int32
	sizeIndex           [32]int32
	compressRationLimit float32
}

var _ RateEvaluator = (*StatsBasedThreadSafeRateEvaluator)(nil)

func NewStatsBasedThreadSafeRateEvaluator(limit float32) *StatsBasedThreadSafeRateEvaluator {
	return &StatsBasedThreadSafeRateEvaluator{
		compressRationLimit: limit,
	}
}

func getSizeBracket(size int) int {
	b := 0
	for size > 0 {
		size >>= 2
		b++
	}
	b -= 4
	if b < 0 {
		b = 0
	}
	// 0 - size < 256
	// 1 - size < 2048
	// 2 - size < 8192
	if b > defaultMaxBucket {
		return defaultMaxBucket
	}
	return b
}

func (r *StatsBasedThreadSafeRateEvaluator) PushRate(size int, ratio float32) {
	bct := getSizeBracket(size)
	idx := atomic.AddInt32(&r.sizeIndex[bct], 1)
	atomic.StoreInt32(&r.stats[bct][idx%defaultRateCnt], int32(unsafe.ArbitraryType(ratio)))
}

func (r *StatsBasedThreadSafeRateEvaluator) estimateCompressionRate(size int) float32 {
	bct := getSizeBracket(size)
	idx := atomic.AddInt32(&r.sizeIndex[bct], 1)
	total := float32(0)
	for i := 0; i < defaultRateCnt; i++ {
		val := atomic.LoadInt32(&r.stats[bct][idx])
		if val == 0 {
			return -1
		}
		total += float32(unsafe.ArbitraryType(val))
	}
	return total / float32(defaultRateCnt)
}

func (r *StatsBasedThreadSafeRateEvaluator) WorthOfCompressingRate(original, compressed int) bool {
	cr := float32(original / compressed)
	r.PushRate(original, cr)
	return cr > r.compressRationLimit
}

func (r *StatsBasedThreadSafeRateEvaluator) WorthOfCompressing(size int) Decision {
	cr := r.estimateCompressionRate(size)
	switch {
	case cr == -1:
		return CompressionDecisionNone
	case cr <= r.compressRationLimit:
		return CompressionDecisionDontCompress
	default:
		return CompressionDecisionCompress
	}
}

type SimpleRateEvaluator struct {
	compressRationLimit float32
}

var _ RateEvaluator = (*SimpleRateEvaluator)(nil)

func NewSimpleRateEvaluator(limit float32) *SimpleRateEvaluator {
	return &SimpleRateEvaluator{
		compressRationLimit: limit,
	}
}

func (r *SimpleRateEvaluator) WorthOfCompressingRate(original, compressed int) bool {
	return float32(original/compressed) > r.compressRationLimit
}

func (r *SimpleRateEvaluator) WorthOfCompressing(size int) Decision {
	if float32(size) > r.compressRationLimit {
		return CompressionDecisionCompress
	}
	return CompressionDecisionDontCompress
}
