/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transport

import (
	"sync"
	"sync/atomic"
)

// framerPool owns one sync.Pool plus the adaptive buffer-sizing state for one
// framer usage class.
type InternalFramerPool struct {
	pool               sync.Pool
	InternalBufAvgSize atomic.Int64
	enabled            atomic.Bool
}

// connFramers owns connection-scoped framer configuration and reader/writer pools.
type InternalConnFramers struct {
	InternalReadPool  InternalFramerPool
	InternalWritePool InternalFramerPool
	InternalDefaults  InternalFramerConfig
}

// framerConfig holds precomputed default framer parameters for the connection.
// Populated once during connection setup and used to initialize framers from the pool.
type InternalFramerConfig struct {
	compressor                  Compressor
	InternalFlagLWT             int
	rateLimitingErrorCode       int
	InternalProto               byte
	InternalFlags               byte
	InternalTabletsRoutingV1    bool
	InternalTabletsRoutingV2    bool
	InternalScyllaUseMetadataID bool
}

// framerBufEWMAWeight controls how quickly the exponential weighted moving average
// of framer buffer sizes adapts. A value of 8 means each sample contributes ~12.5%,
// so it takes roughly 8 samples to converge to a new steady state.
//
// Lower values (e.g., 4) adapt faster but are more sensitive to outliers.
// Higher values (e.g., 16) are more stable but adapt slower to workload changes.
// The value 8 was chosen as a reasonable balance for typical CQL query patterns.
const InternalFramerBufEWMAWeight = 8

// framerBufShrinkThreshold is the multiplier applied to the EWMA to decide when a
// framer's read buffer is too large relative to typical usage and should be shrunk.
const InternalFramerBufShrinkThreshold = 2

// maxReasonableBufferSize is a safety limit to prevent overflow in EWMA calculations
// and to catch pathological cases where buffers grow unreasonably large.
const InternalMaxReasonableBufferSize = 512 * 1024 * 1024 // 512MB

// maxCASRetries is the maximum number of CAS retries for updating EWMA.
// This prevents infinite loops under extreme contention.
const InternalMaxCASRetries = 100

// initFramerCache precomputes framer fields from cqlProtoExts so that
// per-query framer creation avoids repeated linear scans and allocations.
func (c *Conn) InternalInitFramerCache() {
	c.InternalFramers.initCache(c)
}

func (cf *InternalConnFramers) initCache(c *Conn) {
	cfg := InternalFramerConfig{
		compressor:    c.InternalCompressor,
		InternalProto: c.InternalVersion & protoVersionMask,
		InternalFlags: defaultFramerFlags(c.InternalCompressor, c.InternalVersion),
	}
	if lwtExt := findCQLProtoExtByName(c.InternalCqlProtoExts, lwtAddMetadataMarkKey); lwtExt != nil {
		if castedExt, ok := lwtExt.(*lwtAddMetadataMarkExt); ok {
			cfg.InternalFlagLWT = castedExt.InternalLwtOptMetaBitMask
		} else {
			c.InternalLogger.Printf("gocql: failed to cast CQL protocol extension %s to %T", lwtAddMetadataMarkKey, lwtAddMetadataMarkExt{})
		}
	}
	if rateLimitErrorExt := findCQLProtoExtByName(c.InternalCqlProtoExts, rateLimitError); rateLimitErrorExt != nil {
		if castedExt, ok := rateLimitErrorExt.(*rateLimitExt); ok {
			cfg.rateLimitingErrorCode = castedExt.InternalRateLimitErrorCode
		} else {
			c.InternalLogger.Printf("gocql: failed to cast CQL protocol extension %s to %T", rateLimitError, rateLimitExt{})
		}
	}
	if tabletsExt := findCQLProtoExtByName(c.InternalCqlProtoExts, tabletsRoutingV1); tabletsExt != nil {
		if _, ok := tabletsExt.(*tabletsRoutingV1Ext); ok {
			cfg.InternalTabletsRoutingV1 = true
		} else {
			c.InternalLogger.Printf("gocql: failed to cast CQL protocol extension %s to %T", tabletsRoutingV1, tabletsRoutingV1Ext{})
		}
	}
	if metadataIDExt := findCQLProtoExtByName(c.InternalCqlProtoExts, scyllaUseMetadataID); metadataIDExt != nil {
		if _, ok := metadataIDExt.(*scyllaUseMetadataIDExt); ok {
			cfg.InternalScyllaUseMetadataID = true
		} else {
			c.InternalLogger.Printf("gocql: failed to cast CQL protocol extension %s to %T", scyllaUseMetadataID, scyllaUseMetadataIDExt{})
		}
	}
	cfg.InternalTabletsRoutingV2 = findCQLProtoExtByName(c.InternalCqlProtoExts, tabletsRoutingV2) != nil
	cf.InternalDefaults = cfg
	c.setTabletSupported(cfg.InternalTabletsRoutingV1 || cfg.InternalTabletsRoutingV2)
	cf.InternalInitPool(c)
}

func (cf *InternalConnFramers) InternalInitPool(c *Conn) {
	defaults := cf.InternalDefaults
	cf.InternalReadPool.init(defaults, func(f *framer) { c.InternalReleaseReadFramer(f) })
	cf.InternalWritePool.init(defaults, func(f *framer) { c.InternalReleaseWriteFramer(f) })
}

// getReadFramer returns a pooled framer for reading responses and events.
func (c *Conn) InternalGetReadFramer() *framer {
	return c.InternalFramers.getRead(c)
}

func (cf *InternalConnFramers) getRead(c *Conn) *framer {
	f := cf.InternalReadPool.get(c)
	f.Released.Store(false)
	return f
}

// getWriteFramer returns a pooled framer for building outgoing requests.
func (c *Conn) InternalGetWriteFramer() *framer {
	return c.InternalFramers.getWrite(c)
}

func (cf *InternalConnFramers) getWrite(c *Conn) *framer {
	f := cf.InternalWritePool.get(c)
	f.Released.Store(false)
	// Before initCache runs, cf.defaults is still zero, so handshake framers get
	// no flags at all. That is what the handshake needs: the compressor is not yet
	// negotiated (so FlagCompress must not be set on OPTIONS/STARTUP), and no
	// version-derived flag exists to seed — see defaultFramerFlags.
	f.Flags = cf.InternalDefaults.InternalFlags
	return f
}

// releaseReadFramer returns a response/event framer to the reader pool.
func (c *Conn) InternalReleaseReadFramer(f *framer) {
	c.InternalFramers.releaseRead(c, f)
}

func (cf *InternalConnFramers) releaseRead(c *Conn, f *framer) {
	if f == nil {
		return
	}
	if f.Released.Swap(true) {
		return // already released
	}
	f.Header = nil
	f.TraceID = nil
	f.CustomPayload = nil
	if !cf.InternalReadPool.enabled.Load() {
		return
	}

	bufCap := int64(cap(f.ReadBuffer))
	newAvg, success := cf.InternalReadPool.updateAvg(c.InternalLogger, bufCap)
	if !success {
		cf.InternalReadPool.resetAndPut(f, false, 0)
		return
	}
	cf.InternalReadPool.resetAndPut(f, true, InternalFpShrinkSize(bufCap, newAvg))
}

// releaseWriteFramer returns a request-builder framer to the writer pool.
func (c *Conn) InternalReleaseWriteFramer(f *framer) {
	c.InternalFramers.releaseWrite(f)
}

func (cf *InternalConnFramers) releaseWrite(f *framer) {
	if f == nil {
		return
	}
	if f.Released.Swap(true) {
		return
	}
	f.Header = nil
	f.TraceID = nil
	f.CustomPayload = nil
	f.Flags = cf.InternalDefaults.InternalFlags
	if !cf.InternalWritePool.enabled.Load() {
		return
	}
	bufCap := int64(cap(f.Buf))
	newAvg, success := cf.InternalWritePool.updateAvg(nil, bufCap)
	if !success {
		cf.InternalWritePool.resetAndPut(f, false, 0)
		return
	}
	cf.InternalWritePool.resetAndPut(f, false, InternalFpShrinkSize(bufCap, newAvg))
}

func (cf *InternalConnFramers) close() {
	cf.InternalReadPool.close()
	cf.InternalWritePool.close()
}

func (fp *InternalFramerPool) init(defaults InternalFramerConfig, release func(*framer)) {
	fp.InternalBufAvgSize.Store(int64(defaultBufSize))
	fp.enabled.Store(true)
	fp.pool = sync.Pool{
		New: func() any {
			buf := make([]byte, defaultBufSize)
			f := &framer{
				Buf:                   buf[:0],
				ReadBuffer:            buf,
				Compressor:            defaults.compressor,
				Proto:                 defaults.InternalProto,
				Flags:                 defaults.InternalFlags,
				FlagLWT:               defaults.InternalFlagLWT,
				RateLimitingErrorCode: defaults.rateLimitingErrorCode,
				TabletsRoutingV1:      defaults.InternalTabletsRoutingV1,
				ScyllaUseMetadataID:   defaults.InternalScyllaUseMetadataID,
			}
			f.ReleaseHook = func() { release(f) }
			return f
		},
	}
}

func (fp *InternalFramerPool) get(c *Conn) *framer {
	if !fp.enabled.Load() {
		return newFramer(c.InternalCompressor, c.InternalVersion)
	}
	return fp.pool.Get().(*framer)
}

func (fp *InternalFramerPool) put(f *framer) {
	if !fp.enabled.Load() {
		return
	}
	fp.pool.Put(f)
}

func (fp *InternalFramerPool) close() {
	fp.enabled.Store(false)
}

func (fp *InternalFramerPool) updateAvg(logger StdLogger, bufCap int64) (int64, bool) {
	if bufCap > InternalMaxReasonableBufferSize {
		bufCap = InternalMaxReasonableBufferSize
	}
	if bufCap < 0 {
		bufCap = defaultBufSize
	}

	for i := 0; i < InternalMaxCASRetries; i++ {
		avg := fp.InternalBufAvgSize.Load()
		// EWMA update with upward-biased rounding: the +framerBufEWMAWeight/2 term
		// biases the integer division toward ceiling for all deltas. This means:
		// - When bufCap > avg (growth): the average increases slightly faster
		// - When bufCap < avg (shrink): the average decreases slightly slower
		// Both effects are intentional — favoring larger buffers reduces
		// reallocation churn at the cost of slightly more memory.
		// In practice, this means the steady-state EWMA settles ~framerBufEWMAWeight/2
		// bytes above the true average when tracking decreasing buffer sizes.
		newAvg := avg + (bufCap-avg+InternalFramerBufEWMAWeight/2)/InternalFramerBufEWMAWeight
		if fp.InternalBufAvgSize.CompareAndSwap(avg, newAvg) {
			return newAvg, true
		}
	}

	if logger != nil {
		logger.Printf("gocql: EWMA update failed after %d retries, skipping shrink decision", InternalMaxCASRetries)
	}
	return fp.InternalBufAvgSize.Load(), false
}

func InternalFpShrinkSize(bufCap, newAvg int64) int64 {
	// If this framer's buffer is much larger than the running average,
	// reallocate it to prevent a single large query from permanently
	// bloating all pooled framers.
	if bufCap <= newAvg*InternalFramerBufShrinkThreshold {
		return 0
	}
	if newAvg < defaultBufSize {
		return defaultBufSize
	}
	return newAvg
}

func (fp *InternalFramerPool) resetAndPut(f *framer, alignBufWithReadBuffer bool, shrinkSize int64) {
	if shrinkSize > 0 {
		buf := make([]byte, shrinkSize)
		f.ReadBuffer = buf
		f.Buf = buf[:0]
		// The v5 wire buffer (prepareModernLayout) is sized after the frame it
		// segmented, so it has to be dropped along with an oversized f.buf,
		// otherwise a single large request keeps bloating this pooled framer.
		f.WireBuf = nil
		fp.put(f)
		return
	}
	if alignBufWithReadBuffer {
		f.Buf = f.ReadBuffer[:0]
	} else {
		f.Buf = f.Buf[:0]
	}
	fp.put(f)
}
