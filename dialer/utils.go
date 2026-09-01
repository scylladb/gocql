// Package dialer provides the record/replay and single-connection benchmark
// harness the driver uses to exercise its own wire handling against captured
// traffic.
//
// It is not part of the driver's supported API. Its exported identifiers exist so
// the recorder and replayer subpackages and the benchmarks can share frame
// parsing, and they change whenever the wire handling they mirror changes —
// GetFrameHash, for instance, needs whatever protocol context a frame's layout
// depends on but its bytes do not reveal. Expect breaking signature changes
// without a major version bump.
package dialer

import (
	"bytes"
	"errors"
	"sort"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/internal/murmur"
)

// ErrSegmentCompressorRequired is returned when a connection negotiated compression
// on protocol v5 but no compressor was supplied to the dialer.
//
// The driver's only segment compressor, lz4, lives in a separate Go module, so this
// module cannot construct one for itself; a caller recording or replaying a compressed
// v5 connection has to hand one in (see recorder.WithSegmentCompressor).
var ErrSegmentCompressorRequired = errors.New("gocql/dialer: protocol v5 connection negotiated compression, but no SegmentCompressor was supplied to the dialer")

// FrameProtoVersion returns the protocol version of the CQL frame b starts, or 0 if
// b is empty.
//
// The direction bit is masked off: the top bit of frame[0] distinguishes a request
// from a response, and folding it into the version makes every response look like a
// far newer protocol. It is only meaningful for bytes at a frame boundary.
func FrameProtoVersion(b []byte) byte {
	if len(b) == 0 {
		return 0
	}
	return b[0] & protoVersionMask
}

// FrameIsProtoV5OrNewer reports whether b starts a CQL frame whose protocol version is
// v5 or newer, and therefore whether the connection segments anything at all.
func FrameIsProtoV5OrNewer(b []byte) bool {
	return FrameProtoVersion(b) >= protoVersion5
}

type Record struct {
	Data     []byte `json:"data"`
	StreamID int    `json:"stream_id"`
	// UseMetadataID reports whether the SCYLLA_USE_METADATA_ID extension was
	// negotiated on the connection this frame belongs to. It governs whether an
	// EXECUTE request carries a resultMetadataID short-bytes field on protocol
	// v4 (see GetFrameHash). The recorder stamps it per connection; the frame
	// bytes alone cannot reveal it.
	UseMetadataID bool `json:"use_metadata_id"`
	// Proto is the protocol version of the connection the frame was recorded on,
	// direction bit masked off. Recordings made before the field existed decode it
	// as 0, "unstamped"; the loader falls back to the frame's own version byte.
	Proto byte `json:"proto,omitempty"`
}

// scyllaUseMetadataIDKey is the STARTUP/SUPPORTED option key for the
// SCYLLA_USE_METADATA_ID protocol extension (see gocql/scylla.go).
const scyllaUseMetadataIDKey = "SCYLLA_USE_METADATA_ID"

// compressionKey is the STARTUP option key naming the algorithm a connection's
// frames are compressed with (see gocql's startupOptions).
const compressionKey = "COMPRESSION"

// forEachStartupOption walks a STARTUP request's [string map] body, calling fn with
// each key and value until fn returns true. It reports whether fn stopped it.
//
// The body is walked as a proper [string map] rather than scanning the frame for a
// literal, and callers compare keys rather than the whole frame: gocql's
// startupOptions puts caller-influenced values into the same map — DRIVER_NAME,
// DRIVER_VERSION, DRIVER_CONFIG, SESSION_ID and whatever ApplicationInfo adds — so a
// substring match would let a caller latch an option by naming their application
// after it. The list keeps growing, which is the point: matching on keys does not
// care.
//
// Detection is restricted to STARTUP requests, so the same key appearing in a
// SUPPORTED response never trips a caller. A malformed or truncated map stops the
// walk, which reads as "the option is not there".
func forEachStartupOption(frame []byte, fn func(key, value []byte) bool) bool {
	if len(frame) < 5 {
		return false
	}
	shift := headerShift(frame)
	if frameOp(frame[3+shift]) != opStartup {
		return false
	}

	// Header: version, flags, stream (1 byte on v1/v2, 2 on v3+), opcode, length(4).
	p := 8 + shift
	readShort := func() (int, bool) {
		if p+2 > len(frame) {
			return 0, false
		}
		v := int(frame[p])<<8 | int(frame[p+1])
		p += 2
		return v, true
	}
	readString := func() ([]byte, bool) {
		n, ok := readShort()
		if !ok || p+n > len(frame) {
			return nil, false
		}
		s := frame[p : p+n]
		p += n
		return s, true
	}

	count, ok := readShort()
	if !ok {
		return false
	}
	for i := 0; i < count; i++ {
		key, ok := readString()
		if !ok {
			return false
		}
		value, ok := readString()
		if !ok {
			return false
		}
		if fn(key, value) {
			return true
		}
	}
	return false
}

// StartupNegotiatesMetadataID reports whether the given raw request frame is a
// STARTUP that opts into the SCYLLA_USE_METADATA_ID extension. The driver serializes
// the extension as the key SCYLLA_USE_METADATA_ID in the STARTUP [string map].
func StartupNegotiatesMetadataID(frame []byte) bool {
	return forEachStartupOption(frame, func(key, _ []byte) bool {
		return string(key) == scyllaUseMetadataIDKey
	})
}

// StartupCompression returns the compression algorithm a STARTUP request opts into,
// and whether it named one at all.
//
// This is how the record/replay dialers learn whether a v5 connection's transport
// segments are compressed, which the segment bytes themselves cannot reveal: the two
// layouts differ in header size, so a decoder has to be told which it is reading.
// Reading it from the STARTUP is exact rather than a guess, because the driver only
// sends the option when it is going to use it — it drops its own compressor if the
// server's SUPPORTED did not advertise the algorithm (see startupCoordinator.startup),
// and STARTUP is written after that decision.
//
// The STARTUP frame is always unsegmented, on every protocol version, so it is
// readable before any of this matters.
func StartupCompression(frame []byte) (string, bool) {
	var algorithm string
	found := forEachStartupOption(frame, func(key, value []byte) bool {
		if string(key) != compressionKey {
			return false
		}
		algorithm = string(value)
		return true
	})
	return algorithm, found
}

// A CQL frame carries the protocol version in the low 7 bits of frame[0]; the
// top bit is the request/response direction. Always mask with protoVersionMask
// before comparing a version, so the version tests in this file cannot disagree
// with each other depending on whether the direction bit happens to be set.
const (
	protoVersionMask = 0x7F
	protoVersion1    = 0x01
	protoVersion2    = 0x02
	protoVersion3    = 0x03
	protoVersion4    = 0x04
	protoVersion5    = 0x05
)

// headerShift reports the extra header byte protocol v3+ spends on its 2-byte
// stream id: v1/v2 put the opcode at frame[3] and the body at frame[8], v3+ put
// them at frame[4] and frame[9]. Callers must have checked that frame is non-empty.
//
// This is the single place the offset is derived, so the parsers in this file cannot
// disagree about where a frame's opcode is. The comparison is masked per the note
// above: the top bit of frame[0] is the request/response direction, and folding it
// into the version makes every response look like a much newer protocol.
func headerShift(frame []byte) int {
	if frame[0]&protoVersionMask > protoVersion2 {
		return 1
	}
	return 0
}

// fits reports whether the n bytes starting at index lie inside frame.
//
// Every length the parsers below walk on is peer- or file-supplied — a recording
// file is just JSON on disk — so each read is checked against this before it
// indexes. The helpers return false rather than a partial result, and their
// callers fall back to hashing the raw bytes, which is what a damaged recording
// deserves and is still stable between record and replay.
//
// n is compared against the space left rather than added to index, because the
// obvious index+n <= len(frame) overflows on a 32-bit int for the largest length
// a [bytes] field can encode, and a negative sum passes every bound.
func fits(frame []byte, index, n int) bool {
	return index >= 0 && index <= len(frame) && n >= 0 && n <= len(frame)-index
}

type frameOp byte

const (
	// header ops
	opError         frameOp = 0x00
	opStartup       frameOp = 0x01
	opReady         frameOp = 0x02
	opAuthenticate  frameOp = 0x03
	opOptions       frameOp = 0x05
	opSupported     frameOp = 0x06
	opQuery         frameOp = 0x07
	opResult        frameOp = 0x08
	opPrepare       frameOp = 0x09
	opExecute       frameOp = 0x0A
	opRegister      frameOp = 0x0B
	opEvent         frameOp = 0x0C
	opBatch         frameOp = 0x0D
	opAuthChallenge frameOp = 0x0E
	opAuthResponse  frameOp = 0x0F
	opAuthSuccess   frameOp = 0x10
)

// addBytes advances index past a [bytes] value: a 4-byte length followed by that
// many bytes.
//
// The length is read as signed, which the CQL spec says it is: -1 encodes a null
// value and -2 an unset one, neither of which carries a payload. Reading it
// unsigned turns a null bind value into a 4 GB payload and the walk never
// recovers, so an EXECUTE with one hashed as raw bytes rather than as itself.
func addBytes(frame []byte, index int) (int, bool) {
	if !fits(frame, index, 4) {
		return 0, false
	}
	bytesLength := int(int32(uint32(frame[index+0])<<24 | uint32(frame[index+1])<<16 | uint32(frame[index+2])<<8 | uint32(frame[index+3])))
	index = index + 4
	if bytesLength > 0 {
		if !fits(frame, index, bytesLength) {
			return 0, false
		}
		index = index + bytesLength
	}
	return index, true
}

// addLongString advances index past a [long string]: a 4-byte length followed by
// that many bytes.
//
// The length is read as signed for the same reason addBytes does: a negative value
// is a damaged frame rather than a null, and reading it unsigned would turn one into
// a 4 GB field that the walk never recovers from.
func addLongString(frame []byte, index int) (int, bool) {
	if !fits(frame, index, 4) {
		return 0, false
	}
	length := int(int32(uint32(frame[index+0])<<24 | uint32(frame[index+1])<<16 | uint32(frame[index+2])<<8 | uint32(frame[index+3])))
	index = index + 4
	if length < 0 || !fits(frame, index, length) {
		return 0, false
	}
	return index + length, true
}

// addShortBytes advances index past a [short bytes] value: a 2-byte length followed
// by that many bytes.
func addShortBytes(frame []byte, index int) (int, bool) {
	if !fits(frame, index, 2) {
		return 0, false
	}
	length := int(frame[index])<<8 | int(frame[index+1])
	if !fits(frame, index, 2+length) {
		return 0, false
	}
	return index + 2 + length, true
}

// addBatchQueries advances index past a BATCH body's type, query count and queries,
// leaving it on the consistency field that starts the batch's parameter block.
//
// That block is laid out exactly like a QUERY's <query_parameters> minus the fields
// a batch never carries, so addQueryParams walks it unchanged: writeBatchFrame only
// ever sets the serial-consistency, default-timestamp, keyspace and now_in_seconds
// flags, so the values, page-size and paging-state branches cannot fire.
//
// Named values need no handling here. writeBatchFrame rejects them outright on every
// protocol version, because Cassandra never implemented them (CASSANDRA-10246).
func addBatchQueries(frame []byte, index int) (int, bool) {
	// batch type
	if !fits(frame, index, 1) {
		return 0, false
	}
	index = index + 1

	if !fits(frame, index, 2) {
		return 0, false
	}
	queryCount := int(frame[index])<<8 | int(frame[index+1])
	index = index + 2

	for i := 0; i < queryCount; i++ {
		if !fits(frame, index, 1) {
			return 0, false
		}
		kind := frame[index]
		index = index + 1

		var ok bool
		switch kind {
		case 0:
			// A query string, as a [long string].
			if index, ok = addLongString(frame, index); !ok {
				return 0, false
			}
		case 1:
			// A prepared statement id, as [short bytes].
			if index, ok = addShortBytes(frame, index); !ok {
				return 0, false
			}
		default:
			// Not a kind the driver writes, so the rest of the body cannot be
			// located. A damaged recording, which the caller hashes raw.
			return 0, false
		}

		if !fits(frame, index, 2) {
			return 0, false
		}
		valuesLen := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2

		for j := 0; j < valuesLen; j++ {
			// A null or unset value carries no payload; addBytes reads the length as
			// signed so both advance by just the length field.
			if index, ok = addBytes(frame, index); !ok {
				return 0, false
			}
		}
	}

	return index, true
}

// addQueryParams walks a <query_parameters> block starting at index, which must
// point at the consistency field.
//
// It reports two ranges rather than one position. end is where the fields that
// belong in a frame's hash stop: consistency through serial consistency. tailStart
// and tailEnd bracket the protocol v5 keyspace override and now_in_seconds fields,
// which also identify a request but are not contiguous with end, because the
// default timestamp sits between them and must stay out of the hash — it is
// time.Now() at send time (framer.writeQueryParams), so hashing it would make a
// recorded frame never match its replay. An empty tail is reported as
// tailStart == tailEnd.
//
// The v5 fields are walked only on v5+, which is what keeps this change invisible
// to protocol v4: framer.validateV5Options rejects a keyspace override and
// now_in_seconds below v5, and FlagWithNowInSeconds (0x100) is not even
// representable in v4's one-byte flags field, so no v4 frame can reach the tail.
func addQueryParams(frame []byte, index int) (end, tailStart, tailEnd int, ok bool) {
	//use consistency
	if !fits(frame, index, 2) {
		return 0, 0, 0, false
	}
	index = index + 2

	//use query flags
	var flags uint32
	protoV5OrNewer := frame[0]&protoVersionMask > protoVersion4
	if protoV5OrNewer {
		// For protocol v5+, flags are a 4-byte big-endian uint32
		if !fits(frame, index, 4) {
			return 0, 0, 0, false
		}
		flags = uint32(frame[index])<<24 |
			uint32(frame[index+1])<<16 |
			uint32(frame[index+2])<<8 |
			uint32(frame[index+3])
		index = index + 4
	} else {
		if !fits(frame, index, 1) {
			return 0, 0, 0, false
		}
		flags = uint32(frame[index])
		index = index + 1
	}

	names := false

	// protoV3 specific things
	if frame[0]&protoVersionMask > protoVersion2 {
		if flags&frm.FlagValues == frm.FlagValues && flags&frm.FlagWithNameValues == frm.FlagWithNameValues {
			names = true
		}
	}

	if flags&frm.FlagValues == frm.FlagValues {
		if !fits(frame, index, 2) {
			return 0, 0, 0, false
		}
		valuesLen := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2

		for i := 0; i < valuesLen; i++ {
			if names {
				if !fits(frame, index, 2) {
					return 0, 0, 0, false
				}
				stringLenght := int(frame[index])<<8 | int(frame[index+1])
				if !fits(frame, index, 2+stringLenght) {
					return 0, 0, 0, false
				}
				index = index + 2 + stringLenght
			}

			var ok bool
			if index, ok = addBytes(frame, index); !ok {
				return 0, 0, 0, false
			}
		}
	}

	if flags&frm.FlagPageSize == frm.FlagPageSize {
		if !fits(frame, index, 4) {
			return 0, 0, 0, false
		}
		index = index + 4
	}

	if flags&frm.FlagWithPagingState == frm.FlagWithPagingState {
		var ok bool
		if index, ok = addBytes(frame, index); !ok {
			return 0, 0, 0, false
		}
	}

	if flags&frm.FlagWithSerialConsistency == frm.FlagWithSerialConsistency {
		if !fits(frame, index, 2) {
			return 0, 0, 0, false
		}
		index = index + 2
	}

	end = index

	// Everything below is protocol v5 only. On v4 the tail is empty and end is the
	// whole answer, exactly as before this function grew a tail.
	if !protoV5OrNewer {
		return end, index, index, true
	}

	// Skipped, never hashed: see the note on the default timestamp above.
	if flags&frm.FlagDefaultTimestamp == frm.FlagDefaultTimestamp {
		if !fits(frame, index, 8) {
			return 0, 0, 0, false
		}
		index = index + 8
	}

	tailStart = index

	if flags&frm.FlagWithKeyspace == frm.FlagWithKeyspace {
		var ok bool
		if index, ok = addShortBytes(frame, index); !ok {
			return 0, 0, 0, false
		}
	}

	if flags&frm.FlagWithNowInSeconds == frm.FlagWithNowInSeconds {
		if !fits(frame, index, 4) {
			return 0, 0, 0, false
		}
		index = index + 4
	}

	return end, tailStart, index, true
}

func addHeader(index int) int {
	return index + 8
}

// pastCustomPayload reports the offset a request's own body fields start at, stepping
// over the custom payload when the frame carries one, along with a canonical hash of
// the payload's entries (0 when the frame carries none).
//
// The [bytes map] a CUSTOM_PAYLOAD frame carries sits between the header and the first
// body field, so QUERY, EXECUTE and BATCH each have to step over it before walking
// anything of their own; a frame without the flag starts where the header ends. Reading
// frame[1] is the caller's to make safe, and GetFrameHash's arms all sit behind its
// header-length guard.
func pastCustomPayload(frame []byte, p int) (int, int64, bool) {
	if frame[1]&frm.FlagCustomPayload != frm.FlagCustomPayload {
		return addHeader(p), 0, true
	}
	return addCustomPayload(frame, p)
}

// addCustomPayload walks the [bytes map] a frame carries when FlagCustomPayload is
// set, and returns the index just past it along with a canonical hash of its entries.
//
// It derives that map's offset from p with addHeader rather than taking it as well,
// so the offset it reads the entry count from and the offset it walks on from cannot
// disagree. Every caller passed addHeader(p) for both, which held only by convention:
// a caller passing an index already advanced past something would have read the count
// from the body start and walked from somewhere else, and the walk would be
// plausible-but-wrong in the way the empty-map case below was.
//
// The hash sorts each entry's raw bytes before folding them together, rather than
// folding them in wire order. writeBytesMap (frame.go) ranges directly over a Go map to
// encode this same [bytes map], so its wire order is randomized across calls whenever
// the payload holds two or more entries: hashing that order-dependent range directly,
// as GetFrameHash used to, made the hash of an otherwise-identical request come out
// differently between a recording and its own replay. Sorting first makes the fold
// independent of which order writeBytesMap happened to produce. Ties are impossible —
// a [bytes map]'s keys are unique, and the key is the first field of each entry, so
// comparing the raw entries already totally orders them.
func addCustomPayload(frame []byte, p int) (int, int64, bool) {
	index := addHeader(p)
	if !fits(frame, index, 2) {
		return 0, 0, false
	}
	customPayloadLength := int(frame[index])<<8 | int(frame[index+1])
	// Skip the [bytes map] count itself unconditionally. A map of zero entries still
	// spends the two bytes that say so, and stopping short of them leaves the walk
	// reading that count as the next field: for a BATCH, its type, then its own second
	// byte as half of the statement count. That misreading is plausible rather than
	// detectably wrong — type LOGGED, zero statements, consistency ONE — so no bounds
	// check fails and nothing falls back to the raw bytes. It simply hashes a handful
	// of bytes of the wrong field, the same handful for every batch, which is a
	// collision the replayer resolves by serving whichever response it finds first.
	index = index + 2

	entries := make([][]byte, 0, customPayloadLength)
	for i := 0; i < customPayloadLength; i++ {
		entryStart := index
		if !fits(frame, index, 2) {
			return 0, 0, false
		}
		stringLength := int(frame[index])<<8 | int(frame[index+1])
		if !fits(frame, index, 2+stringLength) {
			return 0, 0, false
		}
		index = index + 2 + stringLength

		var ok bool
		if index, ok = addBytes(frame, index); !ok {
			return 0, 0, false
		}
		entries = append(entries, frame[entryStart:index])
	}

	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i], entries[j]) < 0 })
	var h int64
	for _, entry := range entries {
		h = foldHash(h, murmur.Murmur3H1(entry))
	}

	return index, h, true
}

// foldHash mixes add into h, so a frame's identity can span two byte ranges that
// are not contiguous.
//
// murmur.Murmur3H1 hashes one slice and has no incremental form, and the ranges a
// request's identity spans are separated by bytes that must not be hashed (the
// default timestamp). Copying them together into a scratch buffer would allocate on
// every request, and GetFrameHash runs per request inside a benchmark harness. The
// mix is boost's hash_combine; the hash only has to be deterministic and collide
// rarely across the handful of frames in a recording, not be cryptographic.
func foldHash(h, add int64) int64 {
	const goldenRatio = 0x9E3779B97F4A7C15
	return int64(uint64(h) ^ (uint64(add) + goldenRatio + uint64(h)<<6 + uint64(h)>>2))
}

// hashWithTail hashes frame[start:end], folding in frame[tailStart:tailEnd] when
// that second range is non-empty. An empty tail returns exactly the single-range
// hash, which is what keeps protocol v4 frames — where the tail is always empty —
// byte-for-byte identical to a plain Murmur3H1 of the range.
func hashWithTail(frame []byte, start, end, tailStart, tailEnd int) int64 {
	h := murmur.Murmur3H1(frame[start:end])
	if tailEnd > tailStart {
		h = foldHash(h, murmur.Murmur3H1(frame[tailStart:tailEnd]))
	}
	return h
}

// hashParams finishes hashing a request whose body ends in a <query_parameters>
// block: it walks the block at paramsStart and hashes frame[hashStart:] up to the
// block's end, folding in the protocol v5 tail, a custom payload's canonical hash and
// the header flags. A block that cannot be walked means the frame cannot be located
// either, so it falls back to hashing the raw bytes, matching the fallbacks at its
// call sites.
//
// payloadHash is folded in rather than covered by [hashStart:end] because its own
// bytes are order-dependent in a way the rest of the range is not — see
// addCustomPayload — while the payload still has to be part of a request's identity:
// two requests alike in everything but their payload must not hash the same. The fold
// is gated on the flag bit itself, not on payloadHash being nonzero, so a payload
// whose canonical hash happens to land on zero cannot be mistaken for no payload.
//
// The flags byte is folded in because it carries identity that no part of the body
// does. Tracing is the case that matters: framer.trace sets FlagTracing and changes
// nothing else, so a traced request and an untraced one are byte-identical from the
// body onwards, and a hash drawn only from the body cannot tell them apart. The
// replayer serves the first hash it matches, so it would answer one with the other's
// response -- and Query.Trace, Batch.Trace and Session.SetTrace are all public. The
// three arms that come through here are also the only ones needing it: STARTUP hashes
// the header down to the opcode, and every other arm hashes the frame whole.
//
// Only a nonzero byte is folded, which is the bargain hashWithTail already strikes for
// an empty v5 tail. A request with no flags set keeps the single-range hash it had, so
// no checked-in recording and no pinned value for an unflagged request moves -- and a
// zero flags byte has no identity to lose. Reading frame[1] is safe for the same reason
// pastCustomPayload's is: every arm reaching here sits behind GetFrameHash's
// FrameHeaderLen guard.
func hashParams(frame []byte, hashStart, paramsStart int, payloadHash int64) int64 {
	end, tailStart, tailEnd, ok := addQueryParams(frame, paramsStart)
	if !ok {
		return murmur.Murmur3H1(frame)
	}

	h := hashWithTail(frame, hashStart, end, tailStart, tailEnd)
	if frame[1]&frm.FlagCustomPayload == frm.FlagCustomPayload {
		h = foldHash(h, payloadHash)
	}
	if frame[1] != 0 {
		h = foldHash(h, murmur.Murmur3H1(frame[1:2]))
	}
	return h
}

func GetFrameHash(frame []byte, useMetadataID bool) int64 {
	// GetFrameHash parses a bare CQL request frame, of any protocol version the
	// driver speaks, and returns a hash standing for the request's identity: enough
	// of it to tell it apart from the other requests on the connection, and no byte
	// that differs between the moment it was recorded and the moment it is replayed.
	// Those two requirements are what every choice below is answering to.
	//
	// useMetadataID reports whether the SCYLLA_USE_METADATA_ID extension was
	// negotiated on the connection. On protocol v4 the extension adds a
	// resultMetadataID short-bytes field to EXECUTE requests (the same field v5
	// always carries); it cannot be inferred from the frame bytes, so it is
	// plumbed in by the recorder/replayer (see Record.UseMetadataID).
	//
	// A bare CQL frame is the caller's responsibility. From protocol v5 the driver
	// wraps each frame in transport segments (framer.prepareModernLayout), and those
	// on-wire bytes are not a frame: frame[0] is segment-header data rather than the
	// CQL version byte. This function does not detect that and cannot — for a v5
	// segment frame[0] is the low byte of a 17-bit length, so it is not
	// distinguishable from a version byte without protocol context the bytes do not
	// carry. Handing it segment bytes hashes a meaningless range. The record/replay
	// dialers unwrap the segments first, which is what Decoder is for.
	//
	// Note the empty and short-frame guards hash the frame as given, while the
	// raw-bytes fallbacks inside the switch hash it with the stream id already
	// blanked. Both are stable between record and replay, which is all the hash has
	// to be — but they are not interchangeable, so a new fallback has to match the
	// one next to it rather than whichever reads better. The two guards have no
	// choice: they run before, or on frames too short to contain, the stream id they
	// would have to blank.
	//
	// The arms below answer to those two requirements in three shapes, not one. QUERY,
	// EXECUTE and BATCH extract a range: it starts past the custom payload, when there
	// is one, and stops where the per-run fields begin, because a request's identity is
	// its statement and its bound values — the query text or the prepared id, the
	// values, and the parameters that change what the statement means — while the
	// default timestamp is time.Now() at send and never in the hash. Those three fold in
	// a canonical hash of the payload as well, and the header flags, for a bit of
	// identity the body never carries; see addCustomPayload and hashParams.
	//
	// One exception, and it is a dead one: the protocol v1 EXECUTE path slices from
	// past its values, so it leaves the prepared id out of the very range that is meant
	// to carry it. It cannot be reached — its values walk advances at least four bytes
	// per value while the bound it is checked against allows one, so every v1 EXECUTE
	// falls back to hashing the frame whole. Left as it is because no live connection is
	// v1 and the path is going away.
	//
	// STARTUP hashes the header down to the opcode and deliberately stops before the
	// body length, for the reason its own comment gives. PREPARE, AUTH_RESPONSE,
	// OPTIONS, REGISTER and the unknown-opcode default hash the frame whole, header
	// included, with the blanked stream id as the only edit.
	if len(frame) == 0 {
		return murmur.Murmur3H1(frame)
	}

	p := headerShift(frame)

	// A frame shorter than its own header — version, flags, stream id, opcode and
	// the 4-byte body length — cannot be parsed at all: the stream-id blanking
	// below and the opcode switch after it both index into it unconditionally.
	if !fits(frame, 0, 8+p) {
		return murmur.Murmur3H1(frame)
	}

	if p == 1 {
		streamID1 := frame[2]
		streamID2 := frame[3]
		defer func() {
			frame[2] = streamID1
			frame[3] = streamID2
		}()
		frame[2] = byte('0')
		frame[3] = byte('0')
	} else {
		streamID1 := frame[2]
		defer func() {
			frame[2] = streamID1
		}()
		frame[2] = byte('0')
	}
	switch frame[3+p] {
	case byte(opStartup):
		// Hash the header up to and including the opcode, deliberately stopping
		// before the 4-byte body length: a connection sends exactly one STARTUP,
		// so the opcode alone identifies it, and the options it carries are of
		// no interest to a replay.
		//
		// Including the length would tie every checked-in recording to the exact
		// set of STARTUP options the driver sent when it was recorded, so adding
		// one (DRIVER_CONFIG, SESSION_ID, ...) would invalidate them all and
		// panic the replay benchmarks until they were regenerated.
		return murmur.Murmur3H1(frame[:4+p])
	case byte(opPrepare):
		return murmur.Murmur3H1(frame)
	case byte(opAuthResponse):
		return murmur.Murmur3H1(frame)
	case byte(opQuery):
		index, payloadHash, ok := pastCustomPayload(frame, p)
		if !ok {
			return murmur.Murmur3H1(frame)
		}

		// A QUERY body is the query text as a [long string] followed by the query
		// parameters, so the parameter walk starts past the text — symmetric with the
		// EXECUTE arm, which steps over its preparedID and resultMetadataID first, and
		// with the BATCH arm, which steps over its statements.
		//
		// The hashed range starts at index rather than the body, so it covers the query
		// text as well as the values but not the custom payload: the payload's own bytes
		// are order-dependent (see addCustomPayload), so hashParams folds its canonical
		// hash in separately instead.
		paramsStart, ok := addLongString(frame, index)
		if !ok {
			return murmur.Murmur3H1(frame)
		}

		return hashParams(frame, index, paramsStart, payloadHash)
	case byte(opExecute):
		index, payloadHash, ok := pastCustomPayload(frame, p)
		if !ok {
			return murmur.Murmur3H1(frame)
		}

		endIndex := index

		// Every length here is peer- or file-supplied, and this branch now runs on
		// protocol v4 rather than being unreachable, so bound the reads: a truncated
		// or wrongly-stamped recording must fall back to hashing the raw bytes (as the
		// v5 guard above does) rather than panic inside loadResponseFramesFromFiles.
		// Both the length field and the payload it announces have to be checked —
		// a plausible length running off the end walks straight into addQueryParams.
		if !fits(frame, index, 2) {
			return murmur.Murmur3H1(frame)
		}
		preparedIDLen := int(frame[index])<<8 | int(frame[index+1])
		if !fits(frame, endIndex, 2+preparedIDLen) {
			return murmur.Murmur3H1(frame)
		}
		endIndex = endIndex + 2 + preparedIDLen

		// EXECUTE frames carry a resultMetadataID (short bytes) between the
		// preparedID and the query params on protocol v5+, and on protocol v4 when
		// the SCYLLA_USE_METADATA_ID extension is negotiated. Skip it so the
		// query-params offset (and therefore the extracted hash) is correct. The v4
		// case cannot be read from the frame bytes, so it is signalled by the
		// caller via useMetadataID.
		if frame[0]&protoVersionMask > protoVersion4 || useMetadataID {
			if !fits(frame, endIndex, 2) {
				return murmur.Murmur3H1(frame)
			}
			resultMetadataIDLen := int(frame[endIndex])<<8 | int(frame[endIndex+1])
			if !fits(frame, endIndex, 2+resultMetadataIDLen) {
				return murmur.Murmur3H1(frame)
			}
			endIndex = endIndex + 2 + resultMetadataIDLen
		}

		if frame[0]&protoVersionMask > protoVersion1 {
			return hashParams(frame, index, endIndex, payloadHash)
		}

		// Protocol v1 has no <query_parameters> block: the values follow the
		// preparedID directly. Bounded by the preparedID length check above, which
		// read the same two bytes at the same index: nothing between here and there
		// moves it.
		valuesLen := int(frame[index])<<8 | int(frame[index+1])
		index = index + 2
		for i := 0; i < valuesLen; i++ {
			if index, ok = addBytes(frame, index); !ok {
				return murmur.Murmur3H1(frame)
			}
		}
		index = index + 2
		// The walk above moves index independently of endIndex and can leave it past
		// the end of the range, so the two still have to be ordered before the slice.
		// endIndex itself is bounded by the helpers.
		if index > endIndex {
			return murmur.Murmur3H1(frame)
		}
		return murmur.Murmur3H1(frame[index:endIndex])
	case byte(opBatch):
		index, payloadHash, ok := pastCustomPayload(frame, p)
		if !ok {
			return murmur.Murmur3H1(frame)
		}

		// A BATCH used to be hashed whole, which put its default timestamp in the
		// hash. writeBatchFrame fills that field with time.Now() unless the caller
		// pinned a value, so a recorded BATCH hashed differently on every run and
		// replaying one could only ever fail to find its response. Walking the body
		// to the end of the parameter block leaves the timestamp out, the same way
		// QUERY and EXECUTE do.
		paramsStart, ok := addBatchQueries(frame, index)
		if !ok {
			return murmur.Murmur3H1(frame)
		}

		return hashParams(frame, index, paramsStart, payloadHash)
	case byte(opOptions):
		return murmur.Murmur3H1(frame)
	case byte(opRegister):
		return murmur.Murmur3H1(frame)
	default:
		return murmur.Murmur3H1(frame)
	}
}
