package replayer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gocql/gocql/dialer"
)

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// requestFrame builds a body-less v4 request with the given opcode and stream id.
//
// The opcode is what tells two requests apart here: GetFrameHash blanks the stream id
// before hashing, so two OPTIONS frames on different streams hash alike and would be
// served the same recorded response.
func requestFrame(opcode byte, streamID int) []byte {
	return []byte{0x04, 0x00, byte(streamID >> 8), byte(streamID), opcode, 0x00, 0x00, 0x00, 0x00}
}

// newTestReplayer builds a ConnectionReplayer the way NewConnectionReplayer does,
// without going through a file on disk. Constructing the literal directly is a trap:
// the request decoder and the framing state come as a pair, and a replayer missing
// either panics on the first write.
func newTestReplayer(proto byte, frames ...*FrameRecorded) *ConnectionReplayer {
	return newTestReplayerWith(nil, proto, frames...)
}

// newTestReplayerWith is the same for a connection given a segment compressor.
func newTestReplayerWith(comp dialer.SegmentCompressor, proto byte, frames ...*FrameRecorded) *ConnectionReplayer {
	framing := dialer.NewFraming(comp)
	return &ConnectionReplayer{
		gotRequest:        make(chan struct{}, 1),
		frames:            frames,
		frameIdsToReplay:  []int{},
		streamIdsToReplay: []int{},
		recordedProto:     proto,
		framing:           framing,
		requests:          framing.NewDecoder(),
	}
}

// TestConnectionReplayerReplaysUnsegmentedProtoV5 pins that a v5 request is matched
// rather than refused.
//
// It used to be refused outright, because the replayer could neither hash a transport
// segment for matching nor patch a stream id into one without invalidating its CRC.
// The handshake is still unsegmented on v5, so this frame goes through the plain path;
// what changed is that reaching it is no longer an error.
func TestConnectionReplayerReplaysUnsegmentedProtoV5(t *testing.T) {
	req := optionsFrame(0x05)
	c := newTestReplayer(0x05, &FrameRecorded{
		Response: optionsFrame(0x85),
		Hash:     dialer.GetFrameHash(append([]byte(nil), req...), false),
	})

	if _, err := c.Write(append([]byte(nil), req...)); err != nil {
		t.Fatalf("Write(v5 frame) error = %v, want nil", err)
	}
}

// TestConnectionReplayerRefusesAProtoV5Query pins that the one request the hashing
// cannot yet locate on protocol v5 is refused by name, rather than surfacing as the
// anonymous unable-to-find-a-response panic (scylladb/gocql#1000).
//
// It panics like that one, and for the same reason: a QUERY arrives mid-session,
// where a returned error is a connection failure the driver answers by reconnecting
// and replaying the same recording into the same refusal, reporting it far from the
// cause if at all.
func TestConnectionReplayerRefusesAProtoV5Query(t *testing.T) {
	query := []byte{
		0x05, 0x00, // version, flags
		0x00, 0x01, // stream id
		0x07,                   // opQuery
		0x00, 0x00, 0x00, 0x00, // body length
	}
	c := newTestReplayer(0x05)

	defer func() {
		r := recover()
		if r == nil {
			t.Error("a protocol v5 QUERY was accepted for replay")
			return
		}
		err, ok := r.(error)
		if !ok {
			t.Fatalf("panic value %v is not an error", r)
		}
		if !strings.Contains(err.Error(), "QUERY") || !strings.Contains(err.Error(), "#1000") {
			t.Errorf("panic %q does not name QUERY and scylladb/gocql#1000", err)
		}
	}()

	_, _ = c.Write(query)
}

// TestConnectionReplayerRejectsAProtocolMismatch pins that replaying a recording at
// the wrong protocol version says so, rather than serving responses the driver rejects
// deep inside its own header parsing.
func TestConnectionReplayerRejectsAProtocolMismatch(t *testing.T) {
	c := newTestReplayer(0x05)

	_, err := c.Write(optionsFrame(0x04))
	if err == nil {
		t.Fatal("a v4 request against a v5 recording was accepted")
	}
	if !strings.Contains(err.Error(), "protocol v5") || !strings.Contains(err.Error(), "protocol v4") {
		t.Errorf("error %q does not name both protocol versions", err)
	}
}

// TestConnectionReplayerAcceptsProtoV4 pins that a v4 frame with a matching recorded
// hash replays normally.
func TestConnectionReplayerAcceptsProtoV4(t *testing.T) {
	req := optionsFrame(0x04)
	c := newTestReplayer(0x04, &FrameRecorded{
		Response: optionsFrame(0x84),
		Hash:     dialer.GetFrameHash(req, false),
	})

	n, err := c.Write(req)
	if err != nil {
		t.Fatalf("Write(v4 frame) error = %v, want nil", err)
	}
	if n != len(req) {
		t.Errorf("Write(v4 frame) = %d bytes, want %d", n, len(req))
	}
}

// writeRecords writes the given records to a file in the recorder's format, one
// JSON object per line, and returns its path.
func writeRecords(t *testing.T, name string, records ...dialer.Record) string {
	t.Helper()
	return writeRecordsTo(t, filepath.Join(t.TempDir(), name), records...)
}

// writeRecordsTo writes the records to an exact path, for the tests that need both
// halves of one recording to share a directory.
func writeRecordsTo(t *testing.T, path string, records ...dialer.Record) string {
	t.Helper()

	var buf bytes.Buffer
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshalling record %d: %v", record.StreamID, err)
		}
		buf.Write(append(line, '\n'))
	}

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
}

// TestNewConnectionReplayerRefusesARecordingThatPairsNothing pins that a recording with
// nothing to replay fails at the dial.
//
// The loader accepts one deliberately -- it still has a protocol version to report off a
// truncated Reads file, and a version check that disarms itself exactly when the recording
// is damaged is not a check -- so the refusal belongs here instead. Without it the dial
// succeeds and the first request panics with "unable to find a response to replay", naming
// neither file, which is what a mistyped directory or a source port this recording never
// saw produces.
func TestNewConnectionReplayerRefusesARecordingThatPairsNothing(t *testing.T) {
	base := filepath.Join(t.TempDir(), "rec")
	writeRecordsTo(t, base+"Writes", dialer.Record{Data: optionsFrame(0x04), StreamID: 1, Proto: 0x04})
	writeRecordsTo(t, base+"Reads")

	conn, err := NewConnectionReplayer(base, nil)
	if err == nil {
		t.Fatalf("a recording pairing no requests with responses was replayed as %T", conn)
	}
	for _, want := range []string{base + "Reads", base + "Writes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %q", err, want)
		}
	}
}

// TestLoadFramesPrefersTheStampedProto pins where a recording's protocol version
// comes from: the Proto field the recorder stamps, read from any request record,
// rather than from the request/response pairing -- a truncated Reads file pairs
// nothing, and a version check that disarms itself exactly when the recording is
// damaged is not a check. A record that predates the field (Proto 0) still derives
// the version from the frame's own bytes.
func TestLoadFramesPrefersTheStampedProto(t *testing.T) {
	t.Run("stamped, nothing paired", func(t *testing.T) {
		writes := writeRecords(t, "Writes", dialer.Record{Data: optionsFrame(0x05), StreamID: 1, Proto: 0x05})
		reads := writeRecords(t, "Reads")

		_, proto, err := loadResponseFramesFromFiles(reads, writes)
		if err != nil {
			t.Fatalf("loadResponseFramesFromFiles: %v", err)
		}
		if proto != 0x05 {
			t.Errorf("proto = %d, want 5 (the stamped version, despite nothing pairing)", proto)
		}
	})

	t.Run("stamped versions disagree", func(t *testing.T) {
		writes := writeRecords(t, "Writes",
			dialer.Record{Data: optionsFrame(0x04), StreamID: 1, Proto: 0x04},
			dialer.Record{Data: optionsFrame(0x05), StreamID: 2, Proto: 0x05})
		reads := writeRecords(t, "Reads")

		_, _, err := loadResponseFramesFromFiles(reads, writes)
		if err == nil {
			t.Fatal("a file holding two protocol versions was loaded")
		}
		for _, want := range []string{"v4", "v5"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	})

	t.Run("derived versions disagree", func(t *testing.T) {
		writes := writeRecords(t, "Writes",
			dialer.Record{Data: optionsFrame(0x04), StreamID: 1},
			dialer.Record{Data: optionsFrame(0x05), StreamID: 2})
		reads := writeRecords(t, "Reads")

		if _, _, err := loadResponseFramesFromFiles(reads, writes); err == nil {
			t.Fatal("a file holding two protocol versions was loaded")
		}
	})

	t.Run("one stamped record does not answer for the unstamped ones", func(t *testing.T) {
		// What a recording directory reused across a recorder upgrade holds: the
		// recorder appends every connection to one addr-sourcePort file, so a v4
		// session recorded before the Proto field existed survives underneath a
		// later stamped one. Taking the version from the stamped record alone left
		// the rest unchecked.
		writes := writeRecords(t, "Writes",
			dialer.Record{Data: optionsFrame(0x05), StreamID: 1, Proto: 0x05},
			dialer.Record{Data: optionsFrame(0x04), StreamID: 2})
		reads := writeRecords(t, "Reads",
			dialer.Record{Data: optionsFrame(0x85), StreamID: 1},
			dialer.Record{Data: optionsFrame(0x84), StreamID: 2})

		_, _, err := loadResponseFramesFromFiles(reads, writes)
		if err == nil {
			t.Fatal("a file mixing a stamped v5 record with an unstamped v4 leftover was loaded")
		}
		for _, want := range []string{"v4", "v5"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name %q", err, want)
			}
		}
	})

	t.Run("unstamped legacy record", func(t *testing.T) {
		writes := writeRecords(t, "Writes", dialer.Record{Data: optionsFrame(0x04), StreamID: 1})
		reads := writeRecords(t, "Reads", dialer.Record{Data: optionsFrame(0x84), StreamID: 1})

		_, proto, err := loadResponseFramesFromFiles(reads, writes)
		if err != nil {
			t.Fatalf("loadResponseFramesFromFiles: %v", err)
		}
		if proto != 0x04 {
			t.Errorf("proto = %d, want 4 (derived from the frame bytes)", proto)
		}
	})
}

// TestLoadFramesFromFileLargeRecord pins that a record is bounded by the frame it
// holds and nothing else. The recorder writes one record per frame, and Record.Data
// is a []byte that encoding/json base64-inflates by 4/3, so a frame over ~48 KiB
// produces a line past bufio.MaxScanTokenSize — which is what the loader used to
// read records with, and it failed the whole recording with "token too long".
func TestLoadFramesFromFileLargeRecord(t *testing.T) {
	big := dialer.Record{StreamID: 1, Data: bytes.Repeat([]byte{0xAB}, 128*1024)}
	small := dialer.Record{StreamID: 2, Data: optionsFrame(0x04), UseMetadataID: true}

	line, err := json.Marshal(big)
	if err != nil {
		t.Fatalf("marshalling the large record: %v", err)
	}
	if len(line) <= bufio.MaxScanTokenSize {
		t.Fatalf("the large record encodes to %d bytes, which does not exceed the old %d-byte cap", len(line), bufio.MaxScanTokenSize)
	}

	records, err := loadFramesFromFile(writeRecords(t, "Reads", big, small))
	if err != nil {
		t.Fatalf("loadFramesFromFile() error = %v, want nil", err)
	}
	if len(records) != 2 {
		t.Fatalf("loaded %d records, want 2", len(records))
	}
	if !bytes.Equal(records[1].Data, big.Data) {
		t.Errorf("record 1 holds %d bytes, want the %d recorded", len(records[1].Data), len(big.Data))
	}
	// The record that follows a large one must still be read, so a big frame cannot
	// silently truncate a recording.
	if !bytes.Equal(records[2].Data, small.Data) || !records[2].UseMetadataID {
		t.Errorf("record 2 = %+v, want %+v", records[2], small)
	}
}

// TestLoadFramesFromFileSkipsDamagedRecord pins the behaviour the read loop keeps: a
// recording is a debugging artefact, so a record that does not decode is skipped and
// the frames around it still load. The last record deliberately has no trailing
// newline, which a truncated recording also looks like.
func TestLoadFramesFromFileSkipsDamagedRecord(t *testing.T) {
	first := dialer.Record{StreamID: 1, Data: optionsFrame(0x04)}
	last := dialer.Record{StreamID: 3, Data: optionsFrame(0x04)}

	firstLine, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("marshalling the first record: %v", err)
	}
	lastLine, err := json.Marshal(last)
	if err != nil {
		t.Fatalf("marshalling the last record: %v", err)
	}

	path := filepath.Join(t.TempDir(), "Reads")
	content := append(append(firstLine, "\n{\"data\":\"not json\n"...), lastLine...)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}

	records, err := loadFramesFromFile(path)
	if err != nil {
		t.Fatalf("loadFramesFromFile() error = %v, want nil", err)
	}
	if len(records) != 2 {
		t.Fatalf("loaded %d records, want 2 (the damaged one skipped)", len(records))
	}
	if !bytes.Equal(records[1].Data, first.Data) {
		t.Errorf("record 1 = %+v, want %+v", records[1], first)
	}
	if !bytes.Equal(records[3].Data, last.Data) {
		t.Errorf("record 3 = %+v, want %+v, so a record without a trailing newline is still read", records[3], last)
	}
}

// responseFrame builds a body-carrying response frame with the given stream id, so a
// test can check which stream id came back out.
func responseFrame(streamID int, bodyLen int) []byte {
	frame := []byte{
		0x84, 0x00,
		byte(streamID >> 8), byte(streamID),
		0x08, // RESULT
		byte(bodyLen >> 24), byte(bodyLen >> 16), byte(bodyLen >> 8), byte(bodyLen),
	}
	return append(frame, bytes.Repeat([]byte{0x5A}, bodyLen)...)
}

// readAll drains the replayer until it has delivered n bytes, using a buffer of
// exactly chunk bytes so the response has to be served across several calls.
func readAll(t *testing.T, c *ConnectionReplayer, n, chunk int) []byte {
	t.Helper()

	got := make([]byte, 0, n)
	buf := make([]byte, chunk)
	for len(got) < n {
		read, err := c.Read(buf)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if read == 0 {
			t.Fatal("Read returned 0 bytes without an error")
		}
		got = append(got, buf[:read]...)
	}
	return got
}

// TestConnectionReplayerPatchesStreamIDAcrossPartialReads pins the stream-id patch
// against a caller whose buffer is smaller than the response.
//
// The patch used to be applied to the caller's buffer, and only on the branch where
// the whole response fitted — so a response larger than the buffer was replayed
// carrying the stream id it was recorded with, and the driver matched it to the wrong
// in-flight request or to none. It went unnoticed because the largest response in the
// checked-in recordings is well under the driver's 4 KiB read buffer, so the short
// branch never ran.
func TestConnectionReplayerPatchesStreamIDAcrossPartialReads(t *testing.T) {
	const (
		recordedStreamID = 0x0040
		liveStreamID     = 0x01F4
		bodyLen          = 500
	)

	request := optionsFrame(0x04)
	request[2] = byte(liveStreamID >> 8)
	request[3] = byte(liveStreamID & 0xFF)

	for _, chunk := range []int{1, 7, 64, 512, 4096} {
		t.Run(fmt.Sprintf("buffer of %d", chunk), func(t *testing.T) {
			response := responseFrame(recordedStreamID, bodyLen)
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: append([]byte(nil), response...),
				Hash:     dialer.GetFrameHash(append([]byte(nil), request...), false),
			})

			if _, err := c.Write(append([]byte(nil), request...)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			got := readAll(t, c, len(response), chunk)

			if len(got) != len(response) {
				t.Fatalf("served %d bytes, want %d", len(got), len(response))
			}
			if gotID := int(got[2])<<8 | int(got[3]); gotID != liveStreamID {
				t.Errorf("served stream id %#04x, want the live request's %#04x", gotID, liveStreamID)
			}
			// Everything except the stream id must be byte-for-byte the recording.
			if !bytes.Equal(got[4:], response[4:]) || got[0] != response[0] || got[1] != response[1] {
				t.Error("the response body was altered")
			}
		})
	}
}

// TestConnectionReplayerDoesNotMutateTheRecording pins that the patch goes into a copy.
// A FrameRecorded is shared and served once per benchmark iteration, so patching it in
// place would rewrite the recording with the first request's stream id and leave every
// later iteration depending on that.
func TestConnectionReplayerDoesNotMutateTheRecording(t *testing.T) {
	const bodyLen = 32

	recorded := responseFrame(0x0040, bodyLen)
	pristine := append([]byte(nil), recorded...)

	frame := &FrameRecorded{Response: recorded}

	for i, streamID := range []int{0x0100, 0x0200, 0x0300} {
		request := optionsFrame(0x04)
		request[2] = byte(streamID >> 8)
		request[3] = byte(streamID & 0xFF)

		c := newTestReplayer(0x04, frame)
		frame.Hash = dialer.GetFrameHash(append([]byte(nil), request...), false)

		if _, err := c.Write(append([]byte(nil), request...)); err != nil {
			t.Fatalf("iteration %d: Write: %v", i, err)
		}
		got := readAll(t, c, len(recorded), 4096)

		if gotID := int(got[2])<<8 | int(got[3]); gotID != streamID {
			t.Errorf("iteration %d: served stream id %#04x, want %#04x", i, gotID, streamID)
		}
		if !bytes.Equal(frame.Response, pristine) {
			t.Fatalf("iteration %d: the recorded response was mutated in place", i)
		}
	}
}

// TestConnectionReplayerRejectsAnEmptyRecord pins the one damaged record that cannot
// be served at all.
//
// A record with no bytes would have Read copy nothing into a buffer with room in it and
// return (0, nil), having already counted the response as delivered -- so the driver
// retries, finds nothing pending, and blocks for a request whose response is gone. A
// line like {"data":null,"stream_id":5} decodes cleanly, so only this check stands
// between a damaged recording and a hung test. It is the same check that catches a
// record truncated part-way through a frame; see the test below.
func TestConnectionReplayerRejectsAnEmptyRecord(t *testing.T) {
	request := optionsFrame(0x04)

	c := newTestReplayer(0x04, &FrameRecorded{
		Response: nil,
		Hash:     dialer.GetFrameHash(append([]byte(nil), request...), false),
	})

	if _, err := c.Write(append([]byte(nil), request...)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	n, err := c.Read(make([]byte, 16))
	if err == nil {
		t.Fatalf("Read returned (%d, nil) for an empty record, want an error", n)
	}
	if n != 0 {
		t.Errorf("Read returned %d bytes alongside its error", n)
	}
	if !strings.Contains(err.Error(), "not one whole CQL frame") {
		t.Errorf("error %q does not say the recording is at fault", err)
	}
}

// TestConnectionReplayerRejectsATruncatedRecord pins that a record which is not one
// whole frame is refused rather than served.
//
// Serving it used to be the documented behaviour, on the grounds that the driver would
// reject it as a protocol error. It cannot: the driver reads a frame with io.ReadFull
// twice -- the nine header bytes, then the body length that header declares -- and this
// connection's SetReadDeadline does nothing, so a record stopping short of a whole frame
// leaves it blocked mid-frame while Read waits for a request that is never coming.
//
// The second case is the one a length-only check misses: a complete header whose body
// arrived short hangs exactly the same way.
func TestConnectionReplayerRejectsATruncatedRecord(t *testing.T) {
	full := responseFrame(0x0040, 32)

	for _, tc := range []struct {
		name     string
		response []byte
	}{
		{"shorter than a header", []byte{0x84, 0x00}},
		{"header declares more body than arrived", full[:dialer.FrameHeaderLen+16]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := optionsFrame(0x04)

			c := newTestReplayer(0x04, &FrameRecorded{
				Response: append([]byte(nil), tc.response...),
				Hash:     dialer.GetFrameHash(append([]byte(nil), request...), false),
			})

			if _, err := c.Write(append([]byte(nil), request...)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			n, err := c.Read(make([]byte, 64))
			if err == nil {
				t.Fatalf("Read returned (%d, nil) for a truncated record, want an error", n)
			}
			if n != 0 {
				t.Errorf("Read returned %d bytes alongside its error", n)
			}
			if !strings.Contains(err.Error(), "not one whole CQL frame") {
				t.Errorf("error %q does not say the recording is at fault", err)
			}
		})
	}
}

// TestConnectionReplayerReleasesOutsizedResponseBuffers pins that neither reused buffer
// keeps an outlier's capacity for the life of the connection.
//
// Both are reused so that serving a response allocates nothing, and a recording may hold
// a response of any size the driver's frame limit allows -- so without a bound one large
// response pins it twice over, patched and encoded, for as long as the connection lives.
func TestConnectionReplayerReleasesOutsizedResponseBuffers(t *testing.T) {
	const (
		big   = 2 * maxRetainedResponse
		small = 8
	)

	first := requestFrame(0x05, 0x0001)  // OPTIONS
	second := requestFrame(0x0B, 0x0002) // REGISTER

	c := newTestReplayer(0x04,
		&FrameRecorded{
			Response: responseFrame(0x0040, big),
			Hash:     dialer.GetFrameHash(append([]byte(nil), first...), false),
		},
		&FrameRecorded{
			Response: responseFrame(0x0041, small),
			Hash:     dialer.GetFrameHash(append([]byte(nil), second...), false),
		},
	)

	if _, err := c.Write(append([]byte(nil), first...)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	readAll(t, c, dialer.FrameHeaderLen+big, 4096)

	// patched is released as soon as the response has been encoded out of it.
	if cap(c.patched) > maxRetainedResponse {
		t.Errorf("patched still holds %d bytes after an outsized response", cap(c.patched))
	}
	// outgoing is what is being served, so it is still here until the next response
	// needs the buffer.
	if cap(c.outgoing) <= maxRetainedResponse {
		t.Fatalf("outgoing holds %d bytes, want the response just served", cap(c.outgoing))
	}

	if _, err := c.Write(append([]byte(nil), second...)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	readAll(t, c, dialer.FrameHeaderLen+small, 4096)

	if cap(c.outgoing) > maxRetainedResponse {
		t.Errorf("outgoing still holds %d bytes after the outsized response drained", cap(c.outgoing))
	}
	// The bound must not cost the reuse it exists to protect: an ordinary response keeps
	// its buffer.
	if cap(c.outgoing) == 0 {
		t.Error("outgoing was released although it is within the bound")
	}
}

// TestConnectionReplayerLatchesAMaterialiseFailure pins that a response that cannot be
// materialised ends the connection rather than being served up as an error forever.
//
// frameIdx is advanced only once the response is ready, so a failure leaves Read's branch
// condition -- outgoingPos == len(outgoing) -- still true and the same record still
// pending: the next Read fetches it again and fails the same way. The recorder latches the
// analogous failure; this is the same latch, on the one goroutine that owns Read.
func TestConnectionReplayerLatchesAMaterialiseFailure(t *testing.T) {
	full := responseFrame(0x0040, 32)

	for _, tc := range []struct {
		name     string
		response []byte
	}{
		{name: "empty record", response: nil},
		{name: "truncated record", response: full[:dialer.FrameHeaderLen+16]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := optionsFrame(0x04)
			c := newTestReplayer(0x04, &FrameRecorded{
				Response: append([]byte(nil), tc.response...),
				Hash:     dialer.GetFrameHash(append([]byte(nil), request...), false),
			})
			if _, err := c.Write(append([]byte(nil), request...)); err != nil {
				t.Fatalf("Write: %v", err)
			}

			n, first := c.Read(make([]byte, 64))
			if first == nil {
				t.Fatalf("Read returned (%d, nil) for a record that is not one whole frame", n)
			}

			// The latch is reported even for a zero-length read, the way dialer.Feed
			// reports one for an empty buffer: a caller that took (0, nil) for "nothing
			// yet, still healthy" would park on a connection that is finished.
			if n, err := c.Read(nil); err == nil || !errors.Is(err, first) {
				t.Errorf("Read(nil) = (%d, %v), want the latched %v", n, err, first)
			}

			// And every later read is the same error value, not another one just like
			// it -- a fresh fmt.Errorf per call is exactly what the unlatched loop
			// produced, and errors.Is compares identity, which is what tells them apart.
			n, second := c.Read(make([]byte, 64))
			if !errors.Is(second, first) {
				t.Errorf("second Read = (%d, %v), want the latched %v", n, second, first)
			}
			if n != 0 {
				t.Errorf("second Read returned %d bytes alongside its error", n)
			}
		})
	}
}

// failAfterWriting appends what it was given and then reports a failure. That is the shape
// segment.AppendCompressed warns about: by the time a compressor is called the segment
// header has already been reserved in the destination buffer, so an error leaves bytes
// behind whatever the compressor itself did.
type failAfterWriting struct{ err error }

func (c failAfterWriting) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), c.err
}

func (failAfterWriting) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// startupV5 builds a v5 STARTUP naming a compression algorithm, which is what fixes the
// segment header layout for the rest of the connection.
func startupV5(streamID int, algorithm string) []byte {
	body := []byte{0x00, 0x01} // a [string map] of one entry
	for _, s := range []string{"COMPRESSION", algorithm} {
		body = append(body, byte(len(s)>>8), byte(len(s)))
		body = append(body, s...)
	}
	frame := []byte{
		0x05, 0x00, byte(streamID >> 8), byte(streamID), 0x01, // STARTUP
		byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body)),
	}
	return append(frame, body...)
}

// TestMaterialiseLeavesNothingServableWhenEncodingFails pins what materialise owes its
// caller when the framing refuses to encode a response.
//
// EncodeResponse encodes into the buffer it is handed and can fail after having written
// into it, so on the error path outgoing's length described bytes that were no longer the
// response it named. Nothing serves them -- outgoingPos == len(outgoing) on the way in,
// and Read latches the failure on the way out -- which is exactly why this is asserted on
// the fields: there is no way left to observe it from outside, and an invariant nothing
// can see is the kind that decays quietly.
func TestMaterialiseLeavesNothingServableWhenEncodingFails(t *testing.T) {
	const previous = 64

	for _, tc := range []struct {
		name string
		// comp covers both ways EncodeResponse fails: nil is refused before anything is
		// written, the other fails after.
		comp dialer.SegmentCompressor
	}{
		{name: "no compressor for a compressed connection", comp: nil},
		{name: "compressor fails after writing", comp: failAfterWriting{err: errors.New("compressor failed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestReplayerWith(tc.comp, 0x05)

			// Drive the framing to where a response is segmented: a STARTUP naming an
			// algorithm fixes the layout, a READY flips the switch.
			if err := c.framing.ObserveRequest(startupV5(1, "lz4")); err != nil {
				t.Fatalf("ObserveRequest: %v", err)
			}
			c.framing.ObserveResponse([]byte{0x85, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00})

			// The state materialise is entered in: a previous response, served and
			// drained, whose buffer is the one the next encode is handed.
			c.streamIdsToReplay = []int{0x0007}
			c.outgoing = make([]byte, previous, 4096)
			c.outgoingPos = previous

			response := responseFrame(0x0040, 32)
			response[0] = 0x85 // the connection is v5; a v4 frame here would be a lie
			if err := c.materialise(&FrameRecorded{Response: response}); err == nil {
				t.Fatal("a response the framing cannot encode was materialised")
			}

			if len(c.outgoing) != 0 {
				t.Errorf("outgoing still reports %d bytes after a failed encode; they are not the response it names", len(c.outgoing))
			}
			if c.outgoingPos != len(c.outgoing) {
				t.Errorf("outgoingPos = %d with len(outgoing) = %d: Read would serve bytes materialise never wrote",
					c.outgoingPos, len(c.outgoing))
			}
		})
	}
}
