package replayer

import (
	"bufio"
	"bytes"
	"encoding/json"
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

// newTestReplayer builds a ConnectionReplayer the way NewConnectionReplayer does,
// without going through a file on disk. Constructing the literal directly is a trap:
// the request decoder and the framing state come as a pair, and a replayer missing
// either panics on the first write.
func newTestReplayer(proto byte, frames ...*FrameRecorded) *ConnectionReplayer {
	framing := dialer.NewFraming(nil)
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
func TestConnectionReplayerRefusesAProtoV5Query(t *testing.T) {
	query := []byte{
		0x05, 0x00, // version, flags
		0x00, 0x01, // stream id
		0x07,                   // opQuery
		0x00, 0x00, 0x00, 0x00, // body length
	}
	c := newTestReplayer(0x05)

	_, err := c.Write(query)
	if err == nil {
		t.Fatal("a protocol v5 QUERY was accepted for replay")
	}
	if !strings.Contains(err.Error(), "QUERY") || !strings.Contains(err.Error(), "#1000") {
		t.Errorf("error %q does not name QUERY and scylladb/gocql#1000", err)
	}
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

	var buf bytes.Buffer
	for _, record := range records {
		line, err := json.Marshal(record)
		if err != nil {
			t.Fatalf("marshalling record %d: %v", record.StreamID, err)
		}
		buf.Write(append(line, '\n'))
	}

	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	return path
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
// between a truncated recording and a hung test.
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
	if !strings.Contains(err.Error(), "empty response frame") {
		t.Errorf("error %q does not say the recording is at fault", err)
	}
}

// TestConnectionReplayerServesShortRecordAsIs pins that a record too short to hold a
// stream id is served rather than indexed past. A recording is a file on disk and can
// be truncated.
func TestConnectionReplayerServesShortRecordAsIs(t *testing.T) {
	short := []byte{0x84, 0x00}
	request := optionsFrame(0x04)

	c := newTestReplayer(0x04, &FrameRecorded{
		Response: append([]byte(nil), short...),
		Hash:     dialer.GetFrameHash(append([]byte(nil), request...), false),
	})

	if _, err := c.Write(append([]byte(nil), request...)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	buf := make([]byte, 16)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(buf[:n], short) {
		t.Errorf("served % X, want % X", buf[:n], short)
	}
}
