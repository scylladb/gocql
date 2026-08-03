package replayer

import (
	"errors"
	"testing"

	"github.com/gocql/gocql/dialer"
)

// optionsFrame builds a body-less OPTIONS frame with the given version byte.
func optionsFrame(version byte) []byte {
	return []byte{version, 0x00, 0x00, 0x01, 0x05, 0x00, 0x00, 0x00, 0x00}
}

// TestConnectionReplayerRejectsProtoV5 pins the rejection path dkropachev asked
// for: past the handshake a v5+ connection carries transport segments, which
// the replayer can neither hash for matching nor patch stream ids into without
// breaking the segment CRCs. The handshake frames are never segmented, so the
// version byte of the first write is genuine and the connection fails there —
// with an explicit error, not the unmatched-response panic.
func TestConnectionReplayerRejectsProtoV5(t *testing.T) {
	c := &ConnectionReplayer{gotRequest: make(chan struct{}, 1)}

	n, err := c.Write(optionsFrame(0x05))
	if !errors.Is(err, dialer.ErrProtoV5NotSupported) {
		t.Fatalf("Write(v5 frame) error = %v, want ErrProtoV5NotSupported", err)
	}
	if n != 0 {
		t.Errorf("Write(v5 frame) reported %d bytes written, want 0", n)
	}
}

// TestConnectionReplayerAcceptsProtoV4 pins that the rejection is scoped to
// v5+: a v4 frame with a matching recorded hash replays normally.
func TestConnectionReplayerAcceptsProtoV4(t *testing.T) {
	req := optionsFrame(0x04)
	c := &ConnectionReplayer{
		frames:     []*FrameRecorded{{Response: optionsFrame(0x84), Hash: dialer.GetFrameHash(req, false)}},
		gotRequest: make(chan struct{}, 1),
	}

	n, err := c.Write(req)
	if err != nil {
		t.Fatalf("Write(v4 frame) error = %v, want nil", err)
	}
	if n != len(req) {
		t.Errorf("Write(v4 frame) = %d bytes, want %d", n, len(req))
	}
}
