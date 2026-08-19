package dialer

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// responseV5 builds a protocol v5 response frame (direction bit set) with the given
// opcode and no body.
func responseV5(op frameOp) []byte {
	return []byte{0x85, 0x00, 0x00, 0x7B, byte(op), 0x00, 0x00, 0x00, 0x00}
}

// responseV4 is the same on protocol v4.
func responseV4(op frameOp) []byte {
	return []byte{0x84, 0x00, 0x00, 0x7B, byte(op), 0x00, 0x00, 0x00, 0x00}
}

// startupWith builds a v5 STARTUP request carrying the given options.
func startupWith(opts ...[2]string) []byte {
	return frameV5(opStartup, 0x00, stringMap(opts...))
}

// TestFramingLatchesSegmentedOnReadyOrAuthenticate pins the framing boundary.
//
// It is one frame earlier than "the handshake finished": the driver flips its own
// switch when READY or AUTHENTICATE arrives and then writes AUTH_RESPONSE through it,
// so authentication traffic is already segmented. A latch that waited for
// AUTH_SUCCESS would decode AUTH_RESPONSE and AUTH_SUCCESS as bare frames and corrupt
// every authenticated recording.
func TestFramingLatchesSegmentedOnReadyOrAuthenticate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
		want  bool
	}{
		{name: "v5 READY", frame: responseV5(opReady), want: true},
		{name: "v5 AUTHENTICATE", frame: responseV5(opAuthenticate), want: true},

		// Everything the server sends before them stays unsegmented.
		{name: "v5 SUPPORTED", frame: responseV5(opSupported), want: false},

		// AUTH_SUCCESS is already past the line; if it were the trigger, the frames
		// between it and AUTHENTICATE would have been decoded wrongly.
		{name: "v5 AUTH_SUCCESS", frame: responseV5(opAuthSuccess), want: false},

		// A v4 connection never segments anything.
		{name: "v4 READY", frame: responseV4(opReady), want: false},
		{name: "v4 AUTHENTICATE", frame: responseV4(opAuthenticate), want: false},

		{name: "short frame", frame: []byte{0x85, 0x00}, want: false},
		{name: "empty", frame: nil, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFraming(nil)
			f.ObserveResponse(tc.frame)
			if got := f.Segmented(); got != tc.want {
				t.Errorf("Segmented() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFramingLatchIsSticky pins that the switch is one-way. A later frame that is not
// READY must not unset it.
func TestFramingLatchIsSticky(t *testing.T) {
	f := NewFraming(nil)
	f.ObserveResponse(responseV5(opReady))
	f.ObserveResponse(responseV5(opResult))
	f.ObserveResponse(responseV4(opResult))

	if !f.Segmented() {
		t.Error("the framing latch was cleared by a later frame")
	}
}

// TestFramingLatchesCompressionFromStartup pins where the segment header layout comes
// from. The two layouts differ in size, so a decoder that guesses wrong misreads every
// subsequent offset.
func TestFramingLatchesCompressionFromStartup(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
		want  bool
	}{
		{
			name:  "STARTUP naming lz4",
			frame: startupWith([2]string{"CQL_VERSION", "3.0.0"}, [2]string{"COMPRESSION", "lz4"}),
			want:  true,
		},
		{
			// An option present but empty names nothing to compress with. Latching on
			// its presence alone would pick the compressed header layout, which is two
			// bytes longer, for an uncompressed stream.
			name:  "STARTUP with an empty COMPRESSION",
			frame: startupWith([2]string{"COMPRESSION", ""}),
			want:  false,
		},
		{
			name:  "STARTUP without COMPRESSION",
			frame: startupWith([2]string{"CQL_VERSION", "3.0.0"}),
			want:  false,
		},
		{
			// A SUPPORTED response advertises COMPRESSION too. Reading it as the
			// negotiated choice would turn on the compressed layout on a connection
			// that never asked for it.
			name:  "SUPPORTED advertising COMPRESSION",
			frame: frameV5(opSupported, 0x00, stringMap([2]string{"COMPRESSION", "lz4"})),
			want:  false,
		},
		{
			name:  "OPTIONS",
			frame: frameV5(opOptions, 0x00, nil),
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFraming(nil)
			if err := f.ObserveRequest(tc.frame); err != nil {
				t.Fatalf("ObserveRequest: %v", err)
			}
			if got := f.Compressed(); got != tc.want {
				t.Errorf("Compressed() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFramingRequiresACompressorForCompressedSegments pins that a missing dependency
// is reported rather than silently decoded with the wrong header size.
func TestFramingRequiresACompressorForCompressedSegments(t *testing.T) {
	f := NewFraming(nil)
	if err := f.ObserveRequest(startupWith([2]string{"COMPRESSION", "lz4"})); err != nil {
		t.Fatalf("ObserveRequest: %v", err)
	}
	f.ObserveResponse(responseV5(opReady))

	d := f.NewDecoder()
	err := d.Feed([]byte{0x00, 0x01, 0x02}, func([]byte) error { return nil })
	if !errors.Is(err, ErrSegmentCompressorRequired) {
		t.Fatalf("Feed error = %v, want ErrSegmentCompressorRequired", err)
	}

	if _, err := f.EncodeResponse(nil, responseV5(opResult)); !errors.Is(err, ErrSegmentCompressorRequired) {
		t.Errorf("EncodeResponse error = %v, want ErrSegmentCompressorRequired", err)
	}
}

// TestDecoderFollowsTheSwitch walks a decoder across the framing boundary, in the
// order a real handshake produces, and checks every frame comes back intact.
func TestDecoderFollowsTheSwitch(t *testing.T) {
	for _, cc := range compressors() {
		t.Run(cc.name, func(t *testing.T) {
			f := NewFraming(cc.comp)

			startup := startupWith([2]string{"CQL_VERSION", "3.0.0"})
			if cc.comp != nil {
				startup = startupWith([2]string{"COMPRESSION", "lz4"})
			}

			requests := f.NewDecoder()
			responses := f.NewDecoder()

			var got [][]byte
			keep := func(frame []byte) error {
				got = append(got, append([]byte(nil), frame...))
				return nil
			}
			// The response direction is what drives the latch.
			observe := func(frame []byte) error {
				f.ObserveResponse(frame)
				return keep(frame)
			}

			// OPTIONS and STARTUP: unsegmented requests.
			options := frameV5(opOptions, 0x00, nil)
			if err := requests.Feed(options, keep); err != nil {
				t.Fatalf("OPTIONS: %v", err)
			}
			if err := f.ObserveRequest(startup); err != nil {
				t.Fatalf("ObserveRequest: %v", err)
			}
			if err := requests.Feed(startup, keep); err != nil {
				t.Fatalf("STARTUP: %v", err)
			}

			// SUPPORTED and AUTHENTICATE: unsegmented responses. AUTHENTICATE flips
			// the latch as it is observed.
			supported := frameV5(opSupported, 0x00, nil)
			authenticate := responseV5(opAuthenticate)
			if err := responses.Feed(append(append([]byte(nil), supported...), authenticate...), observe); err != nil {
				t.Fatalf("SUPPORTED+AUTHENTICATE: %v", err)
			}
			if !f.Segmented() {
				t.Fatal("AUTHENTICATE did not flip the framing latch")
			}

			// AUTH_RESPONSE is the first segmented request, and AUTH_SUCCESS the first
			// segmented response. This is the boundary that is easy to get wrong.
			authResponse := frameV5(opAuthResponse, 0x00, []byte{0x00, 0x00, 0x00, 0x00})
			wire, err := AppendSegmented(nil, authResponse, cc.comp)
			if err != nil {
				t.Fatalf("encode AUTH_RESPONSE: %v", err)
			}
			if err := requests.Feed(wire, keep); err != nil {
				t.Fatalf("AUTH_RESPONSE: %v", err)
			}

			authSuccess := responseV5(opAuthSuccess)
			if wire, err = f.EncodeResponse(nil, authSuccess); err != nil {
				t.Fatalf("encode AUTH_SUCCESS: %v", err)
			}
			if err := responses.Feed(wire, observe); err != nil {
				t.Fatalf("AUTH_SUCCESS: %v", err)
			}

			want := [][]byte{options, startup, supported, authenticate, authResponse, authSuccess}
			if len(got) != len(want) {
				t.Fatalf("recovered %d frames, want %d", len(got), len(want))
			}
			for i := range want {
				if !bytes.Equal(got[i], want[i]) {
					t.Errorf("frame %d: got % X, want % X", i, got[i], want[i])
				}
			}
			if requests.pending() || responses.pending() {
				t.Error("a decoder still holds something incomplete")
			}
		})
	}
}

// TestEncodeResponseFollowsTheSwitch pins that a response is wrapped only once the
// connection has switched — before that the driver expects bare frames.
func TestEncodeResponseFollowsTheSwitch(t *testing.T) {
	f := NewFraming(nil)
	frame := responseV5(opSupported)

	before, err := f.EncodeResponse(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, frame) {
		t.Error("a pre-switch response was wrapped")
	}

	f.ObserveResponse(responseV5(opReady))

	after, err := f.EncodeResponse(nil, frame)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(after, frame) {
		t.Error("a post-switch response was not wrapped")
	}

	// And it must decode back to the frame that went in.
	s := NewSegmentSplitter(nil)
	got := decode(t, s, after)
	if len(got) != 1 || !bytes.Equal(got[0], frame) {
		t.Error("a wrapped response did not decode back to itself")
	}
}

// TestDecoderRejectsASwitchMidFrame pins the assertion behind Framing's no-straddling
// argument. The ordering makes this unreachable on a real connection; if it ever fires,
// continuing would feed the tail of a bare frame to a segment decoder.
func TestDecoderRejectsASwitchMidFrame(t *testing.T) {
	f := NewFraming(nil)
	d := f.NewDecoder()

	partial := frameV5(opQuery, 0x00, []byte("0123456789"))[:6]
	if err := d.Feed(partial, func([]byte) error { return nil }); err != nil {
		t.Fatalf("Feed: %v", err)
	}

	f.ObserveResponse(responseV5(opReady))

	err := d.Feed([]byte{0x00}, func([]byte) error { return nil })
	if err == nil {
		t.Fatal("a mid-frame switch was accepted")
	}
}

// TestObserveRequestRefusesPreV5Compression pins that a compressed connection below
// protocol v5 is refused up front.
//
// Below v5 a negotiated compressor compresses frame *bodies*, not transport segments.
// Every recorded body would be compressed bytes, which GetFrameHash cannot walk: it
// falls back to hashing the whole frame, default timestamp included, so the hash
// differs on every run and the replayer panics looking for a response to a request it
// was given. Nothing in that failure names compression as the cause.
func TestObserveRequestRefusesPreV5Compression(t *testing.T) {
	startupV4 := frameV4(opStartup, 0x00, stringMap(
		[2]string{"CQL_VERSION", "3.0.0"}, [2]string{"COMPRESSION", "lz4"}))

	f := NewFraming(nil)
	err := f.ObserveRequest(startupV4)
	if err == nil {
		t.Fatal("a v4 STARTUP naming compression was accepted")
	}
	for _, want := range []string{"lz4", "v4"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if f.Compressed() {
		t.Error("the refused connection still latched the compressed layout")
	}
}

// namedCompressor is a passthrough compressor that reports a name, as the real ones do
// -- lz4 satisfies gocql.Compressor, so it has a Name. The segment compressor interface
// deliberately does not require one, so the check is by assertion.
type namedCompressor struct{ name string }

func (c namedCompressor) Name() string { return c.name }

func (namedCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (namedCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return append(dst, src...), nil
}

// TestObserveRequestChecksTheCompressorName pins that a recording naming one algorithm
// is not replayed with a compressor for another.
//
// The segment CRCs cover the compressed payload, so they pass whatever the payload was
// compressed with, and the mismatch surfaces further on as corrupt CQL frames -- the
// outcome ErrSegmentCompressorRequired exists to avoid.
func TestObserveRequestChecksTheCompressorName(t *testing.T) {
	startup := startupWith([2]string{"COMPRESSION", "snappy"})

	t.Run("mismatch", func(t *testing.T) {
		f := NewFraming(namedCompressor{name: "lz4"})
		err := f.ObserveRequest(startup)
		if err == nil {
			t.Fatal("a snappy connection was accepted with an lz4 compressor")
		}
		for _, want := range []string{"snappy", "lz4"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("match, case-insensitively", func(t *testing.T) {
		f := NewFraming(namedCompressor{name: "SNAPPY"})
		if err := f.ObserveRequest(startup); err != nil {
			t.Fatalf("ObserveRequest: %v", err)
		}
		if !f.Compressed() {
			t.Error("a matching compressor did not latch the compressed layout")
		}
	})

	t.Run("a compressor without a name is taken as given", func(t *testing.T) {
		f := NewFraming(passthroughCompressor{})
		if err := f.ObserveRequest(startup); err != nil {
			t.Fatalf("ObserveRequest: %v", err)
		}
		if !f.Compressed() {
			t.Error("an unnamed compressor did not latch the compressed layout")
		}
	})
}

// TestDecoderFollowsTheLatchWithinOneFeed pins that the framing is re-checked at every
// frame boundary, not once per call.
//
// emit is what flips the latch -- the recorder observes the READY it has just been
// handed -- so a buffer carrying READY and the first transport segment together would
// otherwise have its remainder read as bare frames: the segment's payload length low
// byte taken for a protocol version, its next four bytes for a body length, and the
// result recorded.
func TestDecoderFollowsTheLatchWithinOneFeed(t *testing.T) {
	f := NewFraming(nil)
	d := f.NewDecoder()

	ready := responseV5(opReady)
	result := responseV5(opResult)
	segmented, err := AppendSegmented(nil, result, nil)
	if err != nil {
		t.Fatalf("AppendSegmented: %v", err)
	}

	var got [][]byte
	err = d.Feed(append(append([]byte(nil), ready...), segmented...), func(frame []byte) error {
		got = append(got, append([]byte(nil), frame...))
		f.ObserveResponse(frame)
		return nil
	})
	if err != nil {
		t.Fatalf("Feed: %v", err)
	}

	want := [][]byte{ready, result}
	if len(got) != len(want) {
		t.Fatalf("recovered %d frames, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("frame %d: got % X, want % X", i, got[i], want[i])
		}
	}
	if d.pending() {
		t.Error("the decoder still holds something incomplete")
	}
}

// TestDecoderRefusesASwitchMidFrame pins the other side of it: the switch happens
// between frames, so reaching a segment with half a bare frame buffered means either
// that reasoning is wrong or the stream is damaged, and reading on would take whatever
// the next bytes look like as a segment header.
func TestDecoderRefusesASwitchMidFrame(t *testing.T) {
	f := NewFraming(nil)
	d := f.NewDecoder()

	partial := responseV5(opResult)[:5]
	if err := d.Feed(partial, func([]byte) error {
		t.Error("a partial frame was emitted")
		return nil
	}); err != nil {
		t.Fatalf("Feed of a partial frame: %v", err)
	}

	f.ObserveResponse(responseV5(opReady))

	err := d.Feed([]byte{0x00, 0x01, 0x02}, func([]byte) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "middle of a frame") {
		t.Errorf("Feed error = %v, want a mid-frame switch to be refused", err)
	}
}
