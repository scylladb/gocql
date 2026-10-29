//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"reflect"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

func parseOpBody(op frm.Op, body []byte) (frame, error) {
	f := newFramer(nil, protoVersion4)
	f.header = &frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: op, Stream: 3}
	f.buf = body
	return f.parseFrame()
}

func bodyOf(write func(f *framer)) []byte {
	f := newFramer(nil, protoVersion4)
	write(f)
	return f.buf
}

func TestParseAuthFrames(t *testing.T) {
	t.Parallel()

	hdr := func(op frm.Op) frm.FrameHeader {
		return frm.FrameHeader{Version: protoVersion4 | protoDirectionMask, Op: op, Stream: 3}
	}
	token := []byte{0x01, 0x02, 0x03}

	tests := []struct {
		name string
		op   frm.Op
		body []byte
		want frame
	}{
		{"Authenticate", frm.OpAuthenticate,
			bodyOf(func(f *framer) { f.writeString("org.apache.cassandra.auth.PasswordAuthenticator") }),
			&frm.AuthenticateFrame{FrameHeader: hdr(frm.OpAuthenticate), Class: "org.apache.cassandra.auth.PasswordAuthenticator"}},
		{"Challenge", frm.OpAuthChallenge,
			bodyOf(func(f *framer) { f.writeBytes(token) }),
			&frm.AuthChallengeFrame{FrameHeader: hdr(frm.OpAuthChallenge), Data: token}},
		{"ChallengeNull", frm.OpAuthChallenge,
			bodyOf(func(f *framer) { f.writeInt(-1) }),
			&frm.AuthChallengeFrame{FrameHeader: hdr(frm.OpAuthChallenge)}},
		{"Success", frm.OpAuthSuccess,
			bodyOf(func(f *framer) { f.writeBytes(token) }),
			&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess), Data: token}},
		// SASL mechanisms distinguish an empty final token from none at all.
		{"SuccessEmpty", frm.OpAuthSuccess,
			bodyOf(func(f *framer) { f.writeBytes([]byte{}) }),
			&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess), Data: []byte{}}},
		{"SuccessNull", frm.OpAuthSuccess,
			bodyOf(func(f *framer) { f.writeInt(-1) }),
			&frm.AuthSuccessFrame{FrameHeader: hdr(frm.OpAuthSuccess)}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseOpBody(tc.op, tc.body)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parsed %#v\nwant   %#v", got, tc.want)
			}

			for n := 0; n < len(tc.body); n++ {
				if got, err := parseOpBody(tc.op, append([]byte(nil), tc.body[:n]...)); err == nil {
					t.Fatalf("body truncated to %d of %d bytes parsed as %#v, want an error", n, len(tc.body), got)
				}
			}
		})
	}
}

// Auth frames are returned after the framer goes back to its pool, so their
// tokens must not alias the read buffer.
func TestParseAuthFrameDataIsCopied(t *testing.T) {
	t.Parallel()

	for _, op := range []frm.Op{frm.OpAuthChallenge, frm.OpAuthSuccess} {
		body := bodyOf(func(f *framer) { f.writeBytes([]byte("token")) })
		got, err := parseOpBody(op, body)
		if err != nil {
			t.Fatal(err)
		}
		for i := range body {
			body[i] = 0xff
		}
		var data []byte
		switch fr := got.(type) {
		case *frm.AuthChallengeFrame:
			data = fr.Data
		case *frm.AuthSuccessFrame:
			data = fr.Data
		}
		if !bytes.Equal(data, []byte("token")) {
			t.Fatalf("%v: Data = %q after the read buffer was overwritten", op, data)
		}
	}
}
