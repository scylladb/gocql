//go:build unit
// +build unit

package gocql

import (
	"net"
	"reflect"
	"strings"
	"testing"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

// errBody builds an ERROR frame body: <code><message> followed by whatever the
// code adds.
func errBody(proto byte, code int, msg string, rest func(f *framer)) []byte {
	f := newFramer(nil, proto)
	f.WriteInt(int32(code))
	f.WriteString(msg)
	if rest != nil {
		rest(f)
	}
	return f.Buf
}

// writeInetOnly writes an [inetaddr]: a one-byte length and the address.
func writeInetOnly(f *framer, ip net.IP) {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	f.WriteByteValue(byte(len(ip)))
	f.Buf = append(f.Buf, ip...)
}

type errMapEntry struct {
	ip   string
	code uint16
}

func writeErrMap(f *framer, entries ...errMapEntry) {
	f.WriteInt(int32(len(entries)))
	for _, e := range entries {
		writeInetOnly(f, net.ParseIP(e.ip))
		f.WriteShort(e.code)
	}
}

func parseErrorBody(proto byte, body []byte, rateLimitCode int) (frame, error) {
	f := newFramer(nil, proto)
	f.RateLimitingErrorCode = rateLimitCode
	f.Header = &frm.FrameHeader{Version: frm.ProtoVersion(proto) | protoDirectionMask, Op: frm.OpError, Stream: 7}
	f.Buf = body
	return f.ParseFrame()
}

const scyllaRateLimitCode = 0xF000

type errorFrameCase struct {
	name  string
	proto byte
	body  []byte
	want  frame
	// rateLimitCode is the code the connection negotiated for Scylla's rate
	// limit error, or 0 when the extension is off.
	rateLimitCode int
	// unread is how many trailing bytes of body the parser is expected to
	// ignore; truncating only those is not malformed.
	unread int
}

func errorFrameCases() []errorFrameCase {
	hdr := func(proto byte) frm.FrameHeader {
		return frm.FrameHeader{Version: frm.ProtoVersion(proto) | protoDirectionMask, Op: frm.OpError, Stream: 7}
	}
	base := func(proto byte, code int, msg string) frm.ErrorFrame {
		return frm.ErrorFrame{FrameHeader: hdr(proto), Code: code, Message: msg}
	}

	var cases []errorFrameCase

	for _, code := range []int{
		ErrCodeServer, ErrCodeProtocol, ErrCodeCredentials, ErrCodeOverloaded, ErrCodeBootstrapping,
		ErrCodeTruncate, ErrCodeSyntax, ErrCodeUnauthorized, ErrCodeInvalid, ErrCodeConfig,
	} {
		cases = append(cases, errorFrameCase{
			name:  "Plain" + strings.ToUpper(errCodeName(code)),
			proto: protoVersion4,
			body:  errBody(protoVersion4, code, "boom", nil),
			want:  base(protoVersion4, code, "boom"),
		})
	}

	cases = append(cases,
		errorFrameCase{
			name: "Unavailable", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeUnavailable, "unavail", func(f *framer) {
				f.WriteConsistency(LocalQuorum)
				f.WriteInt(3)
				f.WriteInt(1)
			}),
			want: &RequestErrUnavailable{ErrorFrame: base(protoVersion4, ErrCodeUnavailable, "unavail"),
				Consistency: LocalQuorum, Required: 3, Alive: 1},
		},
		errorFrameCase{
			name: "WriteTimeout", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeWriteTimeout, "wt", func(f *framer) {
				f.WriteConsistency(Quorum)
				f.WriteInt(1)
				f.WriteInt(2)
				f.WriteString("BATCH_LOG")
			}),
			want: &RequestErrWriteTimeout{ErrorFrame: base(protoVersion4, ErrCodeWriteTimeout, "wt"),
				Consistency: Quorum, Received: 1, BlockFor: 2, WriteType: "BATCH_LOG"},
		},
		errorFrameCase{
			name: "ReadTimeout", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeReadTimeout, "rt", func(f *framer) {
				f.WriteConsistency(One)
				f.WriteInt(0)
				f.WriteInt(1)
				f.WriteByteValue(1)
			}),
			want: &RequestErrReadTimeout{ErrorFrame: base(protoVersion4, ErrCodeReadTimeout, "rt"),
				Consistency: One, Received: 0, BlockFor: 1, DataPresent: 1},
		},
		errorFrameCase{
			name: "AlreadyExists", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeAlreadyExists, "exists", func(f *framer) {
				f.WriteString("ks")
				f.WriteString("tbl")
			}),
			want: &RequestErrAlreadyExists{ErrorFrame: base(protoVersion4, ErrCodeAlreadyExists, "exists"),
				Keyspace: "ks", Table: "tbl"},
		},
		errorFrameCase{
			name: "Unprepared", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeUnprepared, "unprep", func(f *framer) {
				f.WriteShortBytes([]byte{0xca, 0xfe})
			}),
			want: &RequestErrUnprepared{ErrorFrame: base(protoVersion4, ErrCodeUnprepared, "unprep"),
				StatementId: []byte{0xca, 0xfe}},
		},
		// v4 carries only a failure count; v5 replaces it with a per-replica
		// reason map and derives the count from it.
		errorFrameCase{
			name: "ReadFailureV4", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeReadFailure, "rf", func(f *framer) {
				f.WriteConsistency(All)
				f.WriteInt(1)
				f.WriteInt(3)
				f.WriteInt(2)
				f.WriteByteValue(0)
			}),
			want: &RequestErrReadFailure{ErrorFrame: base(protoVersion4, ErrCodeReadFailure, "rf"),
				Consistency: All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: false},
		},
		errorFrameCase{
			name: "ReadFailureV5", proto: protoVersion5,
			body: errBody(protoVersion5, ErrCodeReadFailure, "rf", func(f *framer) {
				f.WriteConsistency(All)
				f.WriteInt(1)
				f.WriteInt(3)
				writeErrMap(f, errMapEntry{"10.0.0.1", 1}, errMapEntry{"2001:db8::1", 2})
				f.WriteByteValue(1)
			}),
			want: &RequestErrReadFailure{ErrorFrame: base(protoVersion5, ErrCodeReadFailure, "rf"),
				Consistency: All, Received: 1, BlockFor: 3, NumFailures: 2, DataPresent: true,
				ErrorMap: ErrorMap{"10.0.0.1": 1, "2001:db8::1": 2}},
		},
		errorFrameCase{
			name: "WriteFailureV4", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeWriteFailure, "wf", func(f *framer) {
				f.WriteConsistency(Two)
				f.WriteInt(0)
				f.WriteInt(2)
				f.WriteInt(1)
				f.WriteString("SIMPLE")
			}),
			want: &RequestErrWriteFailure{ErrorFrame: base(protoVersion4, ErrCodeWriteFailure, "wf"),
				Consistency: Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE"},
		},
		errorFrameCase{
			name: "WriteFailureV5", proto: protoVersion5,
			body: errBody(protoVersion5, ErrCodeWriteFailure, "wf", func(f *framer) {
				f.WriteConsistency(Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f, errMapEntry{"10.0.0.2", 3})
				f.WriteString("SIMPLE")
			}),
			want: &RequestErrWriteFailure{ErrorFrame: base(protoVersion5, ErrCodeWriteFailure, "wf"),
				Consistency: Two, Received: 0, BlockFor: 2, NumFailures: 1, WriteType: "SIMPLE",
				ErrorMap: ErrorMap{"10.0.0.2": 3}},
		},
		errorFrameCase{
			name: "WriteFailureV5EmptyMap", proto: protoVersion5,
			body: errBody(protoVersion5, ErrCodeWriteFailure, "wf", func(f *framer) {
				f.WriteConsistency(Two)
				f.WriteInt(0)
				f.WriteInt(2)
				writeErrMap(f)
				f.WriteString("SIMPLE")
			}),
			want: &RequestErrWriteFailure{ErrorFrame: base(protoVersion5, ErrCodeWriteFailure, "wf"),
				Consistency: Two, BlockFor: 2, WriteType: "SIMPLE", ErrorMap: ErrorMap{}},
		},
		errorFrameCase{
			name: "FunctionFailure", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeFunctionFailure, "ff", func(f *framer) {
				f.WriteString("ks")
				f.WriteString("fn")
				f.WriteStringList([]string{"int", "text"})
			}),
			want: &RequestErrFunctionFailure{ErrorFrame: base(protoVersion4, ErrCodeFunctionFailure, "ff"),
				Keyspace: "ks", Function: "fn", ArgTypes: []string{"int", "text"}},
		},
		errorFrameCase{
			name: "CDCWriteFailure", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeCDCWriteFailure, "cdc", nil),
			want: &RequestErrCDCWriteFailure{ErrorFrame: base(protoVersion4, ErrCodeCDCWriteFailure, "cdc")},
		},
		errorFrameCase{
			name: "CASWriteUnknown", proto: protoVersion4,
			body: errBody(protoVersion4, ErrCodeCASWriteUnknown, "cas", func(f *framer) {
				f.WriteConsistency(Serial)
				f.WriteInt(1)
				f.WriteInt(2)
			}),
			want: &RequestErrCASWriteUnknown{ErrorFrame: base(protoVersion4, ErrCodeCASWriteUnknown, "cas"),
				Consistency: Serial, Received: 1, BlockFor: 2},
		},
		errorFrameCase{
			name: "RateLimitReached", proto: protoVersion4, rateLimitCode: scyllaRateLimitCode,
			body: errBody(protoVersion4, scyllaRateLimitCode, "slow down", func(f *framer) {
				f.WriteByteValue(byte(OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: &RequestErrRateLimitReached{ErrorFrame: base(protoVersion4, scyllaRateLimitCode, "slow down"),
				OpType: OpTypeWrite, RejectedByCoordinator: true},
		},
		// Without the extension the same code is just unknown, and its extra
		// bytes are not read.
		errorFrameCase{
			name: "RateLimitCodeWithoutExtension", proto: protoVersion4, unread: 2,
			body: errBody(protoVersion4, scyllaRateLimitCode, "slow down", func(f *framer) {
				f.WriteByteValue(byte(OpTypeWrite))
				f.WriteByteValue(1)
			}),
			want: &UnknownServerError{ErrorFrame: base(protoVersion4, scyllaRateLimitCode, "slow down")},
		},
		errorFrameCase{
			name: "Unknown", proto: protoVersion4,
			body: errBody(protoVersion4, 0x7777, "what", nil),
			want: &UnknownServerError{ErrorFrame: base(protoVersion4, 0x7777, "what")},
		},
	)
	return cases
}

func errCodeName(code int) string {
	switch code {
	case ErrCodeServer:
		return "server"
	case ErrCodeProtocol:
		return "protocol"
	case ErrCodeCredentials:
		return "credentials"
	case ErrCodeOverloaded:
		return "overloaded"
	case ErrCodeBootstrapping:
		return "bootstrapping"
	case ErrCodeTruncate:
		return "truncate"
	case ErrCodeSyntax:
		return "syntax"
	case ErrCodeUnauthorized:
		return "unauthorized"
	case ErrCodeInvalid:
		return "invalid"
	case ErrCodeConfig:
		return "config"
	}
	return "other"
}

func TestParseErrorFrame(t *testing.T) {
	t.Parallel()

	for _, tc := range errorFrameCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseErrorBody(tc.proto, tc.body, tc.rateLimitCode)
			if err != nil {
				t.Fatalf("parseFrame: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("parsed %#v\nwant   %#v", got, tc.want)
			}
			// Every server error must surface as an error with the server's
			// message, whatever its concrete type.
			e, ok := got.(error)
			if !ok || e.Error() != messageOf(tc.want) {
				t.Fatalf("parsed frame %T is not an error carrying the server message", got)
			}
		})
	}
}

// messageOf reads the server message off any parsed error, through the
// ErrorFrame each one embeds.
func messageOf(f frame) string {
	return reflect.Indirect(reflect.ValueOf(f)).FieldByName("Message").String()
}

// Every prefix of a valid error body is malformed. Decoding one must return a
// protocol error, not panic out of parseFrame.
func TestParseErrorFrameTruncated(t *testing.T) {
	t.Parallel()

	for _, tc := range errorFrameCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for n := 0; n < len(tc.body)-tc.unread; n++ {
				body := append([]byte(nil), tc.body[:n]...)
				if got, err := parseErrorBody(tc.proto, body, tc.rateLimitCode); err == nil {
					t.Fatalf("body truncated to %d of %d bytes parsed as %#v, want an error", n, len(tc.body), got)
				}
			}
		})
	}
}

func TestReadErrorMapRejectsBadAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		write   func(f *framer)
		wantErr string
	}{
		{"InvalidSize", func(f *framer) {
			f.WriteInt(1)
			f.WriteByteValue(5)
			f.Buf = append(f.Buf, 1, 2, 3, 4, 5)
			f.WriteShort(1)
		}, "invalid IP size: 5"},
		{"ZeroSize", func(f *framer) {
			f.WriteInt(1)
			f.WriteByteValue(0)
			f.WriteShort(1)
		}, "invalid IP size: 0"},
		{"ShortAddress", func(f *framer) {
			f.WriteInt(1)
			f.WriteByteValue(16)
			f.Buf = append(f.Buf, 1, 2, 3, 4)
		}, "not enough bytes in buffer to read inet"},
		{"MoreEntriesThanData", func(f *framer) {
			f.WriteInt(3)
			writeInetOnly(f, net.ParseIP("10.0.0.1"))
			f.WriteShort(1)
			// what follows is read as the next address and rejected
		}, "invalid IP size"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := errBody(protoVersion5, ErrCodeReadFailure, "rf", func(f *framer) {
				f.WriteConsistency(One)
				f.WriteInt(0)
				f.WriteInt(1)
				tc.write(f)
				f.WriteByteValue(0)
			})
			_, err := parseErrorBody(protoVersion5, body, 0)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// A negative failure count reads as an empty map rather than an error.
func TestReadErrorMapNegativeCount(t *testing.T) {
	t.Parallel()

	body := errBody(protoVersion5, ErrCodeReadFailure, "rf", func(f *framer) {
		f.WriteConsistency(One)
		f.WriteInt(0)
		f.WriteInt(1)
		f.WriteInt(-1)
		f.WriteByteValue(0)
	})
	got, err := parseErrorBody(protoVersion5, body, 0)
	if err != nil {
		t.Fatal(err)
	}
	rf := got.(*RequestErrReadFailure)
	if len(rf.ErrorMap) != 0 || rf.NumFailures != 0 {
		t.Fatalf("ErrorMap = %v, NumFailures = %d; want empty", rf.ErrorMap, rf.NumFailures)
	}
}
