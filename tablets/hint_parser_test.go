//go:build unit
// +build unit

package tablets

import (
	"encoding/binary"
	"strings"
	"testing"
)

func hintField(payload []byte) []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

func hintNullField() []byte {
	out := make([]byte, 4)
	binary.BigEndian.PutUint32(out, 0xFFFFFFFF) // CQL null: length -1
	return out
}

// A reader reports a CQL null and a truncated or wrong-length payload the same
// way, so ParseHint must not describe a malformed payload as a null value.
func TestParseHintDistinguishesNullFromMalformed(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantSub string
	}{
		{
			name:    "null first token",
			data:    hintNullField(),
			wantSub: "first token is null",
		},
		{
			name: "truncated first token",
			// Claims 8 bytes of payload but supplies 4.
			data:    append([]byte{0, 0, 0, 8}, 0, 0, 0, 0),
			wantSub: "first token: ",
		},
		{
			name:    "wrong-length first token",
			data:    hintField([]byte{1, 2, 3, 4}), // 4 bytes where bigint needs 8
			wantSub: "first token: ",
		},
		{
			name:    "null last token",
			data:    append(hintField(make([]byte, 8)), hintNullField()...),
			wantSub: "last token is null",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseHint(tc.data, "ks", "tbl")
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("got %q, want it to contain %q", err.Error(), tc.wantSub)
			}
			if tc.wantSub == "first token: " && strings.Contains(err.Error(), "is null") {
				t.Fatalf("malformed payload reported as a null value: %q", err.Error())
			}
		})
	}
}
