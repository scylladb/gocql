package gocql

import (
	"github.com/gocql/gocql/marshal/tests/gen/funcs"
	"github.com/gocql/gocql/marshal/tests/gen/mod"
	"github.com/gocql/gocql/marshal/tests/gen/serialization"
	"net"
	"testing"
)

func TestMarshalsInet(t *testing.T) {
	tType := NativeType{proto: 4, typ: TypeInet}

	var nilIP net.IP = nil

	cases := serialization.Group{
		Name: tType.Type().String(),
		Funcs: funcs.Default(
			func(i interface{}) ([]byte, error) { return Marshal(tType, i) },
			func(bytes []byte, i interface{}) error { return Unmarshal(tType, bytes, i) },
		),
		DefaultMods: mod.Ref,
		Sets: []*serialization.Set{
			{
				Name:           "custom string",
				Mods:           mod.Non,
				Data:           []byte{192, 168, 0, 1},
				Values:         []interface{}{mod.String("")},
				IssueUnmarshal: "not supported by gocql",
				IssueMarshal:   "not supported by gocql",
			},
			{
				Name:   "[nil]refs",
				Mods:   mod.Non,
				Data:   nil,
				Values: mod.NilRefs("", net.IP{}),
			},
			{
				Name:  "unmarshal nil data to string",
				Mods:  mod.Non,
				Funcs: funcs.ExcludedMarshal(),
				Data:  nil,
				Value: "",
			},
			{
				Name:           "unmarshal nil data to ip",
				Mods:           mod.Non,
				Funcs:          funcs.ExcludedMarshal(),
				Data:           nil,
				Value:          nilIP,
				IssueUnmarshal: "error: cannot unmarshal inet into *net.IP: invalid sized IP: got 0 bytes not 4 or 16",
			},
			{
				Name:  "unmarshal zero data to sting",
				Funcs: funcs.ExcludedMarshal(),
				Data:  make([]byte, 0),
				Value: "",
			},
			{
				Name:           "unmarshal zero data to ip",
				Funcs:          funcs.ExcludedMarshal(),
				Data:           make([]byte, 0),
				Value:          make(net.IP, 0),
				IssueUnmarshal: "error: cannot unmarshal inet into *net.IP: invalid sized IP: got 0 bytes not 4 or 16",
			},
			{
				Name: "zeros1",
				Data: []byte{0, 0, 0, 0},
				Values: []interface{}{
					"0.0.0.0",
					net.IP{0, 0, 0, 0},
				},
			},
			{
				Name: "zeros2",
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				Values: []interface{}{
					"::",
					net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				},
			},
			{
				Name: "localhost",
				Data: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
				Values: []interface{}{
					"::1",
					net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
				},
			},
			{
				Name: "ip_v4",
				Data: []byte{192, 168, 0, 1},
				Values: []interface{}{
					"192.168.0.1",
					net.IP{192, 168, 0, 1},
				},
			},
			{
				Name: "ip_v6",
				Data: []byte("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
				Values: []interface{}{
					"fe80:cd00:0:cde:1257:0:211e:729c",
					net.IP("\xfe\x80\xcd\x00\x00\x00\x0c\xde\x12\x57\x00\x00\x21\x1e\x72\x9c"),
				},
			},
		},
	}

	cases.Gen().RunGroup(t)
}
