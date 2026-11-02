// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

func TestSharedBufferTransfersUnreadPrefixAndRestoresSuffix(t *testing.T) {
	conn := &aheadConn{data: []byte("header-body-next-frame")}
	reader := &InternalConnReader{InternalConn: conn, InternalR: bufio.NewReaderSize(conn, 8<<10), transferable: true}
	first := make([]byte, len("header-"))
	if _, err := reader.Read(first); err != nil {
		t.Fatal(err)
	}
	reads, deadlines := conn.reads, conn.deadlines
	if reader.InternalR.Buffered() == 0 {
		t.Fatal("fixture did not prefetch")
	}
	transferred := exclusiveReadSource{raw: reader, buffered: reader.InternalR}
	reader.InternalR = nil
	body := make([]byte, len("body-"))
	if _, err := io.ReadFull(&transferred, body); err != nil {
		t.Fatal(err)
	}
	reader.InternalR = transferred.buffered
	suffix := make([]byte, len("next-frame"))
	if _, err := reader.Read(suffix); err != nil {
		t.Fatal(err)
	}
	if string(first) != "header-" || string(body) != "body-" || string(suffix) != "next-frame" {
		t.Fatal(string(first), string(body), string(suffix))
	}
	if conn.reads != reads || conn.deadlines != deadlines {
		t.Fatal("buffered reads repeated socket/deadline work")
	}
	if !bytes.Equal(conn.data, nil) {
		t.Fatal("unconsumed source")
	}
}
