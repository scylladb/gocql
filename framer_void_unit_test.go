//go:build unit
// +build unit

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"bytes"
	"testing"

	frm "github.com/gocql/gocql/internal/frame"
)

func readVoidResult(t *testing.T, f *framer, head *frm.FrameHeader, body []byte) *resultVoidFrame {
	t.Helper()
	head.Length = len(body)
	if err := f.readFrame(bytes.NewReader(body), head); err != nil {
		t.Fatal(err)
	}
	fr, err := f.parseFrame()
	if err != nil {
		t.Fatal(err)
	}
	v, ok := fr.(*resultVoidFrame)
	if !ok {
		t.Fatalf("got %T, want *resultVoidFrame", fr)
	}
	return v
}

// The void result and header now live in the framer: they must reflect the
// frame just read, not the caller's head or a previous frame.
func TestFramerVoidResultHeader(t *testing.T) {
	t.Parallel()

	f := newFramer(nil, protoVersion4)
	resp := frm.ProtoVersion(protoVersion4 | 0x80)

	head := frm.FrameHeader{Version: resp, Flags: frm.FlagWarning, Stream: 7, Op: frm.OpResult}
	// <string list ["w1"]><int kind=void>
	v := readVoidResult(t, f, &head, []byte{0, 1, 0, 2, 'w', '1', 0, 0, 0, 1})
	if v.Stream != 7 || v.Flags != frm.FlagWarning || len(v.Warnings) != 1 || v.Warnings[0] != "w1" {
		t.Fatalf("first void result header = %+v", v.FrameHeader)
	}
	if head.Warnings != nil {
		t.Fatalf("parsing wrote warnings into the caller's header: %v", head.Warnings)
	}

	head = frm.FrameHeader{Version: resp, Stream: 9, Op: frm.OpResult}
	v = readVoidResult(t, f, &head, []byte{0, 0, 0, 1})
	if v.Stream != 9 || v.Flags != 0 || v.Warnings != nil {
		t.Fatalf("reused framer void result header = %+v", v.FrameHeader)
	}
	if got := f.header.Stream; got != 9 {
		t.Fatalf("framer header stream = %d, want 9", got)
	}
}

// A detached void result must survive the framer being released and reused.
func TestDetachVoidFrameSurvivesRelease(t *testing.T) {
	t.Parallel()

	c := newTestConnWithFramerPool()
	f := c.getReadFramer()
	resp := frm.ProtoVersion(protoVersion4 | 0x80)
	head := frm.FrameHeader{Version: resp, Flags: frm.FlagWarning, Stream: 7, Op: frm.OpResult}
	v := readVoidResult(t, f, &head, []byte{0, 1, 0, 2, 'w', '1', 0, 0, 0, 1})

	d, ok := detachFrame(v).(*resultVoidFrame)
	if !ok || d == v {
		t.Fatalf("detachFrame returned %p (%T), want a copy of %p", d, d, v)
	}
	c.releaseReadFramer(f)
	if f.voidFrame.Warnings != nil || f.headerStore.Warnings != nil {
		t.Fatalf("release kept warnings: void=%v header=%v", f.voidFrame.Warnings, f.headerStore.Warnings)
	}
	// Reuse a framer for another void result; the detached copy must not see it.
	f2 := c.getReadFramer()
	head = frm.FrameHeader{Version: resp, Flags: frm.FlagWarning, Stream: 9, Op: frm.OpResult}
	readVoidResult(t, f2, &head, []byte{0, 1, 0, 2, 'w', '2', 0, 0, 0, 1})
	c.releaseReadFramer(f2)
	if d.Stream != 7 || d.Flags != frm.FlagWarning || len(d.Warnings) != 1 || d.Warnings[0] != "w1" {
		t.Fatalf("detached header changed after framer reuse: %+v", d.FrameHeader)
	}
}
