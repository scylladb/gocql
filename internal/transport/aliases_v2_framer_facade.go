// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package transport

import protocol "github.com/scylladb/gocql/v2/internal/protocol"

import frm "github.com/scylladb/gocql/v2/internal/frame"
import "github.com/scylladb/gocql/v2/internal/segment"
import "io"

type framer = protocol.Framer

type Compressor = protocol.Compressor

var ErrReadHeaderTimeout = protocol.ErrReadHeaderTimeout

func NewErrProtocol(a0 string, a1 ...any) error { return protocol.NewErrProtocol(a0, a1...) }

func asSegmentCompressor(a0 Compressor) (segment.Compressor, error) {
	return protocol.AsSegmentCompressor(a0)
}

const defaultBufSize = protocol.DefaultBufSize

func defaultFramerFlags(a0 Compressor, a1 byte) byte { return protocol.DefaultFramerFlags(a0, a1) }

type frame = protocol.Frame
type frameBuilder = protocol.FrameBuilder

const headSize = protocol.HeadSize

func newFramer(a0 Compressor, a1 byte) *framer { return protocol.NewFramer(a0, a1) }

const protoVersion4 = protocol.ProtoVersion4

const protoVersionMask = protocol.ProtoVersionMask

func readHeader(a0 io.Reader, a1 []byte) (frm.FrameHeader, error) {
	return protocol.ReadHeader(a0, a1)
}

type resultKeyspaceFrame = protocol.ResultKeyspaceFrame

type useKeyspaceFrame = protocol.UseKeyspaceFrame

type writeAuthResponseFrame = protocol.WriteAuthResponseFrame
type writeOptionsFrame = protocol.WriteOptionsFrame

type writeStartupFrame = protocol.WriteStartupFrame
