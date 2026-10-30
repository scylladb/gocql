// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
	"github.com/scylladb/gocql/v2/internal/segment"
	"io"
)

type Compressor = protocol.Compressor
type ErrProtocol = protocol.ErrProtocol

var ErrReadHeaderTimeout = protocol.ErrReadHeaderTimeout

func NewErrProtocol(a0 string, a1 ...any) error { return protocol.NewErrProtocol(a0, a1...) }

type SegmentCompressor = protocol.SegmentCompressor

const apacheCassandraTypePrefix = cqltype.ApacheCassandraTypePrefix

func asSegmentCompressor(a0 Compressor) (segment.Compressor, error) {
	return protocol.AsSegmentCompressor(a0)
}

const defaultBufSize = protocol.DefaultBufSize

func defaultFramerFlags(a0 Compressor, a1 byte) byte { return protocol.DefaultFramerFlags(a0, a1) }

type frame = protocol.Frame
type frameBuilder = protocol.FrameBuilder
type frameWriterFunc = protocol.FrameWriterFunc
type framer = protocol.Framer
type framerInterface = protocol.FramerInterface

const headSize = protocol.HeadSize

type malformedFrameError = protocol.MalformedFrameError

func malformedFramef(a0 string, a1 ...any) malformedFrameError {
	return protocol.MalformedFramef(a0, a1...)
}
func newFramer(a0 Compressor, a1 byte) *framer { return protocol.NewFramer(a0, a1) }

type preparedMetadata = protocol.PreparedMetadata

const protoDirectionMask = protocol.ProtoDirectionMask
const protoVersion2 = protocol.ProtoVersion2
const protoVersion3 = protocol.ProtoVersion3
const protoVersion4 = protocol.ProtoVersion4
const protoVersion5 = protocol.ProtoVersion5
const protoVersionMask = protocol.ProtoVersionMask

func readHeader(a0 io.Reader, a1 []byte) (frm.FrameHeader, error) {
	return protocol.ReadHeader(a0, a1)
}
func readInt(a0 []byte) int32                 { return protocol.ReadInt(a0) }
func recoverMalformedFrame(a0 any, a1 *error) { protocol.RecoverMalformedFrame(a0, a1) }

type resultKeyspaceFrame = protocol.ResultKeyspaceFrame
type resultMetadata = protocol.ResultMetadata
type resultPreparedFrame = protocol.ResultPreparedFrame
type resultRowsFrame = protocol.ResultRowsFrame
type useKeyspaceFrame = protocol.UseKeyspaceFrame

func validateShortBytes(a0 string, a1 []byte) error { return protocol.ValidateShortBytes(a0, a1) }

type writeAuthResponseFrame = protocol.WriteAuthResponseFrame
type writeOptionsFrame = protocol.WriteOptionsFrame
type writeRegisterFrame = protocol.WriteRegisterFrame
type writeStartupFrame = protocol.WriteStartupFrame
