// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"github.com/scylladb/gocql/v2/internal/cqltype"
	frm "github.com/scylladb/gocql/v2/internal/frame"

	"github.com/scylladb/gocql/v2/internal/segment"
	"io"
)

const apacheCassandraTypePrefix = cqltype.ApacheCassandraTypePrefix

func asSegmentCompressor(a0 Compressor) (segment.Compressor, error) {
	return AsSegmentCompressor(a0)
}

const defaultBufSize = DefaultBufSize

func defaultFramerFlags(a0 Compressor, a1 byte) byte { return DefaultFramerFlags(a0, a1) }

type frame = Frame
type frameBuilder = FrameBuilder
type frameWriterFunc = FrameWriterFunc
type framer = Framer
type framerInterface = FramerInterface

const headSize = HeadSize

type malformedFrameError = MalformedFrameError

func malformedFramef(a0 string, a1 ...any) malformedFrameError {
	return MalformedFramef(a0, a1...)
}
func newFramer(a0 Compressor, a1 byte) *framer { return NewFramer(a0, a1) }

type preparedMetadata = PreparedMetadata

const protoDirectionMask = ProtoDirectionMask
const protoVersion2 = ProtoVersion2
const protoVersion3 = ProtoVersion3
const protoVersion4 = ProtoVersion4
const protoVersion5 = ProtoVersion5
const protoVersionMask = ProtoVersionMask

func readHeader(a0 io.Reader, a1 []byte) (frm.FrameHeader, error) {
	return ReadHeader(a0, a1)
}
func readInt(a0 []byte) int32                 { return ReadInt(a0) }
func recoverMalformedFrame(a0 any, a1 *error) { RecoverMalformedFrame(a0, a1) }

type resultKeyspaceFrame = ResultKeyspaceFrame
type resultMetadata = ResultMetadata
type resultPreparedFrame = ResultPreparedFrame
type resultRowsFrame = ResultRowsFrame
type useKeyspaceFrame = UseKeyspaceFrame

func validateShortBytes(a0 string, a1 []byte) error { return ValidateShortBytes(a0, a1) }

type writeAuthResponseFrame = WriteAuthResponseFrame
type writeOptionsFrame = WriteOptionsFrame
type writeRegisterFrame = WriteRegisterFrame
type writeStartupFrame = WriteStartupFrame
