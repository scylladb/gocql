// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

type Compressor = protocol.Compressor

type SegmentCompressor = protocol.SegmentCompressor

type frame = protocol.Frame
type frameBuilder = protocol.FrameBuilder

const protoVersion4 = protocol.ProtoVersion4
const protoVersion5 = protocol.ProtoVersion5

func readInt(a0 []byte) int32 { return protocol.ReadInt(a0) }

type writeOptionsFrame = protocol.WriteOptionsFrame
type writeRegisterFrame = protocol.WriteRegisterFrame
