/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package frame

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/scylladb/gocql/v2/internal/wire"
)

const HeaderSize = 9

var ErrStreamFrameCompression = errors.New("gocql: frame compression is unsupported for incremental receive")

// DecodeHeader is shared by buffered and streaming framing. It does not read
// or allocate a declared body; mode-specific validation follows separately.
func DecodeHeader(p []byte) (FrameHeader, error) {
	if len(p) < HeaderSize {
		return FrameHeader{}, io.ErrUnexpectedEOF
	}
	h := FrameHeader{
		Version: ProtoVersion(p[0]),
		Flags:   p[1],
		Stream:  int(int16(binary.BigEndian.Uint16(p[2:4]))),
		Op:      Op(p[4]),
		Length:  int(int32(binary.BigEndian.Uint32(p[5:9]))),
	}
	if h.Version.Version() < 3 || h.Version.Version() > 5 {
		return FrameHeader{}, fmt.Errorf("gocql: unsupported protocol response version: %d", h.Version.Version())
	}
	if h.Length < 0 {
		return FrameHeader{}, fmt.Errorf("gocql: invalid frame body length: %d", h.Length)
	}
	return h, nil
}

// CheckResponseHeader validates the common response direction, version, flags,
// opcode and event-stream rules before any body-specific parser runs.
func CheckResponseHeader(h FrameHeader, version byte) error {
	if version < 3 || version > 5 || h.Version.Request() || h.Version.Version() != version {
		return fmt.Errorf("gocql: unexpected response version %d", h.Version)
	}
	allowed := byte(FlagCompress | FlagTracing)
	if version >= 4 {
		allowed |= FlagCustomPayload | FlagWarning
	}
	if version >= 5 {
		allowed |= FlagBetaProtocol
	}
	if h.Flags & ^allowed != 0 {
		return fmt.Errorf("gocql: unsupported response flags 0x%x", h.Flags)
	}

	switch h.Op {
	case OpError, OpReady, OpAuthenticate, OpSupported, OpResult, OpEvent, OpAuthChallenge, OpAuthSuccess:
	default:
		return fmt.Errorf("gocql: unsupported response opcode %d", h.Op)
	}
	if h.Stream < 0 && (h.Op != OpEvent || h.Stream != -1) || h.Op == OpEvent && h.Stream != -1 {
		return fmt.Errorf("gocql: invalid response stream %d", h.Stream)
	}
	return nil
}

// ReadStreamEnvelope checks a native uncompressed envelope and returns an
// incremental limited body. A v5 source must already expose verified units,
// rather than raw segmented bytes. The transport must interrupt blocking I/O
// on cancellation; this helper never launches a background reader.
func ReadStreamEnvelope(ctx context.Context, source io.Reader, version byte, limits wire.StreamLimits) (FrameHeader, *wire.Stream, error) {
	if ctx == nil || source == nil {
		return FrameHeader{}, nil, wire.ErrStreamScope
	}
	if err := ctx.Err(); err != nil {
		return FrameHeader{}, nil, err
	}
	// Validate budgets before reading even the header.
	if _, err := wire.CheckStreamLimits(limits); err != nil {
		return FrameHeader{}, nil, err
	}
	if version < 3 || version > 5 {
		return FrameHeader{}, nil, fmt.Errorf("gocql: invalid streaming protocol version %d", version)
	}
	var raw [HeaderSize]byte
	if _, err := io.ReadFull(source, raw[:]); err != nil {
		return FrameHeader{}, nil, err
	}
	if err := ctx.Err(); err != nil {
		return FrameHeader{}, nil, err
	}
	h, err := DecodeHeader(raw[:])
	if err != nil {
		return FrameHeader{}, nil, err
	}
	if err := CheckResponseHeader(h, version); err != nil {
		return FrameHeader{}, nil, err
	}
	if h.Flags&FlagCompress != 0 {
		return FrameHeader{}, nil, ErrStreamFrameCompression
	}
	body, err := wire.NewStream(ctx, source, int64(h.Length), limits)
	if err != nil {
		return FrameHeader{}, nil, err
	}
	return h, body, nil
}
