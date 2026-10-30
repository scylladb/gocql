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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package transport

import (
	"context"

	"fmt"

	"time"

	frm "github.com/scylladb/gocql/v2/internal/frame"
)

type ObservedFrameHeader struct {
	// StartHeader is the time we started reading the frame header off the network connection.
	//
	// On protocol v5 the header arrives inside a transport segment, so this is
	// when the read of that segment began. Frames packed into one self-contained
	// segment therefore share a window: they did all arrive in the same read.
	Start time.Time
	// EndHeader is the time we finished reading the frame header off the network connection.
	//
	// On protocol v5 this is when the segment carrying the header had been read —
	// or, for a frame whose header is itself split across segments, when the last
	// segment needed to complete the 9 header bytes arrived. It is not extended to
	// cover the rest of a frame spanning further segments.
	End time.Time
	// Host is Host of the connection the frame header was read from.
	Host    *HostInfo
	Length  int32
	Stream  int16
	Version frm.ProtoVersion
	Flags   byte
	Opcode  frm.Op
}

func (f ObservedFrameHeader) String() string {
	return fmt.Sprintf("[observed header version=%s flags=0x%x stream=%d op=%s length=%d]", f.Version, f.Flags, f.Stream, f.Opcode, f.Length)
}

// FrameHeaderObserver is the interface implemented by frame observers / stat collectors.
//
// Experimental, this interface and use may change
type FrameHeaderObserver interface {
	// ObserveFrameHeader gets called on every received frame header.
	ObserveFrameHeader(context.Context, ObservedFrameHeader)
}
