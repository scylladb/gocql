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

// Native protocol v5 ("modern framing") transport segments. The wire codec lives
// in internal/segment, which the record/replay dialers share; what remains here is
// the one thing that cannot move, because it needs this package's Compressor
// interface.

package gocql

import (
	"fmt"

	"github.com/gocql/gocql/internal/segment"
)

// asSegmentCompressor asserts that compressor supports native protocol v5 segment
// (de)compression, narrowing it to the interface the segment codec takes.
//
// The ClusterConfig validation already rejects a non-segment compressor on v5, so
// this is a defensive check whose error should never surface in practice. It stays
// in this package because it is the only part of the segment path that needs
// Compressor.Name, and naming the offending compressor is the whole value of the
// message; internal/segment deliberately requires only the two Append methods, so
// that the lz4 compressor — a separate module, which cannot import internal
// packages — keeps satisfying it structurally.
//
// A nil compressor is not a failure: it is the uncompressed layout, and yields a nil
// segment.Compressor, which is exactly how internal/segment spells that.
func asSegmentCompressor(compressor Compressor) (segment.Compressor, error) {
	if compressor == nil {
		return nil, nil
	}
	segComp, ok := compressor.(SegmentCompressor)
	if !ok {
		return nil, fmt.Errorf("gocql: compressor %q does not support protocol v5 segment compression", compressor.Name())
	}
	return segComp, nil
}
