// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import (
	"github.com/scylladb/gocql/v2/internal/segment"
	"io"
)

// readUncompressedSegment reads a full uncompressed segment (header + payload) in
// one call, into a payload buffer of its own.
func readUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	var scratch segment.Scratch

	h, err := segment.ReadUncompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadUncompressedPayload(r, h, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// readCompressedSegment reads a full compressed segment (header + payload) in one
// call, into payload buffers of its own.
func readCompressedSegment(r io.Reader, compressor Compressor) ([]byte, bool, error) {
	segComp, err := asSegmentCompressor(compressor)
	if err != nil {
		return nil, false, err
	}

	var scratch segment.Scratch

	h, err := segment.ReadCompressedHeader(r)
	if err != nil {
		return nil, false, err
	}
	payload, err := segment.ReadCompressedPayload(r, h, segComp, &scratch)
	if err != nil {
		return nil, false, err
	}
	return payload, h.IsSelfContained, nil
}

// newUncompressedSegment returns payload as a standalone uncompressed segment.
func newUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	return segment.AppendUncompressed(nil, payload, isSelfContained)
}

// newCompressedSegment returns payload as a standalone compressed segment.
func newCompressedSegment(payload []byte, isSelfContained bool, compressor Compressor) ([]byte, error) {
	segComp, err := asSegmentCompressor(compressor)
	if err != nil {
		return nil, err
	}
	return segment.AppendCompressed(nil, payload, isSelfContained, segComp)
}

// testSegmentCompressor is a Compressor that also supports v5 segment compression,
// passing bytes through unchanged.
//
