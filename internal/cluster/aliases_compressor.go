// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import "github.com/scylladb/gocql/v2/internal/wirecompress"

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. It uses the S2 compression algorithm,
// which is compatible with snappy and aims for high throughput.
//
// SnappyCompressor deliberately does not implement SegmentCompressor: the
// native protocol v5 spec allows only lz4 for segment compression, so
// SnappyCompressor cannot be used with ProtoVersion >= 5. Such a configuration
// is rejected up front by the cluster config validation. v5 is also not
// auto-negotiated (discoverProtocol caps at v4), so this only affects users who
// explicitly set ProtoVersion: 5.
type SnappyCompressor = wirecompress.SnappyCompressor
