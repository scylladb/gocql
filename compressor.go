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
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

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
