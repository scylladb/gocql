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

// Package gocql implements the typed Cassandra and ScyllaDB native protocol API.
// Connect opens a validated Config. Statements declare parameter encoders and row
// decoders. Prepare creates shareable immutable plans; NewQuery creates a reusable
// worker-owned query whose Exec, Run, RunPage and RunUnpaged methods accept input
// directly. Parameters are copied during execution admission. Overlapping use
// returns ErrBusy; Wait joins transport writes and callbacks before reuse.
//
// NewStreamQuery decodes into seeded caller storage and buffers one response page
// on normal multiplexed connections by default. StreamOptions.Incremental opts
// into bounded incremental receive through an exclusive connection reservation.
// Page-buffered memory scales with concurrent response pages and is bounded by
// configured frame and decompression limits. Close permanently stops a query.
// Borrowed result views expire on advancement or cursor Close unless a PageLease
// retains their page. OwnedBlobCodec fills reusable owned slices with RowInto.
package gocql
