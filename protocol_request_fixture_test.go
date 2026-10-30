/*
 * Copyright (C) 2026 ScyllaDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"fmt"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"time"
)

// Independent native request fixtures produce byte oracles for the canonical
// span writer. These own already-encoded test bytes only; no production request
// can enter them. Oracle self-tests are distinct from typed writer regressions.
type fixtureQueryValue struct {
	name    string
	value   []byte
	isUnset bool
}

type fixtureQueryParams struct {
	nowInSeconds          *int
	keyspace              string
	values                []fixtureQueryValue
	pagingState           []byte
	pageSize              int
	defaultTimestampValue int64
	consistency           Consistency
	serialConsistency     Consistency
	skipMeta              bool
	defaultTimestamp      bool
}

func (q fixtureQueryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v default_timestamp=%v values=%v keyspace=%s now_in_seconds=%v]",
		q.consistency, q.skipMeta, q.pageSize, q.pagingState, q.serialConsistency, q.defaultTimestamp, q.values, q.keyspace, q.nowInSeconds)
}

func (f *framer) writeFixtureQueryParams(opts *fixtureQueryParams) error {
	// Validated again here, not only in the callers' buildFrame: this function is
	// package-internal and nothing else would enforce the precondition.
	if err := f.validateV5Options(opts.keyspace, opts.nowInSeconds); err != nil {
		return err
	}

	f.writeConsistency(opts.consistency)

	var flags uint32
	names := false

	if len(opts.values) > 0 {
		flags |= frm.FlagValues
	}
	if opts.skipMeta {
		flags |= frm.FlagSkipMetaData
	}
	if opts.pageSize > 0 {
		flags |= frm.FlagPageSize
	}
	if len(opts.pagingState) > 0 {
		flags |= frm.FlagWithPagingState
	}
	if opts.serialConsistency > 0 {
		flags |= frm.FlagWithSerialConsistency
	}

	// protoV3 specific things
	if opts.defaultTimestamp {
		flags |= frm.FlagDefaultTimestamp
	}

	if len(opts.values) > 0 && opts.values[0].name != "" {
		flags |= frm.FlagWithNameValues
		names = true
	}

	if opts.keyspace != "" {
		flags |= frm.FlagWithKeyspace
	}

	if opts.nowInSeconds != nil {
		flags |= frm.FlagWithNowInSeconds
	}

	if f.proto > protoVersion4 {
		f.writeUint(flags)
	} else {
		f.writeByte(byte(flags))
	}

	if n := len(opts.values); n > 0 {
		f.writeShort(uint16(n))

		for i := 0; i < n; i++ {
			if names {
				f.writeString(opts.values[i].name)
			}
			if opts.values[i].isUnset {
				f.writeUnset()
			} else {
				f.writeBytes(opts.values[i].value)
			}
		}
	}

	if opts.pageSize > 0 {
		f.writeInt(int32(opts.pageSize))
	}

	if len(opts.pagingState) > 0 {
		f.writeBytes(opts.pagingState)
	}

	if opts.serialConsistency > 0 {
		f.writeConsistency(opts.serialConsistency)
	}

	if opts.defaultTimestamp {
		// timestamp in microseconds
		var ts int64
		if opts.defaultTimestampValue != 0 {
			ts = opts.defaultTimestampValue
		} else {
			ts = time.Now().UnixNano() / 1000
		}
		f.writeLong(ts)
	}

	if opts.keyspace != "" {
		f.writeString(opts.keyspace)
	}

	if opts.nowInSeconds != nil {
		// Bounds already validated at the top of this function.
		f.writeInt(int32(*opts.nowInSeconds))
	}
	return nil
}

type fixtureQueryFrame struct {
	customPayload map[string][]byte
	statement     string
	params        fixtureQueryParams
}

func (w *fixtureQueryFrame) String() string {
	return fmt.Sprintf("[query statement=%q params=%v]", w.statement, w.params)
}

func (w *fixtureQueryFrame) buildFrame(framer *framer, streamID int) error {
	return framer.fixtureQueryFrame(streamID, w.statement, &w.params, w.customPayload)
}

func (f *framer) fixtureQueryFrame(streamID int, statement string, params *fixtureQueryParams, customPayload map[string][]byte) error {
	// Validate before writing anything into f.buf, as the PREPARE and BATCH
	// builders do. writeFixtureQueryParams performs the same check, but by the time it
	// runs the header, the custom payload and the statement have already been
	// written, so its own "nothing was written yet" guarantee would not hold for
	// this path.
	if err := f.validateV5Options(params.keyspace, params.nowInSeconds); err != nil {
		return err
	}

	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, frm.OpQuery, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeLongString(statement)
	if err := f.writeFixtureQueryParams(params); err != nil {
		return err
	}

	return f.finish()
}

type fixtureExecuteFrame struct {
	customPayload    map[string][]byte
	preparedID       []byte
	resultMetadataID []byte
	params           fixtureQueryParams
}

func (e *fixtureExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.preparedID, &e.params)
}

func (e *fixtureExecuteFrame) buildFrame(fr *framer, streamID int) error {
	return fr.fixtureExecuteFrame(streamID, e.preparedID, e.resultMetadataID, &e.params, &e.customPayload)
}

func (f *framer) fixtureExecuteFrame(streamID int, preparedID, resultMetadataID []byte, params *fixtureQueryParams, customPayload *map[string][]byte) error {
	// Validate first, as in fixtureQueryFrame: the prepared id (and, on v5, the
	// result metadata id) are written before writeFixtureQueryParams runs.
	if err := f.validateV5Options(params.keyspace, params.nowInSeconds); err != nil {
		return err
	}
	writeResultMetadataID := f.proto > protoVersion4 || f.scyllaUseMetadataID
	if err := validateShortBytes("prepared id", preparedID); err != nil {
		return err
	}
	if writeResultMetadataID {
		if err := validateShortBytes("result metadata id", resultMetadataID); err != nil {
			return err
		}
	}

	if len(*customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, frm.OpExecute, streamID)
	f.writeCustomPayload(customPayload)
	if err := f.writeShortBytes(preparedID); err != nil {
		return err
	}

	if writeResultMetadataID {
		if err := f.writeShortBytes(resultMetadataID); err != nil {
			return err
		}
	}

	if err := f.writeFixtureQueryParams(params); err != nil {
		return err
	}

	return f.finish()
}

// TODO: can we replace BatchStatemt with batchStatement? As they prety much
// duplicate each other
type fixtureBatchStatement struct {
	preparedID []byte
	statement  string
	values     []fixtureQueryValue
}

type fixtureBatchFrame struct {
	customPayload         map[string][]byte
	nowInSeconds          *int
	keyspace              string
	statements            []fixtureBatchStatement
	defaultTimestampValue int64
	consistency           Consistency
	serialConsistency     Consistency
	typ                   BatchKind
	defaultTimestamp      bool
}

func (w *fixtureBatchFrame) buildFrame(framer *framer, streamID int) error {
	return framer.fixtureBatchFrame(streamID, w, w.customPayload)
}

func (f *framer) fixtureBatchFrame(streamID int, w *fixtureBatchFrame, customPayload map[string][]byte) error {
	// Validate everything that can fail BEFORE writing anything into f.buf, so
	// an error never leaves a partial frame in the reusable framer buffer.
	if err := f.validateV5Options(w.keyspace, w.nowInSeconds); err != nil {
		return err
	}

	// Named values are not supported in batches on any protocol version
	// (CASSANDRA-10246). Reject them up front, before any bytes are written,
	// so a rejected batch never leaves a partial frame in the reusable buffer.
	for i := range w.statements {
		for j := range w.statements[i].values {
			if w.statements[i].values[j].name != "" {
				return fmt.Errorf("gocql: named query values are not supported in batches, please see https://issues.apache.org/jira/browse/CASSANDRA-10246")
			}
		}
		if err := validateShortBytes("prepared id", w.statements[i].preparedID); err != nil {
			return err
		}
	}

	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, frm.OpBatch, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeByte(byte(w.typ))

	n := len(w.statements)
	f.writeShort(uint16(n))

	var flags uint32

	for i := 0; i < n; i++ {
		b := &w.statements[i]
		if len(b.preparedID) == 0 {
			f.writeByte(0)
			f.writeLongString(b.statement)
		} else {
			f.writeByte(1)
			if err := f.writeShortBytes(b.preparedID); err != nil {
				return err
			}
		}

		f.writeShort(uint16(len(b.values)))
		for j := range b.values {
			col := b.values[j]
			if col.isUnset {
				f.writeUnset()
			} else {
				f.writeBytes(col.value)
			}
		}
	}

	f.writeConsistency(w.consistency)

	if f.proto > protoVersion2 {
		if w.serialConsistency > 0 {
			flags |= frm.FlagWithSerialConsistency
		}
		if w.defaultTimestamp {
			flags |= frm.FlagDefaultTimestamp
		}
	}

	if w.keyspace != "" {
		flags |= frm.FlagWithKeyspace
	}

	if w.nowInSeconds != nil {
		flags |= frm.FlagWithNowInSeconds
	}

	if f.proto > protoVersion4 {
		f.writeUint(flags)
	} else {
		f.writeByte(byte(flags))
	}

	// serialConsistency and defaultTimestamp are only signalled by flags on
	// proto > v2, so their fields must only be written on proto > v2 as well;
	// otherwise the bytes would not be described by any flag and would corrupt
	// the frame. (In practice proto < v3 is unreachable: readHeader rejects
	// response versions below protoVersion3.)
	if f.proto > protoVersion2 {
		if w.serialConsistency > 0 {
			f.writeConsistency(w.serialConsistency)
		}

		if w.defaultTimestamp {
			var ts int64
			if w.defaultTimestampValue != 0 {
				ts = w.defaultTimestampValue
			} else {
				ts = time.Now().UnixNano() / 1000
			}
			f.writeLong(ts)
		}
	}

	if w.keyspace != "" {
		f.writeString(w.keyspace)
	}

	if w.nowInSeconds != nil {
		// Bounds already validated at the top of this function.
		f.writeInt(int32(*w.nowInSeconds))
	}

	return f.finish()
}
