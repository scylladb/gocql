//go:build unit

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

import implCluster "github.com/scylladb/gocql/v2/internal/cluster"

import (
	"context"
	"encoding/binary"
	"fmt"
	frm "github.com/scylladb/gocql/v2/internal/frame"
	"net"
	"net/netip"
	"time"
)

// nativeResultFixture contains preframed native cells used by metadata peers.
// It has no query, iterator, reflection serialization or scanning behavior.
type nativeResultFixture struct {
	framer  framerInterface
	err     error
	meta    resultMetadata
	numRows int
}

func fixtureBody[T any](typ DataType, codec Encoder[T], v T) ([]byte, error) {
	encoded, err := EncodeValueOwned(typ, codec, v)
	if err != nil {
		return nil, err
	}
	return encoded.Bytes().OwnedBytes(), nil
}
func encodeFixtureCell(info TypeInfo, value any) ([]byte, error) {
	typ, err := dataTypeFromInfo(info)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	switch v := value.(type) {
	case string:
		if typ.Kind() == TypeInet {
			ip, err := netip.ParseAddr(v)
			if err != nil {
				return nil, err
			}
			return fixtureBody(typ, InetCodec, ip)
		}
		return fixtureBody(typ, StringEncoder, v)
	case net.IP:
		ip, ok := netip.AddrFromSlice(v)
		if !ok {
			return nil, fmt.Errorf("invalid fixture IP")
		}
		return fixtureBody(typ, InetCodec, ip.Unmap())
	case UUID:
		return fixtureBody(typ, UUIDCodec, v)
	case []byte:
		return fixtureBody(typ, BlobCodec, BorrowBytes(v))
	case int8:
		return fixtureBody(typ, Int8, v)
	case int16:
		return fixtureBody(typ, Int16, v)
	case float32:
		return fixtureBody(typ, Float32, v)
	case map[string][]byte:
		entries := make([]Entry[string, Bytes], 0, len(v))
		for k, x := range v {
			entries = append(entries, Entry[string, Bytes]{Key: k, Value: BorrowBytes(x)})
		}
		return fixtureBody(typ, MapEncoder(StringEncoder, BlobCodec), entries)
	case bool:
		return fixtureBody(typ, Bool, v)
	case int:
		return fixtureBody(typ, Int32, int32(v))
	case int32:
		return fixtureBody(typ, Int32, v)
	case int64:
		return fixtureBody(typ, Int64, v)
	case float64:
		return fixtureBody(typ, Float64, v)
	case []string:
		if typ.Kind() == TypeSet {
			return fixtureBody(typ, SetEncoder(StringEncoder), v)
		}
		return fixtureBody(typ, ListEncoder(StringEncoder), v)
	case map[string]string:
		entries := make([]Entry[string, string], 0, len(v))
		for k, x := range v {
			entries = append(entries, Entry[string, string]{Key: k, Value: x})
		}
		return fixtureBody(typ, MapEncoder(StringEncoder, StringEncoder), entries)
	default:
		return nil, fmt.Errorf("unsupported explicit fixture value %T", value)
	}
}

func execTestStatement(ctx context.Context, s *Session, cql string, opts Options) error {
	bound, err := BindUnpreparedOwned(Statement[Unit, Unit]{CQL: cql, Params: NoParams, Rows: NoRows}, NewColumns(), Unit{})
	if err != nil {
		return err
	}
	defer bound.Close()
	if !opts.Timeout.Set {
		opts.Timeout = Option[time.Duration]{Set: true, Value: implCluster.SessionData(s).InternalCfg.Timeout}
	}
	_, err = Exec(ctx, s, bound, opts)
	return err
}

// hostInfoFromFixture exercises the same typed topology decoder as control I/O.
func hostInfoFromFixture(f *nativeResultFixture, port int) (*HostInfo, error) {
	rows, err := readV2SystemRowsVia(context.Background(), 4, 0, func(_ context.Context, _ *attemptRequest) (*responsePage, error) {
		return v2SchemaMockResponse(qrySystemLocal, f)
	}, Statement[Unit, v2TopologyRow]{CQL: qrySystemLocal, Params: NoParams, Rows: v2DecodeTopology()}, NewColumns(), Unit{})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("query returned 0 rows")
	}
	return rows[0].InternalHost(port)
}

func writeErrorResult(resp *framer, stream int, code int, msg string) {
	resp.WriteHeader(0, frm.OpError, stream)
	resp.WriteInt(int32(code))
	resp.WriteString(msg)
}

type testPreparedCol struct {
	keyspace, table, name string
	typ                   Type
}

// testPrepared describes a RESULT/Prepared reply. keyspace/table set means a
// global table spec; otherwise each column carries its own.
type testPrepared struct {
	id              uint64
	flags           int
	keyspace, table string
	cols            []testPreparedCol
	pkIndexes       []uint16
}

func (p testPrepared) write(resp *framer, stream int, writeResultMetadataID func(uint64)) {
	resp.WriteHeader(0, frm.OpResult, stream)
	resp.WriteInt(frm.ResultKindPrepared)
	resp.WriteShortBytes(binary.BigEndian.AppendUint64(nil, p.id))
	writeResultMetadataID(p.id)

	global := p.keyspace != ""
	flags := p.flags
	if global {
		flags |= frm.FlagGlobalTableSpec
	}
	resp.WriteInt(int32(flags))
	resp.WriteInt(int32(len(p.cols)))
	if resp.Proto >= protoVersion4 {
		resp.WriteInt(int32(len(p.pkIndexes)))
		for _, idx := range p.pkIndexes {
			resp.WriteShort(idx)
		}
	}
	if global {
		resp.WriteString(p.keyspace)
		resp.WriteString(p.table)
	}
	for _, c := range p.cols {
		if !global {
			resp.WriteString(c.keyspace)
			resp.WriteString(c.table)
		}
		resp.WriteString(c.name)
		resp.WriteShort(uint16(c.typ))
	}

	// <result_metadata>
	resp.WriteInt(int32(frm.FlagNoMetaData))
	resp.WriteInt(0)
}

// routingTestServer is a TestServer answering PREPARE from a fixed table of
// statements and counting how many PREPAREs each one received. Statements not
// in the table get a server error.

func int32Bytes(v int32) []byte { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
