// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package protocol

import cqltype "github.com/scylladb/gocql/v2/internal/cqltype"

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"fmt"
	"net"
	"strconv"
	"strings"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	model "github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// typedParser folds scalar fields directly across immutable receive spans.
// Its sticky error makes every metadata read checked, without panic/recover.

type TypedParser struct {
	MaxDepth   int
	Cursor     typedMetadataCursor
	Err        error
	Proto      byte
	MetadataID bool
	owner      *wire.Lease
}

func (p *TypedParser) Check(err error) {
	if p.Err == nil {
		p.Err = err
	}
}
func (p *TypedParser) U8() byte {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint8()
	p.Check(e)
	return v
}
func (p *TypedParser) U16() uint16 {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint16()
	p.Check(e)
	return v
}
func (p *TypedParser) I32() int {
	if p.Err != nil {
		return 0
	}
	v, e := p.Cursor.Uint32()
	p.Check(e)
	return int(int32(v))
}
func (p *TypedParser) Take(n int64) wire.Chain {
	if p.Err != nil {
		return wire.Chain{}
	}
	v, e := p.Cursor.Take(n)
	p.Check(e)
	return v
}
func (p *TypedParser) Copy(n int) []byte {
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		if p.Err != nil {
			return nil
		}
		b, err := v.CopyMetadata(int64(n))
		p.Check(err)
		return b
	}
	c := p.Take(int64(n))
	if p.Err != nil {
		return nil
	}
	out := make([]byte, n)
	_, err := c.CopyTo(out)
	p.Check(err)
	return out
}
func (p *TypedParser) Str() string {
	n := int(p.U16())
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		if p.Err != nil {
			return ""
		}
		p.ReserveAllocation(int64(n))
		if p.Err != nil {
			return ""
		}
		b, err := v.CopyMetadata(int64(n))
		p.Check(err)
		return string(b)
	}
	c := p.Take(int64(n))
	if p.Err != nil {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	_ = c.Each(func(s wire.Span) error { _, err := b.Write(s.Bytes()); return err })
	return b.String()
}
func (p *TypedParser) shortBytes() []byte { return p.Copy(int(p.U16())) }
func (p *TypedParser) Bytes() []byte {
	n := p.I32()
	if n == -1 {
		return nil
	}
	if n < 0 {
		p.Check(fmt.Errorf("gocql: invalid bytes length %d", n))
		return nil
	}
	return p.Copy(n)
}
func (p *TypedParser) Count(minBytes int) int {
	n := p.I32()
	if n < 0 || int64(n) > p.Cursor.Remaining()/int64(minBytes) {
		p.Check(fmt.Errorf("gocql: invalid metadata count %d", n))
		return 0
	}
	return n
}
func (p *TypedParser) Strings() []string {
	n := int(p.U16())
	if int64(n) > p.Cursor.Remaining()/2 {
		p.Check(fmt.Errorf("gocql: invalid string list count"))
		return nil
	}
	p.ReserveAllocation(int64(n) * 16)
	if p.Err != nil {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = p.Str()
	}
	return out
}
func (p *TypedParser) dataType(depth int) DataType {
	maxDepth := p.MaxDepth
	if maxDepth == 0 {
		maxDepth = 64
	}
	if depth > maxDepth {
		p.Check(fmt.Errorf("gocql: metadata nesting exceeds %d", maxDepth))
		return DataType{}
	}
	p.ReserveAllocation(160)
	kind := Type(p.U16())
	if p.Err != nil {
		return DataType{}
	}
	var result DataType
	var err error
	switch kind {
	case TypeList, TypeSet:
		element := p.dataType(depth + 1)
		if kind == TypeList {
			result, err = ListType(element)
		} else {
			result, err = SetType(element)
		}
	case TypeMap:
		key := p.dataType(depth + 1)
		element := p.dataType(depth + 1)
		result, err = MapType(key, element)
	case TypeTuple:
		n := int(p.U16())
		if int64(n) > p.Cursor.Remaining()/2 {
			p.Check(fmt.Errorf("gocql: truncated tuple metadata"))
			return DataType{}
		}
		p.ReserveAllocation(int64(n) * 8)
		if p.Err != nil {
			return DataType{}
		}
		fields := make([]DataType, n)
		for i := range fields {
			fields[i] = p.dataType(depth + 1)
		}
		result, err = TupleType(fields...)
	case TypeUDT:
		keyspace, name := p.Str(), p.Str()
		n := int(p.U16())
		if int64(n) > p.Cursor.Remaining()/4 {
			p.Check(fmt.Errorf("gocql: truncated UDT metadata"))
			return DataType{}
		}
		p.ReserveAllocation(int64(n) * 96)
		if p.Err != nil {
			return DataType{}
		}
		fields := make([]ColumnSpec, n)
		for i := range fields {
			fields[i] = ColumnSpec{Name: p.Str(), Type: p.dataType(depth + 1)}
		}
		result, err = UDTType(keyspace, name, fields...)
	case TypeCustom:
		name := p.Str()
		if scalar := GetApacheCassandraType(name); scalar != TypeCustom {
			result, err = ScalarType(scalar)
			break
		}
		prefix := ApacheCassandraTypePrefix + "VectorType("
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, ")") {
			args := name[len(prefix) : len(name)-1]
			split := strings.LastIndex(args, ",")
			if split < 0 {
				err = fmt.Errorf("gocql: malformed vector type %q", name)
				break
			}
			dim, parseErr := strconv.ParseUint(strings.TrimSpace(args[split+1:]), 10, 16)
			if parseErr != nil || dim == 0 {
				err = fmt.Errorf("gocql: malformed vector dimensions %q", name)
				break
			}
			// This legacy adapter translates schema descriptions only, never values.
			element, parseErr := DataTypeFromInfo(GetCassandraLongType(strings.TrimSpace(args[:split]), p.Proto, NopTypeLogger{}))
			if parseErr != nil {
				err = parseErr
				break
			}
			result, err = VectorDataType(element, uint16(dim))
		} else {
			err = fmt.Errorf("gocql: unsupported custom protocol type %q", name)
		}
	default:
		result, err = ScalarType(kind)
	}
	p.Check(err)
	return result
}

type typedMetadataCursor interface {
	Uint8() (uint8, error)
	Uint16() (uint16, error)
	Uint32() (uint32, error)
	Uint64() (uint64, error)
	Take(int64) (wire.Chain, error)
	Remaining() int64
	RequireEnd() error
}
type ParsedMetadata struct {
	PagingState, MetadataID          []byte
	pkIndexes                        []int
	lwt, NoMetadata, MetadataChanged bool
}

func (p *TypedParser) ReserveAllocation(n int64) {
	if v, ok := p.Cursor.(budgetedMetadataCursor); ok {
		p.Check(v.ReserveMetadata(n))
	}
}
func (p *TypedParser) metadata(page *model.ResponseData, prepared bool, flagLWT int) (Columns, int) {
	m := ParsedMetadata{}
	cols, count := p.MetadataFields(&m, prepared, flagLWT)
	if prepared {
		page.PkIndexes = m.pkIndexes
		page.Lwt = m.lwt
	} else {
		page.NoMetadata = m.NoMetadata
		page.MetadataChanged = m.MetadataChanged
	}
	if m.PagingState != nil {
		page.PagingState = m.PagingState
	}
	if m.MetadataID != nil {
		page.MetadataID = m.MetadataID
	}
	return cols, count
}
func (p *TypedParser) MetadataFields(page *ParsedMetadata, prepared bool, flagLWT int) (Columns, int) {
	flags := uint32(p.I32())
	allowed := uint32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagNoMetaData | frm.FlagMetaDataChanged)
	if prepared {
		allowed |= uint32(flagLWT)
	}
	if flags & ^allowed != 0 {
		p.Check(fmt.Errorf("gocql: unknown metadata flags 0x%x", flags))
	}
	if !prepared {
		page.MetadataChanged = flags&uint32(frm.FlagMetaDataChanged) != 0
	}
	count := p.I32()
	if count < 0 {
		p.Check(fmt.Errorf("gocql: negative column count %d", count))
		return Columns{}, 0
	}
	if prepared && p.Proto >= 4 {
		n := p.Count(2)
		page.pkIndexes = make([]int, n)
		for i := range page.pkIndexes {
			page.pkIndexes[i] = int(p.U16())
			if page.pkIndexes[i] >= count {
				p.Check(fmt.Errorf("gocql: partition key index exceeds column count"))
			}
		}
	}
	if prepared {
		page.lwt = flagLWT != 0 && flags&uint32(flagLWT) != 0
	}
	if flags&uint32(frm.FlagHasMorePages) != 0 {
		page.PagingState = p.Bytes()
		if page.PagingState == nil {
			p.Check(fmt.Errorf("gocql: continuation flag has null paging state"))
		}
	}
	if !prepared && p.MetadataID && flags&uint32(frm.FlagMetaDataChanged) != 0 {
		page.MetadataID = p.shortBytes()
	}
	if flags&uint32(frm.FlagNoMetaData) != 0 {
		if !prepared && flags&uint32(frm.FlagMetaDataChanged) != 0 {
			p.Check(fmt.Errorf("gocql: metadata changed response omitted its column metadata"))
		}
		if !prepared {
			page.NoMetadata = true
		}
		return Columns{}, count
	}
	global := flags&uint32(frm.FlagGlobalTableSpec) != 0
	keyspace, table := "", ""
	if global {
		keyspace, table = p.Str(), p.Str()
	}
	if int64(count) > p.Cursor.Remaining()/4 {
		p.Check(fmt.Errorf("gocql: column count %d exceeds remaining metadata", count))
		return Columns{}, 0
	}
	p.ReserveAllocation(int64(count) * 96)
	if p.Err != nil {
		return Columns{}, 0
	}
	columns := make([]ColumnSpec, count)
	for i := range columns {
		if !global {
			keyspace, table = p.Str(), p.Str()
		}
		columns[i] = ColumnSpec{Keyspace: keyspace, Table: table, Name: p.Str(), Type: p.dataType(0)}
	}
	return cqltype.ColumnsOwned(columns), count
}

func (c Settings) ParseTypedResponseInto(head frm.FrameHeader, body wire.Chain, owner *wire.Lease, page *model.ResponseData) (*model.ResponseData, error) {

	if body.Len() > c.MaxDecompressedBytes {
		return nil, ErrFrameTooBig
	}
	p := &TypedParser{Cursor: wire.NewCursor(body), Proto: c.Protocol, MetadataID: c.Protocol >= 5 || c.MetadataID, owner: owner, MaxDepth: c.MaxMetadataDepth}
	*page = model.ResponseData{Owner: owner}
	if head.Flags&frm.FlagTracing != 0 {
		page.TraceID = p.Copy(16)
	}
	if head.Flags&frm.FlagWarning != 0 {
		page.Warnings = p.Strings()
	}
	if head.Flags&frm.FlagCustomPayload != 0 {
		count := int(p.U16())
		if int64(count) > p.Cursor.Remaining()/6 {
			p.Check(fmt.Errorf("gocql: payload count exceeds remaining body"))
		} else {
			page.Payload = make([]model.PayloadEntry, count)
			for i := range page.Payload {
				entry := &page.Payload[i]
				entry.Key = p.Str()
				length := p.I32()
				if length == -1 {
					continue
				}
				if length < 0 {
					p.Check(fmt.Errorf("gocql: invalid payload length %d", length))
					break
				}
				value := p.Take(int64(length))
				entry.Value = Nullable[Bytes]{Valid: true, Value: cqltype.BytesView(value, owner)}
			}
		}
	}
	if p.Err != nil {
		return nil, p.Err
	}
	switch head.Op {
	case frm.OpError:
		serverErr := p.ServerError(head, c.RateLimitingErrorCode)
		if p.Err != nil {
			return nil, p.Err
		}
		if err := p.Cursor.RequireEnd(); err != nil {
			return nil, err
		}
		return page, serverErr
	case frm.OpResult:
		page.Kind = p.I32()
		switch page.Kind {
		case frm.ResultKindVoid:
		case frm.ResultKindRows:
			page.Columns, page.ColumnCount = p.metadata(page, false, 0)
			page.RowCount = int32(p.I32())
			if page.RowCount < 0 {
				p.Check(fmt.Errorf("gocql: negative row count"))
			}
			if page.ColumnCount > 0 && int64(page.RowCount) > p.Cursor.Remaining()/int64(page.ColumnCount)/4 {
				p.Check(fmt.Errorf("gocql: rows exceed remaining response body"))
			}
			page.Rows = p.Take(p.Cursor.Remaining())
		case frm.ResultKindPrepared:
			page.PreparedID = p.shortBytes()
			if p.MetadataID {
				page.MetadataID = p.shortBytes()
			}
			page.BindColumns, _ = p.metadata(page, true, c.FlagLWT)
			page.Columns, page.ColumnCount = p.metadata(page, false, 0)
		case frm.ResultKindKeyspace:
			page.Keyspace = p.Str()
		case frm.ResultKindSchemaChanged:
			page.SchemaChange = true
			_ = p.Str()
			target := p.Str()
			_ = p.Str()
			switch target {
			case "KEYSPACE":
			case "TABLE", "TYPE":
				_ = p.Str()
			case "FUNCTION", "AGGREGATE":
				_ = p.Str()
				_ = p.Strings()
			default:
				p.Check(fmt.Errorf("gocql: unknown schema change target %q", target))
			}
		default:
			p.Check(fmt.Errorf("gocql: unknown result kind %d", page.Kind))
		}
	default:
		p.Check(fmt.Errorf("gocql: unexpected typed response opcode %v", head.Op))
	}
	if p.Err != nil {
		return nil, p.Err
	}
	if err := p.Cursor.RequireEnd(); err != nil {
		return nil, err
	}
	return page, nil
}

func (p *TypedParser) errorMap() ErrorMap {
	n := p.Count(4)
	p.ReserveAllocation(int64(n) * 64)
	if p.Err != nil {
		return nil
	}
	out := make(ErrorMap, n)
	for i := 0; i < n; i++ {
		length := int(p.U8())
		if length != 4 && length != 16 {
			p.Check(fmt.Errorf("gocql: invalid error map address size %d", length))
			break
		}
		ip := net.IP(p.Copy(length))
		out[ip.String()] = p.U16()
	}
	return out
}
func (p *TypedParser) ServerError(head frm.FrameHeader, rateLimitCode int) error {
	code, msg := p.I32(), p.Str()
	base := frm.ErrorFrame{FrameHeader: head, Code: code, Message: msg}
	switch code {
	case ErrCodeUnavailable:
		return &RequestErrUnavailable{ErrorFrame: base, Consistency: Consistency(p.U16()), Required: p.I32(), Alive: p.I32()}
	case ErrCodeWriteTimeout:
		return &RequestErrWriteTimeout{ErrorFrame: base, Consistency: Consistency(p.U16()), Received: p.I32(), BlockFor: p.I32(), WriteType: p.Str()}
	case ErrCodeReadTimeout:
		return &RequestErrReadTimeout{ErrorFrame: base, Consistency: Consistency(p.U16()), Received: p.I32(), BlockFor: p.I32(), DataPresent: p.U8()}
	case ErrCodeAlreadyExists:
		return &RequestErrAlreadyExists{ErrorFrame: base, Keyspace: p.Str(), Table: p.Str()}
	case ErrCodeUnprepared:
		return &RequestErrUnprepared{ErrorFrame: base, StatementId: p.shortBytes()}
	case ErrCodeReadFailure:
		e := &RequestErrReadFailure{ErrorFrame: base, Consistency: Consistency(p.U16()), Received: p.I32(), BlockFor: p.I32()}
		if p.Proto >= 5 {
			e.ErrorMap = p.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = p.I32()
		}
		e.DataPresent = p.U8() != 0
		return e
	case ErrCodeWriteFailure:
		e := &RequestErrWriteFailure{ErrorFrame: base, Consistency: Consistency(p.U16()), Received: p.I32(), BlockFor: p.I32()}
		if p.Proto >= 5 {
			e.ErrorMap = p.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = p.I32()
		}
		e.WriteType = p.Str()
		return e
	case ErrCodeFunctionFailure:
		return &RequestErrFunctionFailure{ErrorFrame: base, Keyspace: p.Str(), Function: p.Str(), ArgTypes: p.Strings()}
	case ErrCodeCDCWriteFailure:
		return &RequestErrCDCWriteFailure{ErrorFrame: base}
	case ErrCodeCASWriteUnknown:
		return &RequestErrCASWriteUnknown{ErrorFrame: base, Consistency: Consistency(p.U16()), Received: p.I32(), BlockFor: p.I32()}
	case ErrCodeInvalid, ErrCodeBootstrapping, ErrCodeConfig, ErrCodeCredentials, ErrCodeOverloaded, ErrCodeProtocol, ErrCodeServer, ErrCodeSyntax, ErrCodeTruncate, ErrCodeUnauthorized:
		return base
	default:
		if rateLimitCode != 0 && code == rateLimitCode {
			return &RequestErrRateLimitReached{ErrorFrame: base, OpType: OpType(p.U8()), RejectedByCoordinator: p.U8() != 0}
		}
		return &UnknownServerError{ErrorFrame: base}
	}
}

// budgetedMetadataCursor accounts all incremental metadata allocations before reading.
type budgetedMetadataCursor interface {
	CopyMetadata(int64) ([]byte, error)
	ReserveMetadata(int64) error
}

func (c Settings) ParseTypedResponse(head frm.FrameHeader, body wire.Chain, owner *wire.Lease) (*model.ResponseData, error) {
	return c.ParseTypedResponseInto(head, body, owner, new(model.ResponseData))
}
