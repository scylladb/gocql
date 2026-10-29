package gocql

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	frm "github.com/scylladb/gocql/v2/internal/frame"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// typedParser folds scalar fields directly across immutable receive spans.
// Its sticky error makes every metadata read checked, without panic/recover.
type typedParser struct {
	maxDepth   int
	cursor     typedMetadataCursor
	err        error
	proto      byte
	metadataID bool
	owner      *wire.Lease
}

func (p *typedParser) check(err error) {
	if p.err == nil {
		p.err = err
	}
}
func (p *typedParser) u8() byte {
	if p.err != nil {
		return 0
	}
	v, e := p.cursor.Uint8()
	p.check(e)
	return v
}
func (p *typedParser) u16() uint16 {
	if p.err != nil {
		return 0
	}
	v, e := p.cursor.Uint16()
	p.check(e)
	return v
}
func (p *typedParser) i32() int {
	if p.err != nil {
		return 0
	}
	v, e := p.cursor.Uint32()
	p.check(e)
	return int(int32(v))
}
func (p *typedParser) take(n int64) wire.Chain {
	if p.err != nil {
		return wire.Chain{}
	}
	v, e := p.cursor.Take(n)
	p.check(e)
	return v
}
func (p *typedParser) copy(n int) []byte {
	if v, ok := p.cursor.(*streamMetadataCursor); ok {
		if p.err != nil {
			return nil
		}
		b, err := v.copy(int64(n))
		p.check(err)
		return b
	}
	c := p.take(int64(n))
	if p.err != nil {
		return nil
	}
	out := make([]byte, n)
	_, err := c.CopyTo(out)
	p.check(err)
	return out
}
func (p *typedParser) str() string {
	n := int(p.u16())
	if v, ok := p.cursor.(*streamMetadataCursor); ok {
		if p.err != nil {
			return ""
		}
		p.reserveAllocation(int64(n))
		if p.err != nil {
			return ""
		}
		b, err := v.copy(int64(n))
		p.check(err)
		return string(b)
	}
	c := p.take(int64(n))
	if p.err != nil {
		return ""
	}
	var b strings.Builder
	b.Grow(n)
	_ = c.Each(func(s wire.Span) error { _, err := b.Write(s.Bytes()); return err })
	return b.String()
}
func (p *typedParser) shortBytes() []byte { return p.copy(int(p.u16())) }
func (p *typedParser) bytes() []byte {
	n := p.i32()
	if n == -1 {
		return nil
	}
	if n < 0 {
		p.check(fmt.Errorf("gocql: invalid bytes length %d", n))
		return nil
	}
	return p.copy(n)
}
func (p *typedParser) count(minBytes int) int {
	n := p.i32()
	if n < 0 || int64(n) > p.cursor.Remaining()/int64(minBytes) {
		p.check(fmt.Errorf("gocql: invalid metadata count %d", n))
		return 0
	}
	return n
}
func (p *typedParser) strings() []string {
	n := int(p.u16())
	if int64(n) > p.cursor.Remaining()/2 {
		p.check(fmt.Errorf("gocql: invalid string list count"))
		return nil
	}
	p.reserveAllocation(int64(n) * 16)
	if p.err != nil {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = p.str()
	}
	return out
}
func (p *typedParser) dataType(depth int) DataType {
	maxDepth := p.maxDepth
	if maxDepth == 0 {
		maxDepth = 64
	}
	if depth > maxDepth {
		p.check(fmt.Errorf("gocql: metadata nesting exceeds %d", maxDepth))
		return DataType{}
	}
	p.reserveAllocation(160)
	kind := Type(p.u16())
	if p.err != nil {
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
		n := int(p.u16())
		if int64(n) > p.cursor.Remaining()/2 {
			p.check(fmt.Errorf("gocql: truncated tuple metadata"))
			return DataType{}
		}
		p.reserveAllocation(int64(n) * 8)
		if p.err != nil {
			return DataType{}
		}
		fields := make([]DataType, n)
		for i := range fields {
			fields[i] = p.dataType(depth + 1)
		}
		result, err = TupleType(fields...)
	case TypeUDT:
		keyspace, name := p.str(), p.str()
		n := int(p.u16())
		if int64(n) > p.cursor.Remaining()/4 {
			p.check(fmt.Errorf("gocql: truncated UDT metadata"))
			return DataType{}
		}
		p.reserveAllocation(int64(n) * 96)
		if p.err != nil {
			return DataType{}
		}
		fields := make([]ColumnSpec, n)
		for i := range fields {
			fields[i] = ColumnSpec{Name: p.str(), Type: p.dataType(depth + 1)}
		}
		result, err = UDTType(keyspace, name, fields...)
	case TypeCustom:
		name := p.str()
		if scalar := getApacheCassandraType(name); scalar != TypeCustom {
			result, err = ScalarType(scalar)
			break
		}
		prefix := apacheCassandraTypePrefix + "VectorType("
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
			element, parseErr := dataTypeFromInfo(getCassandraLongType(strings.TrimSpace(args[:split]), p.proto, nopLogger{}))
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
	p.check(err)
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
type parsedMetadata struct {
	pagingState, metadataID          []byte
	pkIndexes                        []int
	lwt, noMetadata, metadataChanged bool
}

func (p *typedParser) reserveAllocation(n int64) {
	if v, ok := p.cursor.(*streamMetadataCursor); ok {
		p.check(v.reserve(n))
	}
}
func (p *typedParser) metadata(page *responsePage, prepared bool, flagLWT int) (Columns, int) {
	m := parsedMetadata{}
	cols, count := p.metadataFields(&m, prepared, flagLWT)
	if prepared {
		page.pkIndexes = m.pkIndexes
		page.lwt = m.lwt
	} else {
		page.noMetadata = m.noMetadata
		page.metadataChanged = m.metadataChanged
	}
	if m.pagingState != nil {
		page.pagingState = m.pagingState
	}
	if m.metadataID != nil {
		page.metadataID = m.metadataID
	}
	return cols, count
}
func (p *typedParser) metadataFields(page *parsedMetadata, prepared bool, flagLWT int) (Columns, int) {
	flags := uint32(p.i32())
	allowed := uint32(frm.FlagGlobalTableSpec | frm.FlagHasMorePages | frm.FlagNoMetaData | frm.FlagMetaDataChanged)
	if prepared {
		allowed |= uint32(flagLWT)
	}
	if flags & ^allowed != 0 {
		p.check(fmt.Errorf("gocql: unknown metadata flags 0x%x", flags))
	}
	if !prepared {
		page.metadataChanged = flags&uint32(frm.FlagMetaDataChanged) != 0
	}
	count := p.i32()
	if count < 0 {
		p.check(fmt.Errorf("gocql: negative column count %d", count))
		return Columns{}, 0
	}
	if prepared && p.proto >= 4 {
		n := p.count(2)
		page.pkIndexes = make([]int, n)
		for i := range page.pkIndexes {
			page.pkIndexes[i] = int(p.u16())
			if page.pkIndexes[i] >= count {
				p.check(fmt.Errorf("gocql: partition key index exceeds column count"))
			}
		}
	}
	if prepared {
		page.lwt = flagLWT != 0 && flags&uint32(flagLWT) != 0
	}
	if flags&uint32(frm.FlagHasMorePages) != 0 {
		page.pagingState = p.bytes()
		if page.pagingState == nil {
			p.check(fmt.Errorf("gocql: continuation flag has null paging state"))
		}
	}
	if !prepared && p.metadataID && flags&uint32(frm.FlagMetaDataChanged) != 0 {
		page.metadataID = p.shortBytes()
	}
	if flags&uint32(frm.FlagNoMetaData) != 0 {
		if !prepared && flags&uint32(frm.FlagMetaDataChanged) != 0 {
			p.check(fmt.Errorf("gocql: metadata changed response omitted its column metadata"))
		}
		if !prepared {
			page.noMetadata = true
		}
		return Columns{}, count
	}
	global := flags&uint32(frm.FlagGlobalTableSpec) != 0
	keyspace, table := "", ""
	if global {
		keyspace, table = p.str(), p.str()
	}
	if int64(count) > p.cursor.Remaining()/4 {
		p.check(fmt.Errorf("gocql: column count %d exceeds remaining metadata", count))
		return Columns{}, 0
	}
	p.reserveAllocation(int64(count) * 96)
	if p.err != nil {
		return Columns{}, 0
	}
	columns := make([]ColumnSpec, count)
	for i := range columns {
		if !global {
			keyspace, table = p.str(), p.str()
		}
		columns[i] = ColumnSpec{Keyspace: keyspace, Table: table, Name: p.str(), Type: p.dataType(0)}
	}
	return Columns{columns: columns}, count
}

func (c *Conn) parseTypedResponse(head frm.FrameHeader, body wire.Chain, owner *wire.Lease) (*responsePage, error) {
	limits := c.typedProtocolLimits()
	if body.Len() > limits.MaxDecompressedBytes {
		return nil, ErrFrameTooBig
	}
	p := &typedParser{cursor: wire.NewCursor(body), proto: c.version, metadataID: c.version >= 5 || c.framers.defaults.scyllaUseMetadataID, owner: owner, maxDepth: limits.MaxMetadataDepth}
	page := &responsePage{owner: owner}
	if head.Flags&frm.FlagTracing != 0 {
		page.traceID = p.copy(16)
	}
	if head.Flags&frm.FlagWarning != 0 {
		page.warnings = p.strings()
	}
	if head.Flags&frm.FlagCustomPayload != 0 {
		count := int(p.u16())
		if int64(count) > p.cursor.Remaining()/6 {
			p.check(fmt.Errorf("gocql: payload count exceeds remaining body"))
		} else {
			page.payload = make([]PayloadEntry, count)
			for i := range page.payload {
				entry := &page.payload[i]
				entry.Key = p.str()
				length := p.i32()
				if length == -1 {
					continue
				}
				if length < 0 {
					p.check(fmt.Errorf("gocql: invalid payload length %d", length))
					break
				}
				value := p.take(int64(length))
				entry.Value = Nullable[Bytes]{Valid: true, Value: Bytes{chain: value, lease: owner}}
			}
		}
	}
	if p.err != nil {
		return nil, p.err
	}
	switch head.Op {
	case frm.OpError:
		serverErr := p.serverError(head, c.framers.defaults.rateLimitingErrorCode)
		if p.err != nil {
			return nil, p.err
		}
		if err := p.cursor.RequireEnd(); err != nil {
			return nil, err
		}
		return page, serverErr
	case frm.OpResult:
		page.kind = p.i32()
		switch page.kind {
		case frm.ResultKindVoid:
		case frm.ResultKindRows:
			page.columns, page.columnCount = p.metadata(page, false, 0)
			page.rowCount = int32(p.i32())
			if page.rowCount < 0 {
				p.check(fmt.Errorf("gocql: negative row count"))
			}
			if page.columnCount > 0 && int64(page.rowCount) > p.cursor.Remaining()/int64(page.columnCount)/4 {
				p.check(fmt.Errorf("gocql: rows exceed remaining response body"))
			}
			page.rows = p.take(p.cursor.Remaining())
		case frm.ResultKindPrepared:
			page.preparedID = p.shortBytes()
			if p.metadataID {
				page.metadataID = p.shortBytes()
			}
			page.bindColumns, _ = p.metadata(page, true, c.framers.defaults.flagLWT)
			page.columns, page.columnCount = p.metadata(page, false, 0)
		case frm.ResultKindKeyspace:
			page.keyspace = p.str()
		case frm.ResultKindSchemaChanged:
			page.schemaChange = true
			_ = p.str()
			target := p.str()
			_ = p.str()
			switch target {
			case "KEYSPACE":
			case "TABLE", "TYPE":
				_ = p.str()
			case "FUNCTION", "AGGREGATE":
				_ = p.str()
				_ = p.strings()
			default:
				p.check(fmt.Errorf("gocql: unknown schema change target %q", target))
			}
		default:
			p.check(fmt.Errorf("gocql: unknown result kind %d", page.kind))
		}
	default:
		p.check(fmt.Errorf("gocql: unexpected typed response opcode %v", head.Op))
	}
	if p.err != nil {
		return nil, p.err
	}
	if err := p.cursor.RequireEnd(); err != nil {
		return nil, err
	}
	return page, nil
}

func (p *typedParser) errorMap() ErrorMap {
	n := p.count(4)
	p.reserveAllocation(int64(n) * 64)
	if p.err != nil {
		return nil
	}
	out := make(ErrorMap, n)
	for i := 0; i < n; i++ {
		length := int(p.u8())
		if length != 4 && length != 16 {
			p.check(fmt.Errorf("gocql: invalid error map address size %d", length))
			break
		}
		ip := net.IP(p.copy(length))
		out[ip.String()] = p.u16()
	}
	return out
}
func (p *typedParser) serverError(head frm.FrameHeader, rateLimitCode int) error {
	code, msg := p.i32(), p.str()
	base := frm.ErrorFrame{FrameHeader: head, Code: code, Message: msg}
	switch code {
	case ErrCodeUnavailable:
		return &RequestErrUnavailable{ErrorFrame: base, Consistency: Consistency(p.u16()), Required: p.i32(), Alive: p.i32()}
	case ErrCodeWriteTimeout:
		return &RequestErrWriteTimeout{ErrorFrame: base, Consistency: Consistency(p.u16()), Received: p.i32(), BlockFor: p.i32(), WriteType: p.str()}
	case ErrCodeReadTimeout:
		return &RequestErrReadTimeout{ErrorFrame: base, Consistency: Consistency(p.u16()), Received: p.i32(), BlockFor: p.i32(), DataPresent: p.u8()}
	case ErrCodeAlreadyExists:
		return &RequestErrAlreadyExists{ErrorFrame: base, Keyspace: p.str(), Table: p.str()}
	case ErrCodeUnprepared:
		return &RequestErrUnprepared{ErrorFrame: base, StatementId: p.shortBytes()}
	case ErrCodeReadFailure:
		e := &RequestErrReadFailure{ErrorFrame: base, Consistency: Consistency(p.u16()), Received: p.i32(), BlockFor: p.i32()}
		if p.proto >= 5 {
			e.ErrorMap = p.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = p.i32()
		}
		e.DataPresent = p.u8() != 0
		return e
	case ErrCodeWriteFailure:
		e := &RequestErrWriteFailure{ErrorFrame: base, Consistency: Consistency(p.u16()), Received: p.i32(), BlockFor: p.i32()}
		if p.proto >= 5 {
			e.ErrorMap = p.errorMap()
			e.NumFailures = len(e.ErrorMap)
		} else {
			e.NumFailures = p.i32()
		}
		e.WriteType = p.str()
		return e
	case ErrCodeFunctionFailure:
		return &RequestErrFunctionFailure{ErrorFrame: base, Keyspace: p.str(), Function: p.str(), ArgTypes: p.strings()}
	case ErrCodeCDCWriteFailure:
		return &RequestErrCDCWriteFailure{ErrorFrame: base}
	case ErrCodeCASWriteUnknown:
		return &RequestErrCASWriteUnknown{ErrorFrame: base, Consistency: Consistency(p.u16()), Received: p.i32(), BlockFor: p.i32()}
	case ErrCodeInvalid, ErrCodeBootstrapping, ErrCodeConfig, ErrCodeCredentials, ErrCodeOverloaded, ErrCodeProtocol, ErrCodeServer, ErrCodeSyntax, ErrCodeTruncate, ErrCodeUnauthorized:
		return base
	default:
		if rateLimitCode != 0 && code == rateLimitCode {
			return &RequestErrRateLimitReached{ErrorFrame: base, OpType: OpType(p.u8()), RejectedByCoordinator: p.u8() != 0}
		}
		return &UnknownServerError{ErrorFrame: base}
	}
}
