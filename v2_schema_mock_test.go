//go:build unit

package gocql

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/scylladb/gocql/v2/internal/wire"
)

// These adapters retain the existing schema fixture factories and their failure
// injection/counting hooks. Production metadata code receives only canonical
// typed request values and owned response pages through v2SystemQueryer.
func (m *schemaDataMock) queryV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	values, err := v2SchemaMockArguments(request)
	if err != nil {
		return nil, err
	}
	return v2SchemaMockResponse(request.Statement, m.querySystem(request.Statement, values...))
}

func (c *systemSchemaTestControl) queryV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return v2SchemaMockResponse(request.Statement, c.iter)
}

// Schema predicates are text values. Decode their wire framing explicitly; the
// []any is used only to call the pre-existing test fixture factory, never a
// production execution or serialization API.
func v2SchemaMockArguments(request *attemptRequest) ([]any, error) {
	cursor := wire.NewCursor(request.Values)
	values := make([]any, request.ValueCount)
	for i := range values {
		size, err := cursor.Uint32()
		if err != nil {
			return nil, err
		}
		if int32(size) < 0 {
			return nil, fmt.Errorf("schema mock argument %d is null or unset", i)
		}
		body, err := cursor.Take(int64(size))
		if err != nil {
			return nil, err
		}
		var value strings.Builder
		if err := body.Each(func(span wire.Span) error {
			_, err := value.Write(span.Bytes())
			return err
		}); err != nil {
			return nil, err
		}
		values[i] = value.String()
	}
	return values, cursor.RequireEnd()
}

func v2SchemaMockResponse(statement string, fixture *nativeResultFixture) (*responsePage, error) {
	if fixture == nil {
		return nil, errNoControl
	}
	release := func() {
		if fixture.framer != nil {
			fixture.framer.Release()
		}
	}
	if fixture.err != nil {
		release()
		return nil, fixture.err
	}
	columns, err := columnsFromInfo(fixture.meta.Columns)
	if err != nil {
		release()
		return nil, err
	}
	if columns.Len() == 0 && fixture.numRows == 0 {
		// An empty native result still carries metadata. The old fixture format
		// omitted it, so supply the actual schema of its selected system table.
		columns, err = v2SchemaMockEmptyColumns(statement)
		if err != nil {
			release()
			return nil, err
		}
	}
	var framed []byte
	for row := 0; row < fixture.numRows; row++ {
		if fixture.framer == nil {
			release()
			return nil, fmt.Errorf("schema mock row %d has no framer", row)
		}
		for column := 0; column < columns.Len(); column++ {
			body, err := fixture.framer.ReadBytesInternal()
			if err != nil {
				release()
				return nil, err
			}
			if body == nil {
				framed = binary.BigEndian.AppendUint32(framed, ^uint32(0))
			} else {
				framed = binary.BigEndian.AppendUint32(framed, uint32(len(body)))
				framed = append(framed, body...)
			}
		}
	}
	chain, err := wire.NewChain(wire.Borrow(framed))
	if err != nil {
		release()
		return nil, err
	}
	return &responsePage{responseData: responseData{Kind: 2,
		Columns:     columns,
		ColumnCount: columns.Len(),
		Rows:        chain,
		RowCount:    int32(fixture.numRows),
		Owner:       wire.NewLease(chain, release),
		State:       completePageState()},
	}, nil
}

func v2SchemaMockEmptyColumns(statement string) (Columns, error) {
	textType, _ := ScalarType(TypeVarchar)
	boolType, _ := ScalarType(TypeBoolean)
	uuidType, _ := ScalarType(TypeUUID)
	listText, _ := ListType(textType)
	mapText, _ := MapType(textType, textType)
	textColumn := func(name string) ColumnSpec { return ColumnSpec{Name: name, Type: textType} }
	switch {
	case strings.Contains(statement, "FROM system_schema.keyspaces"):
		return NewColumns(textColumn("keyspace_name"), ColumnSpec{Name: "durable_writes", Type: boolType}, ColumnSpec{Name: "replication", Type: mapText}), nil
	case strings.Contains(statement, "FROM system_schema.tables"):
		return columnsFromInfo(tableMeta.Columns)
	case strings.Contains(statement, "FROM system_schema.columns"):
		return columnsFromInfo(columnMeta.Columns)
	case strings.Contains(statement, "FROM system_schema.scylla_tables"):
		return NewColumns(textColumn("table_name")), nil
	case strings.Contains(statement, "FROM system_schema.types"):
		return NewColumns(textColumn("type_name"), ColumnSpec{Name: "field_names", Type: listText}, ColumnSpec{Name: "field_types", Type: listText}), nil
	case strings.Contains(statement, "FROM system_schema.functions"):
		return NewColumns(textColumn("function_name"), ColumnSpec{Name: "argument_types", Type: listText}, ColumnSpec{Name: "argument_names", Type: listText}, textColumn("body"), ColumnSpec{Name: "called_on_null_input", Type: boolType}, textColumn("language"), textColumn("return_type")), nil
	case strings.Contains(statement, "FROM system_schema.aggregates"):
		return NewColumns(textColumn("aggregate_name"), ColumnSpec{Name: "argument_types", Type: listText}, textColumn("final_func"), textColumn("initcond"), textColumn("return_type"), textColumn("state_func"), textColumn("state_type")), nil
	case strings.Contains(statement, "FROM system_schema.indexes"):
		return NewColumns(textColumn("index_name"), textColumn("keyspace_name"), textColumn("table_name"), textColumn("kind"), ColumnSpec{Name: "options", Type: mapText}), nil
	case strings.Contains(statement, "FROM system_schema.views"):
		return NewColumns(ColumnSpec{Name: "id", Type: uuidType}, textColumn("view_name"), ColumnSpec{Name: "base_table_id", Type: uuidType}, textColumn("base_table_name"), ColumnSpec{Name: "include_all_columns", Type: boolType}, textColumn("where_clause")), nil
	case strings.HasPrefix(statement, "DESCRIBE KEYSPACE "):
		return NewColumns(textColumn("create_statement")), nil
	default:
		return Columns{}, fmt.Errorf("schema mock has no result metadata for %q", statement)
	}
}

func (c *mockConnection) executeTyped(ctx context.Context, r *attemptRequest) (*responsePage, error) {
	return v2SchemaMockResponse(r.Statement, c.querySystem(ctx, r.Statement))
}
func (c *trackingRingConnection) executeTyped(ctx context.Context, r *attemptRequest) (*responsePage, error) {
	return v2SchemaMockResponse(r.Statement, c.querySystem(ctx, r.Statement))
}
func (f *fakeControlConn) queryV2System(ctx context.Context, r *attemptRequest) (*responsePage, error) {
	f.statement = r.Statement
	f.values = nil
	cursor := wire.NewCursor(r.Values)
	for i := 0; i < r.ValueCount; i++ {
		size, err := cursor.Uint32()
		if err != nil {
			return nil, err
		}
		body, err := cursor.Take(int64(size))
		if err != nil {
			return nil, err
		}
		view := wire.NewCursor(body)
		n, err := view.Uint32()
		if err != nil {
			return nil, err
		}
		vs := make([]string, 0, n)
		for j := uint32(0); j < n; j++ {
			size, err := view.Uint32()
			if err != nil {
				return nil, err
			}
			v, err := view.Take(int64(size))
			if err != nil {
				return nil, err
			}
			var value strings.Builder
			v.Each(func(span wire.Span) error { value.Write(span.Bytes()); return nil })
			vs = append(vs, value.String())
		}
		f.values = append(f.values, vs)
	}
	if f.iter != nil {
		return v2SchemaMockResponse(r.Statement, f.iter)
	}
	text, _ := ScalarType(TypeVarchar)
	integer, _ := ScalarType(TypeInt)
	cols := NewColumns(ColumnSpec{Name: "connection_id", Type: text}, ColumnSpec{Name: "host_id", Type: text}, ColumnSpec{Name: "address", Type: text}, ColumnSpec{Name: "port", Type: integer}, ColumnSpec{Name: "tls_port", Type: integer})
	chain, _ := wire.NewChain()
	return &responsePage{responseData: responseData{Columns: cols, ColumnCount: 5, Rows: chain, Owner: wire.NewLease(chain, nil)}}, nil
}
