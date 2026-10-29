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
	return v2SchemaMockResponse(request.statement, m.querySystem(request.statement, values...))
}

func (c *systemSchemaTestControl) queryV2System(ctx context.Context, request *attemptRequest) (*responsePage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return v2SchemaMockResponse(request.statement, c.iter)
}

// Schema predicates are text values. Decode their wire framing explicitly; the
// []any is used only to call the pre-existing test fixture factory, never a
// production execution or serialization API.
func v2SchemaMockArguments(request *attemptRequest) ([]any, error) {
	cursor := wire.NewCursor(request.values)
	values := make([]any, request.valueCount)
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

func v2SchemaMockResponse(statement string, fixture *Iter) (*responsePage, error) {
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
	columns, err := columnsFromInfo(fixture.meta.columns)
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
	return &responsePage{
		kind:        2,
		columns:     columns,
		columnCount: columns.Len(),
		rows:        chain,
		rowCount:    int32(fixture.numRows),
		owner:       wire.NewLease(chain, release),
		state:       completePageState(),
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
		return columnsFromInfo(tableMeta.columns)
	case strings.Contains(statement, "FROM system_schema.columns"):
		return columnsFromInfo(columnMeta.columns)
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
