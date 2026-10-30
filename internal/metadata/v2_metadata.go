// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import "maps"
import "context"
import "errors"
import "fmt"
import "sort"
import "strconv"
import "strings"
import "golang.org/x/sync/errgroup"

func cloneV2Any(v any) any {
	switch x := v.(type) {
	case map[string]any:
		return InternalCloneV2AnyMap(x)
	case map[string]string:
		return maps.Clone(x)
	case []byte:
		return append([]byte(nil), x...)
	case []string:
		return append([]string(nil), x...)
	case []any:
		r := make([]any, len(x))
		for i, v := range x {
			r[i] = cloneV2Any(v)
		}
		return r
	default:
		return v
	}
}
func InternalCloneV2AnyMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	r := make(map[string]any, len(m))
	for k, v := range m {
		r[k] = cloneV2Any(v)
	}
	return r
}
func cloneV2Column(c *ColumnMetadata) *ColumnMetadata {
	if c == nil {
		return nil
	}
	r := *c
	r.Index.Options = InternalCloneV2AnyMap(c.Index.Options)
	return &r
}
func cloneV2Columns(c map[string]*ColumnMetadata) map[string]*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make(map[string]*ColumnMetadata, len(c))
	for k, v := range c {
		r[k] = cloneV2Column(v)
	}
	return r
}
func cloneV2ColumnList(c []*ColumnMetadata) []*ColumnMetadata {
	if c == nil {
		return nil
	}
	r := make([]*ColumnMetadata, len(c))
	for i, v := range c {
		r[i] = cloneV2Column(v)
	}
	return r
}
func cloneV2TableOptions(o TableMetadataOptions) TableMetadataOptions {
	o.Caching = maps.Clone(o.Caching)
	o.Compaction = maps.Clone(o.Compaction)
	o.Compression = maps.Clone(o.Compression)
	o.CDC = maps.Clone(o.CDC)
	return o
}
func cloneV2Function(f FunctionMetadata) FunctionMetadata {
	f.ArgumentTypes = append([]string(nil), f.ArgumentTypes...)
	f.ArgumentNames = append([]string(nil), f.ArgumentNames...)
	return f
}
func InternalCloneV2Keyspace(k *KeyspaceMetadata) *KeyspaceMetadata {
	if k == nil {
		return nil
	}
	r := *k
	r.StrategyOptions = InternalCloneV2AnyMap(k.StrategyOptions)
	KeyspaceData(&r).InternalTablesInvalidated = maps.Clone(KeyspaceData(k).InternalTablesInvalidated)
	r.Tables = make(map[string]*TableMetadata, len(k.Tables))
	for n, t := range k.Tables {
		if t == nil {
			r.Tables[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = InternalCloneV2AnyMap(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Flags = append([]string(nil), t.Flags...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Tables[n] = &v
	}
	r.Functions = make(map[string]*FunctionMetadata, len(k.Functions))
	for n, f := range k.Functions {
		if f == nil {
			r.Functions[n] = nil
			continue
		}
		v := cloneV2Function(*f)
		r.Functions[n] = &v
	}
	r.Aggregates = make(map[string]*AggregateMetadata, len(k.Aggregates))
	for n, a := range k.Aggregates {
		if a == nil {
			r.Aggregates[n] = nil
			continue
		}
		v := *a
		v.ArgumentTypes = append([]string(nil), a.ArgumentTypes...)
		v.FinalFunc = cloneV2Function(a.FinalFunc)
		v.StateFunc = cloneV2Function(a.StateFunc)
		r.Aggregates[n] = &v
	}
	r.Types = make(map[string]*TypeMetadata, len(k.Types))
	for n, t := range k.Types {
		if t == nil {
			r.Types[n] = nil
			continue
		}
		v := *t
		v.FieldNames = append([]string(nil), t.FieldNames...)
		v.FieldTypes = append([]string(nil), t.FieldTypes...)
		r.Types[n] = &v
	}
	r.Views = make(map[string]*ViewMetadata, len(k.Views))
	for n, t := range k.Views {
		if t == nil {
			r.Views[n] = nil
			continue
		}
		v := *t
		v.Columns = cloneV2Columns(t.Columns)
		v.Extensions = InternalCloneV2AnyMap(t.Extensions)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		v.Options = cloneV2TableOptions(t.Options)
		r.Views[n] = &v
	}
	r.Indexes = make(map[string]*IndexMetadata, len(k.Indexes))
	for n, t := range k.Indexes {
		if t == nil {
			r.Indexes[n] = nil
			continue
		}
		v := *t
		v.Options = maps.Clone(t.Options)
		v.Columns = cloneV2Columns(t.Columns)
		v.PartitionKey = cloneV2ColumnList(t.PartitionKey)
		v.ClusteringColumns = cloneV2ColumnList(t.ClusteringColumns)
		v.OrderedColumns = append([]string(nil), t.OrderedColumns...)
		r.Indexes[n] = &v
	}
	return &r
}

// PartialRefreshError reports keyspaces whose previous valid schema was kept.
type PartialRefreshError struct {
	Generation   uint64
	Keyspaces    map[string]error
	Topology     error
	ClientRoutes error
}

func (e *PartialRefreshError) Error() string {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	var details []string
	for _, name := range names {
		details = append(details, fmt.Sprintf("%s: %v", name, e.Keyspaces[name]))
	}
	if e.Topology != nil {
		details = append(details, "topology: "+e.Topology.Error())
	}
	if e.ClientRoutes != nil {
		details = append(details, "client routes: "+e.ClientRoutes.Error())
	}
	return fmt.Sprintf("gocql: metadata generation %d retained stale data (%s)", e.Generation, strings.Join(details, "; "))
}
func (e *PartialRefreshError) Unwrap() []error {
	names := make([]string, 0, len(e.Keyspaces))
	for name := range e.Keyspaces {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]error, 0, len(names))
	for _, name := range names {
		out = append(out, e.Keyspaces[name])
	}
	if e.Topology != nil {
		out = append(out, e.Topology)
	}
	if e.ClientRoutes != nil {
		out = append(out, e.ClientRoutes)
	}
	return out
}

// NoHostsError distinguishes an empty eligible peer scope from agreement.
type NoHostsError struct {
	Operation string
	Causes    []error
}

func (e *NoHostsError) Error() string   { return "gocql: no eligible hosts for " + e.Operation }
func (e *NoHostsError) Unwrap() []error { return append([]error(nil), e.Causes...) }

// Schema rows intentionally materialize small metadata values on the cold path.
// Fields are explicitly registered; no reflection or destination inference runs.
type InternalV2SchemaField[R any] struct {
	name     string
	required bool
	compile  func(DataType) (func(*R, *RowReader) error, error)
}

func InternalV2SchemaColumn[R, V any](name string, decoder Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, compile: func(typ DataType) (func(*R, *RowReader) error, error) {
		nullable := NullableDecoder(decoder)
		if err := nullable.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *RowReader) error {
			v, err := ReadColumn(reader, nullable)
			if err != nil {
				return err
			}
			if v.Valid {
				*field(row) = v.Value
			}
			return nil
		}, nil
	}}
}
func InternalV2RequiredSchemaColumn[R, V any](name string, decoder Decoder[V], field func(*R) *V) InternalV2SchemaField[R] {
	return InternalV2SchemaField[R]{name: name, required: true, compile: func(typ DataType) (func(*R, *RowReader) error, error) {
		if err := decoder.CheckType(typ); err != nil {
			return nil, err
		}
		return func(row *R, reader *RowReader) error {
			value, err := ReadColumn(reader, decoder)
			if err != nil {
				return err
			}
			*field(row) = value
			return nil
		}, nil
	}}
}

type InternalV2SchemaRows[R any] struct {
	InternalFields []InternalV2SchemaField[R]
	initialize     func() *R
}
type InternalV2SchemaRowPlan[R any] struct {
	steps      []func(*R, *RowReader) error
	initialize func() *R
}

func (d InternalV2SchemaRows[R]) Compile(columns Columns) (RowPlan[R], error) {
	fields := make(map[string]InternalV2SchemaField[R], len(d.InternalFields))
	for _, f := range d.InternalFields {
		if _, exists := fields[f.name]; exists {
			return nil, typeValidation("schema row", "duplicate application field %q", f.name)
		}
		fields[f.name] = f
	}
	steps := make([]func(*R, *RowReader) error, columns.Len())
	seen := make(map[string]bool, columns.Len())
	for i := range steps {
		c, _ := columns.At(i)
		if seen[c.Name] {
			return nil, typeValidation("schema row", "duplicate column %q", c.Name)
		}
		seen[c.Name] = true
		if f, ok := fields[c.Name]; ok {
			step, err := f.compile(c.Type)
			if err != nil {
				return nil, fmt.Errorf("metadata column %s: %w", c.Name, err)
			}
			steps[i] = step
		} else {
			steps[i] = func(_ *R, r *RowReader) error { return r.SkipColumn() }
		}
	}
	for _, f := range d.InternalFields {
		if f.required && !seen[f.name] {
			return nil, typeValidation("schema row", "missing column %q", f.name)
		}
	}
	return InternalV2SchemaRowPlan[R]{steps: steps, initialize: d.initialize}, nil
}
func (d InternalV2SchemaRows[R]) Clone(v R) R { return v }
func (p InternalV2SchemaRowPlan[R]) Decode(reader *RowReader) (R, error) {
	var result R
	err := p.DecodeInto(reader, &result)
	if err != nil {
		return *new(R), err
	}
	return result, nil
}
func (p InternalV2SchemaRowPlan[R]) DecodeInto(reader *RowReader, dst *R) error {
	if dst == nil {
		return typeValidation("destination", "nil schema row destination")
	}
	var zero R
	*dst = zero
	if p.initialize != nil {
		*dst = *p.initialize()
	}
	for _, step := range p.steps {
		if err := step(dst, reader); err != nil {
			return err
		}
	}
	return nil
}
func (p InternalV2SchemaRowPlan[R]) Clone(v R) R { return v }

type InternalV2MetadataText struct{}

type InternalV2MetadataUUIDString struct{}

type InternalV2MetadataInt struct{}

type InternalV2MetadataKind struct{}

type InternalV2MetadataStrings struct{}

type InternalV2MetadataStringMap struct{}

type InternalV2MetadataExtensions struct{}

type InternalV2SchemaParams struct{ count int }

func (InternalV2SchemaParams) Compile(columns Columns) (ParamPlan[[]string], error) {
	for i := 0; i < columns.Len(); i++ {
		c, _ := columns.At(i)
		if err := StringEncoder.CheckType(c.Type); err != nil {
			return nil, err
		}
	}
	return InternalV2SchemaParams{count: columns.Len()}, nil
}
func (p InternalV2SchemaParams) Encode(w *ParamWriter, values []string) error {
	if len(values) != p.count {
		return typeValidation("schema parameters", "wrong count")
	}
	for _, value := range values {
		if err := WriteParam(w, StringEncoder, value); err != nil {
			return err
		}
	}
	return nil
}

func InternalQueryV2SchemaRows[R any](ctx context.Context, s Source, stmt string, args []string, decoder RowDecoder[R]) ([]R, error) {
	settings := s.InternalSchemaSettings()
	if !settings.SystemSchema {
		return nil, &UnsupportedSettingError{Setting: "schema metadata", Reason: "server does not provide system_schema tables"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !settings.Available {
		return nil, InternalErrNoControl
	}
	typ, err := ScalarType(TypeVarchar)
	if err != nil {
		return nil, err
	}
	columns := make([]ColumnSpec, len(args))
	for i := range columns {
		columns[i] = ColumnSpec{Type: typ}
	}
	rows, err := ReadRows(ctx, 4, settings.Timeout, s.InternalQuerySystem, Query[[]string, R]{CQL: stmt, Params: InternalV2SchemaParams{}, Rows: decoder}, NewColumns(columns...), args)
	return rows, InternalWrapV2SchemaQueryError(stmt, err)
}

func InternalV2SchemaContext(s Source) context.Context {
	if s != nil {
		return s.InternalSchemaContext()
	}
	return context.Background()
}

func InternalV2DecodeTableMetadata(keyspace string) RowDecoder[TableMetadata] {
	return InternalV2SchemaRows[TableMetadata]{initialize: func() *TableMetadata { return &TableMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[TableMetadata]{
		InternalV2RequiredSchemaColumn("id", InternalV2MetadataUUIDString{}, func(r *TableMetadata) *string { return &r.ID }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Name }),
		InternalV2SchemaColumn("flags", InternalV2MetadataStrings{}, func(r *TableMetadata) *[]string { return &r.Flags }),
		InternalV2SchemaColumn("extensions", InternalV2MetadataExtensions{}, func(r *TableMetadata) *map[string]any { return &r.Extensions }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", Float64, func(r *TableMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		InternalV2SchemaColumn("caching", InternalV2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", InternalV2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", InternalV2MetadataStringMap{}, func(r *TableMetadata) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", Float64, func(r *TableMetadata) *float64 { return &r.Options.CrcCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.GcGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *TableMetadata) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *TableMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

func InternalV2DecodeColumnMetadata(keyspace string) RowDecoder[ColumnMetadata] {
	return InternalV2SchemaRows[ColumnMetadata]{initialize: func() *ColumnMetadata { return &ColumnMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[ColumnMetadata]{
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Table }),
		InternalV2RequiredSchemaColumn("column_name", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Name }),
		InternalV2SchemaColumn("clustering_order", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.ClusteringOrder }),
		InternalV2RequiredSchemaColumn("type", InternalV2MetadataText{}, func(r *ColumnMetadata) *string { return &r.Type }),
		InternalV2RequiredSchemaColumn("kind", InternalV2MetadataKind{}, func(r *ColumnMetadata) *ColumnKind { return &r.Kind }),
		InternalV2RequiredSchemaColumn("position", InternalV2MetadataInt{}, func(r *ColumnMetadata) *int { return &r.ComponentIndex }),
	}}
}

func InternalV2DecodeTypeMetadata(keyspace string) RowDecoder[TypeMetadata] {
	return InternalV2SchemaRows[TypeMetadata]{initialize: func() *TypeMetadata { return &TypeMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[TypeMetadata]{
		InternalV2RequiredSchemaColumn("type_name", InternalV2MetadataText{}, func(r *TypeMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("field_names", InternalV2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldNames }),
		InternalV2RequiredSchemaColumn("field_types", InternalV2MetadataStrings{}, func(r *TypeMetadata) *[]string { return &r.FieldTypes }),
	}}
}

func InternalV2DecodeFunctionMetadata(keyspace string) RowDecoder[FunctionMetadata] {
	return InternalV2SchemaRows[FunctionMetadata]{initialize: func() *FunctionMetadata { return &FunctionMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[FunctionMetadata]{
		InternalV2RequiredSchemaColumn("function_name", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", InternalV2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentTypes }),
		InternalV2RequiredSchemaColumn("argument_names", InternalV2MetadataStrings{}, func(r *FunctionMetadata) *[]string { return &r.ArgumentNames }),
		InternalV2RequiredSchemaColumn("body", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Body }),
		InternalV2RequiredSchemaColumn("called_on_null_input", Bool, func(r *FunctionMetadata) *bool { return &r.CalledOnNullInput }),
		InternalV2RequiredSchemaColumn("language", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.Language }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *FunctionMetadata) *string { return &r.ReturnType }),
	}}
}

func InternalV2DecodeAggregateMetadata(keyspace string) RowDecoder[AggregateMetadata] {
	return InternalV2SchemaRows[AggregateMetadata]{initialize: func() *AggregateMetadata { return &AggregateMetadata{Keyspace: keyspace} }, InternalFields: []InternalV2SchemaField[AggregateMetadata]{
		InternalV2RequiredSchemaColumn("aggregate_name", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("argument_types", InternalV2MetadataStrings{}, func(r *AggregateMetadata) *[]string { return &r.ArgumentTypes }),
		InternalV2SchemaColumn("final_func", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.privateStorage.InternalFinalFunc }),
		InternalV2SchemaColumn("initcond", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.InitCond }),
		InternalV2RequiredSchemaColumn("return_type", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.ReturnType }),
		InternalV2RequiredSchemaColumn("state_func", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.privateStorage.InternalStateFunc }),
		InternalV2RequiredSchemaColumn("state_type", InternalV2MetadataText{}, func(r *AggregateMetadata) *string { return &r.StateType }),
	}}
}

func InternalV2DecodeIndexMetadata(keyspace string) RowDecoder[IndexMetadata] {
	return InternalV2SchemaRows[IndexMetadata]{initialize: func() *IndexMetadata { return &IndexMetadata{KeyspaceName: keyspace} }, InternalFields: []InternalV2SchemaField[IndexMetadata]{
		InternalV2RequiredSchemaColumn("index_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.Name }),
		InternalV2RequiredSchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.KeyspaceName }),
		InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.TableName }),
		InternalV2RequiredSchemaColumn("kind", InternalV2MetadataText{}, func(r *IndexMetadata) *string { return &r.Kind }),
		InternalV2RequiredSchemaColumn("options", InternalV2MetadataStringMap{}, func(r *IndexMetadata) *map[string]string { return &r.Options }),
	}}
}

func InternalV2DecodeViewMetadata(keyspace string) RowDecoder[ViewMetadata] {
	return InternalV2SchemaRows[ViewMetadata]{initialize: func() *ViewMetadata { return &ViewMetadata{KeyspaceName: keyspace} }, InternalFields: []InternalV2SchemaField[ViewMetadata]{
		InternalV2RequiredSchemaColumn("id", InternalV2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.ID }),
		InternalV2RequiredSchemaColumn("view_name", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.ViewName }),
		InternalV2RequiredSchemaColumn("base_table_id", InternalV2MetadataUUIDString{}, func(r *ViewMetadata) *string { return &r.BaseTableID }),
		InternalV2RequiredSchemaColumn("base_table_name", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.BaseTableName }),
		InternalV2RequiredSchemaColumn("include_all_columns", Bool, func(r *ViewMetadata) *bool { return &r.IncludeAllColumns }),
		InternalV2RequiredSchemaColumn("where_clause", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.WhereClause }),
		InternalV2SchemaColumn("extensions", InternalV2MetadataExtensions{}, func(r *ViewMetadata) *map[string]any { return &r.Extensions }),
		InternalV2SchemaColumn("dclocal_read_repair_chance", Float64, func(r *ViewMetadata) *float64 { return &r.DcLocalReadRepairChance }),
		InternalV2SchemaColumn("read_repair_chance", Float64, func(r *ViewMetadata) *float64 { return &r.ReadRepairChance }),
		InternalV2SchemaColumn("bloom_filter_fp_chance", Float64, func(r *ViewMetadata) *float64 { return &r.Options.BloomFilterFpChance }),
		InternalV2SchemaColumn("caching", InternalV2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Caching }),
		InternalV2SchemaColumn("comment", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.Comment }),
		InternalV2SchemaColumn("compaction", InternalV2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compaction }),
		InternalV2SchemaColumn("compression", InternalV2MetadataStringMap{}, func(r *ViewMetadata) *map[string]string { return &r.Options.Compression }),
		InternalV2SchemaColumn("crc_check_chance", Float64, func(r *ViewMetadata) *float64 { return &r.Options.CrcCheckChance }),
		InternalV2SchemaColumn("default_time_to_live", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.DefaultTimeToLive }),
		InternalV2SchemaColumn("gc_grace_seconds", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.GcGraceSeconds }),
		InternalV2SchemaColumn("max_index_interval", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MaxIndexInterval }),
		InternalV2SchemaColumn("memtable_flush_period_in_ms", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MemtableFlushPeriodInMs }),
		InternalV2SchemaColumn("min_index_interval", InternalV2MetadataInt{}, func(r *ViewMetadata) *int { return &r.Options.MinIndexInterval }),
		InternalV2SchemaColumn("speculative_retry", InternalV2MetadataText{}, func(r *ViewMetadata) *string { return &r.Options.SpeculativeRetry }),
	}}
}

type InternalV2ScyllaTableRow struct {
	Name    string
	Options TableMetadataOptions
}

func InternalV2DecodeScyllaTable() RowDecoder[InternalV2ScyllaTableRow] {
	return InternalV2SchemaRows[InternalV2ScyllaTableRow]{InternalFields: []InternalV2SchemaField[InternalV2ScyllaTableRow]{InternalV2RequiredSchemaColumn("table_name", InternalV2MetadataText{}, func(r *InternalV2ScyllaTableRow) *string { return &r.Name }),
		InternalV2SchemaColumn("cdc", InternalV2MetadataStringMap{}, func(r *InternalV2ScyllaTableRow) *map[string]string { return &r.Options.CDC }),
		InternalV2SchemaColumn("in_memory", Bool, func(r *InternalV2ScyllaTableRow) *bool { return &r.Options.InMemory }),
		InternalV2SchemaColumn("partitioner", InternalV2MetadataText{}, func(r *InternalV2ScyllaTableRow) *string { return &r.Options.Partitioner }),
		InternalV2SchemaColumn("version", InternalV2MetadataUUIDString{}, func(r *InternalV2ScyllaTableRow) *string { return &r.Options.Version }),
	}}
}

type InternalV2KeyspaceRow struct {
	InternalName string
	durable      bool
	replication  map[string]string
}

func InternalV2DecodeKeyspace() RowDecoder[InternalV2KeyspaceRow] {
	return InternalV2SchemaRows[InternalV2KeyspaceRow]{InternalFields: []InternalV2SchemaField[InternalV2KeyspaceRow]{InternalV2SchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *InternalV2KeyspaceRow) *string { return &r.InternalName }), InternalV2RequiredSchemaColumn("durable_writes", Bool, func(r *InternalV2KeyspaceRow) *bool { return &r.durable }), InternalV2RequiredSchemaColumn("replication", InternalV2MetadataStringMap{}, func(r *InternalV2KeyspaceRow) *map[string]string { return &r.replication })}}
}
func InternalV2KeyspaceFromRow(name string, row InternalV2KeyspaceRow) *KeyspaceMetadata {
	ks := &KeyspaceMetadata{Name: name, DurableWrites: row.durable, StrategyClass: row.replication["class"], StrategyOptions: make(map[string]any, len(row.replication))}
	for k, v := range row.replication {
		if k != "class" {
			ks.StrategyOptions[k] = v
		}
	}
	return ks
}
func InternalGetV2KeyspaceMetadata(ctx context.Context, s Source, name string) (*KeyspaceMetadata, error) {
	rows, err := InternalQueryV2SchemaRows(ctx, s, `SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeKeyspace())
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, ErrKeyspaceDoesNotExist
	}
	if len(rows) != 1 {
		return nil, typeValidation("keyspace schema", "duplicate identity %q", name)
	}
	ks := InternalV2KeyspaceFromRow(name, rows[0])
	if err := InternalReadV2ScyllaKeyspaces(ctx, s, map[string]*KeyspaceMetadata{name: ks}, name); err != nil {
		return nil, err
	}
	return ks, nil
}
func InternalGetV2TableMetadata(ctx context.Context, s Source, keyspace, table string) ([]TableMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	rows, err := InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.tables`+where, args, InternalV2DecodeTableMetadata(keyspace))
	if err != nil {
		return nil, err
	}
	if !s.InternalSchemaSettings().Scylla {
		return rows, nil
	}
	extra, err := InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.scylla_tables`+where, args, InternalV2DecodeScyllaTable())
	if err != nil {
		return nil, err
	}
	options := make(map[string]TableMetadataOptions, len(extra))
	for _, row := range extra {
		if _, ok := options[row.Name]; ok {
			return nil, typeValidation("scylla table schema", "duplicate identity %q", row.Name)
		}
		options[row.Name] = row.Options
	}
	for i := range rows {
		if extra, ok := options[rows[i].Name]; ok {
			rows[i].Options.CDC = extra.CDC
			rows[i].Options.InMemory = extra.InMemory
			rows[i].Options.Partitioner = extra.Partitioner
			rows[i].Options.Version = extra.Version
		}
	}
	return rows, nil
}
func InternalGetV2ColumnMetadata(ctx context.Context, s Source, keyspace, table string) ([]ColumnMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT `+InternalColumnMetadataColumns+` FROM system_schema.columns`+where, args, InternalV2DecodeColumnMetadata(keyspace))
}
func InternalGetV2IndexMetadata(ctx context.Context, s Source, keyspace, table string) ([]IndexMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND table_name = ?`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.indexes`+where, args, InternalV2DecodeIndexMetadata(keyspace))
}
func InternalGetV2ViewMetadata(ctx context.Context, s Source, keyspace, table string) ([]ViewMetadata, error) {
	where := ` WHERE keyspace_name = ?`
	args := []string{keyspace}
	if table != "" {
		where += ` AND base_table_name = ? ALLOW FILTERING`
		args = append(args, table)
	}
	return InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.views`+where, args, InternalV2DecodeViewMetadata(keyspace))
}
func InternalGetV2CreateStatements(ctx context.Context, s Source, keyspace string) ([]byte, error) {
	decoder := InternalV2SchemaRows[string]{InternalFields: []InternalV2SchemaField[string]{InternalV2RequiredSchemaColumn("create_statement", InternalV2MetadataText{}, func(s *string) *string { return s })}}
	stmt := `DESCRIBE KEYSPACE "` + strings.ReplaceAll(keyspace, `"`, `""`) + `" WITH INTERNALS`
	rows, err := InternalQueryV2SchemaRows(ctx, s, stmt, nil, decoder)
	if err != nil {
		var server RequestError
		if errors.As(err, &server) && server.GetCode() == ErrCodeSyntax {
			return nil, nil
		}
		return nil, err
	}
	return []byte(strings.Join(rows, "\n")), nil
}

func InternalFetchV2Keyspace(ctx context.Context, s Source, base *KeyspaceMetadata) (*KeyspaceMetadata, error) {
	var tables []TableMetadata
	var columns []ColumnMetadata
	var types []TypeMetadata
	var functions []FunctionMetadata
	var aggregates []AggregateMetadata
	var indexes []IndexMetadata
	var views []ViewMetadata
	var create []byte
	group, ctx := errgroup.WithContext(ctx)
	name := base.Name
	group.Go(func() error { var err error; tables, err = InternalGetV2TableMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; columns, err = InternalGetV2ColumnMetadata(ctx, s, name, ""); return err })
	group.Go(func() error {
		var err error
		types, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.types WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeTypeMetadata(name))
		return err
	})
	if s.InternalSchemaSettings().Aggregates {
		group.Go(func() error {
			var err error
			functions, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeFunctionMetadata(name))
			return err
		})
		group.Go(func() error {
			var err error
			aggregates, err = InternalQueryV2SchemaRows(ctx, s, `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`, []string{name}, InternalV2DecodeAggregateMetadata(name))
			return err
		})
	}
	group.Go(func() error { var err error; indexes, err = InternalGetV2IndexMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; views, err = InternalGetV2ViewMetadata(ctx, s, name, ""); return err })
	group.Go(func() error { var err error; create, err = InternalGetV2CreateStatements(ctx, s, name); return err })
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := InternalValidateV2SchemaParts(base, tables, columns, types, functions, aggregates, indexes, views, s.InternalSchemaSettings().Depth); err != nil {
		return nil, err
	}
	InternalCompileMetadata(base, tables, columns, functions, aggregates, types, indexes, views, create)
	return base, nil
}

func InternalValidateV2SchemaParts(ks *KeyspaceMetadata, tables []TableMetadata, columns []ColumnMetadata, types []TypeMetadata, functions []FunctionMetadata, aggregates []AggregateMetadata, indexes []IndexMetadata, views []ViewMetadata, depthLimit ...int) error {
	maxDepth := 64
	if len(depthLimit) > 0 && depthLimit[0] > 0 {
		maxDepth = depthLimit[0]
	}
	if ks == nil || ks.Name == "" {
		return typeValidation("schema", "missing keyspace identity")
	}
	unique := func(kind string, names []string) error {
		seen := make(map[string]bool, len(names))
		for _, name := range names {
			if name == "" || seen[name] {
				return typeValidation("schema", "empty or duplicate %s identity %q", kind, name)
			}
			seen[name] = true
		}
		return nil
	}
	names := make([]string, 0, len(tables))
	knownTables := make(map[string]bool)
	for _, v := range tables {
		names = append(names, v.Name)
		knownTables[v.Name] = true
	}
	if err := unique("table", names); err != nil {
		return err
	}
	names = nil
	for _, v := range views {
		names = append(names, v.ViewName)
		if knownTables[v.ViewName] {
			return typeValidation("schema", "table/view identity collision %q", v.ViewName)
		}
		knownTables[v.ViewName] = true
	}
	if err := unique("view", names); err != nil {
		return err
	}
	names = nil
	for _, v := range indexes {
		names = append(names, v.Name)
		knownTables[v.Name+"_index"] = true
	}
	if err := unique("index", names); err != nil {
		return err
	}
	udts := make(map[string]bool, len(types))
	names = nil
	for _, v := range types {
		names = append(names, v.Name)
		udts[v.Name] = true
	}
	if err := unique("UDT", names); err != nil {
		return err
	}
	dependencies := make(map[string][]string, len(types))
	for _, v := range types {
		if len(v.FieldNames) != len(v.FieldTypes) {
			return typeValidation("schema UDT", "field count mismatch for %q", v.Name)
		}
		if err := unique("UDT field", v.FieldNames); err != nil {
			return err
		}
		for _, typ := range v.FieldTypes {
			refs, err := InternalValidateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth)
			if err != nil {
				return err
			}
			dependencies[v.Name] = append(dependencies[v.Name], refs...)
		}
	}
	visited := make(map[string]uint8)
	var visit func(string) error
	visit = func(name string) error {
		if visited[name] == 1 {
			return typeValidation("schema UDT", "cycle through %q", name)
		}
		if visited[name] == 2 {
			return nil
		}
		visited[name] = 1
		for _, dep := range dependencies[name] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		visited[name] = 2
		return nil
	}
	for name := range udts {
		if err := visit(name); err != nil {
			return err
		}
	}
	identities := make(map[metadataTable]bool, len(columns))
	positions := make(map[string]map[ColumnKind]map[int]bool)
	for _, v := range columns {
		if !knownTables[v.Table] {
			return typeValidation("schema column", "unknown table %q", v.Table)
		}
		identity := metadataTable{InternalKeyspace: v.Table, InternalTable: v.Name}
		if identities[identity] || v.Name == "" {
			return typeValidation("schema column", "duplicate or empty identity %q.%q", v.Table, v.Name)
		}
		identities[identity] = true
		if _, err := InternalValidateV2CQLTypeLimit(ks.Name, v.Type, udts, maxDepth); err != nil {
			return err
		}
		if v.Kind == ColumnPartitionKey || v.Kind == ColumnClusteringKey {
			if v.ComponentIndex < 0 || v.ComponentIndex >= len(columns) {
				return typeValidation("schema key order", "invalid position %d", v.ComponentIndex)
			}
			if positions[v.Table] == nil {
				positions[v.Table] = make(map[ColumnKind]map[int]bool)
			}
			if positions[v.Table][v.Kind] == nil {
				positions[v.Table][v.Kind] = make(map[int]bool)
			}
			if positions[v.Table][v.Kind][v.ComponentIndex] {
				return typeValidation("schema key order", "duplicate position %d", v.ComponentIndex)
			}
			positions[v.Table][v.Kind][v.ComponentIndex] = true
		}
	}
	for _, table := range tables {
		if len(positions[table.Name][ColumnPartitionKey]) == 0 {
			return typeValidation("schema table", "missing partition key for %q", table.Name)
		}
	}
	for table, kinds := range positions {
		for _, pos := range kinds {
			for i := 0; i < len(pos); i++ {
				if !pos[i] {
					return typeValidation("schema key order", "noncontiguous positions for %q", table)
				}
			}
		}
	}
	names = nil
	for _, v := range functions {
		names = append(names, InternalV2FunctionIdentity(v.Name, v.ArgumentTypes))
		if len(v.ArgumentNames) != len(v.ArgumentTypes) {
			return typeValidation("schema function", "argument count mismatch for %q", v.Name)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.ReturnType) {
			if _, err := InternalValidateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	if err := unique("function", names); err != nil {
		return err
	}
	names = nil
	for _, v := range aggregates {
		names = append(names, InternalV2FunctionIdentity(v.Name, v.ArgumentTypes))
		if InternalResolveV2Function(functions, v.privateStorage.InternalStateFunc, append([]string{v.StateType}, v.ArgumentTypes...)) == nil {
			return typeValidation("schema aggregate", "unknown state function %q", v.privateStorage.InternalStateFunc)
		}
		if v.privateStorage.InternalFinalFunc != "" && InternalResolveV2Function(functions, v.privateStorage.InternalFinalFunc, []string{v.StateType}) == nil {
			return typeValidation("schema aggregate", "unknown final function %q", v.privateStorage.InternalFinalFunc)
		}
		for _, typ := range append(append([]string(nil), v.ArgumentTypes...), v.StateType, v.ReturnType) {
			if _, err := InternalValidateV2CQLTypeLimit(ks.Name, typ, udts, maxDepth); err != nil {
				return err
			}
		}
	}
	return unique("aggregate", names)
}
func InternalV2FunctionIdentity(name string, args []string) string {
	return name + "(" + strings.Join(args, ",") + ")"
}
func InternalResolveV2Function(functions []FunctionMetadata, name string, args []string) *FunctionMetadata {
	for i := range functions {
		f := &functions[i]
		if f.Name != name || len(f.ArgumentTypes) != len(args) {
			continue
		}
		same := true
		for i := range args {
			if f.ArgumentTypes[i] != args[i] {
				same = false
				break
			}
		}
		if same {
			return f
		}
	}
	return nil
}

type InternalV2CQLTypeParser struct {
	source, keyspace string
	offset           int
	maxDepth         int
	udts             map[string]bool
	refs             []string
}

func InternalValidateV2CQLType(keyspace, source string, udts map[string]bool) ([]string, error) {
	return InternalValidateV2CQLTypeLimit(keyspace, source, udts, 64)
}
func InternalValidateV2CQLTypeLimit(keyspace, source string, udts map[string]bool, maxDepth int) ([]string, error) {

	p := InternalV2CQLTypeParser{source: source, keyspace: keyspace, udts: udts, maxDepth: maxDepth}
	if err := p.parse(0, false); err != nil {
		return nil, err
	}
	p.space()
	if p.offset != len(source) {
		return nil, typeValidation("schema type", "trailing syntax in %q", source)
	}
	return p.refs, nil
}
func (p *InternalV2CQLTypeParser) space() {
	for p.offset < len(p.source) {
		switch p.source[p.offset] {
		case ' ', '\t', '\r', '\n':
			p.offset++
		default:
			return
		}
	}
}
func (p *InternalV2CQLTypeParser) identifier() (string, bool, error) {
	p.space()
	start := p.offset
	if start >= len(p.source) {
		return "", false, typeValidation("schema type", "truncated type %q", p.source)
	}
	if p.source[start] == '"' {
		p.offset++
		var out strings.Builder
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '"' {
				if p.offset < len(p.source) && p.source[p.offset] == '"' {
					p.offset++
					out.WriteByte('"')
					continue
				}
				if out.Len() == 0 {
					return "", true, typeValidation("schema type", "empty identifier")
				}
				return out.String(), true, nil
			}
			out.WriteByte(c)
		}
		return "", true, typeValidation("schema type", "unterminated identifier")
	}
	for p.offset < len(p.source) {
		c := p.source[p.offset]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || p.offset > start && c >= '0' && c <= '9' {
			p.offset++
		} else {
			break
		}
	}
	if p.offset == start {
		return "", false, typeValidation("schema type", "invalid identifier in %q", p.source)
	}
	return strings.ToLower(p.source[start:p.offset]), false, nil
}
func (p *InternalV2CQLTypeParser) parse(depth int, frozen bool) error {
	if depth >= p.maxDepth {
		return typeValidation("schema type", "maximum nesting depth exceeded")
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '\'' {
		p.offset++
		for p.offset < len(p.source) {
			c := p.source[p.offset]
			p.offset++
			if c == '\'' {
				if p.offset < len(p.source) && p.source[p.offset] == '\'' {
					p.offset++
					continue
				}
				return nil
			}
		}
		return typeValidation("schema type", "unterminated custom type")
	}
	name, quoted, err := p.identifier()
	if err != nil {
		return err
	}
	p.space()
	if p.offset < len(p.source) && p.source[p.offset] == '.' {
		if name != p.keyspace {
			return typeValidation("schema type", "foreign keyspace %q", name)
		}
		p.offset++
		name, quoted, err = p.identifier()
		if err != nil {
			return err
		}
		quoted = true
		p.space()
	}
	if p.offset < len(p.source) && p.source[p.offset] == '<' {
		if quoted {
			return typeValidation("schema type", "UDT cannot have parameters")
		}
		switch name {
		case "list", "set", "map", "tuple", "frozen", "vector":
		default:
			return typeValidation("schema type", "unknown constructor %q", name)
		}
		p.offset++
		count := 0
		for {
			if name == "vector" && count == 1 {
				p.space()
				start := p.offset
				for p.offset < len(p.source) && p.source[p.offset] >= '0' && p.source[p.offset] <= '9' {
					p.offset++
				}
				dimension, err := strconv.ParseUint(p.source[start:p.offset], 10, 16)
				if err != nil || dimension == 0 {
					return typeValidation("schema type", "invalid vector dimension")
				}
			} else {
				if err := p.parse(depth+1, name == "frozen"); err != nil {
					return err
				}
			}
			count++
			p.space()
			if p.offset >= len(p.source) {
				return typeValidation("schema type", "unterminated parameters")
			}
			c := p.source[p.offset]
			p.offset++
			if c == '>' {
				break
			}
			if c != ',' {
				return typeValidation("schema type", "invalid parameter separator")
			}
		}
		expected := 1
		if name == "map" || name == "vector" {
			expected = 2
		}
		if name != "tuple" && count != expected {
			return typeValidation("schema type", "wrong arity for %q", name)
		}
		return nil
	}
	if !quoted && !frozen {
		if _, ok := cqlNativeTypes[name]; ok || name == "empty" {
			return nil
		}
	}
	if !p.udts[name] {
		return typeValidation("schema type", "unknown UDT %q", name)
	}
	p.refs = append(p.refs, name)
	return nil
}

func InternalWrapV2SchemaQueryError(statement string, err error) error {
	if err == nil {
		return nil
	}
	kind := "keyspace"
	switch {
	case strings.Contains(statement, "system_schema.scylla_tables"):
		kind = "scylla table"
	case strings.Contains(statement, "system_schema.tables"):
		kind = "table"
	case strings.Contains(statement, "system_schema.columns"):
		kind = "column"
	case strings.Contains(statement, "system_schema.views"):
		kind = "view"
	case strings.Contains(statement, "system_schema.types"):
		kind = "type"
	case strings.Contains(statement, "system_schema.indexes"):
		kind = "index"
	case strings.Contains(statement, "system_schema.functions"):
		kind = "function"
	case strings.Contains(statement, "system_schema.aggregates"):
		kind = "aggregate"
	}
	return fmt.Errorf("error querying %s schema: %w", kind, err)
}
