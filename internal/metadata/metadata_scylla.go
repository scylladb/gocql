// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package metadata

import "fmt"
import "maps"
import "reflect"
import "sync"
import "sync/atomic"
import "strings"

// schema metadata for a keyspace
type KeyspaceMetadata struct {
	TabletBased     bool
	ConsistencyMode string
	StrategyOptions map[string]any
	Tables          map[string]*TableMetadata
	Functions       map[string]*FunctionMetadata
	Aggregates      map[string]*AggregateMetadata
	Types           map[string]*TypeMetadata
	Indexes         map[string]*IndexMetadata
	Views           map[string]*ViewMetadata

	Name          string
	StrategyClass string
	CreateStmts   string
	DurableWrites bool
	state         KeyspaceState
}

// Clone returns a shallow copy of the keyspace metadata with
// cloned Tables, Indexes, Views, and tablesInvalidated maps so that mutations
// do not race with concurrent readers of the original.
func (ks *KeyspaceMetadata) Clone() *KeyspaceMetadata {
	cloned := &KeyspaceMetadata{
		TabletBased: ks.TabletBased, ConsistencyMode: ks.ConsistencyMode,
		Name:            ks.Name,
		DurableWrites:   ks.DurableWrites,
		StrategyClass:   ks.StrategyClass,
		StrategyOptions: maps.Clone(ks.StrategyOptions),
		Tables:          maps.Clone(ks.Tables),
		Functions:       maps.Clone(ks.Functions),
		Aggregates:      maps.Clone(ks.Aggregates),
		Types:           maps.Clone(ks.Types),
		Indexes:         maps.Clone(ks.Indexes),
		Views:           maps.Clone(ks.Views),
		CreateStmts:     ks.CreateStmts,
	}
	if KeyspaceData(ks).InternalTablesInvalidated != nil {
		KeyspaceData(cloned).InternalTablesInvalidated = maps.Clone(KeyspaceData(ks).InternalTablesInvalidated)
	}
	return cloned
}

func (ks *KeyspaceMetadata) removeTableData(tableName string) {
	if ks.Tables != nil {
		delete(ks.Tables, tableName)
	}
	for name, idx := range ks.Indexes {
		if idx != nil && idx.TableName == tableName {
			delete(ks.Indexes, name)
		}
	}
	for name, view := range ks.Views {
		if view != nil && view.BaseTableName == tableName {
			delete(ks.Views, name)
		}
	}
}

func (ks *KeyspaceMetadata) invalidateTable(tableName string) {
	ks.removeTableData(tableName)
	if KeyspaceData(ks).InternalTablesInvalidated == nil {
		KeyspaceData(ks).InternalTablesInvalidated = make(map[string]struct{})
	}
	KeyspaceData(ks).InternalTablesInvalidated[tableName] = struct{}{}
}

func InternalKeyspaceMetadataRemoveTable(ks *KeyspaceMetadata, tableName string) {
	ks.removeTableData(tableName)
	if KeyspaceData(ks).InternalTablesInvalidated != nil {
		delete(KeyspaceData(ks).InternalTablesInvalidated, tableName)
	}
}

// schema metadata for a table (a.k.a. column family)
type TableMetadata struct {
	ID                string
	Columns           map[string]*ColumnMetadata
	Extensions        map[string]any
	Keyspace          string
	Name              string
	PartitionKey      []*ColumnMetadata
	ClusteringColumns []*ColumnMetadata
	OrderedColumns    []string
	Flags             []string
	Options           TableMetadataOptions
}

type TableMetadataOptions struct {
	Caching                 map[string]string
	Compaction              map[string]string
	Compression             map[string]string
	CDC                     map[string]string
	SpeculativeRetry        string
	Comment                 string
	Version                 string
	Partitioner             string
	GcGraceSeconds          int
	MaxIndexInterval        int
	MemtableFlushPeriodInMs int
	MinIndexInterval        int
	ReadRepairChance        float64
	BloomFilterFpChance     float64
	DefaultTimeToLive       int
	DcLocalReadRepairChance float64
	CrcCheckChance          float64
	InMemory                bool
}

func (t *TableMetadataOptions) Equals(other *TableMetadataOptions) bool {
	if t == nil || other == nil {
		return t == other // Both must be nil to be equal
	}

	if t.BloomFilterFpChance != other.BloomFilterFpChance ||
		t.Comment != other.Comment ||
		t.CrcCheckChance != other.CrcCheckChance ||
		t.DcLocalReadRepairChance != other.DcLocalReadRepairChance ||
		t.DefaultTimeToLive != other.DefaultTimeToLive ||
		t.GcGraceSeconds != other.GcGraceSeconds ||
		t.MaxIndexInterval != other.MaxIndexInterval ||
		t.MemtableFlushPeriodInMs != other.MemtableFlushPeriodInMs ||
		t.MinIndexInterval != other.MinIndexInterval ||
		t.ReadRepairChance != other.ReadRepairChance ||
		t.SpeculativeRetry != other.SpeculativeRetry ||
		t.InMemory != other.InMemory ||
		t.Partitioner != other.Partitioner ||
		t.Version != other.Version {
		return false
	}

	if !compareStringMaps(t.Caching, other.Caching) ||
		!compareStringMaps(t.Compaction, other.Compaction) ||
		!compareStringMaps(t.Compression, other.Compression) ||
		!compareStringMaps(t.CDC, other.CDC) {
		return false
	}

	return true
}

type ViewMetadata struct {
	Columns                 map[string]*ColumnMetadata
	Extensions              map[string]any
	WhereClause             string
	BaseTableName           string
	ID                      string
	KeyspaceName            string
	BaseTableID             string
	ViewName                string
	OrderedColumns          []string
	PartitionKey            []*ColumnMetadata
	ClusteringColumns       []*ColumnMetadata
	Options                 TableMetadataOptions
	DcLocalReadRepairChance float64 // After Scylla 4.2 by default read_repair turned off
	ReadRepairChance        float64 // After Scylla 4.2 by default read_repair turned off
	IncludeAllColumns       bool
}

type ColumnMetadata struct {
	Index           ColumnIndexMetadata
	Keyspace        string
	Table           string
	Name            string
	Type            string
	ClusteringOrder string
	ComponentIndex  int
	Kind            ColumnKind
	Order           ColumnOrder
}

func (c *ColumnMetadata) Equals(other *ColumnMetadata) bool {
	if c == nil || other == nil {
		return c == other
	}

	return c.Keyspace == other.Keyspace &&
		c.Table == other.Table &&
		c.Name == other.Name &&
		c.ComponentIndex == other.ComponentIndex &&
		c.Kind == other.Kind &&
		c.Type == other.Type &&
		c.ClusteringOrder == other.ClusteringOrder &&
		c.Order == other.Order &&
		c.Index.Equals(&other.Index)
}

// FunctionMetadata holds metadata for function constructs
type FunctionMetadata struct {
	Keyspace          string
	Name              string
	Body              string
	Language          string
	ReturnType        string
	ArgumentTypes     []string
	ArgumentNames     []string
	CalledOnNullInput bool
}

// AggregateMetadata holds metadata for aggregate constructs
type AggregateMetadata struct {
	Keyspace   string
	Name       string
	InitCond   string
	ReturnType string
	StateType  string

	ArgumentTypes  []string
	FinalFunc      FunctionMetadata
	StateFunc      FunctionMetadata
	privateStorage AggregateMetadataStorage
}

// TypeMetadata holds the metadata for views.
type TypeMetadata struct {
	Keyspace   string
	Name       string
	FieldNames []string
	FieldTypes []string
}

type IndexMetadata struct {
	Name              string
	KeyspaceName      string
	TableName         string // Name of corresponding view.
	Kind              string
	Options           map[string]string
	Columns           map[string]*ColumnMetadata
	OrderedColumns    []string
	PartitionKey      []*ColumnMetadata
	ClusteringColumns []*ColumnMetadata
}

func (t *TableMetadata) Equals(other *TableMetadata) bool {
	if t == nil || other == nil {
		return t == other
	}

	if t.ID != other.ID || t.Keyspace != other.Keyspace || t.Name != other.Name {
		return false
	}

	if len(t.PartitionKey) != len(other.PartitionKey) || !compareColumnSlices(t.PartitionKey, other.PartitionKey) {
		return false
	}

	if len(t.ClusteringColumns) != len(other.ClusteringColumns) || !compareColumnSlices(t.ClusteringColumns, other.ClusteringColumns) {
		return false
	}

	if len(t.Columns) != len(other.Columns) || !compareColumnsMap(t.Columns, other.Columns) {
		return false
	}

	if len(t.OrderedColumns) != len(other.OrderedColumns) || !compareStringSlices(t.OrderedColumns, other.OrderedColumns) {
		return false
	}

	if !t.Options.Equals(&other.Options) {
		return false
	}

	if len(t.Flags) != len(other.Flags) || !compareStringSlices(t.Flags, other.Flags) {
		return false
	}

	if len(t.Extensions) != len(other.Extensions) || !InternalCompareInterfaceMaps(t.Extensions, other.Extensions) {
		return false
	}

	return true
}

func compareColumnSlices(a, b []*ColumnMetadata) bool {
	for i := range a {
		if !a[i].Equals(b[i]) {
			return false
		}
	}
	return true
}

func compareColumnsMap(a, b map[string]*ColumnMetadata) bool {
	for k, v := range a {
		otherValue, exists := b[k]
		if !exists || !v.Equals(otherValue) {
			return false
		}
	}
	return true
}

func compareStringSlices(a, b []string) bool {
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func compareStringMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if otherValue, exists := b[k]; !exists || v != otherValue {
			return false
		}
	}
	return true
}

func InternalCompareInterfaceMaps(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		otherValue, exists := b[k]
		if !exists || !reflect.DeepEqual(v, otherValue) {
			return false
		}
	}
	return true
}

// cowTabletList implements a copy on write keyspace metadata map, its equivalent type is map[string]*KeyspaceMetadata
type InternalCowKeyspaceMetadataMap struct {
	InternalKeyspaceMap atomic.Pointer[map[string]*KeyspaceMetadata]
	mu                  sync.Mutex
}

func (c *InternalCowKeyspaceMetadataMap) InternalGet() map[string]*KeyspaceMetadata {
	l := c.InternalKeyspaceMap.Load()
	if l == nil {
		return nil
	}
	return *l
}

func (c *InternalCowKeyspaceMetadataMap) InternalGetKeyspace(keyspaceName string) (*KeyspaceMetadata, bool) {
	m := c.InternalKeyspaceMap.Load()
	if m == nil {
		return nil, false
	}
	val, ok := (*m)[keyspaceName]
	return val, ok
}

func (c *InternalCowKeyspaceMetadataMap) InternalSet(keyspaceName string, keyspaceMetadata *KeyspaceMetadata) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()

	newM := map[string]*KeyspaceMetadata{}
	for name, metadata := range m {
		newM[name] = metadata
	}
	newM[keyspaceName] = keyspaceMetadata

	c.InternalKeyspaceMap.Store(&newM)
	return true
}

func (c *InternalCowKeyspaceMetadataMap) InternalInvalidateTable(keyspaceName, tableName string) {
	c.InternalUpdateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateTable(tableName)
	})
}

func (c *InternalCowKeyspaceMetadataMap) InternalRemoveKeyspace(keyspaceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()
	newM := maps.Clone(m)
	delete(newM, keyspaceName)

	c.InternalKeyspaceMap.Store(&newM)
}

// updateKeyspace atomically clones a keyspace's mutable maps, applies fn to
// the clone, and publishes the result. This prevents data races between
// concurrent readers and writers of the same KeyspaceMetadata.
// Returns false if the keyspace was not found (no update applied).
func (c *InternalCowKeyspaceMetadataMap) InternalUpdateKeyspace(keyspaceName string, fn func(ks *KeyspaceMetadata)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.InternalGet()
	ks, ok := m[keyspaceName]
	if !ok || ks == nil {
		return false
	}

	cloned := ks.Clone()
	fn(cloned)

	newM := maps.Clone(m)
	newM[keyspaceName] = cloned
	c.InternalKeyspaceMap.Store(&newM)
	return true
}

const (
	IndexKindCustom = "CUSTOM"
)

// the ordering of the column with regard to its comparator
type ColumnOrder bool

const (
	ASC  ColumnOrder = false
	DESC             = true
)

type ColumnIndexMetadata struct {
	Options map[string]any
	Name    string
	Type    string
}

func (c *ColumnIndexMetadata) Equals(other *ColumnIndexMetadata) bool {
	if c == nil || other == nil {
		return c == other
	}

	if c.Name != other.Name || c.Type != other.Type {
		return false
	}

	// Compare the Options map
	if len(c.Options) != len(other.Options) {
		return false
	}
	for k, v := range c.Options {
		otherValue, exists := other.Options[k]
		if !exists || !reflect.DeepEqual(v, otherValue) {
			return false
		}
	}

	return true
}

type ColumnKind int

const (
	ColumnUnkownKind ColumnKind = iota
	ColumnPartitionKey
	ColumnClusteringKey
	ColumnRegular
	ColumnCompact
	ColumnStatic
)

func (c ColumnKind) String() string {
	switch c {
	case ColumnPartitionKey:
		return "partition_key"
	case ColumnClusteringKey:
		return "clustering_key"
	case ColumnRegular:
		return "regular"
	case ColumnCompact:
		return "compact"
	case ColumnStatic:
		return "static"
	default:
		return fmt.Sprintf("unknown_column_%d", c)
	}
}

func InternalColumnKindFromSchema(kind string) (ColumnKind, error) {
	switch kind {
	case "partition_key":
		return ColumnPartitionKey, nil
	case "clustering_key", "clustering":
		return ColumnClusteringKey, nil
	case "regular":
		return ColumnRegular, nil
	case "compact_value":
		return ColumnCompact, nil
	case "static":
		return ColumnStatic, nil
	default:
		return -1, fmt.Errorf("unknown column kind: %q", kind)
	}
}

type KeyspaceState struct {
	InternalTablesInvalidated map[string]struct{}
}

func InternalCompileMetadata(
	keyspace *KeyspaceMetadata,
	tables []TableMetadata,
	columns []ColumnMetadata,
	functions []FunctionMetadata,
	aggregates []AggregateMetadata,
	types []TypeMetadata,
	indexes []IndexMetadata,
	views []ViewMetadata,
	createStmts []byte,
) {
	keyspace.Tables = make(map[string]*TableMetadata)
	for i := range tables {
		tables[i].Columns = make(map[string]*ColumnMetadata)
		keyspace.Tables[tables[i].Name] = &tables[i]
	}
	keyspace.Functions = make(map[string]*FunctionMetadata, len(functions))
	functionNames := make(map[string]int, len(functions))
	for _, function := range functions {
		functionNames[function.Name]++
	}
	for i := range functions {
		key := functions[i].Name
		if functionNames[key] > 1 {
			key = InternalV2FunctionIdentity(key, functions[i].ArgumentTypes)
		}
		keyspace.Functions[key] = &functions[i]
	}
	keyspace.Aggregates = make(map[string]*AggregateMetadata, len(aggregates))
	aggregateNames := make(map[string]int, len(aggregates))
	for _, aggregate := range aggregates {
		aggregateNames[aggregate.Name]++
	}
	for _, aggregate := range aggregates {
		if f := InternalResolveV2Function(functions, aggregate.privateStorage.InternalFinalFunc, []string{aggregate.StateType}); f != nil {
			aggregate.FinalFunc = *f
		} else if f := keyspace.Functions[aggregate.privateStorage.InternalFinalFunc]; f != nil {
			aggregate.FinalFunc = *f
		}
		if f := InternalResolveV2Function(functions, aggregate.privateStorage.InternalStateFunc, append([]string{aggregate.StateType}, aggregate.ArgumentTypes...)); f != nil {
			aggregate.StateFunc = *f
		} else if f := keyspace.Functions[aggregate.privateStorage.InternalStateFunc]; f != nil {
			aggregate.StateFunc = *f
		}
		key := aggregate.Name
		if aggregateNames[key] > 1 {
			key = InternalV2FunctionIdentity(key, aggregate.ArgumentTypes)
		}
		keyspace.Aggregates[key] = &aggregate
	}
	keyspace.Types = make(map[string]*TypeMetadata, len(types))
	for i := range types {
		keyspace.Types[types[i].Name] = &types[i]
	}
	keyspace.Indexes = make(map[string]*IndexMetadata, len(indexes))
	for i := range indexes {
		indexes[i].Columns = make(map[string]*ColumnMetadata)
		keyspace.Indexes[indexes[i].Name] = &indexes[i]

	}
	keyspace.Views = make(map[string]*ViewMetadata, len(views))
	for i := range views {
		v := &views[i]
		if _, ok := keyspace.Indexes[strings.TrimSuffix(v.ViewName, "_index")]; ok {
			continue
		}

		v.Columns = make(map[string]*ColumnMetadata)
		keyspace.Views[v.ViewName] = v
	}

	for i := range columns {
		col := &columns[i]
		col.Order = ASC
		if col.ClusteringOrder == "desc" {
			col.Order = DESC
		}

		table, ok := keyspace.Tables[col.Table]
		if !ok {

			if indexName, found := strings.CutSuffix(col.Table, "_index"); found {
				ix, ok := keyspace.Indexes[indexName]
				if ok {
					ix.Columns[col.Name] = col
					ix.OrderedColumns = append(ix.OrderedColumns, col.Name)
					continue
				}
			}

			view, ok := keyspace.Views[col.Table]
			if !ok {

				continue
			}

			view.Columns[col.Name] = col
			view.OrderedColumns = append(view.OrderedColumns, col.Name)
			continue
		}

		table.Columns[col.Name] = col
		table.OrderedColumns = append(table.OrderedColumns, col.Name)
	}

	for i := range tables {
		t := &tables[i]
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = InternalCompileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = InternalCompileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = InternalCompileColumns(ix.Columns, ix.OrderedColumns)
	}

	keyspace.CreateStmts = string(createStmts)
}

func InternalCompileTableMetadata(
	keyspace *KeyspaceMetadata,
	tables []TableMetadata,
	columns []ColumnMetadata,
	indexes []IndexMetadata,
	views []ViewMetadata,
) {
	if keyspace.Tables == nil {
		keyspace.Tables = make(map[string]*TableMetadata)
	}
	for i := range tables {
		tables[i].Columns = make(map[string]*ColumnMetadata)
		keyspace.Tables[tables[i].Name] = &tables[i]
	}

	if keyspace.Indexes == nil {
		keyspace.Indexes = make(map[string]*IndexMetadata)
	}
	for name, ix := range keyspace.Indexes {
		for i := range tables {
			if ix.TableName == tables[i].Name {
				delete(keyspace.Indexes, name)
			}
		}
	}
	for i := range indexes {
		indexes[i].Columns = make(map[string]*ColumnMetadata)
		keyspace.Indexes[indexes[i].Name] = &indexes[i]
	}

	if keyspace.Views == nil {
		keyspace.Views = make(map[string]*ViewMetadata)
	}
	for name, v := range keyspace.Views {
		for i := range tables {
			if v.BaseTableName == tables[i].Name {
				delete(keyspace.Views, name)
			}
		}
	}
	for i := range views {
		v := &views[i]
		if _, ok := keyspace.Indexes[strings.TrimSuffix(v.ViewName, "_index")]; ok {
			continue
		}
		v.Columns = make(map[string]*ColumnMetadata)
		keyspace.Views[v.ViewName] = v
	}

	for i := range columns {
		col := &columns[i]
		col.Order = ASC
		if col.ClusteringOrder == "desc" {
			col.Order = DESC
		}

		table, ok := keyspace.Tables[col.Table]
		if !ok {
			if indexName, found := strings.CutSuffix(col.Table, "_index"); found {
				ix, ok := keyspace.Indexes[indexName]
				if ok {
					ix.Columns[col.Name] = col
					ix.OrderedColumns = append(ix.OrderedColumns, col.Name)
					continue
				}
			}

			view, ok := keyspace.Views[col.Table]
			if !ok {
				continue
			}

			view.Columns[col.Name] = col
			view.OrderedColumns = append(view.OrderedColumns, col.Name)
			continue
		}

		table.Columns[col.Name] = col
		table.OrderedColumns = append(table.OrderedColumns, col.Name)
	}

	for i := range tables {
		t := &tables[i]
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = InternalCompileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = InternalCompileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = InternalCompileColumns(ix.Columns, ix.OrderedColumns)
	}
}

func InternalCompileColumns(columns map[string]*ColumnMetadata, orderedColumns []string) (
	partitionKey, clusteringColumns []*ColumnMetadata, sortedColumns []string) {
	clusteringColumnCount := InternalComponentColumnCountOfType(columns, ColumnClusteringKey)
	clusteringColumns = make([]*ColumnMetadata, clusteringColumnCount)

	partitionKeyCount := InternalComponentColumnCountOfType(columns, ColumnPartitionKey)
	partitionKey = make([]*ColumnMetadata, partitionKeyCount)

	var otherColumns []string
	for _, columnName := range orderedColumns {
		column := columns[columnName]
		if column.Kind == ColumnPartitionKey {
			partitionKey[column.ComponentIndex] = column
		} else if column.Kind == ColumnClusteringKey {
			clusteringColumns[column.ComponentIndex] = column
		} else {
			otherColumns = append(otherColumns, columnName)
		}
	}

	sortedColumns = orderedColumns[:0]
	for _, pk := range partitionKey {
		sortedColumns = append(sortedColumns, pk.Name)
	}
	for _, ck := range clusteringColumns {
		sortedColumns = append(sortedColumns, ck.Name)
	}
	for _, oc := range otherColumns {
		sortedColumns = append(sortedColumns, oc)
	}

	return
}

func InternalComponentColumnCountOfType(columns map[string]*ColumnMetadata, kind ColumnKind) int {
	maxComponentIndex := -1
	for _, column := range columns {
		if column.Kind == kind && column.ComponentIndex > maxComponentIndex {
			maxComponentIndex = column.ComponentIndex
		}
	}
	return maxComponentIndex + 1
}

const InternalColumnMetadataColumns = `table_name, column_name, clustering_order, type, kind, position`
