// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

	"github.com/scylladb/gocql/v2/tablets"
)

// schema metadata for a keyspace
type KeyspaceMetadata struct {
	StrategyOptions   map[string]any
	Tables            map[string]*TableMetadata
	Functions         map[string]*FunctionMetadata
	Aggregates        map[string]*AggregateMetadata
	Types             map[string]*TypeMetadata
	Indexes           map[string]*IndexMetadata
	Views             map[string]*ViewMetadata
	tablesInvalidated map[string]struct{}
	Name              string
	StrategyClass     string
	CreateStmts       string
	DurableWrites     bool
}

// Clone returns a shallow copy of the keyspace metadata with
// cloned Tables, Indexes, Views, and tablesInvalidated maps so that mutations
// do not race with concurrent readers of the original.
func (ks *KeyspaceMetadata) Clone() *KeyspaceMetadata {
	cloned := &KeyspaceMetadata{
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
	if ks.tablesInvalidated != nil {
		cloned.tablesInvalidated = maps.Clone(ks.tablesInvalidated)
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
	if ks.tablesInvalidated == nil {
		ks.tablesInvalidated = make(map[string]struct{})
	}
	ks.tablesInvalidated[tableName] = struct{}{}
}

func (ks *KeyspaceMetadata) removeTable(tableName string) {
	ks.removeTableData(tableName)
	if ks.tablesInvalidated != nil {
		delete(ks.tablesInvalidated, tableName)
	}
}

// schema metadata for a table (a.k.a. column family)
type TableMetadata struct {
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
	Keyspace      string
	Name          string
	InitCond      string
	ReturnType    string
	StateType     string
	stateFunc     string
	finalFunc     string
	ArgumentTypes []string
	FinalFunc     FunctionMetadata
	StateFunc     FunctionMetadata
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

	if t.Keyspace != other.Keyspace || t.Name != other.Name {
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

	if len(t.Extensions) != len(other.Extensions) || !compareInterfaceMaps(t.Extensions, other.Extensions) {
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

func compareInterfaceMaps(a, b map[string]any) bool {
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
type cowKeyspaceMetadataMap struct {
	keyspaceMap atomic.Pointer[map[string]*KeyspaceMetadata]
	mu          sync.Mutex
}

func (c *cowKeyspaceMetadataMap) get() map[string]*KeyspaceMetadata {
	l := c.keyspaceMap.Load()
	if l == nil {
		return nil
	}
	return *l
}

func (c *cowKeyspaceMetadataMap) getKeyspace(keyspaceName string) (*KeyspaceMetadata, bool) {
	m := c.keyspaceMap.Load()
	if m == nil {
		return nil, false
	}
	val, ok := (*m)[keyspaceName]
	return val, ok
}

func (c *cowKeyspaceMetadataMap) set(keyspaceName string, keyspaceMetadata *KeyspaceMetadata) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.get()

	newM := map[string]*KeyspaceMetadata{}
	for name, metadata := range m {
		newM[name] = metadata
	}
	newM[keyspaceName] = keyspaceMetadata

	c.keyspaceMap.Store(&newM)
	return true
}

func (c *cowKeyspaceMetadataMap) invalidateTable(keyspaceName, tableName string) {
	c.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateTable(tableName)
	})
}

func (c *cowKeyspaceMetadataMap) removeKeyspace(keyspaceName string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.get()
	newM := maps.Clone(m)
	delete(newM, keyspaceName)

	c.keyspaceMap.Store(&newM)
}

// updateKeyspace atomically clones a keyspace's mutable maps, applies fn to
// the clone, and publishes the result. This prevents data races between
// concurrent readers and writers of the same KeyspaceMetadata.
// Returns false if the keyspace was not found (no update applied).
func (c *cowKeyspaceMetadataMap) updateKeyspace(keyspaceName string, fn func(ks *KeyspaceMetadata)) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.get()
	ks, ok := m[keyspaceName]
	if !ok || ks == nil {
		return false
	}

	cloned := ks.Clone()
	fn(cloned)

	newM := maps.Clone(m)
	newM[keyspaceName] = cloned
	c.keyspaceMap.Store(&newM)
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

func (c *ColumnKind) UnmarshalCQL(typ TypeInfo, p []byte) error {
	if typ.Type() != TypeVarchar {
		return unmarshalErrorf("unable to marshall %s into ColumnKind, expected Varchar", typ)
	}

	kind, err := columnKindFromSchema(string(p))
	if err != nil {
		return err
	}
	*c = kind

	return nil
}

func columnKindFromSchema(kind string) (ColumnKind, error) {
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

type Metadata struct {
	tabletsMetadata  *tablets.CowTabletList
	keyspaceMetadata cowKeyspaceMetadataMap
}

// queries the cluster for schema information for a specific keyspace and for tablets
type metadataDescriber struct {
	keyspaceGroup singleflight.Group
	tableGroup    singleflight.Group
	session       *Session
	metadata      *Metadata

	// mu serialises refreshAllSchema calls so the snapshot-compare-refresh
	// cycle runs as an atomic batch.  Individual keyspace/table refreshes
	// are deduplicated by the singleflight groups above and do NOT need
	// this lock.
	//
	// Lock ordering: s.mu → cowKeyspaceMetadataMap.mu (never reversed).
	mu sync.Mutex
}

// creates a session bound schema describer which will query and cache
// keyspace metadata and tablets metadata
func newMetadataDescriber(session *Session) *metadataDescriber {
	return &metadataDescriber{
		session: session,
		metadata: &Metadata{
			tabletsMetadata: tablets.NewCowTabletList(),
		},
	}
}

func (s *metadataDescriber) getKeyspaceInternal(keyspaceName string) (metadata *KeyspaceMetadata, wasReloaded bool, err error) {
	var found bool
	metadata, found = s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
	if !found {
		wasReloaded = true
		err = s.deduplicatedRefreshKeyspace(keyspaceName)
		if err != nil {
			return metadata, wasReloaded, err
		}

		metadata, found = s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
		if !found {
			return nil, true, fmt.Errorf("keyspace %s: %w", keyspaceName, ErrNotFound)
		}
	}

	return metadata, wasReloaded, nil
}

func (s *metadataDescriber) GetKeyspace(keyspaceName string) (*KeyspaceMetadata, error) {
	metadata, _, err := s.getKeyspaceInternal(keyspaceName)
	return metadata, err
}

func tableNotFoundError(keyspaceName, tableName string) error {
	return fmt.Errorf("table %s.%s: %w", keyspaceName, tableName, ErrNotFound)
}

// getTableFromSnapshot resolves a table lookup against a keyspace snapshot.
// It re-reads the latest published keyspace metadata before deciding that an
// invalidated table still needs a refresh, which avoids duplicate refreshes
// for callers holding a stale snapshot.
func (s *metadataDescriber) getTableFromSnapshot(
	keyspaceName, tableName string,
	keyspaceMetadata *KeyspaceMetadata,
	wasReloaded bool,
) (tableMetadata *TableMetadata, refreshNeeded bool, err error) {
	if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
		return tableMetadata, false, nil
	}

	if latestMetadata, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName); found && latestMetadata != keyspaceMetadata {
		keyspaceMetadata = latestMetadata
		if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
			return tableMetadata, false, nil
		}
	}

	if wasReloaded {
		return nil, false, tableNotFoundError(keyspaceName, tableName)
	}

	if _, ok := keyspaceMetadata.tablesInvalidated[tableName]; !ok {
		return nil, false, tableNotFoundError(keyspaceName, tableName)
	}

	return nil, true, nil
}

func (s *metadataDescriber) GetTable(keyspaceName, tableName string) (*TableMetadata, error) {
	keyspaceMetadata, wasReloaded, err := s.getKeyspaceInternal(keyspaceName)
	if err != nil {
		return nil, err
	}

	tableMetadata, refreshNeeded, err := s.getTableFromSnapshot(keyspaceName, tableName, keyspaceMetadata, wasReloaded)
	if err != nil {
		return nil, err
	}
	if !refreshNeeded {
		return tableMetadata, nil
	}

	err = s.deduplicatedRefreshTable(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	keyspaceMetadata, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
	if !found {
		return nil, tableNotFoundError(keyspaceName, tableName)
	}

	tableMetadata, found = keyspaceMetadata.Tables[tableName]
	if !found {
		return nil, tableNotFoundError(keyspaceName, tableName)
	}

	return tableMetadata, nil
}

func (s *metadataDescriber) getTablets() tablets.TabletInfoList {
	return s.metadata.tabletsMetadata.Get()
}

func (s *metadataDescriber) getTableTablets(keyspace, table string) tablets.TabletEntryList {
	return s.metadata.tabletsMetadata.GetTableTablets(keyspace, table)
}

func (s *metadataDescriber) forEachTablet(fn func(keyspace, table string, entries tablets.TabletEntryList) bool) {
	s.metadata.tabletsMetadata.ForEach(fn)
}

func (s *metadataDescriber) AddTablet(tablet tablets.TabletInfo) {
	s.metadata.tabletsMetadata.AddTablet(tablet)
}

// RemoveTabletsWithHost removes tablets that contains given host.
// to be used outside the metadataDescriber
func (s *metadataDescriber) RemoveTabletsWithHost(host *HostInfo) {
	s.metadata.tabletsMetadata.RemoveTabletsWithHost(tablets.HostUUID(host.hostUUID()))
}

// RemoveTabletsWithKeyspace removes tablets for given keyspace.
// to be used outside the metadataDescriber
func (s *metadataDescriber) RemoveTabletsWithKeyspace(keyspace string) {
	s.metadata.tabletsMetadata.RemoveTabletsWithKeyspace(keyspace)
}

// RemoveTabletsWithTable removes tablets for given table.
// to be used outside the metadataDescriber
func (s *metadataDescriber) RemoveTabletsWithTable(keyspace string, table string) {
	s.metadata.tabletsMetadata.RemoveTabletsWithTable(keyspace, table)
}

// invalidateKeyspaceSchema clears the cached keyspace metadata
func (s *metadataDescriber) invalidateKeyspaceSchema(keyspaceName string) {
	s.metadata.keyspaceMetadata.removeKeyspace(keyspaceName)
}

func (s *metadataDescriber) invalidateTableSchema(keyspaceName, tableName string) {
	s.metadata.keyspaceMetadata.invalidateTable(keyspaceName, tableName)
}

// deduplicatedRefreshKeyspace collapses concurrent refreshKeyspaceSchema calls
// for the same keyspace into a single in-flight operation.
func (s *metadataDescriber) deduplicatedRefreshKeyspace(keyspaceName string) error {
	_, err, _ := s.keyspaceGroup.Do(keyspaceName, func() (any, error) {
		return nil, s.refreshKeyspaceSchema(keyspaceName)
	})
	return err
}

// deduplicatedRefreshTable collapses concurrent refreshTableSchema calls
// for the same keyspace/table into a single in-flight operation.
func (s *metadataDescriber) deduplicatedRefreshTable(keyspaceName, tableName string) error {
	key := keyspaceName + "\x00" + tableName
	_, err, _ := s.tableGroup.Do(key, func() (any, error) {
		// A caller may arrive here after deciding to refresh an old snapshot but
		// after another flight already published and completed. Recheck the current
		// immutable snapshot inside this flight before issuing network queries.
		if current, ok := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName); ok {
			if _, present := current.Tables[tableName]; present {
				return nil, nil
			}
		}
		return nil, s.refreshTableSchema(keyspaceName, tableName)
	})
	return err
}

func (s *metadataDescriber) refreshAllSchema() error {
	return s.session.refreshV2AllSchema(v2SchemaContext(s.session))
}

// forcibly updates the current KeyspaceMetadata held by the schema describer
// for a given named keyspace.
//
// All system schema queries are issued concurrently since none of them
// depend on each other's results. The results are only combined in
// compileMetadata after all queries complete.
func (s *metadataDescriber) refreshKeyspaceSchema(keyspaceName string) error {
	return s.session.refreshV2Keyspace(v2SchemaContext(s.session), keyspaceName)
}

func (s *metadataDescriber) refreshTableSchema(keyspaceName, tableName string) error {
	p := s.session.peripheral()
	p.mu.Lock()
	epoch := p.schemaEpoch[keyspaceName]
	revision := p.schemaRevision[keyspaceName]
	tableKey := metadataTable{keyspaceName, tableName}
	tableRevision := p.tableRevision[tableKey]
	p.mu.Unlock()
	_, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
	if !found {
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}

	// Perform network queries outside the lock.
	tables, err := getTableMetadataByName(s.session, keyspaceName, tableName)
	if err != nil {
		return err
	}

	columns, err := getColumnMetadataByTable(s.session, keyspaceName, tableName)
	if err != nil {
		return err
	}

	indexes, err := getIndexMetadataByTable(s.session, keyspaceName, tableName)
	if err != nil {
		return err
	}

	views, err := getViewMetadataByTable(s.session, keyspaceName, tableName)
	if err != nil {
		return err
	}

	ks, _ := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
	var types []TypeMetadata
	if ks != nil {
		for _, typ := range ks.Types {
			if typ != nil {
				types = append(types, *typ)
			}
		}
	}
	if err := validateV2SchemaParts(&KeyspaceMetadata{Name: keyspaceName}, tables, columns, types, nil, nil, indexes, views, s.session.v2MetadataDepthLimit()); err != nil {
		return err
	}
	// Atomically clone-and-swap the keyspace metadata to avoid data races
	// with concurrent readers.
	p.mu.Lock()
	if p.schemaEpoch[keyspaceName] != epoch || p.schemaRevision[keyspaceName] != revision || p.tableRevision[tableKey] != tableRevision || p.schemaDropped[keyspaceName] {
		p.mu.Unlock()
		s.session.scheduleV2SchemaRetry()
		return &PartialRefreshError{Generation: s.session.Metadata().Generation(), Keyspaces: map[string]error{keyspaceName: fmt.Errorf("schema changed during table refresh")}}
	}
	applied := s.metadata.keyspaceMetadata.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		if len(tables) == 0 {
			ks.removeTable(tableName)
		} else {
			compileTableMetadata(ks, tables, columns, indexes, views)
			if ks.tablesInvalidated != nil {
				delete(ks.tablesInvalidated, tableName)
			}
		}
	})
	if applied {
		p.tableRevision[tableKey]++
	}
	p.mu.Unlock()
	if !applied {
		// Keyspace was removed between the initial check and the update.
		// Fall back to a full keyspace refresh to recover.
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}
	s.session.peripheral().schemaGeneration.Add(1)
	s.session.publishV2Metadata()
	return nil
}

// "compiles" derived information about keyspace, table, and column metadata
// for a keyspace from the basic queried metadata objects returned by
// getKeyspaceMetadata, getTableMetadata, and getColumnMetadata respectively;
// Links the metadata objects together and derives the column composition of
// the partition key and clustering key for a table.
func compileMetadata(
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
			key = v2FunctionIdentity(key, functions[i].ArgumentTypes)
		}
		keyspace.Functions[key] = &functions[i]
	}
	keyspace.Aggregates = make(map[string]*AggregateMetadata, len(aggregates))
	aggregateNames := make(map[string]int, len(aggregates))
	for _, aggregate := range aggregates {
		aggregateNames[aggregate.Name]++
	}
	for _, aggregate := range aggregates {
		if f := resolveV2Function(functions, aggregate.finalFunc, []string{aggregate.StateType}); f != nil {
			aggregate.FinalFunc = *f
		} else if f := keyspace.Functions[aggregate.finalFunc]; f != nil {
			aggregate.FinalFunc = *f
		}
		if f := resolveV2Function(functions, aggregate.stateFunc, append([]string{aggregate.StateType}, aggregate.ArgumentTypes...)); f != nil {
			aggregate.StateFunc = *f
		} else if f := keyspace.Functions[aggregate.stateFunc]; f != nil {
			aggregate.StateFunc = *f
		}
		key := aggregate.Name
		if aggregateNames[key] > 1 {
			key = v2FunctionIdentity(key, aggregate.ArgumentTypes)
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

	// add columns from the schema data
	for i := range columns {
		col := &columns[i]
		col.Order = ASC
		if col.ClusteringOrder == "desc" {
			col.Order = DESC
		}

		table, ok := keyspace.Tables[col.Table]
		if !ok {
			// If column owned by a table that the table name ends with `_index`
			// suffix then the table is a view corresponding to some index.
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
				// if the schema is being updated we will race between seeing
				// the metadata be complete. Potentially we should check for
				// schema versions before and after reading the metadata and
				// if they dont match try again.
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
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = compileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = compileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = compileColumns(ix.Columns, ix.OrderedColumns)
	}

	keyspace.CreateStmts = string(createStmts)
}

func compileTableMetadata(
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
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = compileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = compileColumns(v.Columns, v.OrderedColumns)
	}
	for i := range indexes {
		ix := &indexes[i]
		ix.PartitionKey, ix.ClusteringColumns, ix.OrderedColumns = compileColumns(ix.Columns, ix.OrderedColumns)
	}
}

func compileColumns(columns map[string]*ColumnMetadata, orderedColumns []string) (
	partitionKey, clusteringColumns []*ColumnMetadata, sortedColumns []string) {
	clusteringColumnCount := componentColumnCountOfType(columns, ColumnClusteringKey)
	clusteringColumns = make([]*ColumnMetadata, clusteringColumnCount)

	partitionKeyCount := componentColumnCountOfType(columns, ColumnPartitionKey)
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

// returns the count of coluns with the given "kind" value.
func componentColumnCountOfType(columns map[string]*ColumnMetadata, kind ColumnKind) int {
	maxComponentIndex := -1
	for _, column := range columns {
		if column.Kind == kind && column.ComponentIndex > maxComponentIndex {
			maxComponentIndex = column.ComponentIndex
		}
	}
	return maxComponentIndex + 1
}

// query for keyspace metadata in the system_schema.keyspaces
const columnMetadataColumns = `table_name, column_name, clustering_order, type, kind, position`

func getKeyspaceMetadata(s *Session, keyspace string) (*KeyspaceMetadata, error) {
	return getV2KeyspaceMetadata(v2SchemaContext(s), s, keyspace)
}
func getTableMetadata(s *Session, keyspace string) ([]TableMetadata, error) {
	return getV2TableMetadata(v2SchemaContext(s), s, keyspace, "")
}
func getTableMetadataByName(s *Session, keyspace, table string) ([]TableMetadata, error) {
	return getV2TableMetadata(v2SchemaContext(s), s, keyspace, table)
}
func getColumnMetadata(s *Session, keyspace string) ([]ColumnMetadata, error) {
	return getV2ColumnMetadata(v2SchemaContext(s), s, keyspace, "")
}
func getColumnMetadataByTable(s *Session, keyspace, table string) ([]ColumnMetadata, error) {
	return getV2ColumnMetadata(v2SchemaContext(s), s, keyspace, table)
}
func getIndexMetadata(s *Session, keyspace string) ([]IndexMetadata, error) {
	return getV2IndexMetadata(v2SchemaContext(s), s, keyspace, "")
}
func getIndexMetadataByTable(s *Session, keyspace, table string) ([]IndexMetadata, error) {
	return getV2IndexMetadata(v2SchemaContext(s), s, keyspace, table)
}
func getViewMetadata(s *Session, keyspace string) ([]ViewMetadata, error) {
	return getV2ViewMetadata(v2SchemaContext(s), s, keyspace, "")
}
func getViewMetadataByTable(s *Session, keyspace, table string) ([]ViewMetadata, error) {
	return getV2ViewMetadata(v2SchemaContext(s), s, keyspace, table)
}
func getTypeMetadata(s *Session, keyspace string) ([]TypeMetadata, error) {
	return queryV2SchemaRows(v2SchemaContext(s), s, `SELECT * FROM system_schema.types WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeTypeMetadata(keyspace))
}
func getFunctionsMetadata(s *Session, keyspace string) ([]FunctionMetadata, error) {
	if !s.hasAggregatesAndFunctions {
		return nil, nil
	}
	return queryV2SchemaRows(v2SchemaContext(s), s, `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeFunctionMetadata(keyspace))
}
func getAggregatesMetadata(s *Session, keyspace string) ([]AggregateMetadata, error) {
	if !s.hasAggregatesAndFunctions {
		return nil, nil
	}
	return queryV2SchemaRows(v2SchemaContext(s), s, `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeAggregateMetadata(keyspace))
}
func getCreateStatements(s *Session, keyspace string) ([]byte, error) {
	return getV2CreateStatements(v2SchemaContext(s), s, keyspace)
}
