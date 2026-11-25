// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	frm "github.com/gocql/gocql/internal/frame"
	"github.com/gocql/gocql/tablets"
)

// schema metadata for a keyspace
type KeyspaceMetadata struct {
	StrategyOptions           map[string]any
	Tables                    map[string]*TableMetadata
	Functions                 map[string]*FunctionMetadata
	Aggregates                map[string]*AggregateMetadata
	Types                     map[string]*TypeMetadata
	Indexes                   map[string]*IndexMetadata
	Views                     map[string]*ViewMetadata
	tablesInvalidated         map[string]struct{}
	Name                      string
	StrategyClass             string
	CreateStmts               string
	createStmtsSeq            uint64 // fetch order of CreateStmts, from invalidationGen
	DurableWrites             bool
	typesInvalidated          bool
	functionsInvalidated      bool
	aggregatesInvalidated     bool
	typesInvalidationGen      uint64
	functionsInvalidationGen  uint64
	aggregatesInvalidationGen uint64
}

// Clone returns a shallow copy of the keyspace metadata with
// cloned Tables, Indexes, Views, and tablesInvalidated maps so that mutations
// do not race with concurrent readers of the original.
func (ks *KeyspaceMetadata) Clone() *KeyspaceMetadata {
	cloned := &KeyspaceMetadata{
		Name:                      ks.Name,
		DurableWrites:             ks.DurableWrites,
		StrategyClass:             ks.StrategyClass,
		StrategyOptions:           maps.Clone(ks.StrategyOptions),
		Tables:                    maps.Clone(ks.Tables),
		Functions:                 maps.Clone(ks.Functions),
		Aggregates:                maps.Clone(ks.Aggregates),
		Types:                     maps.Clone(ks.Types),
		Indexes:                   maps.Clone(ks.Indexes),
		Views:                     maps.Clone(ks.Views),
		CreateStmts:               ks.CreateStmts,
		createStmtsSeq:            ks.createStmtsSeq,
		typesInvalidated:          ks.typesInvalidated,
		functionsInvalidated:      ks.functionsInvalidated,
		aggregatesInvalidated:     ks.aggregatesInvalidated,
		typesInvalidationGen:      ks.typesInvalidationGen,
		functionsInvalidationGen:  ks.functionsInvalidationGen,
		aggregatesInvalidationGen: ks.aggregatesInvalidationGen,
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

func (ks *KeyspaceMetadata) needsPartialRefresh() bool {
	return ks.typesInvalidated || ks.functionsInvalidated || ks.aggregatesInvalidated
}

// Globally unique, monotonic: per-keyspace counters would restart at 0 when
// an entry is recreated, letting stale partial results match (ABA).
var invalidationGen atomic.Uint64

func (ks *KeyspaceMetadata) invalidateTypes() {
	ks.typesInvalidated = true
	ks.typesInvalidationGen = invalidationGen.Add(1)
}

func (ks *KeyspaceMetadata) invalidateFunctions() {
	ks.functionsInvalidated = true
	ks.aggregatesInvalidated = true
	ks.functionsInvalidationGen = invalidationGen.Add(1)
	// Aggregates embed copies of function metadata, so they must be refreshed
	// whenever functions change.
	ks.aggregatesInvalidationGen = invalidationGen.Add(1)
}

func (ks *KeyspaceMetadata) invalidateAggregates() {
	ks.aggregatesInvalidated = true
	ks.aggregatesInvalidationGen = invalidationGen.Add(1)
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

// hasRelation reports whether name is a table, a view, or an index's backing view.
// Mirrors compileMetadata, which omits index backing views from Views.
func (ks *KeyspaceMetadata) hasRelation(name string) bool {
	_, isTable := ks.Tables[name]
	_, isView := ks.Views[name]
	isIndex := false
	if base, ok := strings.CutSuffix(name, "_index"); ok {
		_, isIndex = ks.Indexes[base]
	}
	return isTable || isView || isIndex
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

// replaceKeyspace publishes replacement, carrying forward categories
// invalidated after the start generations so stale fetched values don't win.
func (c *cowKeyspaceMetadataMap) replaceKeyspace(keyspaceName string, replacement *KeyspaceMetadata, startTypesGeneration, startFunctionsGeneration, startAggregatesGeneration uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.get()
	if current, ok := m[keyspaceName]; ok && current != nil {
		preserved := false
		if current.typesInvalidationGen > startTypesGeneration {
			preserved = true
			replacement.typesInvalidationGen = current.typesInvalidationGen
			replacement.typesInvalidated = current.typesInvalidated
			replacement.Types = current.Types
		}
		if current.functionsInvalidationGen > startFunctionsGeneration {
			preserved = true
			replacement.functionsInvalidationGen = current.functionsInvalidationGen
			replacement.functionsInvalidated = current.functionsInvalidated
			replacement.Functions = current.Functions
		}
		if current.aggregatesInvalidationGen > startAggregatesGeneration {
			preserved = true
			replacement.aggregatesInvalidationGen = current.aggregatesInvalidationGen
			replacement.aggregatesInvalidated = current.aggregatesInvalidated
			replacement.Aggregates = current.Aggregates
		}
		// Keep the DESCRIBE fetched later; a still-dirty entry has no matching text.
		if preserved {
			if current.needsPartialRefresh() {
				replacement.CreateStmts, replacement.createStmtsSeq = "", 0
			} else if current.createStmtsSeq > replacement.createStmtsSeq {
				replacement.CreateStmts, replacement.createStmtsSeq = current.CreateStmts, current.createStmtsSeq
			}
		}
	}

	newM := maps.Clone(m)
	if newM == nil {
		newM = make(map[string]*KeyspaceMetadata)
	}
	newM[keyspaceName] = replacement
	c.keyspaceMap.Store(&newM)
}

func (c *cowKeyspaceMetadataMap) invalidateTable(keyspaceName, tableName string) {
	c.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateTable(tableName)
	})
}

func (c *cowKeyspaceMetadataMap) invalidateTypes(keyspaceName string) {
	c.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateTypes()
	})
}

func (c *cowKeyspaceMetadataMap) invalidateFunctions(keyspaceName string) {
	c.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateFunctions()
	})
}

func (c *cowKeyspaceMetadataMap) invalidateAggregates(keyspaceName string) {
	c.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		ks.invalidateAggregates()
	})
}

// removeKeyspaceIf removes keyspaceName only while it still maps to expected.
func (c *cowKeyspaceMetadataMap) removeKeyspaceIf(keyspaceName string, expected *KeyspaceMetadata) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.get()
	if cur, ok := m[keyspaceName]; !ok || cur != expected {
		return false
	}
	newM := maps.Clone(m)
	delete(newM, keyspaceName)
	c.keyspaceMap.Store(&newM)
	return true
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
	keyspaceGroup       singleflight.Group
	partialRefreshGroup singleflight.Group
	tableGroup          singleflight.Group
	session             *Session
	metadata            *Metadata

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

	// Bounded: a sustained invalidation burst would otherwise spin forever;
	// fall back to a full refresh below.
	const maxPartialRefreshAttempts = 3
	for attempt := 0; attempt < maxPartialRefreshAttempts && metadata.needsPartialRefresh(); attempt++ {
		if err = s.deduplicatedPartialRefreshKeyspace(keyspaceName); err != nil {
			return nil, wasReloaded, err
		}
		metadata, found = s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
		if !found {
			return nil, wasReloaded, fmt.Errorf("keyspace %s: %w", keyspaceName, ErrNotFound)
		}
	}

	if metadata.needsPartialRefresh() {
		// The invalidation burst outran the partial loop; a full refresh clears
		// every category at once when it lands on a quiet instant.
		if err := s.refreshKeyspaceOrEvict(keyspaceName, metadata); err != nil {
			return nil, wasReloaded, err
		}
		metadata, found = s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
		if !found {
			return nil, wasReloaded, fmt.Errorf("keyspace %s: %w", keyspaceName, ErrNotFound)
		}
		// The joined full refresh may predate the latest invalidation.
		if metadata.needsPartialRefresh() {
			if err := s.deduplicatedPartialRefreshKeyspace(keyspaceName); err != nil {
				return nil, wasReloaded, err
			}
			metadata, found = s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
			if !found {
				return nil, wasReloaded, fmt.Errorf("keyspace %s: %w", keyspaceName, ErrNotFound)
			}
		}
	}

	return metadata, wasReloaded, nil
}

// refreshKeyspaceOrEvict drops the cached entry of a keyspace the server no
// longer has, so it cannot be served dirty forever.
func (s *metadataDescriber) refreshKeyspaceOrEvict(keyspaceName string, seen *KeyspaceMetadata) error {
	err := s.deduplicatedRefreshKeyspace(keyspaceName)
	if errors.Is(err, ErrKeyspaceDoesNotExist) {
		s.evictKeyspace(keyspaceName, seen)
	}
	return err
}

// evictKeyspace removes a dropped keyspace only if the cache still holds the
// entry seen before the refresh, so a late result cannot evict a recreated one.
func (s *metadataDescriber) evictKeyspace(keyspaceName string, seen *KeyspaceMetadata) {
	if s.metadata.keyspaceMetadata.removeKeyspaceIf(keyspaceName, seen) {
		s.RemoveTabletsWithKeyspace(keyspaceName)
	}
}

func (s *metadataDescriber) GetKeyspace(keyspaceName string) (*KeyspaceMetadata, error) {
	metadata, _, err := s.getKeyspaceInternal(keyspaceName)
	// Invalidated tables (ALTER TABLE, or ALTER TYPE's dependent tables) are
	// absent from Tables until refetched. Bounded, as tables may keep changing.
	const maxTableRefreshAttempts = 3
	for attempt := 0; err == nil && len(metadata.tablesInvalidated) > 0 && attempt < maxTableRefreshAttempts; attempt++ {
		if err = s.refreshInvalidatedTables(keyspaceName, metadata, attempt == maxTableRefreshAttempts-1); err != nil {
			return nil, err
		}
		metadata, _, err = s.getKeyspaceInternal(keyspaceName)
	}
	return metadata, err
}

// refreshInvalidatedTables refetches each invalidated table of the snapshot,
// or the whole keyspace when that is cheaper or a table refresh fails.
func (s *metadataDescriber) refreshInvalidatedTables(keyspaceName string, snapshot *KeyspaceMetadata, forceFull bool) error {
	// shortcut: unmeasured; per table is 4 queries, a full refresh a few more plus all rows.
	const maxPerTableRefreshes = 4
	if !forceFull && len(snapshot.tablesInvalidated) <= maxPerTableRefreshes {
		var err error
		for tableName := range snapshot.tablesInvalidated { // snapshot is immutable
			if err = s.deduplicatedRefreshTable(keyspaceName, tableName); err != nil {
				break
			}
		}
		if err == nil {
			return nil
		}
		s.session.logger.Printf("gocql: table schema refresh in keyspace %q failed, falling back to full refresh: %v", keyspaceName, err)
	}
	// Table refreshes above may have republished the entry; evict only the current one.
	if current, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName); found {
		snapshot = current
	}
	return s.refreshKeyspaceOrEvict(keyspaceName, snapshot)
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

func (s *metadataDescriber) invalidateTypesSchema(keyspaceName string) {
	s.metadata.keyspaceMetadata.invalidateTypes(keyspaceName)
}

func (s *metadataDescriber) invalidateFunctionsSchema(keyspaceName string) {
	s.metadata.keyspaceMetadata.invalidateFunctions(keyspaceName)
}

func (s *metadataDescriber) invalidateAggregatesSchema(keyspaceName string) {
	s.metadata.keyspaceMetadata.invalidateAggregates(keyspaceName)
}

// deduplicatedRefreshKeyspace collapses concurrent refreshKeyspaceSchema calls
// for the same keyspace into a single in-flight operation.
func (s *metadataDescriber) deduplicatedRefreshKeyspace(keyspaceName string) error {
	_, err, _ := s.keyspaceGroup.Do(keyspaceName, func() (any, error) {
		return nil, s.refreshKeyspaceSchema(keyspaceName)
	})
	return err
}

// deduplicatedPartialRefreshKeyspace collapses concurrent partial refreshes
// of a keyspace into one in-flight operation to avoid query floods.
func (s *metadataDescriber) deduplicatedPartialRefreshKeyspace(keyspaceName string) error {
	_, err, _ := s.partialRefreshGroup.Do(keyspaceName, func() (any, error) {
		// Re-read inside the shared flight so every caller uses the newest
		// generation counters, not its own stale snapshot.
		snapshot, ok := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
		if !ok {
			return nil, s.deduplicatedRefreshKeyspace(keyspaceName)
		}
		err := s.refreshPartialKeyspaceSchema(keyspaceName, snapshot)
		if err != nil && !errors.Is(err, ErrKeyspaceDoesNotExist) {
			// Logged here, not per caller, so a shared failure logs once.
			s.session.logger.Printf("gocql: partial schema refresh of keyspace %q failed, falling back to full refresh: %v", keyspaceName, err)
			err = s.deduplicatedRefreshKeyspace(keyspaceName)
		}
		if errors.Is(err, ErrKeyspaceDoesNotExist) {
			s.evictKeyspace(keyspaceName, snapshot)
		}
		return nil, err
	})
	return err
}

// deduplicatedRefreshTable collapses concurrent refreshTableSchema calls
// for the same keyspace/table into a single in-flight operation.
func (s *metadataDescriber) deduplicatedRefreshTable(keyspaceName, tableName string) error {
	key := keyspaceName + "\x00" + tableName
	_, err, _ := s.tableGroup.Do(key, func() (any, error) {
		return nil, s.refreshTableSchema(keyspaceName, tableName)
	})
	return err
}

func (s *metadataDescriber) refreshAllSchema() error {
	// mu serialises concurrent refreshAllSchema calls so each one sees a
	// consistent snapshot before deciding what changed.  Individual keyspace
	// refreshes inside the loop go through singleflight, so two overlapping
	// refreshAllSchema calls will not duplicate network queries — the second
	// caller blocks on mu while the first finishes.
	s.mu.Lock()
	defer s.mu.Unlock()

	copiedMap := make(map[string]*KeyspaceMetadata)
	for key, value := range s.metadata.keyspaceMetadata.get() {
		if value != nil {
			copiedMap[key] = value.Clone()
		} else {
			copiedMap[key] = nil
		}
	}

	for keyspaceName, metadata := range copiedMap {
		// Route through singleflight to dedup concurrent refreshes.
		err := s.deduplicatedRefreshKeyspace(keyspaceName)
		if errors.Is(err, ErrKeyspaceDoesNotExist) {
			s.invalidateKeyspaceSchema(keyspaceName)
			s.RemoveTabletsWithKeyspace(keyspaceName)
			continue
		} else if err != nil {
			return err
		}

		// Read the cache: GetKeyspace may run a partial refresh whose failure would abort the loop.
		updatedMetadata, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName)
		if !found {
			continue
		}

		if !compareInterfaceMaps(metadata.StrategyOptions, updatedMetadata.StrategyOptions) {
			s.RemoveTabletsWithKeyspace(keyspaceName)
			continue
		}

		for tableName := range metadata.Tables {
			if _, ok := updatedMetadata.Tables[tableName]; !ok {
				s.RemoveTabletsWithTable(keyspaceName, tableName)
			}
		}
		for tableName := range metadata.tablesInvalidated {
			if !updatedMetadata.hasRelation(tableName) {
				s.RemoveTabletsWithTable(keyspaceName, tableName)
			}
		}
	}
	return nil
}

// forcibly updates the current KeyspaceMetadata held by the schema describer
// for a given named keyspace.
//
// All system schema queries are issued concurrently since none of them
// depend on each other's results. The results are only combined in
// compileMetadata after all queries complete.
func (s *metadataDescriber) refreshKeyspaceSchema(keyspaceName string) error {
	var (
		startTypesGeneration      uint64
		startFunctionsGeneration  uint64
		startAggregatesGeneration uint64
	)
	if current, found := s.metadata.keyspaceMetadata.getKeyspace(keyspaceName); found && current != nil {
		startTypesGeneration = current.typesInvalidationGen
		startFunctionsGeneration = current.functionsInvalidationGen
		startAggregatesGeneration = current.aggregatesInvalidationGen
	}

	var (
		keyspace       *KeyspaceMetadata
		tables         []TableMetadata
		columns        []ColumnMetadata
		functions      []FunctionMetadata
		aggregates     []AggregateMetadata
		types          []TypeMetadata
		indexes        []IndexMetadata
		views          []ViewMetadata
		createStmts    []byte
		createStmtsSeq uint64
	)

	// Each goroutine writes to its own dedicated variable, so no
	// synchronisation is needed beyond errgroup itself.
	var g errgroup.Group

	var keyspaceErr error
	g.Go(func() error {
		keyspace, keyspaceErr = getKeyspaceMetadata(s.session, keyspaceName)
		return keyspaceErr
	})
	g.Go(func() error {
		var err error
		tables, err = getTableMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		columns, err = getColumnMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		functions, err = getFunctionsMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		aggregates, err = getAggregatesMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		types, err = getTypeMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		indexes, err = getIndexMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		views, err = getViewMetadata(s.session, keyspaceName)
		return err
	})
	g.Go(func() error {
		var err error
		createStmtsSeq = invalidationGen.Add(1)
		createStmts, err = getCreateStatements(s.session, keyspaceName)
		return err
	})

	if err := g.Wait(); err != nil {
		// errgroup keeps the first error; a dropped keyspace must still report ErrKeyspaceDoesNotExist.
		if errors.Is(keyspaceErr, ErrKeyspaceDoesNotExist) {
			return keyspaceErr
		}
		return err
	}

	compileMetadata(keyspace, tables, columns, functions, aggregates, types, indexes, views, createStmts)
	keyspace.createStmtsSeq = createStmtsSeq
	keyspace.typesInvalidationGen = startTypesGeneration
	keyspace.functionsInvalidationGen = startFunctionsGeneration
	keyspace.aggregatesInvalidationGen = startAggregatesGeneration

	s.metadata.keyspaceMetadata.replaceKeyspace(keyspaceName, keyspace, startTypesGeneration, startFunctionsGeneration, startAggregatesGeneration)

	return nil
}

func (s *metadataDescriber) refreshTableSchema(keyspaceName, tableName string) error {
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

	// Views, including index backing views, keep their columns under their own name.
	for _, view := range views {
		viewColumns, err := getColumnMetadataByTable(s.session, keyspaceName, view.ViewName)
		if err != nil {
			return err
		}
		columns = append(columns, viewColumns...)
	}

	// Atomically clone-and-swap the keyspace metadata to avoid data races
	// with concurrent readers.
	dropped := false
	applied := s.metadata.keyspaceMetadata.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		if len(tables) == 0 {
			ks.removeTable(tableName)
			// removeTable clears the marker refreshAllSchema would use to drop these tablets.
			dropped = !ks.hasRelation(tableName)
		} else {
			compileTableMetadata(ks, tables, columns, indexes, views)
			if ks.tablesInvalidated != nil {
				delete(ks.tablesInvalidated, tableName)
			}
		}
	})
	if !applied {
		// Keyspace was removed between the initial check and the update.
		// Fall back to a full keyspace refresh to recover.
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}
	if dropped {
		s.RemoveTabletsWithTable(keyspaceName, tableName)
	}
	return nil
}

// refreshPartialKeyspaceSchema refetches only the invalidated categories
// (types, functions, aggregates) and atomically updates the cached keyspace.
func (s *metadataDescriber) refreshPartialKeyspaceSchema(keyspaceName string, snapshot *KeyspaceMetadata) error {
	typesGeneration := snapshot.typesInvalidationGen
	functionsGeneration := snapshot.functionsInvalidationGen
	aggregatesGeneration := snapshot.aggregatesInvalidationGen

	needTypes := snapshot.typesInvalidated
	needFunctions := snapshot.functionsInvalidated
	needAggregates := snapshot.aggregatesInvalidated

	// Pre-2.2 servers have no UDF/UDA: skip the fetch and only clear the
	// dirty flags.
	hasFunctions := s.session.hasAggregatesAndFunctions
	// DESCRIBE still covers UDF/UDA when their tables are not queried.
	fetchText := needTypes || needFunctions || needAggregates

	var (
		types      []TypeMetadata
		functions  []FunctionMetadata
		aggregates []AggregateMetadata
	)

	var g errgroup.Group

	// Type/function queries return no rows for a dropped keyspace and DESCRIBE
	// may be unsupported, so only this query reliably detects the drop.
	var keyspaceErr error
	if s.session.useSystemSchema && fetchText {
		g.Go(func() error {
			_, keyspaceErr = getKeyspaceMetadata(s.session, keyspaceName)
			return keyspaceErr
		})
	}
	if needTypes {
		g.Go(func() error {
			var err error
			types, err = getTypeMetadata(s.session, keyspaceName)
			return err
		})
	}
	// Aggregates depend on functions, so if aggregates are invalidated we
	// must also refresh functions to compile aggregate metadata correctly.
	if (needFunctions || needAggregates) && hasFunctions {
		g.Go(func() error {
			var err error
			functions, err = getFunctionsMetadata(s.session, keyspaceName)
			return err
		})
	}
	if needAggregates && hasFunctions {
		g.Go(func() error {
			var err error
			aggregates, err = getAggregatesMetadata(s.session, keyspaceName)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		// errgroup keeps the first error; a dropped keyspace must still report ErrKeyspaceDoesNotExist.
		if errors.Is(keyspaceErr, ErrKeyspaceDoesNotExist) {
			return keyspaceErr
		}
		return err
	}

	// Fetched after the maps so the text is at least as new as they are.
	// nil (DESCRIBE unsupported) clears CreateStmts for client-side rebuild.
	var createStmts []byte
	var createStmtsSeq uint64
	if fetchText {
		var err error
		createStmtsSeq = invalidationGen.Add(1)
		if createStmts, err = getCreateStatements(s.session, keyspaceName); err != nil {
			return err
		}
	}

	applied := s.metadata.keyspaceMetadata.updateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		changed := false

		applyFunctions := false
		if needFunctions {
			applyFunctions = ks.functionsInvalidated && ks.functionsInvalidationGen == functionsGeneration
		} else if needAggregates {
			// When functions are fetched only to recompile aggregates, avoid
			// overwriting a concurrently full-refreshed function map.
			applyFunctions = ks.aggregatesInvalidated && ks.aggregatesInvalidationGen == aggregatesGeneration
		}

		if needTypes && ks.typesInvalidated && ks.typesInvalidationGen == typesGeneration {
			ks.Types = make(map[string]*TypeMetadata, len(types))
			for i := range types {
				ks.Types[types[i].Name] = &types[i]
			}
			ks.typesInvalidated = false
			changed = true
		}
		if applyFunctions {
			if hasFunctions {
				ks.Functions = make(map[string]*FunctionMetadata, len(functions))
				for i := range functions {
					ks.Functions[functions[i].Name] = &functions[i]
				}
			}
			ks.functionsInvalidated = false
			changed = true
		}
		if needAggregates && ks.aggregatesInvalidated && ks.aggregatesInvalidationGen == aggregatesGeneration {
			if hasFunctions {
				ks.Aggregates = make(map[string]*AggregateMetadata, len(aggregates))
				for i := range aggregates {
					aggregate := &aggregates[i]
					if fn, ok := ks.Functions[aggregate.finalFunc]; ok {
						aggregate.FinalFunc = *fn
					}
					if fn, ok := ks.Functions[aggregate.stateFunc]; ok {
						aggregate.StateFunc = *fn
					}
					ks.Aggregates[aggregate.Name] = aggregate
				}
			}
			ks.aggregatesInvalidated = false
			changed = true
		}
		if changed {
			if ks.needsPartialRefresh() {
				// The fetched text can't match a category still dirty.
				ks.CreateStmts, ks.createStmtsSeq = "", 0
			} else if createStmtsSeq > ks.createStmtsSeq {
				ks.CreateStmts, ks.createStmtsSeq = string(createStmts), createStmtsSeq
			}
		}
	})
	if !applied {
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}
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
	for i := range functions {
		keyspace.Functions[functions[i].Name] = &functions[i]
	}
	keyspace.Aggregates = make(map[string]*AggregateMetadata, len(aggregates))
	for _, aggregate := range aggregates {
		// FINALFUNC is optional in CQL.
		if fn, ok := keyspace.Functions[aggregate.finalFunc]; ok {
			aggregate.FinalFunc = *fn
		}
		if fn, ok := keyspace.Functions[aggregate.stateFunc]; ok {
			aggregate.StateFunc = *fn
		}
		keyspace.Aggregates[aggregate.Name] = &aggregate
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
func getKeyspaceMetadata(session *Session, keyspaceName string) (*KeyspaceMetadata, error) {
	if !session.useSystemSchema {
		return nil, ErrKeyspaceDoesNotExist
	}
	keyspace := &KeyspaceMetadata{Name: keyspaceName}

	const stmt = `SELECT durable_writes, replication FROM system_schema.keyspaces WHERE keyspace_name = ?`

	var replication map[string]string

	iter := session.control.querySystem(stmt, keyspaceName)
	if iter.NumRows() == 0 {
		// A failed query has no rows either; only an answer with no rows means
		// the keyspace is gone.
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("error querying keyspace schema: %w", err)
		}
		return nil, ErrKeyspaceDoesNotExist
	}
	iter.Scan(&keyspace.DurableWrites, &replication)
	err := iter.Close()
	if err != nil {
		return nil, fmt.Errorf("error querying keyspace schema: %w", err)
	}

	keyspace.StrategyClass = replication["class"]
	delete(replication, "class")

	keyspace.StrategyOptions = make(map[string]any, len(replication))
	for k, v := range replication {
		keyspace.StrategyOptions[k] = v
	}

	return keyspace, nil
}

// query for table metadata in the system_schema.tables, and system_schema.scylla_tables
// if connected to ScyllaDB
func getTableMetadata(session *Session, keyspaceName string) ([]TableMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.tables WHERE keyspace_name = ?`
	iter := session.control.querySystem(stmt, keyspaceName)

	var tables []TableMetadata
	table := TableMetadata{Keyspace: keyspaceName}
	for iter.MapScan(map[string]any{
		"table_name":                  &table.Name,
		"bloom_filter_fp_chance":      &table.Options.BloomFilterFpChance,
		"caching":                     &table.Options.Caching,
		"comment":                     &table.Options.Comment,
		"compaction":                  &table.Options.Compaction,
		"compression":                 &table.Options.Compression,
		"crc_check_chance":            &table.Options.CrcCheckChance,
		"default_time_to_live":        &table.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &table.Options.GcGraceSeconds,
		"max_index_interval":          &table.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &table.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &table.Options.MinIndexInterval,
		"speculative_retry":           &table.Options.SpeculativeRetry,
		"flags":                       &table.Flags,
		"extensions":                  &table.Extensions,
	}) {
		tables = append(tables, table)
		table = TableMetadata{Keyspace: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying table schema: %w", err)
	}

	conn := session.getConn()
	if conn == nil || !conn.isScyllaConn() {
		return tables, nil
	}

	// Fetch all ScyllaDB-specific table properties in a single query
	// instead of issuing one query per table (N+1 elimination).
	stmt = `SELECT * FROM system_schema.scylla_tables WHERE keyspace_name = ?`
	iter = session.control.querySystem(stmt, keyspaceName)

	scyllaOpts := make(map[string]TableMetadataOptions, len(tables))
	var opts TableMetadataOptions
	var tblName string
	for iter.MapScan(map[string]any{
		"table_name":  &tblName,
		"cdc":         &opts.CDC,
		"in_memory":   &opts.InMemory,
		"partitioner": &opts.Partitioner,
		"version":     &opts.Version,
	}) {
		scyllaOpts[tblName] = opts
		opts = TableMetadataOptions{}
		tblName = ""
	}
	if err := iter.Close(); err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying scylla table schema: %w", err)
	}

	for i, t := range tables {
		if sopts, ok := scyllaOpts[t.Name]; ok {
			tables[i].Options.CDC = sopts.CDC
			tables[i].Options.InMemory = sopts.InMemory
			tables[i].Options.Partitioner = sopts.Partitioner
			tables[i].Options.Version = sopts.Version
		}
	}

	return tables, nil
}

func getTableMetadataByName(session *Session, keyspaceName, tableName string) ([]TableMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`
	iter := session.control.querySystem(stmt, keyspaceName, tableName)

	var tables []TableMetadata
	table := TableMetadata{Keyspace: keyspaceName}
	for iter.MapScan(map[string]any{
		"table_name":                  &table.Name,
		"bloom_filter_fp_chance":      &table.Options.BloomFilterFpChance,
		"caching":                     &table.Options.Caching,
		"comment":                     &table.Options.Comment,
		"compaction":                  &table.Options.Compaction,
		"compression":                 &table.Options.Compression,
		"crc_check_chance":            &table.Options.CrcCheckChance,
		"default_time_to_live":        &table.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &table.Options.GcGraceSeconds,
		"max_index_interval":          &table.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &table.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &table.Options.MinIndexInterval,
		"speculative_retry":           &table.Options.SpeculativeRetry,
		"flags":                       &table.Flags,
		"extensions":                  &table.Extensions,
	}) {
		tables = append(tables, table)
		table = TableMetadata{Keyspace: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying table schema: %w", err)
	}

	if conn := session.getConn(); conn == nil || !conn.isScyllaConn() {
		return tables, nil
	}

	stmt = `SELECT * FROM system_schema.scylla_tables WHERE keyspace_name = ? AND table_name = ?`
	for i, t := range tables {
		iter := session.control.querySystem(stmt, keyspaceName, t.Name)

		table := TableMetadata{}
		if iter.MapScan(map[string]any{
			"cdc":         &table.Options.CDC,
			"in_memory":   &table.Options.InMemory,
			"partitioner": &table.Options.Partitioner,
			"version":     &table.Options.Version,
		}) {
			tables[i].Options.CDC = table.Options.CDC
			tables[i].Options.Version = table.Options.Version
			tables[i].Options.Partitioner = table.Options.Partitioner
			tables[i].Options.InMemory = table.Options.InMemory
		}
		if err := iter.Close(); err != nil && err != ErrNotFound {
			return nil, fmt.Errorf("error querying scylla table schema: %w", err)
		}
	}

	return tables, nil
}

// columnMetadataColumns lists the columns consumed by getColumnMetadata and
// getColumnMetadataByTable. Selecting only these columns (instead of SELECT *)
// avoids deserializing unused fields such as keyspace_name (already known from
// the WHERE clause) and column_name_bytes (ScyllaDB-specific), which can add
// over 50 KB of wasted payload per keyspace with 80+ tables.
const columnMetadataColumns = `table_name, column_name, clustering_order, type, kind, position`

func getColumnMetadataByTable(session *Session, keyspaceName, tableName string) ([]ColumnMetadata, error) {
	const stmt = `SELECT ` + columnMetadataColumns + ` FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ?`

	var columns []ColumnMetadata

	iter := session.control.querySystem(stmt, keyspaceName, tableName)
	column := ColumnMetadata{Keyspace: keyspaceName}

	for iter.MapScan(map[string]any{
		"table_name":       &column.Table,
		"column_name":      &column.Name,
		"clustering_order": &column.ClusteringOrder,
		"type":             &column.Type,
		"kind":             &column.Kind,
		"position":         &column.ComponentIndex,
	}) {
		columns = append(columns, column)
		column = ColumnMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying column schema: %w", err)
	}

	return columns, nil
}

func getIndexMetadataByTable(session *Session, keyspaceName, tableName string) ([]IndexMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	const stmt = `SELECT * FROM system_schema.indexes WHERE keyspace_name = ? AND table_name = ?`

	var indexes []IndexMetadata
	index := IndexMetadata{}

	iter := session.control.querySystem(stmt, keyspaceName, tableName)
	for iter.MapScan(map[string]any{
		"index_name":    &index.Name,
		"keyspace_name": &index.KeyspaceName,
		"table_name":    &index.TableName,
		"kind":          &index.Kind,
		"options":       &index.Options,
	}) {
		indexes = append(indexes, index)
		index = IndexMetadata{}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return indexes, nil
}

func getViewMetadataByTable(session *Session, keyspaceName, tableName string) ([]ViewMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.views WHERE keyspace_name = ? AND base_table_name = ? ALLOW FILTERING`

	iter := session.control.querySystem(stmt, keyspaceName, tableName)

	var views []ViewMetadata
	view := ViewMetadata{KeyspaceName: keyspaceName}

	for iter.MapScan(map[string]any{
		"id":                          &view.ID,
		"view_name":                   &view.ViewName,
		"base_table_id":               &view.BaseTableID,
		"base_table_name":             &view.BaseTableName,
		"include_all_columns":         &view.IncludeAllColumns,
		"where_clause":                &view.WhereClause,
		"bloom_filter_fp_chance":      &view.Options.BloomFilterFpChance,
		"caching":                     &view.Options.Caching,
		"comment":                     &view.Options.Comment,
		"compaction":                  &view.Options.Compaction,
		"compression":                 &view.Options.Compression,
		"crc_check_chance":            &view.Options.CrcCheckChance,
		"default_time_to_live":        &view.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &view.Options.GcGraceSeconds,
		"max_index_interval":          &view.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &view.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &view.Options.MinIndexInterval,
		"speculative_retry":           &view.Options.SpeculativeRetry,
		"extensions":                  &view.Extensions,
		"dclocal_read_repair_chance":  &view.DcLocalReadRepairChance,
		"read_repair_chance":          &view.ReadRepairChance,
	}) {
		views = append(views, view)
		view = ViewMetadata{KeyspaceName: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying view schema: %w", err)
	}

	return views, nil
}

// query for column metadata in the system_schema.columns
func getColumnMetadata(session *Session, keyspaceName string) ([]ColumnMetadata, error) {
	const stmt = `SELECT ` + columnMetadataColumns + ` FROM system_schema.columns WHERE keyspace_name = ?`

	var columns []ColumnMetadata

	iter := session.control.querySystem(stmt, keyspaceName)
	column := ColumnMetadata{Keyspace: keyspaceName}

	for iter.MapScan(map[string]any{
		"table_name":       &column.Table,
		"column_name":      &column.Name,
		"clustering_order": &column.ClusteringOrder,
		"type":             &column.Type,
		"kind":             &column.Kind,
		"position":         &column.ComponentIndex,
	}) {
		columns = append(columns, column)
		column = ColumnMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying column schema: %w", err)
	}

	return columns, nil
}

// query for type metadata in the system_schema.types
func getTypeMetadata(session *Session, keyspaceName string) ([]TypeMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.types WHERE keyspace_name = ?`
	iter := session.control.querySystem(stmt, keyspaceName)

	var types []TypeMetadata
	tm := TypeMetadata{Keyspace: keyspaceName}

	for iter.MapScan(map[string]any{
		"type_name":   &tm.Name,
		"field_names": &tm.FieldNames,
		"field_types": &tm.FieldTypes,
	}) {
		types = append(types, tm)
		tm = TypeMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return types, nil
}

// query for function metadata in the system_schema.functions
func getFunctionsMetadata(session *Session, keyspaceName string) ([]FunctionMetadata, error) {
	if !session.hasAggregatesAndFunctions || !session.useSystemSchema {
		return nil, nil
	}
	stmt := `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`

	var functions []FunctionMetadata
	function := FunctionMetadata{Keyspace: keyspaceName}

	iter := session.control.querySystem(stmt, keyspaceName)
	for iter.MapScan(map[string]any{
		"function_name":        &function.Name,
		"argument_types":       &function.ArgumentTypes,
		"argument_names":       &function.ArgumentNames,
		"body":                 &function.Body,
		"called_on_null_input": &function.CalledOnNullInput,
		"language":             &function.Language,
		"return_type":          &function.ReturnType,
	}) {
		functions = append(functions, function)
		function = FunctionMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return functions, nil
}

// query for aggregate metadata in the system_schema.aggregates
func getAggregatesMetadata(session *Session, keyspaceName string) ([]AggregateMetadata, error) {
	if !session.hasAggregatesAndFunctions || !session.useSystemSchema {
		return nil, nil
	}

	const stmt = `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`

	var aggregates []AggregateMetadata
	aggregate := AggregateMetadata{Keyspace: keyspaceName}

	iter := session.control.querySystem(stmt, keyspaceName)
	for iter.MapScan(map[string]any{
		"aggregate_name": &aggregate.Name,
		"argument_types": &aggregate.ArgumentTypes,
		"final_func":     &aggregate.finalFunc,
		"initcond":       &aggregate.InitCond,
		"return_type":    &aggregate.ReturnType,
		"state_func":     &aggregate.stateFunc,
		"state_type":     &aggregate.StateType,
	}) {
		aggregates = append(aggregates, aggregate)
		aggregate = AggregateMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return aggregates, nil
}

// query for index metadata in the system_schema.indexes
func getIndexMetadata(session *Session, keyspaceName string) ([]IndexMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	const stmt = `SELECT * FROM system_schema.indexes WHERE keyspace_name = ?`

	var indexes []IndexMetadata
	index := IndexMetadata{}

	iter := session.control.querySystem(stmt, keyspaceName)
	for iter.MapScan(map[string]any{
		"index_name":    &index.Name,
		"keyspace_name": &index.KeyspaceName,
		"table_name":    &index.TableName,
		"kind":          &index.Kind,
		"options":       &index.Options,
	}) {
		indexes = append(indexes, index)
		index = IndexMetadata{}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// get create statements for the keyspace
func getCreateStatements(session *Session, keyspaceName string) ([]byte, error) {
	if !session.useSystemSchema {
		return nil, nil
	}
	iter := session.control.query(fmt.Sprintf(`DESCRIBE KEYSPACE %s WITH INTERNALS`, keyspaceName))

	var createStatements []string

	var stmt string
	for iter.Scan(nil, nil, nil, &stmt) {
		if stmt == "" {
			continue
		}
		createStatements = append(createStatements, stmt)
	}

	if err := iter.Close(); err != nil {
		if errFrame, ok := err.(frm.ErrorFrame); ok && errFrame.Code == ErrCodeSyntax {
			// DESCRIBE KEYSPACE is not supported on older versions of Cassandra and Scylla
			// For such case schema statement is going to be recreated on the client side
			return nil, nil
		}
		return nil, fmt.Errorf("error querying keyspace schema: %w", err)
	}

	return []byte(strings.Join(createStatements, "\n")), nil
}

// query for view metadata in the system_schema.views
func getViewMetadata(session *Session, keyspaceName string) ([]ViewMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.views WHERE keyspace_name = ?`

	iter := session.control.querySystem(stmt, keyspaceName)

	var views []ViewMetadata
	view := ViewMetadata{KeyspaceName: keyspaceName}

	for iter.MapScan(map[string]any{
		"id":                          &view.ID,
		"view_name":                   &view.ViewName,
		"base_table_id":               &view.BaseTableID,
		"base_table_name":             &view.BaseTableName,
		"include_all_columns":         &view.IncludeAllColumns,
		"where_clause":                &view.WhereClause,
		"bloom_filter_fp_chance":      &view.Options.BloomFilterFpChance,
		"caching":                     &view.Options.Caching,
		"comment":                     &view.Options.Comment,
		"compaction":                  &view.Options.Compaction,
		"compression":                 &view.Options.Compression,
		"crc_check_chance":            &view.Options.CrcCheckChance,
		"default_time_to_live":        &view.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &view.Options.GcGraceSeconds,
		"max_index_interval":          &view.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &view.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &view.Options.MinIndexInterval,
		"speculative_retry":           &view.Options.SpeculativeRetry,
		"extensions":                  &view.Extensions,
		"dclocal_read_repair_chance":  &view.DcLocalReadRepairChance,
		"read_repair_chance":          &view.ReadRepairChance,
	}) {
		views = append(views, view)
		view = ViewMetadata{KeyspaceName: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying view schema: %w", err)
	}

	return views, nil
}
