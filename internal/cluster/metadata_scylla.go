// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"

	"fmt"

	"sync"

	"golang.org/x/sync/singleflight"

	"github.com/scylladb/gocql/v2/tablets"
)

type Metadata struct {
	privateStorage MetadataStorage
}

// queries the cluster for schema information for a specific keyspace and for tablets
type InternalMetadataDescriber struct {
	keyspaceGroup    singleflight.Group
	tableGroup       singleflight.Group
	InternalSession  *Session
	InternalMetadata *Metadata

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
func InternalNewMetadataDescriber(session *Session) *InternalMetadataDescriber {
	return &InternalMetadataDescriber{
		InternalSession: session,
		InternalMetadata: func() *Metadata {
			opaqueStorageValue := new(Metadata)
			opaqueStorageValue.privateStorage.InternalTabletsMetadata = tablets.NewCowTabletList()
			return opaqueStorageValue
		}(),
	}
}

func (s *InternalMetadataDescriber) InternalGetKeyspaceInternal(keyspaceName string) (metadata *KeyspaceMetadata, wasReloaded bool, err error) {
	var found bool
	metadata, found = s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		wasReloaded = true
		err = s.deduplicatedRefreshKeyspace(keyspaceName)
		if err != nil {
			return metadata, wasReloaded, err
		}

		metadata, found = s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
		if !found {
			return nil, true, fmt.Errorf("keyspace %s: %w", keyspaceName, ErrNotFound)
		}
	}

	return metadata, wasReloaded, nil
}

func (s *InternalMetadataDescriber) GetKeyspace(keyspaceName string) (*KeyspaceMetadata, error) {
	metadata, _, err := s.InternalGetKeyspaceInternal(keyspaceName)
	return metadata, err
}

func InternalTableNotFoundError(keyspaceName, tableName string) error {
	return fmt.Errorf("table %s.%s: %w", keyspaceName, tableName, ErrNotFound)
}

// getTableFromSnapshot resolves a table lookup against a keyspace snapshot.
// It re-reads the latest published keyspace metadata before deciding that an
// invalidated table still needs a refresh, which avoids duplicate refreshes
// for callers holding a stale snapshot.
func (s *InternalMetadataDescriber) InternalGetTableFromSnapshot(
	keyspaceName, tableName string,
	keyspaceMetadata *KeyspaceMetadata,
	wasReloaded bool,
) (tableMetadata *TableMetadata, refreshNeeded bool, err error) {
	if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
		return tableMetadata, false, nil
	}

	if latestMetadata, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName); found && latestMetadata != keyspaceMetadata {
		keyspaceMetadata = latestMetadata
		if tableMetadata, found := keyspaceMetadata.Tables[tableName]; found {
			return tableMetadata, false, nil
		}
	}

	if wasReloaded {
		return nil, false, InternalTableNotFoundError(keyspaceName, tableName)
	}

	if _, ok := implMetadata.KeyspaceData(keyspaceMetadata).InternalTablesInvalidated[tableName]; !ok {
		return nil, false, InternalTableNotFoundError(keyspaceName, tableName)
	}

	return nil, true, nil
}

func (s *InternalMetadataDescriber) GetTable(keyspaceName, tableName string) (*TableMetadata, error) {
	keyspaceMetadata, wasReloaded, err := s.InternalGetKeyspaceInternal(keyspaceName)
	if err != nil {
		return nil, err
	}

	tableMetadata, refreshNeeded, err := s.InternalGetTableFromSnapshot(keyspaceName, tableName, keyspaceMetadata, wasReloaded)
	if err != nil {
		return nil, err
	}
	if !refreshNeeded {
		return tableMetadata, nil
	}

	err = s.InternalDeduplicatedRefreshTable(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	keyspaceMetadata, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		return nil, InternalTableNotFoundError(keyspaceName, tableName)
	}

	tableMetadata, found = keyspaceMetadata.Tables[tableName]
	if !found {
		return nil, InternalTableNotFoundError(keyspaceName, tableName)
	}

	return tableMetadata, nil
}

func (s *InternalMetadataDescriber) InternalGetTablets() tablets.TabletInfoList {
	return s.InternalMetadata.privateStorage.InternalTabletsMetadata.Get()
}

func (s *InternalMetadataDescriber) getTableTablets(keyspace, table string) tablets.TabletEntryList {
	return s.InternalMetadata.privateStorage.InternalTabletsMetadata.GetTableTablets(keyspace, table)
}

func (s *InternalMetadataDescriber) forEachTablet(fn func(keyspace, table string, entries tablets.TabletEntryList) bool) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.ForEach(fn)
}

func (s *InternalMetadataDescriber) AddTablet(tablet tablets.TabletInfo) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.AddTablet(tablet)
}

// RemoveTabletsWithHost removes tablets that contains given host.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithHost(host *HostInfo) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithHost(tablets.HostUUID(implEndpoint.InternalHostInfoHostUUID(host)))
}

// RemoveTabletsWithKeyspace removes tablets for given keyspace.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithKeyspace(keyspace string) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithKeyspace(keyspace)
}

// RemoveTabletsWithTable removes tablets for given table.
// to be used outside the metadataDescriber
func (s *InternalMetadataDescriber) RemoveTabletsWithTable(keyspace string, table string) {
	s.InternalMetadata.privateStorage.InternalTabletsMetadata.RemoveTabletsWithTable(keyspace, table)
}

// invalidateKeyspaceSchema clears the cached keyspace metadata
func (s *InternalMetadataDescriber) InternalInvalidateKeyspaceSchema(keyspaceName string) {
	s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalRemoveKeyspace(keyspaceName)
}

func (s *InternalMetadataDescriber) InternalInvalidateTableSchema(keyspaceName, tableName string) {
	s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalInvalidateTable(keyspaceName, tableName)
}

// deduplicatedRefreshKeyspace collapses concurrent refreshKeyspaceSchema calls
// for the same keyspace into a single in-flight operation.
func (s *InternalMetadataDescriber) deduplicatedRefreshKeyspace(keyspaceName string) error {
	_, err, _ := s.keyspaceGroup.Do(keyspaceName, func() (any, error) {
		return nil, s.refreshKeyspaceSchema(keyspaceName)
	})
	return err
}

// deduplicatedRefreshTable collapses concurrent refreshTableSchema calls
// for the same keyspace/table into a single in-flight operation.
func (s *InternalMetadataDescriber) InternalDeduplicatedRefreshTable(keyspaceName, tableName string) error {
	key := keyspaceName + "\x00" + tableName
	_, err, _ := s.tableGroup.Do(key, func() (any, error) {
		// A caller may arrive here after deciding to refresh an old snapshot but
		// after another flight already published and completed. Recheck the current
		// immutable snapshot inside this flight before issuing network queries.
		if current, ok := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName); ok {
			if _, present := current.Tables[tableName]; present {
				return nil, nil
			}
		}
		return nil, s.InternalRefreshTableSchema(keyspaceName, tableName)
	})
	return err
}

func (s *InternalMetadataDescriber) InternalRefreshAllSchema() error {
	return InternalSessionRefreshV2AllSchema(s.InternalSession, v2SchemaContext(InternalNewSchemaSource(s.InternalSession)))
}

// forcibly updates the current KeyspaceMetadata held by the schema describer
// for a given named keyspace.
//
// All system schema queries are issued concurrently since none of them
// depend on each other's results. The results are only combined in
// compileMetadata after all queries complete.
func (s *InternalMetadataDescriber) refreshKeyspaceSchema(keyspaceName string) error {
	return InternalSessionRefreshV2Keyspace(s.InternalSession, v2SchemaContext(InternalNewSchemaSource(s.InternalSession)), keyspaceName)
}

func (s *InternalMetadataDescriber) InternalRefreshTableSchema(keyspaceName, tableName string) error {
	p := InternalSessionPeripheral(s.InternalSession)
	p.InternalMu.Lock()
	epoch := p.InternalSchemaEpoch[keyspaceName]
	revision := p.schemaRevision[keyspaceName]
	tableKey := metadataTable{InternalKeyspace: keyspaceName, InternalTable: tableName}
	tableRevision := p.InternalTableRevision[tableKey]
	p.InternalMu.Unlock()
	_, found := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	if !found {
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}

	// Perform network queries outside the lock.
	tables, err := InternalGetTableMetadataByName(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	columns, err := InternalGetColumnMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	indexes, err := InternalGetIndexMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	views, err := InternalGetViewMetadataByTable(s.InternalSession, keyspaceName, tableName)
	if err != nil {
		return err
	}

	ks, _ := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalGetKeyspace(keyspaceName)
	var types []TypeMetadata
	if ks != nil {
		for _, typ := range ks.Types {
			if typ != nil {
				types = append(types, *typ)
			}
		}
	}
	if err := validateV2SchemaParts(&KeyspaceMetadata{Name: keyspaceName}, tables, columns, types, nil, nil, indexes, views, s.InternalSession.v2MetadataDepthLimit()); err != nil {
		return err
	}
	// Atomically clone-and-swap the keyspace metadata to avoid data races
	// with concurrent readers.
	p.InternalMu.Lock()
	if p.InternalSchemaEpoch[keyspaceName] != epoch || p.schemaRevision[keyspaceName] != revision || p.InternalTableRevision[tableKey] != tableRevision || p.schemaDropped[keyspaceName] {
		p.InternalMu.Unlock()
		s.InternalSession.scheduleV2SchemaRetry()
		return &PartialRefreshError{Generation: s.InternalSession.Metadata().Generation(), Keyspaces: map[string]error{keyspaceName: fmt.Errorf("schema changed during table refresh")}}
	}
	applied := s.InternalMetadata.privateStorage.InternalKeyspaceMetadata.InternalUpdateKeyspace(keyspaceName, func(ks *KeyspaceMetadata) {
		if len(tables) == 0 {
			implMetadata.InternalKeyspaceMetadataRemoveTable(ks, tableName)
		} else {
			compileTableMetadata(ks, tables, columns, indexes, views)
			if implMetadata.KeyspaceData(ks).InternalTablesInvalidated != nil {
				delete(implMetadata.KeyspaceData(ks).InternalTablesInvalidated, tableName)
			}
		}
	})
	if applied {
		p.InternalTableRevision[tableKey]++
	}
	p.InternalMu.Unlock()
	if !applied {
		// Keyspace was removed between the initial check and the update.
		// Fall back to a full keyspace refresh to recover.
		return s.deduplicatedRefreshKeyspace(keyspaceName)
	}
	InternalSessionPeripheral(s.InternalSession).schemaGeneration.Add(1)
	InternalSessionPublishV2Metadata(s.InternalSession)
	return nil
}

func InternalGetKeyspaceMetadata(s *Session, keyspace string) (*KeyspaceMetadata, error) {
	return getV2KeyspaceMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace)
}
func InternalGetTableMetadata(s *Session, keyspace string) ([]TableMetadata, error) {
	return getV2TableMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, "")
}
func InternalGetTableMetadataByName(s *Session, keyspace, table string) ([]TableMetadata, error) {
	return getV2TableMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, table)
}
func InternalGetColumnMetadata(s *Session, keyspace string) ([]ColumnMetadata, error) {
	return getV2ColumnMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, "")
}
func InternalGetColumnMetadataByTable(s *Session, keyspace, table string) ([]ColumnMetadata, error) {
	return getV2ColumnMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, table)
}
func InternalGetIndexMetadata(s *Session, keyspace string) ([]IndexMetadata, error) {
	return getV2IndexMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, "")
}
func InternalGetIndexMetadataByTable(s *Session, keyspace, table string) ([]IndexMetadata, error) {
	return getV2IndexMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, table)
}
func InternalGetViewMetadata(s *Session, keyspace string) ([]ViewMetadata, error) {
	return getV2ViewMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, "")
}
func InternalGetViewMetadataByTable(s *Session, keyspace, table string) ([]ViewMetadata, error) {
	return getV2ViewMetadata(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace, table)
}
func InternalGetTypeMetadata(s *Session, keyspace string) ([]TypeMetadata, error) {
	return queryV2SchemaRows(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), `SELECT * FROM system_schema.types WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeTypeMetadata(keyspace))
}
func InternalGetFunctionsMetadata(s *Session, keyspace string) ([]FunctionMetadata, error) {
	if !s.privateStorage.InternalHasAggregatesAndFunctions {
		return nil, nil
	}
	return queryV2SchemaRows(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeFunctionMetadata(keyspace))
}
func InternalGetAggregatesMetadata(s *Session, keyspace string) ([]AggregateMetadata, error) {
	if !s.privateStorage.InternalHasAggregatesAndFunctions {
		return nil, nil
	}
	return queryV2SchemaRows(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`, []string{keyspace}, v2DecodeAggregateMetadata(keyspace))
}
func InternalGetCreateStatements(s *Session, keyspace string) ([]byte, error) {
	return getV2CreateStatements(v2SchemaContext(InternalNewSchemaSource(s)), InternalNewSchemaSource(s), keyspace)
}
