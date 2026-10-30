// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
)

type NoHostsError = implMetadata.NoHostsError
type PartialRefreshError = implMetadata.PartialRefreshError

func compileTableMetadata(a0 *KeyspaceMetadata, a1 []TableMetadata, a2 []ColumnMetadata, a3 []IndexMetadata, a4 []ViewMetadata) {
	implMetadata.InternalCompileTableMetadata(a0, a1, a2, a3, a4)
}

var errNoControl = implMetadata.InternalErrNoControl

func fetchV2Keyspace(a0 context.Context, a1 implMetadata.Source, a2 *KeyspaceMetadata) (*KeyspaceMetadata, error) {
	return implMetadata.InternalFetchV2Keyspace(a0, a1, a2)
}
func getV2ColumnMetadata(a0 context.Context, a1 implMetadata.Source, a2 string, a3 string) ([]ColumnMetadata, error) {
	return implMetadata.InternalGetV2ColumnMetadata(a0, a1, a2, a3)
}
func getV2CreateStatements(a0 context.Context, a1 implMetadata.Source, a2 string) ([]byte, error) {
	return implMetadata.InternalGetV2CreateStatements(a0, a1, a2)
}
func getV2IndexMetadata(a0 context.Context, a1 implMetadata.Source, a2 string, a3 string) ([]IndexMetadata, error) {
	return implMetadata.InternalGetV2IndexMetadata(a0, a1, a2, a3)
}
func getV2KeyspaceMetadata(a0 context.Context, a1 implMetadata.Source, a2 string) (*KeyspaceMetadata, error) {
	return implMetadata.InternalGetV2KeyspaceMetadata(a0, a1, a2)
}
func getV2TableMetadata(a0 context.Context, a1 implMetadata.Source, a2 string, a3 string) ([]TableMetadata, error) {
	return implMetadata.InternalGetV2TableMetadata(a0, a1, a2, a3)
}
func getV2ViewMetadata(a0 context.Context, a1 implMetadata.Source, a2 string, a3 string) ([]ViewMetadata, error) {
	return implMetadata.InternalGetV2ViewMetadata(a0, a1, a2, a3)
}
func queryV2SchemaRows[R any](a0 context.Context, a1 implMetadata.Source, a2 string, a3 []string, a4 RowDecoder[R]) ([]R, error) {
	return implMetadata.InternalQueryV2SchemaRows[R](a0, a1, a2, a3, a4)
}
func readV2ScyllaKeyspaces(a0 context.Context, a1 implMetadata.Source, a2 map[string]*KeyspaceMetadata, a3 string) error {
	return implMetadata.InternalReadV2ScyllaKeyspaces(a0, a1, a2, a3)
}

func v2DecodeAggregateMetadata(a0 string) RowDecoder[AggregateMetadata] {
	return implMetadata.InternalV2DecodeAggregateMetadata(a0)
}

func v2DecodeFunctionMetadata(a0 string) RowDecoder[FunctionMetadata] {
	return implMetadata.InternalV2DecodeFunctionMetadata(a0)
}

func v2DecodeKeyspace() RowDecoder[v2KeyspaceRow] { return implMetadata.InternalV2DecodeKeyspace() }

func v2DecodeTopology() RowDecoder[v2TopologyRow] { return implMetadata.InternalV2DecodeTopology() }
func v2DecodeTypeMetadata(a0 string) RowDecoder[TypeMetadata] {
	return implMetadata.InternalV2DecodeTypeMetadata(a0)
}

func v2KeyspaceFromRow(a0 string, a1 v2KeyspaceRow) *KeyspaceMetadata {
	return implMetadata.InternalV2KeyspaceFromRow(a0, a1)
}

type v2KeyspaceRow = implMetadata.InternalV2KeyspaceRow

type v2MetadataInt = implMetadata.InternalV2MetadataInt

type v2MetadataText = implMetadata.InternalV2MetadataText
type v2MetadataUUIDString = implMetadata.InternalV2MetadataUUIDString

func v2RequiredSchemaColumn[R any, V any](a0 string, a1 Decoder[V], a2 func(*R) *V) v2SchemaField[R] {
	return implMetadata.InternalV2RequiredSchemaColumn[R, V](a0, a1, a2)
}
func v2SchemaColumn[R any, V any](a0 string, a1 Decoder[V], a2 func(*R) *V) v2SchemaField[R] {
	return implMetadata.InternalV2SchemaColumn[R, V](a0, a1, a2)
}
func v2SchemaContext(a0 implMetadata.Source) context.Context {
	return implMetadata.InternalV2SchemaContext(a0)
}

type v2SchemaField[R any] = implMetadata.InternalV2SchemaField[R]

type v2SchemaRows[R any] = implMetadata.InternalV2SchemaRows[R]

type v2TopologyRow = implMetadata.InternalV2TopologyRow

func validateV2SchemaParts(a0 *KeyspaceMetadata, a1 []TableMetadata, a2 []ColumnMetadata, a3 []TypeMetadata, a4 []FunctionMetadata, a5 []AggregateMetadata, a6 []IndexMetadata, a7 []ViewMetadata, a8 ...int) error {
	return implMetadata.InternalValidateV2SchemaParts(a0, a1, a2, a3, a4, a5, a6, a7, a8...)
}
