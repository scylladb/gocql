// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"context"
	"errors"
)

type InternalV2ScyllaKeyspaceRow struct {
	name, consistency string
	initial           int32
}

// SELECT * avoids guessing whether an experimental consistency column exists.
// Only the negotiated V2 connection can authorize its leader meaning.
func InternalReadV2ScyllaKeyspaces(ctx context.Context, s Source, keyspaces map[string]*KeyspaceMetadata, name string) error {
	settings := s.InternalSchemaSettings()
	if !settings.Scylla {
		return nil
	}
	decoder := InternalV2SchemaRows[InternalV2ScyllaKeyspaceRow]{initialize: func() *InternalV2ScyllaKeyspaceRow { return &InternalV2ScyllaKeyspaceRow{initial: -1} }, InternalFields: []InternalV2SchemaField[InternalV2ScyllaKeyspaceRow]{
		InternalV2RequiredSchemaColumn("keyspace_name", InternalV2MetadataText{}, func(r *InternalV2ScyllaKeyspaceRow) *string { return &r.name }),
		InternalV2SchemaColumn("initial_tablets", Int32, func(r *InternalV2ScyllaKeyspaceRow) *int32 { return &r.initial }),
		InternalV2SchemaColumn("consistency", InternalV2MetadataText{}, func(r *InternalV2ScyllaKeyspaceRow) *string { return &r.consistency }),
	}}
	stmt := `SELECT * FROM system_schema.scylla_keyspaces`
	var args []string
	if name != "" {
		stmt += ` WHERE keyspace_name = ?`
		args = []string{name}
	}
	rows, err := InternalQueryV2SchemaRows(ctx, s, stmt, args, decoder)
	if err != nil {
		var invalid RequestError
		if errors.As(err, &invalid) && invalid.GetCode() == ErrCodeInvalid {
			return nil
		}
		return err
	}
	seen := make(map[string]bool, len(rows))
	for _, r := range rows {
		if seen[r.name] {
			return typeValidation("scylla keyspace schema", "duplicate identity %q", r.name)
		}
		seen[r.name] = true
		if ks := keyspaces[r.name]; ks != nil {
			ks.TabletBased = r.initial >= 0
			if settings.TabletsV2 && r.consistency == "global" {
				ks.ConsistencyMode = "global"
			}
		}
	}
	return nil
}
