// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package gocql

import (
	"context"
	"errors"
)

type v2ScyllaKeyspaceRow struct {
	name, consistency string
	initial           int32
}

// SELECT * avoids guessing whether an experimental consistency column exists.
// Only the negotiated V2 connection can authorize its leader meaning.
func readV2ScyllaKeyspaces(ctx context.Context, s *Session, keyspaces map[string]*KeyspaceMetadata, name string) error {
	conn := s.getConn()
	if conn == nil || !conn.isScyllaConn() {
		return nil
	}
	decoder := v2SchemaRows[v2ScyllaKeyspaceRow]{initialize: func() *v2ScyllaKeyspaceRow { return &v2ScyllaKeyspaceRow{initial: -1} }, fields: []v2SchemaField[v2ScyllaKeyspaceRow]{
		v2RequiredSchemaColumn("keyspace_name", v2MetadataText{}, func(r *v2ScyllaKeyspaceRow) *string { return &r.name }),
		v2SchemaColumn("initial_tablets", Int32, func(r *v2ScyllaKeyspaceRow) *int32 { return &r.initial }),
		v2SchemaColumn("consistency", v2MetadataText{}, func(r *v2ScyllaKeyspaceRow) *string { return &r.consistency }),
	}}
	stmt := `SELECT * FROM system_schema.scylla_keyspaces`
	var args []string
	if name != "" {
		stmt += ` WHERE keyspace_name = ?`
		args = []string{name}
	}
	rows, err := queryV2SchemaRows(ctx, s, stmt, args, decoder)
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
			if conn.framers.defaults.tabletsRoutingV2 && r.consistency == "global" {
				ks.ConsistencyMode = "global"
			}
		}
	}
	return nil
}
