// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import implEndpoint "github.com/scylladb/gocql/v2/internal/endpoint"

import (
	"context"

	"time"
)

func InternalReadV2Topology(ctx context.Context, conn ConnInterface, stmt string, port int) ([]*HostInfo, error) {
	backend, ok := conn.(interface {
		InternalExecuteTyped(context.Context, *attemptRequest) (*responsePage, error)
	})
	if !ok {
		return nil, typeValidation("topology query", "connection does not support typed requests")
	}
	timeout := InternalTimeForTopology(conn)
	execute := backend.InternalExecuteTyped
	if c, ok := conn.(*Conn); ok {
		execute = c.InternalExecuteV2System
	}
	rows, err := InternalReadV2SystemRowsVia(ctx, 4, timeout, execute, Statement[Unit, v2TopologyRow]{CQL: stmt, Params: NoParams, Rows: v2DecodeTopology()}, NewColumns(), Unit{})
	if err != nil {
		return nil, err
	}
	hosts := make([]*HostInfo, 0, len(rows))
	for _, row := range rows {
		h, err := row.InternalHost(port)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
	}
	return hosts, nil
}

func InternalTimeForTopology(conn ConnInterface) time.Duration {
	if c, ok := conn.(*Conn); ok {
		return c.InternalGetSystemRequestState().InternalTimeout
	}
	return time.Minute
}

// Resolve each explicitly configured endpoint through its own system.local.
// This establishes real identities without discovering additional peers.
func (s *Session) v2ContactIdentities(ctx context.Context, contacts []*HostInfo) ([]*HostInfo, error) {
	var hosts []*HostInfo
	current := s.privateStorage.InternalControl.InternalGetConn()
	seen := make(map[UUID]bool)
	for _, contact := range contacts {
		var conn *Conn
		if current != nil {
			if c, ok := current.InternalConn.(*Conn); ok && contact.ConnectAddress().Equal(c.InternalHost.ConnectAddress()) && contact.Port() == c.InternalHost.Port() {
				conn = c
			}
		}
		temporary := conn == nil
		var err error
		if temporary {
			conn, err = InternalSessionDial(s, ctx, contact, InternalSessionControlConnConfig(s), connErrorHandlerFn(func(c *Conn, _ error, _ bool) { c.Close() }))
			if err != nil {
				return nil, err
			}
		}
		rows, err := InternalReadV2Topology(ctx, conn, qrySystemLocal, s.privateStorage.InternalCfg.Port)
		if temporary {
			conn.Close()
			conn.InternalWaitEventDispatch()
		}
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 || implEndpoint.InternalHostInfoHostUUID(rows[0]).IsEmpty() {
			return nil, typeValidation("contact identity", "expected one actual local UUID")
		}
		implEndpoint.InternalHostInfoUpdate(contact, rows[0])
		if !seen[implEndpoint.InternalHostInfoHostUUID(contact)] {
			hosts = append(hosts, contact)
			seen[implEndpoint.InternalHostInfoHostUUID(contact)] = true
		}
	}
	return hosts, nil
}
