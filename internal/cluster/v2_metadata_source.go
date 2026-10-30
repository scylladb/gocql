// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package cluster

import (
	"context"
	implMetadata "github.com/scylladb/gocql/v2/internal/metadata"
	"github.com/scylladb/gocql/v2/internal/model"
)

type InternalSchemaSource struct{ session *Session }

func InternalNewSchemaSource(s *Session) implMetadata.Source { return InternalSchemaSource{s} }
func (source InternalSchemaSource) InternalSchemaContext() context.Context {
	s := source.session
	if s != nil && s.privateStorage.InternalCtx != nil {
		return s.privateStorage.InternalCtx
	}
	return context.Background()
}
func (source InternalSchemaSource) InternalSchemaSettings() implMetadata.QuerySettings {
	s := source.session
	v := implMetadata.QuerySettings{SystemSchema: s.privateStorage.InternalUseSystemSchema, Available: s.privateStorage.InternalControl != nil, Aggregates: s.privateStorage.InternalHasAggregatesAndFunctions, Timeout: s.privateStorage.InternalCfg.MetadataSchemaRequestTimeout, Depth: s.v2MetadataDepthLimit()}
	if s.privateStorage.InternalHostSource != nil && s.privateStorage.InternalPool != nil {
		if c := InternalSessionGetConn(s); c != nil {
			v.Scylla = c.InternalIsScyllaConn()
			v.TabletsV2 = c.InternalFramers.InternalDefaults.InternalTabletsRoutingV2
		}
	}
	return v
}
func (source InternalSchemaSource) InternalQuerySystem(ctx context.Context, request *model.AttemptRequest) (*model.ResponseData, error) {
	s := source.session
	if s.privateStorage.InternalControl == nil {
		return nil, errNoControl
	}
	var response *responsePage
	var err error
	if backend, ok := s.privateStorage.InternalControl.(InternalV2SystemQueryer); ok {
		response, err = backend.InternalQueryV2System(ctx, request)
	} else {
		h := s.privateStorage.InternalControl.InternalGetConn()
		if h == nil || h.InternalConn == nil {
			return nil, ErrNoConnections
		}
		c, ok := h.InternalConn.(*Conn)
		if !ok {
			return nil, typeValidation("schema query", "control connection does not support typed requests")
		}
		response, err = c.InternalExecuteV2System(ctx, request)
	}
	if response == nil {
		return nil, err
	}
	return &response.InternalResponseData, err
}
