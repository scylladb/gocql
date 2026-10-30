// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package metadata

import (
	"context"
	"fmt"
	"time"

	"github.com/scylladb/gocql/v2/internal/binding"
	"github.com/scylladb/gocql/v2/internal/codec"
	"github.com/scylladb/gocql/v2/internal/cqltype"
	"github.com/scylladb/gocql/v2/internal/model"
	"github.com/scylladb/gocql/v2/internal/paging"
	"github.com/scylladb/gocql/v2/internal/wire"
)

// Source supplies encoded system I/O and immutable capabilities, independently
// of the public session or its generic application execution facade.
type Source interface {
	InternalQuerySystem(context.Context, *model.AttemptRequest) (*model.ResponseData, error)
	InternalSchemaSettings() QuerySettings
	InternalSchemaContext() context.Context
}
type QuerySettings struct {
	SystemSchema, Available, Scylla, TabletsV2, Aggregates bool
	Timeout                                                time.Duration
	Depth                                                  int
}
type Query[P, R any] struct {
	CQL    string
	Params codec.ParamEncoder[P]
	Rows   codec.RowDecoder[R]
}

// ReadRows uses the canonical binding and row plans. System metadata is
// explicitly detached before each leased response is released.
func ReadRows[P, R any](ctx context.Context, version byte, timeout time.Duration, execute func(context.Context, *model.AttemptRequest) (*model.ResponseData, error), query Query[P, R], columns cqltype.Columns, value P) ([]R, error) {
	if ctx == nil || execute == nil {
		return nil, cqltype.TypeValidation("system query", "nil context or connection")
	}
	if query.Params == nil || query.Rows == nil {
		return nil, cqltype.TypeValidation("statement", "parameter and row codecs are required")
	}
	plan, err := query.Params.Compile(columns)
	if err != nil {
		return nil, err
	}
	values, err := binding.InternalEncodeParameters(plan, columns, value, true, version >= 4)
	if err != nil {
		return nil, err
	}
	req := &model.AttemptRequest{Statement: query.CQL, Values: values.Framed, ValueCount: len(values.Cells), Consistency: cqltype.One, PageSize: 1000, Timeout: timeout}
	var rows []R
	var history paging.InternalPagingHistory
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pageCtx := ctx
		cancel := func() {}
		if timeout > 0 {
			pageCtx, cancel = context.WithTimeout(ctx, timeout)
		} else {
			pageCtx, cancel = context.WithCancel(ctx)
		}
		response, err := execute(pageCtx, req)
		if err != nil {
			cancel()
			if response != nil && response.Owner != nil {
				response.Owner.Release()
			}
			return nil, err
		}
		if response == nil {
			cancel()
			return nil, &cqltype.ProtocolError{Stage: "system query", Err: fmt.Errorf("missing response")}
		}
		if response.NoMetadata {
			cancel()
			response.Owner.Release()
			return nil, &cqltype.ProtocolError{Stage: "system query", Err: fmt.Errorf("unexpected omitted metadata")}
		}
		rows, err = decodeSystemPage(pageCtx, response, query.Rows, rows)
		token := append([]byte(nil), response.PagingState...)
		more := response.PagingState != nil
		response.Owner.Release()
		cancel()
		if err != nil {
			return nil, err
		}
		if !more {
			return rows, nil
		}
		if err := history.InternalAdd(model.PageStateOwned(token, 1)); err != nil {
			return nil, err
		}
		if token == nil {
			token = []byte{}
		}
		req.PagingState = token
	}
}
func decodeSystemPage[R any](ctx context.Context, response *model.ResponseData, decoder codec.RowDecoder[R], rows []R) ([]R, error) {
	plan, err := codec.CompileRowPlan(decoder, response.Columns)
	if err != nil {
		return rows, err
	}
	if response.RowCount < 0 {
		return rows, &cqltype.ProtocolError{Stage: "rows", Err: fmt.Errorf("negative row count")}
	}
	cursor := wire.NewCursor(response.Rows)
	for i := int32(0); i < response.RowCount; i++ {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		reader := codec.RowReaderFromCursor(response.Columns, cursor, response.Owner)
		value, err := plan.Decode(&reader)
		if err == nil {
			err = codec.FinishRowReader(&reader)
		}
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			return rows, err
		}
		rows = append(rows, plan.Clone(value))
	}
	if err := ctx.Err(); err != nil {
		return rows, err
	}
	if err := cursor.RequireEnd(); err != nil {
		return rows, &cqltype.ProtocolError{Stage: "rows", Err: err}
	}
	return rows, nil
}
