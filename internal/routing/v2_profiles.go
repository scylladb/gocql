// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package routing

import (
	"context"
	"errors"
	model "github.com/scylladb/gocql/v2/internal/model"

	runtimeerrors "github.com/scylladb/gocql/v2/internal/runtimeerrors"
	"time"
)

type Target = model.Target

type QueryPlan interface{ Next() (Target, bool) }
type RoutingPolicy interface {
	Validate(*MetadataSnapshot) error
	Plan(RoutingInfo, *MetadataSnapshot) QueryPlan
	Observe(AttemptEvent)
}
type RetryDecision struct {
	Action      RetryAction
	Consistency Option[Consistency]
	Delay       time.Duration
}
type RetryAction uint8

const (
	StopRetry RetryAction = iota
	RetrySame
	RetryNext
	IgnoreWriteError
)

type AttemptError struct {
	Err          error
	Attempt      int
	Idempotent   bool
	Consistency  Consistency
	Conditional  bool
	BytesWritten bool
}

type AttemptEvent struct {
	RequestID              uint64
	Attempt                int
	Target                 Target
	Started                time.Time
	Latency                time.Duration
	Err                    error
	Cancelled, Speculative bool
}

// SafeRetryPolicy retries bounded transient failures only when replay is safe.
type SafeRetryPolicy struct{ MaxAttempts int }

func (p SafeRetryPolicy) Decide(a AttemptError) RetryDecision {
	limit := p.MaxAttempts
	if limit <= 0 {
		limit = 3
	}
	if a.Attempt >= limit || errors.Is(a.Err, context.Canceled) || errors.Is(a.Err, context.DeadlineExceeded) {
		return RetryDecision{}
	}
	var unavailable *RequestErrUnavailable
	if errors.As(a.Err, &unavailable) {
		return RetryDecision{Action: RetryNext}
	}
	var read *RequestErrReadTimeout
	if errors.As(a.Err, &read) && read.Received >= read.BlockFor && read.DataPresent == 0 {
		return RetryDecision{Action: RetrySame}
	}
	if !a.Idempotent {
		var query *runtimeerrors.QueryError
		if !a.BytesWritten && errors.As(a.Err, &query) && !query.PotentiallyExecuted() {
			return RetryDecision{Action: RetryNext}
		}
		return RetryDecision{}
	}
	var server RequestError
	if errors.As(a.Err, &server) {
		switch server.GetCode() {
		case ErrCodeOverloaded, ErrCodeBootstrapping, ErrCodeServer:
			return RetryDecision{Action: RetryNext}
		default:
			return RetryDecision{}
		}
	}
	var schema *runtimeerrors.SchemaChangedError
	var validation *ValidationError
	var protocol *ProtocolError
	if errors.As(a.Err, &schema) || errors.As(a.Err, &validation) || errors.As(a.Err, &protocol) {
		return RetryDecision{}
	}
	return RetryDecision{Action: RetryNext}
}
