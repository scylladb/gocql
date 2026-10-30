// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package model

import (
	. "github.com/scylladb/gocql/v2/internal/cqltype"

	"time"
)

type Result struct {
	HostID              UUID
	Attempts            int
	Latency             time.Duration
	Rows                int64
	Warnings            []string
	TraceIDs            []UUID
	CustomPayload       []PayloadEntry
	AchievedConsistency Consistency
}

func CloneResult(result Result) Result {
	result.Warnings = append([]string(nil), result.Warnings...)
	result.TraceIDs = append([]UUID(nil), result.TraceIDs...)
	result.CustomPayload = append([]PayloadEntry(nil), result.CustomPayload...)
	return result
}
func MergeResult(dst *Result, src Result) {
	dst.HostID = src.HostID
	dst.Attempts += src.Attempts
	dst.Latency += src.Latency
	dst.Rows += src.Rows
	dst.Warnings = append(dst.Warnings, src.Warnings...)
	dst.TraceIDs = append(dst.TraceIDs, src.TraceIDs...)
	dst.CustomPayload = append(dst.CustomPayload, src.CustomPayload...)
	dst.AchievedConsistency = src.AchievedConsistency
}

type PageState struct {
	token []byte
	kind  uint8
}

func StartPage() PageState { return PageState{} }
func RestorePageState(token []byte) PageState {
	return PageState{token: append([]byte{}, token...), kind: 1}
}
func (p PageState) More() bool     { return p.kind == 1 }
func (p PageState) Complete() bool { return p.kind == 2 }
func (p PageState) Bytes() []byte {
	if p.kind != 1 {
		return nil
	}
	return append([]byte{}, p.token...)
}
func CompletePageState() PageState { return PageState{kind: 2} }
