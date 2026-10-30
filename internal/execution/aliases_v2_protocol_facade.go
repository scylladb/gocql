// Copyright 2026 The Gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

package execution

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

type PageState = model.PageState

type RequestErrUnprepared = protocol.RequestErrUnprepared

func RestorePageState(a0 []byte) PageState { return model.RestorePageState(a0) }

type Result = model.Result

func StartPage() PageState { return model.StartPage() }

func cloneResult(a0 Result) Result      { return model.CloneResult(a0) }
func completePageState() PageState      { return model.CompletePageState() }
func mergeResult(a0 *Result, a1 Result) { model.MergeResult(a0, a1) }

type responseData = model.ResponseData
