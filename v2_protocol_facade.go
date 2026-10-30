package gocql

import (
	model "github.com/scylladb/gocql/v2/internal/model"
	protocol "github.com/scylladb/gocql/v2/internal/protocol"
)

const ErrCodeAlreadyExists = protocol.ErrCodeAlreadyExists
const ErrCodeBootstrapping = protocol.ErrCodeBootstrapping
const ErrCodeCASWriteUnknown = protocol.ErrCodeCASWriteUnknown
const ErrCodeCDCWriteFailure = protocol.ErrCodeCDCWriteFailure
const ErrCodeConfig = protocol.ErrCodeConfig
const ErrCodeCredentials = protocol.ErrCodeCredentials
const ErrCodeFunctionFailure = protocol.ErrCodeFunctionFailure
const ErrCodeInvalid = protocol.ErrCodeInvalid
const ErrCodeOverloaded = protocol.ErrCodeOverloaded
const ErrCodeProtocol = protocol.ErrCodeProtocol
const ErrCodeReadFailure = protocol.ErrCodeReadFailure
const ErrCodeReadTimeout = protocol.ErrCodeReadTimeout
const ErrCodeServer = protocol.ErrCodeServer
const ErrCodeSyntax = protocol.ErrCodeSyntax
const ErrCodeTruncate = protocol.ErrCodeTruncate
const ErrCodeUnauthorized = protocol.ErrCodeUnauthorized
const ErrCodeUnavailable = protocol.ErrCodeUnavailable
const ErrCodeUnprepared = protocol.ErrCodeUnprepared
const ErrCodeWriteFailure = protocol.ErrCodeWriteFailure
const ErrCodeWriteTimeout = protocol.ErrCodeWriteTimeout

var ErrFrameTooBig = protocol.ErrFrameTooBig

type ErrorMap = protocol.ErrorMap
type OpType = protocol.OpType

const OpTypeRead = protocol.OpTypeRead
const OpTypeWrite = protocol.OpTypeWrite

type PageState = model.PageState
type RequestErrAlreadyExists = protocol.RequestErrAlreadyExists
type RequestErrCASWriteUnknown = protocol.RequestErrCASWriteUnknown
type RequestErrCDCWriteFailure = protocol.RequestErrCDCWriteFailure
type RequestErrFunctionFailure = protocol.RequestErrFunctionFailure
type RequestErrRateLimitReached = protocol.RequestErrRateLimitReached
type RequestErrReadFailure = protocol.RequestErrReadFailure
type RequestErrReadTimeout = protocol.RequestErrReadTimeout
type RequestErrUnavailable = protocol.RequestErrUnavailable
type RequestErrUnprepared = protocol.RequestErrUnprepared
type RequestErrWriteFailure = protocol.RequestErrWriteFailure
type RequestErrWriteTimeout = protocol.RequestErrWriteTimeout
type RequestError = protocol.RequestError

func RestorePageState(a0 []byte) PageState { return model.RestorePageState(a0) }

type Result = model.Result

func StartPage() PageState { return model.StartPage() }

type UnknownServerError = protocol.UnknownServerError

func cloneResult(a0 Result) Result      { return model.CloneResult(a0) }
func completePageState() PageState      { return model.CompletePageState() }
func mergeResult(a0 *Result, a1 Result) { model.MergeResult(a0, a1) }
func newTypedEnvelope() *typedEnvelope  { return protocol.NewTypedEnvelope() }

type parsedMetadata = protocol.ParsedMetadata
type responseData = model.ResponseData
type typedEnvelope = protocol.TypedEnvelope
type typedParser = protocol.TypedParser
