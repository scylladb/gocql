package redis

import (
	"errors"
	"fmt"
)

// RedisNil mirrors go-redis "nil key" errors so existing comparisons such as
// err == redis.Nil keep working.
type RedisNil string

func (e RedisNil) Error() string {
	return string(e)
}

// Nil is returned when a key or field does not exist.
const Nil = RedisNil("redis: nil")

// Error is a Redis protocol style error. Values are constants, so they can be
// compared with == and used with errors.Is, mirroring how go-redis surfaces
// server side errors to callers.
type Error string

func (e Error) Error() string {
	return string(e)
}

// Redis protocol errors reproduced verbatim so migrated code that matches on
// error text keeps working.
const (
	ErrWrongType       = Error("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoSuchKey       = Error("ERR no such key")
	ErrValueNotInteger = Error("ERR value is not an integer or out of range")
	ErrIncrOverflow    = Error("ERR increment or decrement would overflow")
	ErrInvalidExpire   = Error("ERR invalid expire time")
	ErrScoreNotDouble  = Error("ERR One or more scores can't be converted into double")
)

// Package level errors describing conditions that have no Redis equivalent.
var (
	// ErrClosed is returned once Close has been called on the client.
	ErrClosed = errors.New("rediscompat: client is closed")

	// ErrNotInitialized is returned when the client could not be constructed.
	ErrNotInitialized = errors.New("rediscompat: client is not initialized")

	// ErrCursorUnknown is returned by Scan when the supplied cursor is not
	// known to this process. Cursors are backed by server paging state held in
	// memory, so they do not survive a restart and cannot be shared between
	// processes. Restart the iteration from cursor 0.
	ErrCursorUnknown = errors.New("rediscompat: scan cursor is unknown or expired, restart iteration at cursor 0")

	// ErrKeyTypeUnsupported is returned for operations this package can only
	// implement for a subset of Redis key types. See the compatibility matrix
	// in the package documentation.
	ErrKeyTypeUnsupported = errors.New("rediscompat: operation is not supported for this key type")

	// ErrCASExhausted is returned when a compare-and-set loop could not make
	// progress within the configured retry budget.
	ErrCASExhausted = errors.New("rediscompat: conflicting concurrent writes, retry budget exhausted")

	// ErrValueTooLarge is returned when a value exceeds Options.MaxValueSize.
	// A single CQL cell that large cannot be written reliably, so the write is
	// refused here instead of failing deeper in the driver or destabilizing a
	// replica.
	ErrValueTooLarge = errors.New("rediscompat: value exceeds the configured maximum size")

	// ErrResultTooLarge is returned when a command that materializes a whole
	// collection (HGetAll, SMembers, Sort, list reads) would exceed
	// Options.MaxCollectionScan. Read the collection in pages instead.
	ErrResultTooLarge = errors.New("rediscompat: result exceeds the configured maximum element count")

	// ErrListPositionExhausted is returned when a list has been pushed on the
	// same side so many times that the next position would overflow int64.
	ErrListPositionExhausted = errors.New("rediscompat: list position space is exhausted, recreate the list")

	// ErrBatchTooLarge is returned when a command would need more statements
	// in one batch than Options.MaxBatchStatements allows. The command is
	// refused rather than split, so a command that returns success was applied
	// atomically. Split the argument list instead.
	ErrBatchTooLarge = errors.New("rediscompat: command needs more statements than MaxBatchStatements allows")

	// ErrTxAborted is returned by Exec when a watched key changed while the
	// transaction was being prepared. It is the equivalent of Redis EXEC
	// returning nil after a WATCH was invalidated: nothing was applied and the
	// caller should read again and retry.
	ErrTxAborted = errors.New("rediscompat: transaction aborted, a watched key changed")

	// ErrTxUnsupported is returned when a transaction is attempted without
	// TransactionsByBucket. Conditional writes are confined to one partition,
	// so a transaction needs the layout that puts a bucket's keys in one.
	ErrTxUnsupported = errors.New("rediscompat: transactions require TransactionsByBucket")

	// ErrTxEmpty is returned by Exec when no command was queued.
	ErrTxEmpty = errors.New("rediscompat: transaction has no queued commands")
)

func errTooManyStatements(need, limit int) error {
	return fmt.Errorf("rediscompat: command needs %d batched statements, limit is %d: %w",
		need, limit, ErrBatchTooLarge)
}

// unsupportedForType converts an internal WRONGTYPE into the clearer "this
// package cannot do that for this type" error used by TTL style commands.
func unsupportedForType(err error) error {
	if errors.Is(err, ErrWrongType) {
		return ErrKeyTypeUnsupported
	}
	return err
}
