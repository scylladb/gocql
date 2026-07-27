package redis

import "errors"

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

	// ErrReservedKey is returned when a caller uses a key reserved for
	// internal bookkeeping.
	ErrReservedKey = errors.New("rediscompat: key is reserved for internal bookkeeping")

	// ErrCASExhausted is returned when a compare-and-set loop could not make
	// progress within the configured retry budget.
	ErrCASExhausted = errors.New("rediscompat: conflicting concurrent writes, retry budget exhausted")
)
