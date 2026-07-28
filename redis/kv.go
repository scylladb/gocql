package redis

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	gocql "github.com/gocql/gocql"
)

// KeepTTL mirrors go-redis behavior where Set keeps the current key TTL.
const KeepTTL = time.Duration(-1)

// readString loads a string key. A key holding another type yields
// ErrWrongType, which the meta row answers directly: there is no separate
// registry to consult and no cache that could be stale.
func (c *Client) readString(ctx context.Context, key string) (m keyMeta, found bool, err error) {
	m, found, err = c.readMeta(ctx, key)
	if err != nil || !found {
		return keyMeta{}, false, err
	}
	if m.typ != typeString {
		return keyMeta{}, false, ErrWrongType
	}
	return m, true, nil
}

// writeString replaces a key with a string value.
//
// SET replaces a key of any type, so the element rows of a collection are
// removed in the same batch as the new meta row. Both statements are in one
// partition, which makes the replacement a single mutation: there is no window
// where the key is half a hash and half a string, and a cancelled call either
// did nothing or did all of it.
func (c *Client) writeString(ctx context.Context, key string, value []byte, expires time.Time, wasCollection bool) error {
	version := nextVersion()
	ttl := cellTTL(expires, c.now())

	write := batchStatement{
		stmt: c.core.schema.strWrite,
		args: c.strWriteArgs(key, value, version, expiryArg(expires), 0),
	}
	if ttl > 0 {
		write = batchStatement{
			stmt: c.core.schema.strWriteTTL,
			args: c.strWriteArgs(key, value, version, expiryArg(expires), ttl),
		}
	}

	if wasCollection {
		if err := c.core.runner.Batch(ctx, gocql.UnloggedBatch, []batchStatement{
			{stmt: c.core.schema.elemsDelete, args: c.keyArgs(key)},
			write,
		}); err != nil {
			return err
		}
	} else if err := c.core.runner.Exec(ctx, write.stmt, write.args...); err != nil {
		return err
	}

	c.noteExpiry(ctx, key, expires)
	return nil
}

// setString is the write path shared by SET, and by the commands that reuse SET
// semantics. The enumeration index entry is written alongside the key rather
// than after it, so a failure leaves an index entry with no key, which
// enumeration repairs, instead of a key that no listing can ever see.
func (c *Client) setString(ctx context.Context, key string, value []byte, expires time.Time) error {
	m, found, err := c.readMeta(ctx, key)
	if err != nil {
		return err
	}
	if err := c.noteKey(ctx, key); err != nil {
		return err
	}
	return c.writeString(ctx, key, value, expires, found && m.typ.collection())
}

// resolveExpiry maps a go-redis expiration onto an absolute expiry.
func (c *Client) resolveExpiry(ctx context.Context, key string, expiration time.Duration) (time.Time, error) {
	switch {
	case expiration == KeepTTL:
		m, found, err := c.readMeta(ctx, key)
		if err != nil {
			return time.Time{}, err
		}
		if !found {
			return time.Time{}, nil
		}
		return m.expires, nil
	case expiration > 0:
		return c.now().Add(expiration), nil
	case expiration == 0:
		return time.Time{}, nil
	default:
		return time.Time{}, ErrInvalidExpire
	}
}

func (c *Client) Set(ctx context.Context, key string, value any, expiration time.Duration) *StatusCmd {
	cmd := &StatusCmd{val: "OK"}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	serialized, err := c.marshalBounded(value)
	if err != nil {
		cmd.err = err
		return cmd
	}

	expires, err := c.resolveExpiry(ctx, key, expiration)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.err = c.setString(ctx, key, serialized, expires)
	return cmd
}

// Put is a convenience alias for Set.
func (c *Client) Put(ctx context.Context, key string, value any, expiration time.Duration) *StatusCmd {
	return c.Set(ctx, key, value, expiration)
}

// SetEx sets a key with a mandatory positive TTL, as Redis SETEX does.
func (c *Client) SetEx(ctx context.Context, key string, value any, expiration time.Duration) *StatusCmd {
	if expiration <= 0 {
		return &StatusCmd{baseCmd: baseCmd{err: ErrInvalidExpire}}
	}
	return c.Set(ctx, key, value, expiration)
}

// SetNX sets a key only if it does not exist. The check and the write are a
// single conditional insert, so it is safe to use for locking.
func (c *Client) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	serialized, err := c.marshalBounded(value)
	if err != nil {
		cmd.err = err
		return cmd
	}

	var expires time.Time
	switch {
	case expiration == KeepTTL, expiration == 0:
	case expiration > 0:
		expires = c.now().Add(expiration)
	default:
		cmd.err = ErrInvalidExpire
		return cmd
	}

	// The index entry goes in first: a lock that exists but cannot be listed is
	// worse than an entry for a lock that was never taken.
	if err := c.noteKey(ctx, key); err != nil {
		cmd.err = err
		return cmd
	}
	applied, err := c.insertStringNX(ctx, key, serialized, expires)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = applied
	return cmd
}

func (c *Client) insertStringNX(ctx context.Context, key string, value []byte, expires time.Time) (bool, error) {
	ttl := cellTTL(expires, c.now())
	stmt := c.core.schema.strWriteNX
	if ttl > 0 {
		stmt = c.core.schema.strWriteNXTTL
	}
	applied, err := c.core.runner.ExecCAS(ctx, stmt,
		c.strWriteArgs(key, value, nextVersion(), expiryArg(expires), ttl))
	if err != nil {
		return false, err
	}
	if applied {
		c.noteExpiry(ctx, key, expires)
	}
	return applied, nil
}

func (c *Client) Get(ctx context.Context, key string) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	m, found, err := c.readString(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		cmd.err = Nil
		return cmd
	}
	cmd.val = string(m.value)
	return cmd
}

// GetSet atomically replaces a value and returns the previous one. Like Redis
// GETSET it clears any TTL.
func (c *Client) GetSet(ctx context.Context, key string, value interface{}) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	serialized, err := c.marshalBounded(value)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.noteKey(ctx, key); err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}

		if !found {
			applied, err := c.insertStringNX(ctx, key, serialized, time.Time{})
			if err != nil {
				cmd.err = err
				return cmd
			}
			if applied {
				cmd.err = Nil
				return cmd
			}
		} else {
			applied, err := c.casString(ctx, key, serialized, time.Time{}, m.version)
			if err != nil {
				cmd.err = err
				return cmd
			}
			if applied {
				cmd.val = string(m.value)
				return cmd
			}
		}

		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// casString rewrites a string value only while the meta row still carries the
// version the caller read.
func (c *Client) casString(ctx context.Context, key string, value []byte, expires time.Time, expect int64) (bool, error) {
	ttl := cellTTL(expires, c.now())
	stmt := c.core.schema.strCAS
	if ttl > 0 {
		stmt = c.core.schema.strCASTTL
	}
	return c.core.runner.ExecCAS(ctx, stmt,
		c.strCASArgs(key, value, nextVersion(), expiryArg(expires), expect, ttl))
}

// GetDel atomically returns and removes a value, so two callers racing for a
// one-shot token cannot both receive it.
func (c *Client) GetDel(ctx context.Context, key string) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if !found {
			cmd.err = Nil
			return cmd
		}

		applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.metaDeleteCAS,
			c.metaDeleteCASArgs(key, m.version))
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			_ = c.forgetKey(ctx, key)
			cmd.val = string(m.value)
			return cmd
		}

		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

func (c *Client) MGet(ctx context.Context, keys ...string) *SliceCmd {
	cmd := &SliceCmd{val: make([]interface{}, len(keys))}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	err := runConcurrent(ctx, len(keys), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		m, found, err := c.readString(ctx, keys[i])
		if err != nil {
			if errors.Is(err, ErrWrongType) {
				// MGET reports nil for keys holding another type.
				return nil
			}
			return err
		}
		if found {
			cmd.val[i] = string(m.value)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
	}
	return cmd
}

// MSet writes several keys in one logged batch.
//
// A logged batch is atomic in the sense that matters here: once the coordinator
// accepts it, every mutation is applied, even across partitions. It is not
// isolated, so a concurrent reader can still observe one key updated and
// another not. Callers who need isolation put the keys in one bucket and use a
// transaction, which is the same instruction a Redis Cluster user follows.
func (c *Client) MSet(ctx context.Context, values ...interface{}) *StatusCmd {
	cmd := &StatusCmd{val: "OK"}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	pairs, err := normalizeMSetValues(values...)
	if err != nil {
		cmd.err = err
		return cmd
	}
	payloads := make([][]byte, len(pairs))
	for i := range pairs {
		if err := validateKey(pairs[i].key); err != nil {
			cmd.err = err
			return cmd
		}
		payload, err := c.marshalBounded(pairs[i].value)
		if err != nil {
			cmd.err = err
			return cmd
		}
		payloads[i] = payload
	}
	// Two statements per key in the worst case, plus the index writes.
	if err := c.checkBatch(2 * len(pairs)); err != nil {
		cmd.err = err
		return cmd
	}

	// Any of the keys may currently hold a collection, whose element rows have
	// to go in the same batch as the new value.
	collections := make([]bool, len(pairs))
	if err := runConcurrent(ctx, len(pairs), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		m, found, err := c.readMeta(ctx, pairs[i].key)
		if err != nil {
			return err
		}
		collections[i] = found && m.typ.collection()
		return c.noteKey(ctx, pairs[i].key)
	}); err != nil {
		cmd.err = err
		return cmd
	}

	stmts := make([]batchStatement, 0, 2*len(pairs))
	for i := range pairs {
		if collections[i] {
			stmts = append(stmts, batchStatement{
				stmt: c.core.schema.elemsDelete,
				args: c.keyArgs(pairs[i].key),
			})
		}
		stmts = append(stmts, batchStatement{
			stmt: c.core.schema.strWrite,
			args: c.strWriteArgs(pairs[i].key, payloads[i], nextVersion(), nil, 0),
		})
	}

	batchType := gocql.LoggedBatch
	if c.core.schema.grouped {
		// Every key shares the bucket partition, so the batch is a single
		// mutation and the batchlog would be pure overhead.
		batchType = gocql.UnloggedBatch
	}
	cmd.err = c.core.runner.Batch(ctx, batchType, stmts)
	return cmd
}

func (c *Client) Incr(ctx context.Context, key string) *IntCmd {
	return c.IncrBy(ctx, key, 1)
}

func (c *Client) Decr(ctx context.Context, key string) *IntCmd {
	return c.IncrBy(ctx, key, -1)
}

func (c *Client) DecrBy(ctx context.Context, key string, decrement int64) *IntCmd {
	if decrement == math.MinInt64 {
		return &IntCmd{baseCmd: baseCmd{err: ErrIncrOverflow}}
	}
	return c.IncrBy(ctx, key, -decrement)
}

// IncrBy applies a counter delta under a version guard, so concurrent
// increments cannot lose updates, and preserves any expiry as Redis does.
func (c *Client) IncrBy(ctx context.Context, key string, value int64) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.noteKey(ctx, key); err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}

		var base int64
		if found {
			base, err = strconv.ParseInt(string(m.value), 10, 64)
			if err != nil {
				cmd.err = ErrValueNotInteger
				return cmd
			}
		}

		next, ok := addChecked(base, value)
		if !ok {
			cmd.err = ErrIncrOverflow
			return cmd
		}

		applied, err := c.rewriteString(ctx, key, []byte(strconv.FormatInt(next, 10)), m, found)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			cmd.val = next
			return cmd
		}

		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// Append extends a string, preserving its expiry.
func (c *Client) Append(ctx context.Context, key, value string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.noteKey(ctx, key); err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}

		next := make([]byte, 0, len(m.value)+len(value))
		next = append(next, m.value...)
		next = append(next, value...)
		if c.core.maxValueSize > 0 && len(next) > c.core.maxValueSize {
			cmd.err = valueTooLarge(len(next), c.core.maxValueSize)
			return cmd
		}

		applied, err := c.rewriteString(ctx, key, next, m, found)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			cmd.val = int64(len(next))
			return cmd
		}

		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// rewriteString writes next only if the key is still in the state the caller
// read: still absent, or still carrying the same version.
func (c *Client) rewriteString(ctx context.Context, key string, next []byte, m keyMeta, exists bool) (bool, error) {
	if !exists {
		return c.insertStringNX(ctx, key, next, time.Time{})
	}
	return c.casString(ctx, key, next, m.expires, m.version)
}

// StrLen returns the length of the string value stored at key, or 0.
func (c *Client) StrLen(ctx context.Context, key string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	m, found, err := c.readString(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if found {
		cmd.val = int64(len(m.value))
	}
	return cmd
}

func addChecked(a, b int64) (int64, bool) {
	sum := a + b
	if (b > 0 && sum < a) || (b < 0 && sum > a) {
		return 0, false
	}
	return sum, true
}
