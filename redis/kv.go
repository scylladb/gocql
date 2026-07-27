package redis

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"
)

// KeepTTL mirrors go-redis behavior where Set keeps the current key TTL.
const KeepTTL = time.Duration(-1)

// readString loads a string key. A row belonging to another Redis type yields
// ErrWrongType, which is how the shared kv namespace enforces one type per key.
func (c *Client) readString(ctx context.Context, key string, withTTL bool) (value []byte, ttl int, found bool, err error) {
	var raw string
	if withTTL {
		var ttlPtr *int
		err = c.core.runner.ScanOne(ctx, c.core.schema.kvSelectTTL, c.keyArgs(key), &raw, &value, &ttlPtr)
		if err == nil && ttlPtr != nil && *ttlPtr > 0 {
			ttl = *ttlPtr
		}
	} else {
		err = c.core.runner.ScanOne(ctx, c.core.schema.kvSelect, c.keyArgs(key), &raw, &value)
	}
	if err != nil {
		if errors.Is(err, errNotFound) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	if kt := keyType(raw); kt != "" && kt != typeString {
		if kt == typeGuard {
			return nil, 0, false, ErrReservedKey
		}
		c.rememberType(key, kt)
		return nil, 0, false, ErrWrongType
	}
	c.rememberType(key, typeString)
	return value, ttl, true, nil
}

func (c *Client) writeString(ctx context.Context, key string, value []byte, ttl int) error {
	var err error
	if ttl > 0 {
		err = c.core.runner.Exec(ctx, c.core.schema.kvUpsertTTL, c.kvWriteTTLArgs(key, typeString, value, ttl)...)
	} else {
		err = c.core.runner.Exec(ctx, c.core.schema.kvUpsert, c.kvWriteArgs(key, typeString, value)...)
	}
	if err != nil {
		return err
	}
	c.rememberType(key, typeString)
	return nil
}

func (c *Client) insertStringNX(ctx context.Context, key string, value []byte, ttl int) (bool, error) {
	existing := map[string]any{}
	var (
		applied bool
		err     error
	)
	if ttl > 0 {
		applied, err = c.core.runner.MapScanCAS(ctx, c.core.schema.kvInsertNXTTL,
			c.kvWriteTTLArgs(key, typeString, value, ttl), existing)
	} else {
		applied, err = c.core.runner.MapScanCAS(ctx, c.core.schema.kvInsertNX,
			c.kvWriteArgs(key, typeString, value), existing)
	}
	if err != nil {
		return false, err
	}
	if applied {
		c.rememberType(key, typeString)
	}
	return applied, nil
}

// casRetry waits before the next compare-and-set attempt, or reports that the
// retry budget is spent.
func (c *Client) casRetry(ctx context.Context, attempt int) error {
	if attempt >= c.core.casRetries {
		return ErrCASExhausted
	}
	return waitWithContext(ctx, backoffFor(c.core.casBackoff, attempt, c.core.blockPollMax))
}

// resolveTTL maps a go-redis expiration onto a CQL TTL in seconds.
func (c *Client) resolveTTL(ctx context.Context, key string, expiration time.Duration) (int, error) {
	switch {
	case expiration == KeepTTL:
		_, ttl, found, err := c.readString(ctx, key, true)
		if err != nil {
			if errors.Is(err, ErrWrongType) {
				// SET replaces a key of any type; there is no TTL to keep.
				return 0, nil
			}
			return 0, err
		}
		if !found {
			return 0, nil
		}
		return ttl, nil
	case expiration > 0:
		return ttlSecondsFromDuration(expiration), nil
	case expiration == 0:
		return 0, nil
	default:
		return 0, ErrInvalidExpire
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

	serialized, err := marshalValue(value)
	if err != nil {
		cmd.err = err
		return cmd
	}

	ttl, err := c.resolveTTL(ctx, key, expiration)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.prepareStringWrite(ctx, key); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.writeString(ctx, key, serialized, ttl); err != nil {
		cmd.err = err
	}
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

	serialized, err := marshalValue(value)
	if err != nil {
		cmd.err = err
		return cmd
	}

	ttl := 0
	switch {
	case expiration == KeepTTL, expiration == 0:
	case expiration > 0:
		ttl = ttlSecondsFromDuration(expiration)
	default:
		cmd.err = ErrInvalidExpire
		return cmd
	}

	applied, err := c.insertStringNX(ctx, key, serialized, ttl)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = applied
	return cmd
}

func (c *Client) Get(ctx context.Context, key string) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	value, _, found, err := c.readString(ctx, key, false)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		cmd.err = Nil
		return cmd
	}
	cmd.val = string(value)
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

	serialized, err := marshalValue(value)
	if err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		old, _, found, err := c.readString(ctx, key, false)
		if err != nil {
			cmd.err = err
			return cmd
		}

		if !found {
			applied, err := c.insertStringNX(ctx, key, serialized, 0)
			if err != nil {
				cmd.err = err
				return cmd
			}
			if applied {
				cmd.err = Nil
				return cmd
			}
		} else {
			applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.kvUpdateCAS,
				c.kvCASArgs(key, typeString, serialized, old))
			if err != nil {
				cmd.err = err
				return cmd
			}
			if applied {
				cmd.val = string(old)
				return cmd
			}
		}

		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
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
		value, _, found, err := c.readString(ctx, key, false)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if !found {
			cmd.err = Nil
			return cmd
		}

		applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.kvDeleteCAS,
			c.kvDeleteCASArgs(key, value))
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			c.forgetType(key)
			cmd.val = string(value)
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

	err := runConcurrent(ctx, len(keys), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		value, _, found, err := c.readString(ctx, keys[i], false)
		if err != nil {
			if errors.Is(err, ErrWrongType) {
				// MGET reports nil for keys holding another type.
				return nil
			}
			return err
		}
		if found {
			cmd.val[i] = string(value)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
	}
	return cmd
}

// MSet writes several keys. Without AtomicMSetByBucket the writes are
// independent, so a failure can leave part of the batch applied.
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
	for i := range pairs {
		if err := validateKey(pairs[i].key); err != nil {
			cmd.err = err
			return cmd
		}
	}

	if c.core.atomicMSet {
		cmd.err = c.msetAtomic(ctx, pairs)
		return cmd
	}

	// Marshal everything up front so an unencodable value fails before any
	// write lands.
	payloads := make([][]byte, len(pairs))
	for i := range pairs {
		payload, err := marshalValue(pairs[i].value)
		if err != nil {
			cmd.err = err
			return cmd
		}
		payloads[i] = payload
	}

	cmd.err = runConcurrent(ctx, len(pairs), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		if err := c.prepareStringWrite(ctx, pairs[i].key); err != nil {
			return err
		}
		return c.writeString(ctx, pairs[i].key, payloads[i], 0)
	})
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

// IncrBy applies a counter delta with compare-and-set, so concurrent
// increments cannot lose updates, and preserves any TTL on the key as Redis
// does.
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

	for attempt := 0; ; attempt++ {
		current, ttl, found, err := c.readString(ctx, key, true)
		if err != nil {
			cmd.err = err
			return cmd
		}

		var base int64
		if found {
			base, err = strconv.ParseInt(string(current), 10, 64)
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
		payload := []byte(strconv.FormatInt(next, 10))

		applied, err := c.compareAndSwap(ctx, key, payload, current, ttl, found)
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

// Append extends a string, preserving its TTL.
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

	for attempt := 0; ; attempt++ {
		current, ttl, found, err := c.readString(ctx, key, true)
		if err != nil {
			cmd.err = err
			return cmd
		}

		next := make([]byte, 0, len(current)+len(value))
		next = append(next, current...)
		next = append(next, value...)

		applied, err := c.compareAndSwap(ctx, key, next, current, ttl, found)
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

// compareAndSwap writes next only if the stored value still equals expect
// (or, when the key was absent, only if it is still absent).
func (c *Client) compareAndSwap(ctx context.Context, key string, next, expect []byte, ttl int, exists bool) (bool, error) {
	if !exists {
		return c.insertStringNX(ctx, key, next, ttl)
	}
	if ttl > 0 {
		return c.core.runner.ExecCAS(ctx, c.core.schema.kvUpdateCASTTL,
			c.kvCASTTLArgs(key, typeString, next, expect, ttl))
	}
	return c.core.runner.ExecCAS(ctx, c.core.schema.kvUpdateCAS,
		c.kvCASArgs(key, typeString, next, expect))
}

// StrLen returns the length of the string value stored at key, or 0.
func (c *Client) StrLen(ctx context.Context, key string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	value, _, found, err := c.readString(ctx, key, false)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if found {
		cmd.val = int64(len(value))
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
