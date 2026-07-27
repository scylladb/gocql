package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// keyType is the Redis type recorded for a key in the kv table.
//
// Redis guarantees one type per key and answers WRONGTYPE otherwise. Because
// strings, hashes, sets and lists live in different CQL tables here, the kv
// table doubles as the key namespace: string keys store their value there and
// collection keys store a type marker row. That single registry is what lets
// Del, Exists, Keys and Scan see every key regardless of type.
type keyType string

const (
	typeString keyType = "string"
	typeHash   keyType = "hash"
	typeSet    keyType = "set"
	typeList   keyType = "list"
	typeGuard  keyType = "guard"
)

func (kt keyType) collectionTable(s *schema) (string, bool) {
	switch kt {
	case typeHash:
		return s.hashDeleteKey, true
	case typeSet:
		return s.setDeleteKey, true
	case typeList:
		return s.listDeleteKey, true
	default:
		return "", false
	}
}

// typeCache remembers verified key types so warm keys skip the registry round
// trip. It is a best effort cache: another process can change a key's type,
// which is why a cached mismatch is re-verified against the cluster before a
// WRONGTYPE is returned.
type typeCache struct {
	mu      sync.Mutex
	max     int
	entries map[string]keyType
}

func newTypeCache(max int) *typeCache {
	if max <= 0 {
		max = 1024
	}
	return &typeCache{max: max, entries: make(map[string]keyType, min(max, 64))}
}

func cacheKey(bucket, key string) string { return bucket + "\x00" + key }

func (c *typeCache) get(bucket, key string) (keyType, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	kt, ok := c.entries[cacheKey(bucket, key)]
	return kt, ok
}

func (c *typeCache) put(bucket, key string, kt keyType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.max {
		// Shed a slice of the cache rather than tracking exact recency: the
		// cache only saves a round trip, so approximate eviction is enough.
		drop := max(c.max/8, 1)
		for k := range c.entries {
			delete(c.entries, k)
			drop--
			if drop <= 0 {
				break
			}
		}
	}
	c.entries[cacheKey(bucket, key)] = kt
}

func (c *typeCache) forget(bucket, key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, cacheKey(bucket, key))
}

func (c *Client) cachedType(key string) (keyType, bool) {
	if c.core.types == nil {
		return "", false
	}
	return c.core.types.get(c.bucket, key)
}

func (c *Client) rememberType(key string, kt keyType) {
	if c.core.types != nil {
		c.core.types.put(c.bucket, key, kt)
	}
}

func (c *Client) forgetType(key string) {
	if c.core.types != nil {
		c.core.types.forget(c.bucket, key)
	}
}

// lookupType reads the recorded type of a key. Rows written before the type
// column existed report an empty type and are treated as strings.
func (c *Client) lookupType(ctx context.Context, key string) (keyType, bool, error) {
	var raw string
	err := c.core.runner.ScanOne(ctx, c.core.schema.kvSelectType, c.keyArgs(key), &raw)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	kt := keyType(raw)
	if kt == "" {
		kt = typeString
	}
	c.rememberType(key, kt)
	return kt, true, nil
}

// ensureKeyType registers a collection key, returning WRONGTYPE if the key is
// already held by another type. The registration is a single conditional
// insert, so a cold key costs one round trip and a warm key costs none.
func (c *Client) ensureKeyType(ctx context.Context, key string, want keyType) error {
	if !c.core.enforceTypes {
		return nil
	}
	if kt, ok := c.cachedType(key); ok {
		if kt == want {
			return nil
		}
		actual, found, err := c.lookupType(ctx, key)
		if err != nil {
			return err
		}
		if found {
			if actual != want {
				return ErrWrongType
			}
			return nil
		}
	}

	existingRow := map[string]any{}
	applied, err := c.core.runner.MapScanCAS(ctx, c.core.schema.kvMarkerNX, c.kvMarkerArgs(key, want), existingRow)
	if err != nil {
		return err
	}
	if !applied {
		existing := typeString
		if raw, ok := existingRow["type"].(string); ok && raw != "" {
			existing = keyType(raw)
		}
		if existing != want {
			return ErrWrongType
		}
	}
	c.rememberType(key, want)
	return nil
}

// checkKeyType is the cheap read path guard: it trusts the cache and never
// issues a query, so reads stay single round trip.
func (c *Client) checkKeyType(key string, want keyType) error {
	if !c.core.enforceTypes {
		return nil
	}
	if kt, ok := c.cachedType(key); ok && kt != want {
		return ErrWrongType
	}
	return nil
}

// prepareStringWrite makes a key ready to hold a string. Redis SET replaces a
// key of any type, so an existing collection is dropped first instead of being
// left behind as unreachable rows.
func (c *Client) prepareStringWrite(ctx context.Context, key string) error {
	if !c.core.enforceTypes {
		return nil
	}
	kt, ok := c.cachedType(key)
	if !ok {
		var (
			found bool
			err   error
		)
		kt, found, err = c.lookupType(ctx, key)
		if err != nil {
			return err
		}
		if !found {
			c.rememberType(key, typeString)
			return nil
		}
	}
	switch kt {
	case typeString, "":
		return nil
	case typeGuard:
		return ErrReservedKey
	}
	if err := c.purgeCollection(ctx, key, kt); err != nil {
		return err
	}
	c.rememberType(key, typeString)
	return nil
}

func (c *Client) purgeCollection(ctx context.Context, key string, kt keyType) error {
	stmt, ok := kt.collectionTable(c.core.schema)
	if !ok {
		return nil
	}
	return c.core.runner.Exec(ctx, stmt, c.keyArgs(key)...)
}

// unsupportedForType converts an internal WRONGTYPE into the clearer "this
// package cannot do that for this type" error used by TTL style commands.
func unsupportedForType(err error) error {
	if errors.Is(err, ErrWrongType) {
		return ErrKeyTypeUnsupported
	}
	return err
}

func (c *Client) Del(ctx context.Context, keys ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	var deleted atomic.Int64
	err := runConcurrent(ctx, len(keys), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		removed, err := c.delOne(ctx, keys[i])
		if err != nil {
			return err
		}
		if removed {
			deleted.Add(1)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = deleted.Load()
	return cmd
}

func (c *Client) delOne(ctx context.Context, key string) (bool, error) {
	if err := validateKey(key); err != nil {
		return false, err
	}

	kt := keyType("")
	if c.core.enforceTypes {
		cached, ok := c.cachedType(key)
		if ok {
			kt = cached
		} else {
			looked, found, err := c.lookupType(ctx, key)
			if err != nil {
				return false, err
			}
			if !found {
				return false, nil
			}
			kt = looked
		}
	}

	// The conditional delete is what makes the returned count correct under
	// concurrency: only the caller whose delete applied counts the key.
	applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.kvDeleteIfExists, c.keyArgs(key))
	if err != nil {
		return false, err
	}
	if err := c.purgeCollection(ctx, key, kt); err != nil {
		return false, err
	}
	c.forgetType(key)
	return applied, nil
}

func (c *Client) Exists(ctx context.Context, keys ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	var found atomic.Int64
	err := runConcurrent(ctx, len(keys), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		var key string
		err := c.core.runner.ScanOne(ctx, c.core.schema.kvSelectKey, c.keyArgs(keys[i]), &key)
		if err != nil {
			if errors.Is(err, errNotFound) {
				return nil
			}
			return err
		}
		found.Add(1)
		return nil
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = found.Load()
	return cmd
}

// Expire sets a TTL on a string key. A non positive expiration deletes the key,
// matching Redis 7. TTL is a property of the stored value in CQL, so this is
// implemented as a conditional rewrite: a concurrent Set is detected and the
// operation retries instead of resurrecting the previous value.
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	if expiration <= 0 {
		removed, err := c.delOne(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}
		cmd.val = removed
		return cmd
	}

	ttl := ttlSecondsFromDuration(expiration)
	applied, found, err := c.retimeValue(ctx, key, ttl)
	if err != nil {
		cmd.err = unsupportedForType(err)
		return cmd
	}
	if !found {
		cmd.val = false
		return cmd
	}
	cmd.val = applied
	return cmd
}

// Persist removes the TTL from a string key.
func (c *Client) Persist(ctx context.Context, key string) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	_, ttl, found, err := c.readString(ctx, key, true)
	if err != nil {
		cmd.err = unsupportedForType(err)
		return cmd
	}
	if !found || ttl <= 0 {
		cmd.val = false
		return cmd
	}

	applied, _, err := c.retimeValue(ctx, key, 0)
	if err != nil {
		cmd.err = unsupportedForType(err)
		return cmd
	}
	cmd.val = applied
	return cmd
}

// retimeValue rewrites a value with a new TTL under a compare-and-set on the
// current value. ttl <= 0 clears the TTL.
func (c *Client) retimeValue(ctx context.Context, key string, ttl int) (applied, found bool, err error) {
	for attempt := 0; ; attempt++ {
		value, _, exists, err := c.readString(ctx, key, false)
		if err != nil {
			return false, false, err
		}
		if !exists {
			return false, false, nil
		}

		var ok bool
		if ttl > 0 {
			ok, err = c.core.runner.ExecCAS(ctx, c.core.schema.kvUpdateCASTTL,
				c.kvCASTTLArgs(key, typeString, value, value, ttl))
		} else {
			ok, err = c.core.runner.ExecCAS(ctx, c.core.schema.kvUpdateCAS,
				c.kvCASArgs(key, typeString, value, value))
		}
		if err != nil {
			return false, true, err
		}
		if ok {
			return true, true, nil
		}
		if attempt >= c.core.casRetries {
			return false, true, ErrCASExhausted
		}
		if err := waitWithContext(ctx, backoffFor(c.core.casBackoff, attempt, c.core.blockPollMax)); err != nil {
			return false, true, err
		}
	}
}

func (c *Client) TTL(ctx context.Context, key string) *DurationCmd {
	cmd := &DurationCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	var (
		raw   string
		value []byte
		ttl   *int
	)
	err := c.core.runner.ScanOne(ctx, c.core.schema.kvSelectTTL, c.keyArgs(key), &raw, &value, &ttl)
	if err != nil {
		if errors.Is(err, errNotFound) {
			cmd.val = -2 * time.Second
			return cmd
		}
		cmd.err = err
		return cmd
	}
	if kt := keyType(raw); kt != "" && kt != typeString {
		// Collections carry no TTL in this mapping; Redis reports -1 for keys
		// without an expiry.
		cmd.val = -1 * time.Second
		return cmd
	}
	if ttl == nil || *ttl <= 0 {
		cmd.val = -1 * time.Second
		return cmd
	}
	cmd.val = time.Duration(*ttl) * time.Second
	return cmd
}

// Rename moves a string key, preserving its TTL. The write to the destination
// and the removal of the source are separate statements, so this is not atomic
// across a failure; the source removal is conditional, so a value written
// concurrently to the source is never silently discarded.
func (c *Client) Rename(ctx context.Context, key, newkey string) *StatusCmd {
	cmd := &StatusCmd{val: "OK"}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(newkey); err != nil {
		cmd.err = err
		return cmd
	}

	if key == newkey {
		// Renaming a key onto itself is a no-op in Redis. Writing then
		// deleting would destroy the key.
		var existing string
		err := c.core.runner.ScanOne(ctx, c.core.schema.kvSelectKey, c.keyArgs(key), &existing)
		if err != nil {
			if errors.Is(err, errNotFound) {
				cmd.err = ErrNoSuchKey
				return cmd
			}
			cmd.err = err
		}
		return cmd
	}

	for attempt := 0; ; attempt++ {
		value, ttl, found, err := c.readString(ctx, key, true)
		if err != nil {
			cmd.err = unsupportedForType(err)
			return cmd
		}
		if !found {
			cmd.err = ErrNoSuchKey
			return cmd
		}

		if err := c.prepareStringWrite(ctx, newkey); err != nil {
			cmd.err = err
			return cmd
		}
		if err := c.writeString(ctx, newkey, value, ttl); err != nil {
			cmd.err = err
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
			return cmd
		}
		if attempt >= c.core.casRetries {
			cmd.err = ErrCASExhausted
			return cmd
		}
		if err := waitWithContext(ctx, backoffFor(c.core.casBackoff, attempt, c.core.blockPollMax)); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// Copy copies a string key, preserving its TTL. A missing source returns 0 with
// no error, as Redis does. Without replace the write is conditional, so two
// concurrent copies cannot both report success.
func (c *Client) Copy(ctx context.Context, source, destination string, _ int, replace bool) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(source); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(destination); err != nil {
		cmd.err = err
		return cmd
	}

	value, ttl, found, err := c.readString(ctx, source, true)
	if err != nil {
		cmd.err = unsupportedForType(err)
		return cmd
	}
	if !found {
		cmd.val = 0
		return cmd
	}

	if err := c.prepareStringWrite(ctx, destination); err != nil {
		cmd.err = err
		return cmd
	}

	if replace {
		if err := c.writeString(ctx, destination, value, ttl); err != nil {
			cmd.err = err
			return cmd
		}
		cmd.val = 1
		return cmd
	}

	applied, err := c.insertStringNX(ctx, destination, value, ttl)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if applied {
		cmd.val = 1
	}
	return cmd
}
