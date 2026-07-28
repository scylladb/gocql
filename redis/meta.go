package redis

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// keyType is the Redis type recorded on a key's meta row. The meta row is the
// key: it exists exactly while the key exists, so its presence answers EXISTS
// and its type column answers WRONGTYPE without a second lookup.
type keyType string

const (
	typeString keyType = "string"
	typeHash   keyType = "hash"
	typeSet    keyType = "set"
	typeList   keyType = "list"
)

func (kt keyType) kind() int8 {
	switch kt {
	case typeHash:
		return kindField
	case typeSet:
		return kindMember
	case typeList:
		return kindPos
	default:
		return kindMeta
	}
}

func (kt keyType) collection() bool { return kt != typeString && kt != "" }

// keyMeta is the meta row: everything about a key that is not one of its
// elements. size, head and tail are only meaningful for collections, and a
// collection with size 0 does not exist, because the command that removes its
// last element removes this row in the same conditional batch.
type keyMeta struct {
	typ     keyType
	value   []byte
	version int64
	size    int64
	head    int64
	tail    int64
	expires time.Time
}

// expired reports whether the logical expiry has passed. Expiry is stored as
// data rather than only as a cell TTL so a conditional write can reason about
// it; the cost is that a read has to apply it.
func (m keyMeta) expired(now time.Time) bool {
	return !m.expires.IsZero() && !m.expires.After(now)
}

// nextVersion returns the version stamped on a mutated meta row.
//
// It is random rather than incremented. A guard reads a version and writes only
// while the row still carries it, so all a version has to do is change; making
// it unpredictable means an unconditional overwrite (SET, which replaces a key
// of any type) cannot accidentally restore a value some other writer is still
// guarding on.
func nextVersion() int64 {
	for {
		if v := rand.Int64(); v != 0 {
			return v
		}
	}
}

func expiryArg(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// cellTTL converts a logical expiry into the TTL written alongside it, so a
// string key is reclaimed by the server without waiting for a reader or a
// sweep. The grace keeps the cell alive slightly past the logical expiry, which
// leaves expires_at as the only thing that decides whether a key is gone.
func cellTTL(expires time.Time, now time.Time) int {
	if expires.IsZero() {
		return 0
	}
	seconds := int(expires.Sub(now).Seconds()) + expiryGraceSeconds
	if seconds < 1 {
		seconds = 1
	}
	if seconds > maxTTLSeconds {
		seconds = maxTTLSeconds
	}
	return seconds
}

// readMeta loads a key's meta row and applies its expiry.
//
// An expired key is reported as absent and dropped in passing: expiry is
// logical, so something has to reclaim it, and the first reader is the cheapest
// place to do that for a key anyone still looks at. Sweep covers the rest.
func (c *Client) readMeta(ctx context.Context, key string) (keyMeta, bool, error) {
	var m keyMeta
	var typ string
	err := c.core.runner.ScanOne(ctx, c.core.schema.metaRead, c.keyArgs(key),
		&typ, &m.value, &m.version, &m.size, &m.head, &m.tail, &m.expires)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return keyMeta{}, false, nil
		}
		return keyMeta{}, false, err
	}
	m.typ = keyType(typ)
	if m.typ == "" {
		m.typ = typeString
	}
	if m.expired(c.now()) {
		c.dropExpired(ctx, key)
		return keyMeta{}, false, nil
	}
	return m, true, nil
}

// readPick loads the meta row and the named elements in one query, which is
// what makes a type assertion free for every element command: the type comes
// back with the data the command needed anyway.
func (c *Client) readPick(ctx context.Context, key string, kind int8, subs [][]byte) (keyMeta, map[string][]byte, bool, error) {
	iter := c.core.runner.Iterate(ctx, c.core.schema.pick(len(subs)), c.pickArgs(key, kind, subs), iterOptions{})

	var (
		m     keyMeta
		found bool
		elems = make(map[string][]byte, len(subs))
	)
	for {
		var (
			rowKind int8
			sub     []byte
			typ     string
			row     keyMeta
		)
		if !iter.Scan(&rowKind, &sub, &typ, &row.value, &row.version, &row.size, &row.head, &row.tail, &row.expires) {
			break
		}
		if rowKind == kindMeta {
			row.typ = keyType(typ)
			if row.typ == "" {
				row.typ = typeString
			}
			m, found = row, true
			continue
		}
		elems[string(sub)] = row.value
	}
	if err := iter.Close(); err != nil {
		return keyMeta{}, nil, false, err
	}
	if !found {
		return keyMeta{}, nil, false, nil
	}
	if m.expired(c.now()) {
		c.dropExpired(ctx, key)
		return keyMeta{}, nil, false, nil
	}
	return m, elems, true, nil
}

// requireType resolves a key for a command that expects one type. A key that is
// absent is not an error: most read commands answer "empty" for it, and the
// caller decides.
func (m keyMeta) requireType(want keyType) error {
	if m.typ != want {
		return ErrWrongType
	}
	return nil
}

// dropExpired removes a key whose logical expiry has passed. Failures are
// ignored: the key already reads as gone, so this is reclamation, not
// correctness, and Sweep will come back to it.
func (c *Client) dropExpired(ctx context.Context, key string) {
	_ = c.core.runner.Exec(ctx, c.core.schema.keyDelete, c.keyArgs(key)...)
	_ = c.core.runner.Exec(ctx, c.core.schema.indexDelete, c.keyArgs(key)...)
}

// noteKey records a key in the enumeration index.
//
// The index is deliberately not authoritative. It is written before or
// alongside the key so the failure mode is an entry with no key, which
// enumeration deletes when it verifies; the opposite order would produce a key
// that exists and cannot be listed, which nothing would ever repair.
func (c *Client) noteKey(ctx context.Context, key string) error {
	return c.core.runner.Exec(ctx, c.core.schema.indexWrite, c.keyArgs(key)...)
}

func (c *Client) forgetKey(ctx context.Context, key string) error {
	return c.core.runner.Exec(ctx, c.core.schema.indexDelete, c.keyArgs(key)...)
}

// noteExpiry records a key in the time bucketed expiry index so Sweep can find
// it without scanning the namespace. It is best effort in the same way the
// enumeration index is: a missing entry costs space until the key is read
// again, never correctness.
func (c *Client) noteExpiry(ctx context.Context, key string, expires time.Time) {
	if expires.IsZero() {
		return
	}
	slot := expirySlot(expires)
	ttl := int(expires.Sub(c.now()).Seconds()) + expirySlotSeconds*2
	if ttl < expirySlotSeconds {
		ttl = expirySlotSeconds
	}
	if ttl > maxTTLSeconds {
		ttl = maxTTLSeconds
	}
	_ = c.core.runner.Exec(ctx, c.core.schema.expiryWrite, slot, c.bucket, key, ttl)
}

func expirySlot(t time.Time) time.Time {
	return t.UTC().Truncate(expirySlotSeconds * time.Second)
}

func (c *Client) now() time.Time {
	if c.core.clock != nil {
		return c.core.clock()
	}
	return time.Now()
}

// casRetry waits before the next attempt, or reports that the retry budget is
// spent. Contention is reported rather than hidden: a caller that sees "empty"
// when the retry budget ran out would stop draining a queue that still holds
// work, and no error would ever tell anyone why.
func (c *Client) casRetry(ctx context.Context, attempt int) error {
	if attempt >= c.core.casRetries {
		return ErrCASExhausted
	}
	return waitWithContext(ctx, backoffFor(c.core.casBackoff, attempt, c.core.casBackoffMax))
}

// checkBatch refuses a command whose batch would exceed the configured
// statement ceiling.
//
// Chunking it instead would make the command atomic per chunk, which turns
// atomicity into a property of argument count. Refusing keeps the contract to
// "atomic or refused", which is a ceiling a caller can design around.
func (c *Client) checkBatch(stmts int) error {
	if limit := c.core.maxBatchStmts; limit > 0 && stmts > limit {
		return errTooManyStatements(stmts, limit)
	}
	return nil
}
