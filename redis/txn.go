package redis

import (
	"context"
	"strconv"
	"time"

	gocql "github.com/gocql/gocql"
)

// Tx is a multi-key transaction over one bucket partition.
//
// It is the Redis WATCH/MULTI/EXEC contract with the scope of Redis Cluster
// rather than of a single node: every key must live in one bucket, because a
// conditional write is confined to one partition. Watch reads the version of
// each key it names; Exec applies the queued commands as one conditional batch
// carrying an independent condition per key. If any of them moved since Watch,
// nothing is applied and Exec returns ErrTxAborted, which is EXEC returning nil
// after an invalidated WATCH.
//
// The order that makes this useful is the Redis one: watch first, then read,
// then queue, then Exec. A value read before its key was watched is not covered
// by anything.
//
// A Tx is not safe for concurrent use; it belongs to the goroutine that built
// it.
type Tx struct {
	client *Client
	// pinned is the state each watched key had when it was watched, which is
	// what every condition is built from.
	pinned  map[string]txSnapshot
	watched []string
	ops     []txOp
	err     error
}

// txSnapshot is what a key looked like at Watch time.
type txSnapshot struct {
	m      keyMeta
	exists bool
}

type txOpKind int

const (
	txSet txOpKind = iota
	txDel
	txIncrBy
	txExpire
)

type txOp struct {
	kind    txOpKind
	key     string
	value   []byte
	delta   int64
	expires time.Duration
}

// Multi starts a transaction. Nothing is read and nothing is sent until Exec.
func (c *Client) Multi() *Tx {
	tx := &Tx{client: c, pinned: map[string]txSnapshot{}}
	if c.configErr != nil {
		tx.err = c.configErr
		return tx
	}
	if c.core == nil {
		tx.err = ErrNotInitialized
		return tx
	}
	if !c.core.schema.grouped {
		tx.err = ErrTxUnsupported
		return tx
	}
	return tx
}

// Watch starts a transaction watching the given keys, mirroring WATCH before
// MULTI in Redis.
func (c *Client) Watch(ctx context.Context, keys ...string) *Tx {
	return c.Multi().Watch(ctx, keys...)
}

// Watch pins the current version of each key, so a change made by anyone else
// from here on aborts Exec. The keys need not be written by the transaction: a
// read-then-write sequence is made safe by watching what it read.
//
// Watching a key twice keeps the first snapshot, which is the state the caller's
// read was based on.
func (tx *Tx) Watch(ctx context.Context, keys ...string) *Tx {
	if tx.err != nil {
		return tx
	}
	c := tx.client
	if err := c.ensureReady(ctx); err != nil {
		tx.err = err
		return tx
	}

	fresh := make([]string, 0, len(keys))
	for _, key := range keys {
		if err := validateKey(key); err != nil {
			tx.err = err
			return tx
		}
		if _, already := tx.pinned[key]; already {
			continue
		}
		fresh = append(fresh, key)
	}

	snapshots := make([]txSnapshot, len(fresh))
	if err := runConcurrent(ctx, len(fresh), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		m, found, err := c.readMeta(ctx, fresh[i])
		if err != nil {
			return err
		}
		snapshots[i] = txSnapshot{m: m, exists: found}
		return nil
	}); err != nil {
		tx.err = err
		return tx
	}
	for i, key := range fresh {
		tx.pinned[key] = snapshots[i]
		tx.watched = append(tx.watched, key)
	}
	return tx
}

// Set queues a string write.
func (tx *Tx) Set(key string, value any) *Tx {
	if tx.err != nil {
		return tx
	}
	if err := validateKey(key); err != nil {
		tx.err = err
		return tx
	}
	payload, err := tx.client.marshalBounded(value)
	if err != nil {
		tx.err = err
		return tx
	}
	tx.ops = append(tx.ops, txOp{kind: txSet, key: key, value: payload})
	return tx
}

// SetEx queues a string write with an expiry.
func (tx *Tx) SetEx(key string, value any, expiration time.Duration) *Tx {
	if tx.err != nil {
		return tx
	}
	if expiration <= 0 {
		tx.err = ErrInvalidExpire
		return tx
	}
	tx.Set(key, value)
	if tx.err == nil {
		tx.ops[len(tx.ops)-1].expires = expiration
	}
	return tx
}

// Del queues a key removal of any type.
func (tx *Tx) Del(keys ...string) *Tx {
	for _, key := range keys {
		if tx.err != nil {
			return tx
		}
		if err := validateKey(key); err != nil {
			tx.err = err
			return tx
		}
		tx.ops = append(tx.ops, txOp{kind: txDel, key: key})
	}
	return tx
}

// IncrBy queues a counter delta. The base value is the one the key had when it
// was watched, or the one Exec reads for an unwatched key, and the write is
// guarded by that same state, so an increment can never be applied on top of a
// value someone else changed meanwhile.
func (tx *Tx) IncrBy(key string, delta int64) *Tx {
	if tx.err != nil {
		return tx
	}
	if err := validateKey(key); err != nil {
		tx.err = err
		return tx
	}
	tx.ops = append(tx.ops, txOp{kind: txIncrBy, key: key, delta: delta})
	return tx
}

// Expire queues an expiry change for a key of any type. A non positive
// expiration is queued as a removal, as Redis 7 does.
func (tx *Tx) Expire(key string, expiration time.Duration) *Tx {
	if tx.err != nil {
		return tx
	}
	if expiration <= 0 {
		return tx.Del(key)
	}
	if err := validateKey(key); err != nil {
		tx.err = err
		return tx
	}
	tx.ops = append(tx.ops, txOp{kind: txExpire, key: key, expires: expiration})
	return tx
}

// Err reports the first error from watching or queueing.
func (tx *Tx) Err() error { return tx.err }

// Exec applies the queued commands atomically, or returns ErrTxAborted when a
// watched key changed since it was watched.
//
// Retrying is the caller's decision, exactly as with Redis: the queued writes
// were computed from a state that no longer holds, so only the caller knows
// whether the same transaction is still the right one.
func (tx *Tx) Exec(ctx context.Context) error {
	if tx.err != nil {
		return tx.err
	}
	c := tx.client
	if err := c.ensureReady(ctx); err != nil {
		return err
	}
	if len(tx.ops) == 0 {
		return ErrTxEmpty
	}

	states, err := tx.resolve(ctx)
	if err != nil {
		return err
	}

	// Expiry index entries are recorded once the batch applies, and only for
	// the keys whose new state actually carries an expiry.
	expiries := make(map[string]time.Time, len(tx.ops))
	writes := make([]batchStatement, 0, 2*len(tx.ops))
	written := make(map[string]bool, len(tx.ops))

	for i := range tx.ops {
		op := tx.ops[i]
		state := states[op.key]
		m, found := state.m, state.exists

		switch op.kind {
		case txSet:
			var expires time.Time
			if op.expires > 0 {
				expires = c.now().Add(op.expires)
				expiries[op.key] = expires
			}
			stmt, args, err := c.txWriteString(op.key, op.value, expires, m, found)
			if err != nil {
				return err
			}
			if found && m.typ.collection() {
				writes = append(writes, batchStatement{stmt: c.core.schema.elemsDelete, args: c.keyArgs(op.key)})
			}
			if err := c.noteKey(ctx, op.key); err != nil {
				return err
			}
			writes = append(writes, batchStatement{stmt: stmt, args: args})
			written[op.key] = true
		case txDel:
			if !found {
				// Nothing to delete. If the key was watched, its absence is
				// still guarded below, so a key that appeared meanwhile aborts
				// the transaction rather than surviving it.
				continue
			}
			writes = append(writes,
				batchStatement{stmt: c.core.schema.metaDeleteCAS, args: c.metaDeleteCASArgs(op.key, m.version)},
				batchStatement{stmt: c.core.schema.elemsDelete, args: c.keyArgs(op.key)},
			)
			written[op.key] = true
		case txIncrBy:
			var base int64
			if found {
				if m.typ != typeString {
					return ErrWrongType
				}
				parsed, err := strconv.ParseInt(string(m.value), 10, 64)
				if err != nil {
					return ErrValueNotInteger
				}
				base = parsed
			}
			next, ok := addChecked(base, op.delta)
			if !ok {
				return ErrIncrOverflow
			}
			stmt, args, err := c.txWriteString(op.key, []byte(strconv.FormatInt(next, 10)), m.expires, m, found)
			if err != nil {
				return err
			}
			if err := c.noteKey(ctx, op.key); err != nil {
				return err
			}
			writes = append(writes, batchStatement{stmt: stmt, args: args})
			written[op.key] = true
		case txExpire:
			if !found {
				continue
			}
			expires := c.now().Add(op.expires)
			expiries[op.key] = expires
			if m.typ == typeString {
				stmt, args, err := c.txWriteString(op.key, m.value, expires, m, found)
				if err != nil {
					return err
				}
				writes = append(writes, batchStatement{stmt: stmt, args: args})
				written[op.key] = true
				continue
			}
			writes = append(writes, batchStatement{
				stmt: c.core.schema.expireCAS,
				args: c.expireCASArgs(op.key, nextVersion(), expiryArg(expires), m.version),
			})
			written[op.key] = true
		}
	}

	// A watched key the transaction does not write needs a condition of its
	// own, so a change to something the transaction only read still aborts it.
	stmts := make([]batchStatement, 0, len(tx.watched)+len(writes))
	for _, key := range tx.watched {
		if written[key] {
			// The write carries the guard already.
			continue
		}
		state := states[key]
		if !state.exists {
			stmts = append(stmts, batchStatement{stmt: c.core.schema.absentCAS, args: c.keyArgs(key)})
			continue
		}
		stmts = append(stmts, batchStatement{
			stmt: c.core.schema.expireCAS,
			args: c.expireCASArgs(key, state.m.version, expiryArg(state.m.expires), state.m.version),
		})
	}
	stmts = append(stmts, writes...)
	if len(stmts) == 0 {
		return nil
	}
	if err := c.checkBatch(len(stmts)); err != nil {
		return err
	}

	applied, err := c.core.runner.BatchCAS(ctx, gocql.UnloggedBatch, stmts)
	if err != nil {
		return err
	}
	if !applied {
		return ErrTxAborted
	}

	for i := range tx.ops {
		if tx.ops[i].kind == txDel {
			_ = c.forgetKey(ctx, tx.ops[i].key)
		}
	}
	for key, expires := range expiries {
		c.noteExpiry(ctx, key, expires)
	}
	return nil
}

// resolve returns the state every condition is built from: the snapshot taken
// when a key was watched, and a fresh read for a key the transaction writes
// without watching.
//
// An unwatched write is still guarded by what Exec read, because the batch has
// to be conditional to be atomic. The window is one round trip rather than the
// caller's whole think time, but it is not zero: a transaction that must not
// lose that race watches the keys it writes.
func (tx *Tx) resolve(ctx context.Context) (map[string]txSnapshot, error) {
	c := tx.client

	states := make(map[string]txSnapshot, len(tx.pinned)+len(tx.ops))
	for key, snapshot := range tx.pinned {
		states[key] = snapshot
	}

	unwatched := make([]string, 0, len(tx.ops))
	queued := make(map[string]struct{}, len(tx.ops))
	for i := range tx.ops {
		key := tx.ops[i].key
		if _, pinned := states[key]; pinned {
			continue
		}
		if _, repeat := queued[key]; repeat {
			continue
		}
		queued[key] = struct{}{}
		unwatched = append(unwatched, key)
	}

	fresh := make([]txSnapshot, len(unwatched))
	if err := runConcurrent(ctx, len(unwatched), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		m, found, err := c.readMeta(ctx, unwatched[i])
		if err != nil {
			return err
		}
		fresh[i] = txSnapshot{m: m, exists: found}
		return nil
	}); err != nil {
		return nil, err
	}
	for i, key := range unwatched {
		states[key] = fresh[i]
	}
	return states, nil
}

// txWriteString builds the guarded write for a string valued key inside a
// transaction: a conditional insert when the key is absent, a version guarded
// update when it is not.
func (c *Client) txWriteString(key string, value []byte, expires time.Time, m keyMeta, found bool) (string, []any, error) {
	if c.core.maxValueSize > 0 && len(value) > c.core.maxValueSize {
		return "", nil, valueTooLarge(len(value), c.core.maxValueSize)
	}
	ttl := cellTTL(expires, c.now())
	if !found {
		stmt := c.core.schema.strWriteNX
		if ttl > 0 {
			stmt = c.core.schema.strWriteNXTTL
		}
		return stmt, c.strWriteArgs(key, value, nextVersion(), expiryArg(expires), ttl), nil
	}
	stmt := c.core.schema.strCAS
	if ttl > 0 {
		stmt = c.core.schema.strCASTTL
	}
	return stmt, c.strCASArgs(key, value, nextVersion(), expiryArg(expires), m.version, ttl), nil
}
