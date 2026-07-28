package redis

import (
	"context"
	"sync/atomic"
	"time"

	gocql "github.com/gocql/gocql"
)

// Del removes keys and reports how many existed.
//
// A key and its elements share one partition, so removing a key of any type is
// one conditional batch: the meta row carries the condition, and the slice
// delete takes every element with it. There is no ordering left to get wrong
// and no window in which a concurrent write can attach an element to a key that
// is being deleted.
func (c *Client) Del(ctx context.Context, keys ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	var deleted atomic.Int64
	err := runConcurrent(ctx, len(keys), c.core.maxConcurrent, func(ctx context.Context, i int) error {
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

	// The read is what makes an already expired key report 0 rather than 1: the
	// rows may still be there, but the key is gone.
	_, found, err := c.readMeta(ctx, key)
	if err != nil {
		return false, err
	}
	if !found {
		// Still clear anything left behind, so a key that expired without being
		// read does not keep its rows until Sweep runs.
		c.dropExpired(ctx, key)
		return false, nil
	}

	applied, err := c.core.runner.BatchCAS(ctx, gocql.UnloggedBatch, []batchStatement{
		{stmt: c.core.schema.metaDeleteIf, args: c.keyArgs(key)},
		{stmt: c.core.schema.elemsDelete, args: c.keyArgs(key)},
	})
	if err != nil {
		return false, err
	}
	if applied {
		_ = c.forgetKey(ctx, key)
	}
	return applied, nil
}

// Exists counts the keys that exist, of any type. One read per key answers it:
// the meta row exists exactly while the key does, so an emptied collection is
// already gone rather than something a probe has to rule out.
func (c *Client) Exists(ctx context.Context, keys ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	var found atomic.Int64
	err := runConcurrent(ctx, len(keys), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		_, ok, err := c.readMeta(ctx, keys[i])
		if err != nil {
			return err
		}
		if ok {
			found.Add(1)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = found.Load()
	return cmd
}

// Expire sets a TTL on a key of any type. A non positive expiration deletes the
// key, matching Redis 7.
//
// Expiry is a column on the meta row, so this is one guarded update rather than
// a rewrite of the value, and it applies to hashes, sets and lists as well as
// strings. A string also gets a cell TTL so the server reclaims it unprompted;
// a collection is reclaimed by the first read after it expires, or by Sweep.
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

	applied, found, err := c.retime(ctx, key, c.now().Add(expiration))
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = found && applied
	return cmd
}

// Persist removes the expiry from a key of any type.
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

	m, found, err := c.readMeta(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found || m.expires.IsZero() {
		return cmd
	}

	applied, _, err := c.retime(ctx, key, time.Time{})
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = applied
	return cmd
}

// retime rewrites a key's expiry under a version guard.
//
// A string is rewritten in full so its cell TTL matches the new expiry;
// leaving the old TTL in place would let the row vanish while the key claims
// to live longer. A collection carries no cell TTL at all, which is what keeps
// Expire and Persist O(1) for it instead of a rewrite of every element.
func (c *Client) retime(ctx context.Context, key string, expires time.Time) (applied, found bool, err error) {
	for attempt := 0; ; attempt++ {
		m, ok, err := c.readMeta(ctx, key)
		if err != nil {
			return false, false, err
		}
		if !ok {
			return false, false, nil
		}

		if m.typ == typeString {
			applied, err = c.casString(ctx, key, m.value, expires, m.version)
		} else {
			applied, err = c.core.runner.ExecCAS(ctx, c.core.schema.expireCAS,
				c.expireCASArgs(key, nextVersion(), expiryArg(expires), m.version))
		}
		if err != nil {
			return false, true, err
		}
		if applied {
			c.noteExpiry(ctx, key, expires)
			return true, true, nil
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			return false, true, err
		}
	}
}

// TTL reports the remaining time to live: -1 for a key without an expiry and
// -2 for a key that does not exist, as Redis does.
func (c *Client) TTL(ctx context.Context, key string) *DurationCmd {
	cmd := &DurationCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	m, found, err := c.readMeta(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		cmd.val = -2 * time.Second
		return cmd
	}
	if m.expires.IsZero() {
		cmd.val = -1 * time.Second
		return cmd
	}
	remaining := m.expires.Sub(c.now())
	if remaining < time.Second {
		remaining = time.Second
	}
	cmd.val = remaining.Round(time.Second)
	return cmd
}

// Rename moves a string key, preserving its expiry.
//
// With TransactionsByBucket both keys share a partition, so the write and the
// removal are one conditional batch and the rename is atomic. Otherwise they
// are separate partitions and the two statements cannot be, so a lost race
// rolls back the write it made rather than leaving the value under two keys.
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
		_, found, err := c.readMeta(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if !found {
			cmd.err = ErrNoSuchKey
		}
		return cmd
	}

	if c.core.schema.grouped {
		cmd.err = c.renameAtomic(ctx, key, newkey)
		return cmd
	}

	// Whether the destination already held something decides if a failed
	// rename may roll its own write back: deleting a destination that existed
	// beforehand would destroy data the caller never asked to touch.
	_, destExisted, err := c.readMeta(ctx, newkey)
	if err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			cmd.err = unsupportedForType(err)
			return cmd
		}
		if !found {
			cmd.err = ErrNoSuchKey
			return cmd
		}

		if err := c.setString(ctx, newkey, m.value, m.expires); err != nil {
			cmd.err = err
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
			return cmd
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			// The destination now holds a copy the source never gave up. Undo
			// it when the destination was empty to begin with, so a failed
			// Rename does not leave the value under two keys.
			if !destExisted {
				if _, rbErr := c.delOne(ctx, newkey); rbErr == nil {
					_ = c.forgetKey(ctx, newkey)
				}
			}
			cmd.err = err
			return cmd
		}
	}
}

// renameAtomic renames within one bucket partition, where the destination write
// and the source removal can be a single conditional batch.
func (c *Client) renameAtomic(ctx context.Context, key, newkey string) error {
	for attempt := 0; ; attempt++ {
		m, found, err := c.readString(ctx, key)
		if err != nil {
			return unsupportedForType(err)
		}
		if !found {
			return ErrNoSuchKey
		}
		dest, destFound, err := c.readMeta(ctx, newkey)
		if err != nil {
			return err
		}
		if err := c.noteKey(ctx, newkey); err != nil {
			return err
		}

		stmts := []batchStatement{
			{stmt: c.core.schema.metaDeleteCAS, args: c.metaDeleteCASArgs(key, m.version)},
			{stmt: c.core.schema.elemsDelete, args: c.keyArgs(key)},
		}
		if destFound && dest.typ.collection() {
			stmts = append(stmts, batchStatement{stmt: c.core.schema.elemsDelete, args: c.keyArgs(newkey)})
		}
		ttl := cellTTL(m.expires, c.now())
		write := batchStatement{
			stmt: c.core.schema.strWrite,
			args: c.strWriteArgs(newkey, m.value, nextVersion(), expiryArg(m.expires), 0),
		}
		if ttl > 0 {
			write = batchStatement{
				stmt: c.core.schema.strWriteTTL,
				args: c.strWriteArgs(newkey, m.value, nextVersion(), expiryArg(m.expires), ttl),
			}
		}
		stmts = append(stmts, write)

		applied, err := c.core.runner.BatchCAS(ctx, gocql.UnloggedBatch, stmts)
		if err != nil {
			return err
		}
		if applied {
			_ = c.forgetKey(ctx, key)
			c.noteExpiry(ctx, newkey, m.expires)
			return nil
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			return err
		}
	}
}

// Copy copies a string key, preserving its expiry. A missing source returns 0
// with no error, as Redis does. Without replace the write is conditional, so
// two concurrent copies cannot both report success and an occupied destination
// is left exactly as it was.
func (c *Client) Copy(ctx context.Context, source, destination string, destinationDB int, replace bool) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if destinationDB != 0 {
		// There is one namespace per client here. Silently copying into the
		// current one would put the value somewhere the caller did not ask for.
		cmd.err = Error("rediscompat: Copy DB must be 0; use Bucketed for a separate namespace")
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

	m, found, err := c.readString(ctx, source)
	if err != nil {
		cmd.err = unsupportedForType(err)
		return cmd
	}
	if !found {
		return cmd
	}

	if replace {
		// Replace is Set semantics: the destination is dropped whatever it
		// held, so clearing a collection there is the intended effect.
		if err := c.setString(ctx, destination, m.value, m.expires); err != nil {
			cmd.err = err
			return cmd
		}
		cmd.val = 1
		return cmd
	}

	if err := c.noteKey(ctx, destination); err != nil {
		cmd.err = err
		return cmd
	}
	applied, err := c.insertStringNX(ctx, destination, m.value, m.expires)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if applied {
		cmd.val = 1
	}
	return cmd
}

// Type reports the Redis type of a key, or an empty string when it is absent.
func (c *Client) Type(ctx context.Context, key string) *StatusCmd {
	cmd := &StatusCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	m, found, err := c.readMeta(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		cmd.val = "none"
		return cmd
	}
	cmd.val = string(m.typ)
	return cmd
}

// Sweep reclaims keys whose logical expiry has passed.
//
// Expiry is a column rather than only a cell TTL, which is what makes it
// conditionable and what lets it apply to collections; the cost is that
// something has to remove the rows. A read of an expired key does it for any key
// anyone still touches, and this covers the rest. It reads the time bucketed
// expiry index, so the work is proportional to the keys that expired rather than
// to the size of the namespace.
//
// Sweep is safe to call concurrently and from any bucket view; it sweeps the
// slots it has not seen yet, up to SweepLookback on the first call. Callers
// schedule it: a driver that starts its own background goroutines is a surprise.
func (c *Client) Sweep(ctx context.Context) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	now := c.now()
	c.core.sweepMu.Lock()
	from := c.core.sweepMark
	if from.IsZero() || now.Sub(from) > c.core.sweepLookback {
		from = now.Add(-c.core.sweepLookback)
	}
	c.core.sweepMu.Unlock()

	var reclaimed int64
	for slot := expirySlot(from); !slot.After(expirySlot(now)); slot = slot.Add(expirySlotSeconds * time.Second) {
		entries, err := c.sweepSlot(ctx, slot)
		if err != nil {
			cmd.err = err
			cmd.val = reclaimed
			return cmd
		}
		reclaimed += entries
	}

	c.core.sweepMu.Lock()
	if now.After(c.core.sweepMark) {
		// One slot of overlap on the next run, so an entry written just after
		// this slot was read is not skipped.
		c.core.sweepMark = now.Add(-expirySlotSeconds * time.Second)
	}
	c.core.sweepMu.Unlock()

	cmd.val = reclaimed
	return cmd
}

func (c *Client) sweepSlot(ctx context.Context, slot time.Time) (int64, error) {
	iter := c.core.runner.Iterate(ctx, c.core.schema.expiryScan, []any{slot},
		iterOptions{pageSize: c.core.scanPageSize})

	type entry struct{ bucket, key string }
	var (
		entries []entry
		bucket  string
		key     string
	)
	for iter.Scan(&bucket, &key) {
		entries = append(entries, entry{bucket, key})
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}

	var reclaimed atomic.Int64
	err := runConcurrent(ctx, len(entries), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		view := c
		if c.core.schema.bucketed && entries[i].bucket != c.bucket {
			view = &Client{core: c.core, bucket: entries[i].bucket}
		}
		// readMeta drops an expired key as a side effect, so the sweep is the
		// same reclamation a reader would have done.
		m, found, err := view.readMeta(ctx, entries[i].key)
		if err != nil {
			return err
		}
		if !found {
			reclaimed.Add(1)
		} else if !m.expires.IsZero() && m.expires.After(c.now()) {
			// The expiry moved out; leave the index entry for its new slot.
			return nil
		}
		return c.core.runner.Exec(ctx, c.core.schema.expiryDrop, slot, entries[i].bucket, entries[i].key)
	})
	return reclaimed.Load(), err
}
