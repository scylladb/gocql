package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	gocql "github.com/gocql/gocql"
)

// msetAtomic applies every pair in one lightweight transaction.
//
// Conditional batches must stay inside a single partition, so the version
// guard is a reserved row in the kv table of the active bucket rather than a
// separate table: bucket mode already puts every key of a bucket in one
// partition, which makes the whole batch legal. A batch spanning the kv table
// and a separate guard table is rejected by the server.
//
// The guarantee is MSet against MSet within a bucket. Plain Set, Del or Incr
// do not take the guard, so they can still interleave with an atomic MSet.
func (c *Client) msetAtomic(ctx context.Context, pairs []kvPair) error {
	core := c.core
	if core.session == nil && core.runner == nil {
		return ErrNotInitialized
	}
	if !core.schema.bucketed {
		return errors.New("rediscompat: atomic MSet requires AtomicMSetByBucket")
	}
	if len(pairs) > core.atomicMaxPairs {
		return fmt.Errorf("rediscompat: atomic MSet accepts at most %d pairs, got %d", core.atomicMaxPairs, len(pairs))
	}

	payloads := make([][]byte, len(pairs))
	for i := range pairs {
		payload, err := marshalValue(pairs[i].value)
		if err != nil {
			return err
		}
		payloads[i] = payload
	}

	for attempt := 0; ; attempt++ {
		version, err := c.guardVersion(ctx)
		if err != nil {
			return err
		}

		stmts := make([]batchStatement, 0, len(pairs)+1)
		for i := range pairs {
			stmts = append(stmts, batchStatement{
				stmt: core.schema.kvUpsert,
				args: c.kvWriteArgs(pairs[i].key, typeString, payloads[i]),
			})
		}
		stmts = append(stmts, batchStatement{
			stmt: core.schema.kvGuardCAS,
			args: c.guardCASArgs(formatVersion(version+1), formatVersion(version)),
		})

		applied, err := core.runner.BatchCAS(ctx, gocql.LoggedBatch, stmts)
		if err != nil {
			return err
		}
		if applied {
			for i := range pairs {
				c.rememberType(pairs[i].key, typeString)
			}
			return nil
		}
		if attempt >= core.atomicRetries {
			return fmt.Errorf("rediscompat: atomic MSet conflict after %d retries: %w", core.atomicRetries, ErrCASExhausted)
		}
		if err := waitWithContext(ctx, backoffFor(core.atomicBackoff, attempt, core.atomicBackoffMax)); err != nil {
			return err
		}
	}
}

// guardVersion returns the current bucket version, creating the guard row on
// first use. The "already created" flag is tracked per bucket so per tenant
// views do not each pay for the conditional insert forever.
func (c *Client) guardVersion(ctx context.Context) (int64, error) {
	if _, ok := c.core.guardBuckets.Load(c.bucket); !ok {
		_, err := c.core.runner.MapScanCAS(ctx, c.core.schema.kvInsertNX,
			c.kvWriteArgs(guardKey, typeGuard, formatVersion(0)), map[string]any{})
		if err != nil {
			return 0, err
		}
		c.core.guardBuckets.Store(c.bucket, true)
	}

	var (
		kind  string
		value []byte
	)
	err := c.core.runner.ScanOne(ctx, c.core.schema.kvSelect, c.keyArgs(guardKey), &kind, &value)
	if err != nil {
		if errors.Is(err, errNotFound) {
			// The guard row expired or was removed; recreate it next round.
			c.core.guardBuckets.Delete(c.bucket)
			return 0, errors.New("rediscompat: atomic MSet guard row is missing")
		}
		return 0, err
	}
	if len(value) == 0 {
		return 0, nil
	}
	version, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("rediscompat: corrupt atomic MSet guard version: %w", err)
	}
	return version, nil
}

func formatVersion(v int64) []byte {
	return []byte(strconv.FormatInt(v, 10))
}
