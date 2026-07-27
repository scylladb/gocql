package redis

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
)

// HSet writes hash fields and returns how many of them were newly created.
// The count comes from conditional inserts rather than a read followed by a
// write, so two clients creating the same field concurrently cannot both count
// it as new.
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	pairs, err := normalizeHashSetValues(values...)
	if err != nil {
		cmd.err = err
		return cmd
	}
	payloads := make([][]byte, len(pairs))
	for i := range pairs {
		payload, err := marshalValue(pairs[i].value)
		if err != nil {
			cmd.err = err
			return cmd
		}
		payloads[i] = payload
	}

	if err := c.ensureKeyType(ctx, key, typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	var created atomic.Int64
	err = runConcurrent(ctx, len(pairs), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		existing := map[string]any{}
		applied, err := c.core.runner.MapScanCAS(ctx, c.core.schema.hashInsertNX,
			c.hashWriteArgs(key, pairs[i].field, payloads[i]), existing)
		if err != nil {
			return err
		}
		if applied {
			created.Add(1)
			return nil
		}
		if current, ok := existing["value"].([]byte); ok && bytes.Equal(current, payloads[i]) {
			// The field already holds this value; skip the redundant write.
			return nil
		}
		return c.core.runner.Exec(ctx, c.core.schema.hashUpsert,
			c.hashWriteArgs(key, pairs[i].field, payloads[i])...)
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = created.Load()
	return cmd
}

func (c *Client) HGet(ctx context.Context, key, field string) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	var value []byte
	err := c.core.runner.ScanOne(ctx, c.core.schema.hashSelect, c.hashFieldArgs(key, field), &value)
	if err != nil {
		if errors.Is(err, errNotFound) {
			cmd.err = Nil
			return cmd
		}
		cmd.err = err
		return cmd
	}
	cmd.val = string(value)
	return cmd
}

func (c *Client) HExists(ctx context.Context, key, field string) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	var value []byte
	err := c.core.runner.ScanOne(ctx, c.core.schema.hashSelect, c.hashFieldArgs(key, field), &value)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return cmd
		}
		cmd.err = err
		return cmd
	}
	cmd.val = true
	return cmd
}

// HDel removes fields and reports how many existed, using conditional deletes
// so the count stays correct when several clients delete the same field.
func (c *Client) HDel(ctx context.Context, key string, fields ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	var removed atomic.Int64
	err := runConcurrent(ctx, len(fields), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.hashDeleteIf, c.hashFieldArgs(key, fields[i]))
		if err != nil {
			return err
		}
		if applied {
			removed.Add(1)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = removed.Load()
	return cmd
}

// HGetAll materializes a whole hash. The partition is read with server side
// paging; a hash large enough to matter should be read field by field instead.
func (c *Client) HGetAll(ctx context.Context, key string) *MapStringStringCmd {
	cmd := &MapStringStringCmd{val: make(map[string]string)}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.hashSelectAll, c.keyArgs(key),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		field string
		value []byte
	)
	for iter.Scan(&field, &value) {
		cmd.val[field] = string(value)
	}
	if err := iter.Close(); err != nil {
		// A mid-stream failure would otherwise hand back a silently truncated
		// map to callers that only check Val.
		cmd.val = nil
		cmd.err = err
	}
	return cmd
}
