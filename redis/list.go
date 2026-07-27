package redis

import (
	"context"
	"errors"
	"time"
)

// LPush inserts values at the list head and returns the resulting length.
func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) *IntCmd {
	return c.listPush(ctx, key, true, values...)
}

// RPush inserts values at the list tail and returns the resulting length.
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) *IntCmd {
	return c.listPush(ctx, key, false, values...)
}

// LPop removes and returns the head of the list, or Nil when it is empty.
func (c *Client) LPop(ctx context.Context, key string) *StringCmd {
	return c.pop(ctx, key, false)
}

// RPop removes and returns the tail of the list, or Nil when it is empty.
func (c *Client) RPop(ctx context.Context, key string) *StringCmd {
	return c.pop(ctx, key, true)
}

// LLen returns the number of elements in the list.
func (c *Client) LLen(ctx context.Context, key string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeList); err != nil {
		cmd.err = err
		return cmd
	}

	size, err := c.listLen(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = size
	return cmd
}

// BLPop waits for an element on any of the keys and pops it from the left.
//
// CQL has no server side blocking primitive, so this polls with exponential
// backoff and jitter. The jitter keeps many idle waiters from querying in
// lockstep; a timeout of zero waits indefinitely and should be used with care.
func (c *Client) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	return c.blockingListPop(ctx, timeout, false, keys...)
}

// BRPop is BLPop from the right hand side of the list.
func (c *Client) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	return c.blockingListPop(ctx, timeout, true, keys...)
}

func (c *Client) listPush(ctx context.Context, key string, toLeft bool, values ...interface{}) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}
	if len(values) == 0 {
		cmd.err = errors.New("rediscompat: LPush/RPush requires at least one value")
		return cmd
	}

	payloads := make([][]byte, len(values))
	for i := range values {
		payload, err := marshalValue(values[i])
		if err != nil {
			cmd.err = err
			return cmd
		}
		payloads[i] = payload
	}

	if err := c.ensureKeyType(ctx, key, typeList); err != nil {
		cmd.err = err
		return cmd
	}

	for i := range payloads {
		if err := c.pushOne(ctx, key, toLeft, payloads[i]); err != nil {
			cmd.err = err
			return cmd
		}
	}

	size, err := c.listLen(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = size
	return cmd
}

// pushOne claims the next position with a conditional insert. Two concurrent
// pushes that compute the same position no longer overwrite each other: the
// loser sees the insert fail and re-reads the edge.
func (c *Client) pushOne(ctx context.Context, key string, toLeft bool, payload []byte) error {
	for attempt := 0; ; attempt++ {
		pos, _, found, err := c.listEdge(ctx, key, !toLeft)
		if err != nil {
			return err
		}
		next := int64(0)
		if found {
			if toLeft {
				next = pos - 1
			} else {
				next = pos + 1
			}
		}

		applied, err := c.core.runner.MapScanCAS(ctx, c.core.schema.listInsertNX,
			c.listWriteArgs(key, next, payload), map[string]any{})
		if err != nil {
			return err
		}
		if applied {
			return nil
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			return err
		}
	}
}

func (c *Client) pop(ctx context.Context, key string, fromRight bool) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeList); err != nil {
		cmd.err = err
		return cmd
	}

	value, found, err := c.popEdge(ctx, key, fromRight)
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

// popEdge removes the head or tail element and returns it. The delete is
// conditional, so exactly one consumer can claim a given element; a consumer
// that loses the race moves on to the next element.
func (c *Client) popEdge(ctx context.Context, key string, fromRight bool) ([]byte, bool, error) {
	for attempt := 0; ; attempt++ {
		pos, value, found, err := c.listEdge(ctx, key, fromRight)
		if err != nil || !found {
			return nil, false, err
		}

		applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.listDeleteIf, c.listPosArgs(key, pos))
		if err != nil {
			return nil, false, err
		}
		if applied {
			return value, true, nil
		}
		if attempt >= c.core.casRetries {
			return nil, false, nil
		}
	}
}

func (c *Client) blockingListPop(ctx context.Context, timeout time.Duration, fromRight bool, keys ...string) *StringSliceCmd {
	cmd := &StringSliceCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if len(keys) == 0 {
		cmd.err = errors.New("rediscompat: BLPop/BRPop requires at least one key")
		return cmd
	}
	for i := range keys {
		if err := c.checkKeyType(keys[i], typeList); err != nil {
			cmd.err = err
			return cmd
		}
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	for attempt := 0; ; attempt++ {
		for i := range keys {
			value, found, err := c.popEdge(ctx, keys[i], fromRight)
			if err != nil {
				cmd.err = err
				return cmd
			}
			if found {
				cmd.val = []string{keys[i], string(value)}
				return cmd
			}
		}

		wait := backoffFor(c.core.blockPollMin, attempt, c.core.blockPollMax)
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				cmd.err = Nil
				return cmd
			}
			if wait > remaining {
				wait = remaining
			}
		}
		if err := waitWithContext(ctx, wait); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

func (c *Client) listEdge(ctx context.Context, key string, fromRight bool) (int64, []byte, bool, error) {
	stmt := c.core.schema.listEdgeAsc
	if fromRight {
		stmt = c.core.schema.listEdgeDesc
	}

	var (
		pos   int64
		value []byte
	)
	err := c.core.runner.ScanOne(ctx, stmt, c.keyArgs(key), &pos, &value)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return 0, nil, false, nil
		}
		return 0, nil, false, err
	}
	return pos, value, true, nil
}

func (c *Client) listLen(ctx context.Context, key string) (int64, error) {
	var count int64
	if err := c.core.runner.ScanOne(ctx, c.core.schema.listCount, c.keyArgs(key), &count); err != nil {
		if errors.Is(err, errNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return count, nil
}

func (c *Client) listValues(ctx context.Context, key string) ([]string, error) {
	iter := c.core.runner.Iterate(ctx, c.core.schema.listSelectAll, c.keyArgs(key),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		out   []string
		value []byte
	)
	for iter.Scan(&value) {
		out = append(out, string(value))
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}
