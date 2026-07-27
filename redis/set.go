package redis

import (
	"context"
	"errors"
	"sync/atomic"
)

// SAdd adds members and reports how many were new, using conditional inserts
// so concurrent adds of the same member are counted once.
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := validateKey(key); err != nil {
		cmd.err = err
		return cmd
	}

	encoded := make([]string, len(members))
	for i := range members {
		raw, err := marshalValue(members[i])
		if err != nil {
			cmd.err = err
			return cmd
		}
		encoded[i] = string(raw)
	}

	if err := c.ensureKeyType(ctx, key, typeSet); err != nil {
		cmd.err = err
		return cmd
	}

	var added atomic.Int64
	err := runConcurrent(ctx, len(encoded), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		applied, err := c.core.runner.MapScanCAS(ctx, c.core.schema.setInsertNX,
			c.setMemberArgs(key, encoded[i]), map[string]any{})
		if err != nil {
			return err
		}
		if applied {
			added.Add(1)
		}
		return nil
	})
	if err != nil {
		cmd.err = err
		return cmd
	}

	cmd.val = added.Load()
	return cmd
}

func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeSet); err != nil {
		cmd.err = err
		return cmd
	}

	encoded := make([]string, len(members))
	for i := range members {
		raw, err := marshalValue(members[i])
		if err != nil {
			cmd.err = err
			return cmd
		}
		encoded[i] = string(raw)
	}

	var removed atomic.Int64
	err := runConcurrent(ctx, len(encoded), c.core.maxConcurrency, func(ctx context.Context, i int) error {
		applied, err := c.core.runner.ExecCAS(ctx, c.core.schema.setDeleteIf, c.setMemberArgs(key, encoded[i]))
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

func (c *Client) SMembers(ctx context.Context, key string) *StringSliceCmd {
	cmd := &StringSliceCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeSet); err != nil {
		cmd.err = err
		return cmd
	}

	members, err := c.setValues(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = members
	return cmd
}

func (c *Client) setValues(ctx context.Context, key string) ([]string, error) {
	iter := c.core.runner.Iterate(ctx, c.core.schema.setSelectAll, c.keyArgs(key),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		out    []string
		member string
	)
	for iter.Scan(&member) {
		out = append(out, member)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeSet); err != nil {
		cmd.err = err
		return cmd
	}

	raw, err := marshalValue(member)
	if err != nil {
		cmd.err = err
		return cmd
	}

	var found string
	err = c.core.runner.ScanOne(ctx, c.core.schema.setSelect, c.setMemberArgs(key, string(raw)), &found)
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

// SCard counts members on the server instead of streaming every member back to
// the client just to length-check it.
func (c *Client) SCard(ctx context.Context, key string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	if err := c.checkKeyType(key, typeSet); err != nil {
		cmd.err = err
		return cmd
	}

	var count int64
	err := c.core.runner.ScanOne(ctx, c.core.schema.setCount, c.keyArgs(key), &count)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return cmd
		}
		cmd.err = err
		return cmd
	}
	cmd.val = count
	return cmd
}
