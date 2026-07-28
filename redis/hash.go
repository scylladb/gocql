package redis

import (
	"bytes"
	"context"
)

// HSet writes hash fields and returns how many of them were newly created.
//
// The count is exact without one conditional write per field: the fields are
// read with the key's meta row in a single query, and the resulting count is
// applied in one batch guarded by the version that read observed. A concurrent
// writer invalidates the guard, so the loser retries rather than double
// counting.
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
	subs := make([][]byte, len(pairs))
	payloads := make([][]byte, len(pairs))
	for i := range pairs {
		if err := validateElement("hash field", pairs[i].field); err != nil {
			cmd.err = err
			return cmd
		}
		payload, err := c.marshalBounded(pairs[i].value)
		if err != nil {
			cmd.err = err
			return cmd
		}
		subs[i] = []byte(pairs[i].field)
		payloads[i] = payload
	}

	for attempt := 0; ; attempt++ {
		m, existing, found, err := c.resolveCollection(ctx, key, typeHash, subs)
		if err != nil {
			cmd.err = err
			return cmd
		}

		// A repeated field in one call is one field, and a field already
		// holding this value needs no write at all.
		var (
			created int64
			elems   = make([]batchStatement, 0, len(pairs))
			seen    = make(map[string]struct{}, len(pairs))
		)
		for i := range pairs {
			field := pairs[i].field
			if _, repeat := seen[field]; !repeat {
				seen[field] = struct{}{}
				if _, ok := existing[field]; !ok {
					created++
				}
			}
			if current, ok := existing[field]; ok && bytes.Equal(current, payloads[i]) {
				continue
			}
			elems = append(elems, batchStatement{
				stmt: c.core.schema.elemWrite,
				args: c.elemWriteArgs(key, kindField, subs[i], payloads[i]),
			})
		}

		next := m
		next.typ = typeHash
		next.version = nextVersion()
		next.size = m.size + created
		if len(elems) == 0 && found {
			// Every field already holds the requested value.
			cmd.val = 0
			return cmd
		}

		applied, err := c.mutateCollection(ctx, key, typeHash, m, found, next, elems)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			cmd.val = created
			return cmd
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

func (c *Client) HGet(ctx context.Context, key, field string) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	_, existing, found, err := c.resolveCollection(ctx, key, typeHash, [][]byte{[]byte(field)})
	if err != nil {
		cmd.err = err
		return cmd
	}
	value, ok := existing[field]
	if !found || !ok {
		cmd.err = Nil
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

	_, existing, found, err := c.resolveCollection(ctx, key, typeHash, [][]byte{[]byte(field)})
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		return cmd
	}
	_, cmd.val = existing[field]
	return cmd
}

// HDel removes fields and reports how many existed. A hash that loses its last
// field stops existing, exactly as in Redis.
func (c *Client) HDel(ctx context.Context, key string, fields ...string) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	subs := make([][]byte, len(fields))
	for i := range fields {
		subs[i] = []byte(fields[i])
	}

	for attempt := 0; ; attempt++ {
		m, existing, found, err := c.resolveCollection(ctx, key, typeHash, subs)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if !found {
			return cmd
		}

		var (
			removed int64
			elems   = make([]batchStatement, 0, len(fields))
			seen    = make(map[string]struct{}, len(fields))
		)
		for i := range fields {
			if _, ok := existing[fields[i]]; !ok {
				continue
			}
			if _, repeat := seen[fields[i]]; repeat {
				continue
			}
			seen[fields[i]] = struct{}{}
			removed++
			elems = append(elems, batchStatement{
				stmt: c.core.schema.elemDelete,
				args: c.elemDeleteArgs(key, kindField, subs[i]),
			})
		}
		if removed == 0 {
			return cmd
		}

		next := m
		next.version = nextVersion()
		next.size = m.size - removed

		applied, err := c.mutateCollection(ctx, key, typeHash, m, found, next, elems)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			cmd.val = removed
			return cmd
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// HLen returns the number of fields, read from the key's meta row rather than
// by counting rows.
func (c *Client) HLen(ctx context.Context, key string) *IntCmd {
	return c.collectionSize(ctx, key, typeHash)
}

// HGetAll materializes a whole hash in one read of the key's partition, which
// also carries the type: there is no separate lookup to get WRONGTYPE right.
func (c *Client) HGetAll(ctx context.Context, key string) *MapStringStringCmd {
	cmd := &MapStringStringCmd{val: make(map[string]string)}

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
		return cmd
	}
	if err := m.requireType(typeHash); err != nil {
		cmd.err = err
		return cmd
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.kindRead, c.kindReadArgs(key, kindField),
		iterOptions{pageSize: c.core.scanPageSize})

	var sub, value []byte
	for iter.Scan(&sub, &value) {
		if c.core.maxCollection > 0 && len(cmd.val) >= c.core.maxCollection {
			_ = iter.Close()
			cmd.val = nil
			cmd.err = ErrResultTooLarge
			return cmd
		}
		cmd.val[string(sub)] = string(value)
	}
	if err := iter.Close(); err != nil {
		// A mid-stream failure would otherwise hand back a silently truncated
		// map to callers that only check Val.
		cmd.val = nil
		cmd.err = err
	}
	return cmd
}

// collectionSize answers HLen, SCard and LLen from the meta row.
func (c *Client) collectionSize(ctx context.Context, key string, kt keyType) *IntCmd {
	cmd := &IntCmd{}

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
		return cmd
	}
	if err := m.requireType(kt); err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = m.size
	return cmd
}
