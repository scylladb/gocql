package redis

import "context"

// SAdd adds members and reports how many were new. Membership is read together
// with the key's meta row, and the resulting count is applied under that read's
// version, so concurrent adds of the same member are counted once.
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

	encoded, err := encodeMembers(members)
	if err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, existing, found, err := c.resolveCollection(ctx, key, typeSet, encoded)
		if err != nil {
			cmd.err = err
			return cmd
		}

		var (
			added int64
			elems = make([]batchStatement, 0, len(encoded))
			seen  = make(map[string]struct{}, len(encoded))
		)
		for i := range encoded {
			member := string(encoded[i])
			if _, repeat := seen[member]; repeat {
				continue
			}
			seen[member] = struct{}{}
			if _, ok := existing[member]; ok {
				continue
			}
			added++
			elems = append(elems, batchStatement{
				stmt: c.core.schema.elemWrite,
				args: c.elemWriteArgs(key, kindMember, encoded[i], nil),
			})
		}
		if added == 0 && found {
			return cmd
		}

		next := m
		next.typ = typeSet
		next.version = nextVersion()
		next.size = m.size + added

		applied, err := c.mutateCollection(ctx, key, typeSet, m, found, next, elems)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			cmd.val = added
			return cmd
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// SRem removes members and reports how many were there. A set that loses its
// last member stops existing.
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) *IntCmd {
	cmd := &IntCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	encoded, err := encodeMembers(members)
	if err != nil {
		cmd.err = err
		return cmd
	}

	for attempt := 0; ; attempt++ {
		m, existing, found, err := c.resolveCollection(ctx, key, typeSet, encoded)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if !found {
			return cmd
		}

		var (
			removed int64
			elems   = make([]batchStatement, 0, len(encoded))
			seen    = make(map[string]struct{}, len(encoded))
		)
		for i := range encoded {
			member := string(encoded[i])
			if _, ok := existing[member]; !ok {
				continue
			}
			if _, repeat := seen[member]; repeat {
				continue
			}
			seen[member] = struct{}{}
			removed++
			elems = append(elems, batchStatement{
				stmt: c.core.schema.elemDelete,
				args: c.elemDeleteArgs(key, kindMember, encoded[i]),
			})
		}
		if removed == 0 {
			return cmd
		}

		next := m
		next.version = nextVersion()
		next.size = m.size - removed

		applied, err := c.mutateCollection(ctx, key, typeSet, m, found, next, elems)
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

func (c *Client) SMembers(ctx context.Context, key string) *StringSliceCmd {
	cmd := &StringSliceCmd{}

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
	if err := m.requireType(typeSet); err != nil {
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
	return c.elementValues(ctx, key, kindMember, false)
}

func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) *BoolCmd {
	cmd := &BoolCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}
	encoded, err := encodeMembers([]interface{}{member})
	if err != nil {
		cmd.err = err
		return cmd
	}

	_, existing, found, err := c.resolveCollection(ctx, key, typeSet, encoded)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if !found {
		return cmd
	}
	_, cmd.val = existing[string(encoded[0])]
	return cmd
}

// SCard returns the member count from the key's meta row, so it costs one read
// regardless of how large the set is.
func (c *Client) SCard(ctx context.Context, key string) *IntCmd {
	return c.collectionSize(ctx, key, typeSet)
}

// encodeMembers renders set members as the bytes they are stored as. Members
// are a blob clustering column, so unlike the key itself they are binary safe;
// only the length is bounded.
func encodeMembers(members []interface{}) ([][]byte, error) {
	if len(members) == 0 {
		return nil, Error("rediscompat: at least one member is required")
	}
	encoded := make([][]byte, len(members))
	for i := range members {
		raw, err := marshalValue(members[i])
		if err != nil {
			return nil, err
		}
		if err := validateElement("set member", string(raw)); err != nil {
			return nil, err
		}
		encoded[i] = raw
	}
	return encoded, nil
}
