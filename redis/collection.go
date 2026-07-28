package redis

import (
	"context"
	"encoding/binary"

	gocql "github.com/gocql/gocql"
)

// mutateCollection applies one collection mutation as a single conditional
// batch: the type assertion, the element writes and the new element count all
// commit together or not at all.
//
// Three shapes cover every command. A key that does not exist yet is created
// with IF NOT EXISTS, which is also the type assertion. A key that exists is
// guarded on its type and version, so a writer that read a stale state is
// rejected rather than allowed to write into another type's rows. A key whose
// last element is leaving has its meta row deleted in the same batch as its
// elements, which is what makes an emptied collection actually stop existing
// instead of lingering as a key with nothing in it.
func (c *Client) mutateCollection(
	ctx context.Context,
	key string,
	kt keyType,
	m keyMeta,
	found bool,
	next keyMeta,
	elems []batchStatement,
) (bool, error) {
	if err := c.checkBatch(len(elems) + 2); err != nil {
		return false, err
	}

	stmts := make([]batchStatement, 0, len(elems)+2)
	drained := found && next.size <= 0
	switch {
	case drained:
		stmts = append(stmts,
			batchStatement{stmt: c.core.schema.metaDeleteCAS, args: c.metaDeleteCASArgs(key, m.version)},
			batchStatement{stmt: c.core.schema.elemsDelete, args: c.keyArgs(key)},
		)
	case !found:
		// The index entry is written first so the failure mode is an entry
		// pointing at nothing, which enumeration repairs, rather than a key no
		// listing can see.
		if err := c.noteKey(ctx, key); err != nil {
			return false, err
		}
		stmts = append(stmts, batchStatement{stmt: c.core.schema.collCreate, args: c.collCreateArgs(key, kt, next)})
		stmts = append(stmts, elems...)
	default:
		stmts = append(stmts, batchStatement{stmt: c.core.schema.collCAS, args: c.collCASArgs(key, kt, next, m.version)})
		stmts = append(stmts, elems...)
	}

	applied, err := c.core.runner.BatchCAS(ctx, gocql.UnloggedBatch, stmts)
	if err != nil || !applied {
		return false, err
	}
	if drained {
		_ = c.forgetKey(ctx, key)
	}
	if !found && !next.expires.IsZero() {
		c.noteExpiry(ctx, key, next.expires)
	}
	return true, nil
}

// resolveCollection reads a key's meta row together with the elements a command
// names, and asserts the type. One query answers "does this key exist", "is it
// the right type" and "which of these elements are already there".
func (c *Client) resolveCollection(ctx context.Context, key string, kt keyType, subs [][]byte) (keyMeta, map[string][]byte, bool, error) {
	m, elems, found, err := c.readPick(ctx, key, kt.kind(), subs)
	if err != nil {
		return keyMeta{}, nil, false, err
	}
	if !found {
		return keyMeta{}, nil, false, nil
	}
	if err := m.requireType(kt); err != nil {
		return keyMeta{}, nil, false, err
	}
	return m, elems, true, nil
}

// elementValues streams one element kind of a key.
func (c *Client) elementValues(ctx context.Context, key string, kind int8, withValue bool) ([]string, error) {
	iter := c.core.runner.Iterate(ctx, c.core.schema.kindRead, c.kindReadArgs(key, kind),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		out   []string
		sub   []byte
		value []byte
	)
	for iter.Scan(&sub, &value) {
		if c.core.maxCollection > 0 && len(out) >= c.core.maxCollection {
			_ = iter.Close()
			return nil, ErrResultTooLarge
		}
		if withValue {
			out = append(out, string(value))
		} else {
			out = append(out, string(sub))
		}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

// encodePos renders a list position as a clustering key that sorts in numeric
// order. Flipping the sign bit maps the signed range onto the unsigned one, so
// bytewise comparison of the blob and numeric comparison of the position agree,
// including across zero.
func encodePos(pos int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(pos)^(1<<63))
	return b[:]
}

func decodePos(b []byte) (int64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(b) ^ (1 << 63)), true
}
