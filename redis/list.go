package redis

import (
	"context"
	"errors"
	"math"
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

// LLen returns the number of elements, read from the key's meta row.
func (c *Client) LLen(ctx context.Context, key string) *IntCmd {
	return c.collectionSize(ctx, key, typeList)
}

// LRange returns the elements between start and stop, inclusive, with negative
// indexes counting back from the end as in Redis.
func (c *Client) LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	cmd := &StringSliceCmd{val: []string{}}

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
	if err := m.requireType(typeList); err != nil {
		cmd.err = err
		return cmd
	}

	from, to := normalizeRange(start, stop, m.size)
	if from > to {
		return cmd
	}

	values, err := c.listValues(ctx, key)
	if err != nil {
		cmd.err = err
		return cmd
	}
	if to >= int64(len(values)) {
		to = int64(len(values)) - 1
	}
	if from > to {
		return cmd
	}
	cmd.val = values[from : to+1]
	return cmd
}

// normalizeRange maps Redis range indexes onto a slice range.
func normalizeRange(start, stop, size int64) (int64, int64) {
	if start < 0 {
		start += size
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += size
	}
	if stop >= size {
		stop = size - 1
	}
	return start, stop
}

// BLPop waits for an element on any of the keys and pops it from the left.
//
// A producer in this process wakes a waiter directly. With
// EnableWakeupChannel a producer in another process does too, through one
// shared partition that this client tails at a fixed cost. Under both there is
// still a poll, because a notification is allowed to be lost and an element is
// not.
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
		payload, err := c.marshalBounded(values[i])
		if err != nil {
			cmd.err = err
			return cmd
		}
		payloads[i] = payload
	}

	for attempt := 0; ; attempt++ {
		m, found, err := c.readMeta(ctx, key)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if found {
			if err := m.requireType(typeList); err != nil {
				cmd.err = err
				return cmd
			}
		} else {
			// An empty list is a list that does not exist, so a cold push
			// starts from the bounds of an empty range.
			m = keyMeta{head: 0, tail: -1}
		}

		positions, head, tail, err := pushPositions(m.head, m.tail, len(payloads), toLeft)
		if err != nil {
			cmd.err = err
			return cmd
		}

		elems := make([]batchStatement, 0, len(payloads))
		for i := range payloads {
			elems = append(elems, batchStatement{
				stmt: c.core.schema.elemWrite,
				args: c.elemWriteArgs(key, kindPos, encodePos(positions[i]), payloads[i]),
			})
		}

		next := m
		next.typ = typeList
		next.version = nextVersion()
		next.size = m.size + int64(len(payloads))
		next.head, next.tail = head, tail

		applied, err := c.mutateCollection(ctx, key, typeList, m, found, next, elems)
		if err != nil {
			cmd.err = err
			return cmd
		}
		if applied {
			c.notifyPush(ctx, key)
			cmd.val = next.size
			return cmd
		}
		if err := c.casRetry(ctx, attempt); err != nil {
			cmd.err = err
			return cmd
		}
	}
}

// pushPositions assigns a position to each pushed value and returns the new
// bounds. The position space is finite, so a list pushed from the same side
// often enough reports exhaustion rather than wrapping around onto itself.
func pushPositions(head, tail int64, n int, toLeft bool) ([]int64, int64, int64, error) {
	positions := make([]int64, n)
	if toLeft {
		if head < math.MinInt64+int64(n) {
			return nil, 0, 0, ErrListPositionExhausted
		}
		for i := 0; i < n; i++ {
			positions[i] = head - 1 - int64(i)
		}
		return positions, head - int64(n), tail, nil
	}
	if tail > math.MaxInt64-int64(n) {
		return nil, 0, 0, ErrListPositionExhausted
	}
	for i := 0; i < n; i++ {
		positions[i] = tail + 1 + int64(i)
	}
	return positions, head, tail + int64(n), nil
}

func (c *Client) pop(ctx context.Context, key string, fromRight bool) *StringCmd {
	cmd := &StringCmd{}

	if err := c.ensureReady(ctx); err != nil {
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

// popEdge removes one end of a list and returns it.
//
// The element removal and the new bounds are one conditional batch guarded by
// the version the read observed, so an element is delivered to exactly one
// consumer and the recorded length can never drift from the rows. Losing every
// race is reported as ErrCASExhausted rather than as an empty list: a caller
// that reads "empty" from a queue that still holds work stops draining it, and
// no error would ever say why.
func (c *Client) popEdge(ctx context.Context, key string, fromRight bool) ([]byte, bool, error) {
	for attempt := 0; ; attempt++ {
		m, pos, value, found, hasElement, err := c.readListEdge(ctx, key, fromRight)
		if err != nil {
			return nil, false, err
		}
		if !found || !hasElement {
			return nil, false, nil
		}

		next := m
		next.version = nextVersion()
		next.size = m.size - 1
		if fromRight {
			next.tail = pos - 1
		} else {
			next.head = pos + 1
		}

		applied, err := c.mutateCollection(ctx, key, typeList, m, true, next, []batchStatement{{
			stmt: c.core.schema.elemDelete,
			args: c.elemDeleteArgs(key, kindPos, encodePos(pos)),
		}})
		if err != nil {
			return nil, false, err
		}
		if applied {
			return value, true, nil
		}
		// Backoff matters more here than in any other retry loop: several
		// consumers guarding on one meta row is the worst case for a
		// lightweight transaction, which already serializes per partition.
		if err := c.casRetry(ctx, attempt); err != nil {
			return nil, false, err
		}
	}
}

// readListEdge returns the meta row and the element at one end.
//
// Ascending, the meta row sorts before every element, so the head element and
// the meta row come back from one bounded slice. The tail needs a descending
// read, which is issued alongside the meta read rather than after it, so either
// direction costs one round trip.
func (c *Client) readListEdge(ctx context.Context, key string, fromRight bool) (
	m keyMeta, pos int64, value []byte, found, hasElement bool, err error,
) {
	if !fromRight {
		iter := c.core.runner.Iterate(ctx, c.core.schema.edgeRead, c.keyArgs(key), iterOptions{})
		for {
			var (
				kind int8
				sub  []byte
				typ  string
				row  keyMeta
			)
			if !iter.Scan(&kind, &sub, &typ, &row.value, &row.version, &row.size, &row.head, &row.tail, &row.expires) {
				break
			}
			if kind == kindMeta {
				row.typ = keyType(typ)
				if row.typ == "" {
					row.typ = typeString
				}
				m, found = row, true
				continue
			}
			if decoded, ok := decodePos(sub); ok {
				pos, value, hasElement = decoded, row.value, true
			}
		}
		if err := iter.Close(); err != nil {
			return keyMeta{}, 0, nil, false, false, err
		}
		if !found {
			return keyMeta{}, 0, nil, false, false, nil
		}
		if m.expired(c.now()) {
			c.dropExpired(ctx, key)
			return keyMeta{}, 0, nil, false, false, nil
		}
		if err := m.requireType(typeList); err != nil {
			return keyMeta{}, 0, nil, false, false, err
		}
		return m, pos, value, true, hasElement, nil
	}

	var (
		meta      keyMeta
		metaFound bool
		lastPos   int64
		lastValue []byte
		lastOK    bool
	)
	if err := runConcurrent(ctx, 2, 2, func(ctx context.Context, i int) error {
		if i == 0 {
			var readErr error
			meta, metaFound, readErr = c.readMeta(ctx, key)
			return readErr
		}
		var sub, value []byte
		readErr := c.core.runner.ScanOne(ctx, c.core.schema.edgeLast, c.keyArgs(key), &sub, &value)
		if readErr != nil {
			if errors.Is(readErr, errNotFound) {
				return nil
			}
			return readErr
		}
		if decoded, ok := decodePos(sub); ok {
			lastPos, lastValue, lastOK = decoded, value, true
		}
		return nil
	}); err != nil {
		return keyMeta{}, 0, nil, false, false, err
	}
	if !metaFound {
		return keyMeta{}, 0, nil, false, false, nil
	}
	if err := meta.requireType(typeList); err != nil {
		return keyMeta{}, 0, nil, false, false, err
	}
	return meta, lastPos, lastValue, true, lastOK, nil
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

	// Registering before the first attempt is what closes the gap between
	// finding the list empty and starting to wait: a push in between wakes this
	// waiter instead of being missed.
	w := c.core.waiters.register(c.bucket, keys)
	defer c.core.waiters.release(w)
	c.ensureTailer()

	var deadline time.Time
	if timeout > 0 {
		deadline = c.now().Add(timeout)
	}

	for attempt := 0; ; attempt++ {
		for i := range keys {
			value, found, err := c.popEdge(ctx, keys[i], fromRight)
			if err != nil {
				// Losing the race for an element is not a failure for a
				// blocking pop: it means another consumer got there first, so
				// keep waiting on the remaining keys instead of giving up.
				if errors.Is(err, ErrCASExhausted) {
					continue
				}
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
			remaining := deadline.Sub(c.now())
			if remaining <= 0 {
				cmd.err = Nil
				return cmd
			}
			if wait > remaining {
				wait = remaining
			}
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			cmd.err = ctx.Err()
			return cmd
		case <-w.ch:
			// A producer said there is something to take; go straight back to
			// the pop instead of sleeping out the interval.
			timer.Stop()
			attempt = -1
		case <-timer.C:
		}
	}
}

func (c *Client) listValues(ctx context.Context, key string) ([]string, error) {
	return c.elementValues(ctx, key, kindPos, true)
}
