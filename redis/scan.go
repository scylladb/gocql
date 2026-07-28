package redis

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"
)

// Keys returns all keys matching pattern.
//
// Keys and their elements share a partition, so enumerating the namespace reads
// a separate index table rather than the key table, which would otherwise return
// one row per element. The index is deliberately a superset: an entry is written
// before the key it names, so a failed write leaves an entry pointing at
// nothing rather than a key no listing can see. Every candidate is therefore
// verified against its meta row, and a stale entry is removed as it is found.
//
// This is a full scan of the key namespace and stays O(n) on the cluster; it is
// meant for debugging, tests and migrations, not for request paths.
func (c *Client) Keys(ctx context.Context, pattern string) *StringSliceCmd {
	cmd := &StringSliceCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.indexScan, c.scanArgs(),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		key       string
		out       []string
		truncated bool
	)
	for iter.Scan(&key) {
		if !globMatch(pattern, key) {
			continue
		}
		out = append(out, key)
		if c.core.maxKeysScan > 0 && len(out) >= c.core.maxKeysScan {
			truncated = true
			break
		}
	}
	if err := iter.Close(); err != nil {
		cmd.err = err
		return cmd
	}
	if truncated {
		cmd.err = fmt.Errorf("rediscompat: Keys matched more than MaxKeysScan (%d) keys, use Scan: %w",
			c.core.maxKeysScan, ErrResultTooLarge)
		return cmd
	}

	out, err := c.keepLive(ctx, out)
	if err != nil {
		cmd.err = err
		return cmd
	}
	cmd.val = out
	return cmd
}

// keepLive drops the candidates that no longer exist and repairs the index for
// them, which is what keeps enumeration exact on top of a best effort index.
// Expired keys are dropped by the same read, since readMeta applies expiry.
func (c *Client) keepLive(ctx context.Context, keys []string) ([]string, error) {
	if len(keys) == 0 {
		return keys, nil
	}

	live := make([]bool, len(keys))
	if err := runConcurrent(ctx, len(keys), c.core.maxConcurrent, func(ctx context.Context, i int) error {
		_, found, err := c.readMeta(ctx, keys[i])
		if err != nil {
			return err
		}
		live[i] = found
		if !found {
			// Self-healing: an entry whose key is gone is deleted the first
			// time enumeration notices, so the index cannot grow without bound.
			_ = c.forgetKey(ctx, keys[i])
		}
		return nil
	}); err != nil {
		return nil, err
	}

	kept := keys[:0]
	for i, key := range keys {
		if live[i] {
			kept = append(kept, key)
		}
	}
	return kept, nil
}

// Scan iterates the key namespace one page at a time.
//
// The cursor is backed by server side paging state, so a page costs one query
// instead of re-scanning and re-sorting the whole namespace, and keys are not
// skipped or duplicated because of concurrent writes the way an offset based
// cursor does. Cursors live in this process: they do not survive a restart and
// cannot be handed to another instance. As in Redis, a page may be empty while
// the returned cursor is still non-zero; iteration is finished when the cursor
// is zero.
func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) *ScanCmd {
	cmd := &ScanCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	if match == "" {
		match = "*"
	}
	if count <= 0 {
		count = 10
	}
	// A caller supplied COUNT is a page size, and a page is materialized by
	// the coordinator before it is sent: without a ceiling one call can ask
	// for the whole namespace in a single response.
	if limit := int64(c.core.maxScanPageSize); count > limit {
		count = limit
	}

	var pageState []byte
	if cursor != 0 {
		// Taking the entry under one lock stops two goroutines from paging the
		// same cursor concurrently; it is put back if this page fails.
		entry, ok := c.core.cursors.take(cursor)
		if !ok {
			cmd.err = ErrCursorUnknown
			return cmd
		}
		// The paging state belongs to one bucket partition and one pattern.
		// Replaying it against a different bucket would page through another
		// tenant's keys, so a mismatched cursor is treated as unknown.
		if entry.bucket != c.bucket {
			c.core.cursors.restore(cursor, entry)
			cmd.err = ErrCursorUnknown
			return cmd
		}
		if entry.pattern != match {
			c.core.cursors.restore(cursor, entry)
			cmd.err = errors.New("rediscompat: Scan MATCH must stay the same for the whole iteration")
			return cmd
		}
		pageState = entry.state
	}

	restore := func() {
		if cursor != 0 {
			// Hand the cursor back so a failed page can be retried instead of
			// forcing the whole iteration to restart at 0.
			c.core.cursors.restore(cursor, &cursorEntry{
				bucket:  c.bucket,
				pattern: match,
				state:   pageState,
				created: c.now(),
			})
			cmd.cursor = cursor
		}
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.indexScan, c.scanArgs(), iterOptions{
		pageSize:   int(count),
		pageState:  pageState,
		singlePage: true,
	})

	var (
		key  string
		page []string
	)
	for iter.Scan(&key) {
		if globMatch(match, key) {
			page = append(page, key)
		}
	}
	next := iter.PageState()
	if err := iter.Close(); err != nil {
		restore()
		cmd.err = err
		return cmd
	}

	page, err := c.keepLive(ctx, page)
	if err != nil {
		restore()
		cmd.err = err
		return cmd
	}

	if len(next) > 0 {
		cmd.cursor = c.core.cursors.save(c.bucket, match, next, c.now())
	}
	cmd.val = page
	return cmd
}

// cursorRegistry hands out opaque integer handles for server paging state so
// the go-redis uint64 cursor API can be preserved. Entries are bounded and
// expire, so an abandoned iteration cannot pin memory.
//
// Handles are random rather than sequential. All bucket views of a client share
// one registry, so a guessable handle would let one tenant pick up another
// tenant's iteration; the bucket recorded on the entry is the actual guard, and
// unguessable handles keep a caller from probing for live cursors at all.
type cursorRegistry struct {
	mu      sync.Mutex
	max     int
	ttl     time.Duration
	entries map[uint64]*cursorEntry
}

type cursorEntry struct {
	bucket  string
	pattern string
	state   []byte
	created time.Time
}

func newCursorRegistry(max int, ttl time.Duration) *cursorRegistry {
	return &cursorRegistry{
		max:     max,
		ttl:     ttl,
		entries: make(map[uint64]*cursorEntry),
	}
}

func (r *cursorRegistry) save(bucket, pattern string, state []byte, now time.Time) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.evictLocked(now)

	id := r.newIDLocked()
	r.entries[id] = &cursorEntry{
		bucket:  bucket,
		pattern: pattern,
		state:   state,
		created: now,
	}
	return id
}

// take removes and returns an entry, so a cursor cannot be paged twice
// concurrently. Callers put it back with restore when the page fails.
func (r *cursorRegistry) take(cursor uint64) (*cursorEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[cursor]
	if !ok {
		return nil, false
	}
	delete(r.entries, cursor)
	if time.Since(entry.created) > r.ttl {
		return nil, false
	}
	return entry, true
}

func (r *cursorRegistry) restore(cursor uint64, entry *cursorEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evictLocked(entry.created)
	r.entries[cursor] = entry
}

func (r *cursorRegistry) evictLocked(now time.Time) {
	for id, entry := range r.entries {
		if now.Sub(entry.created) > r.ttl {
			delete(r.entries, id)
		}
	}
	for len(r.entries) >= r.max {
		oldestID, oldest := uint64(0), time.Time{}
		for id, entry := range r.entries {
			if oldest.IsZero() || entry.created.Before(oldest) {
				oldestID, oldest = id, entry.created
			}
		}
		delete(r.entries, oldestID)
	}
}

func (r *cursorRegistry) newIDLocked() uint64 {
	for {
		// Zero means "iteration finished" in the Redis protocol.
		id := rand.Uint64()
		if id == 0 {
			continue
		}
		if _, taken := r.entries[id]; !taken {
			return id
		}
	}
}

// globMatch implements Redis style glob matching for *, ?, [...] and escapes.
//
// The matcher backtracks iteratively over the last '*' instead of recursing per
// wildcard, so a pattern such as "*a*a*a*b" costs O(len(pattern)*len(key))
// rather than blowing up exponentially and burning CPU on every scanned key.
func globMatch(pattern, s string) bool {
	if pattern == "*" {
		return true
	}

	var (
		p, i         int
		starP, starI = -1, -1
	)

	for i < len(s) {
		if p < len(pattern) {
			switch pattern[p] {
			case '*':
				starP, starI = p, i
				p++
				continue
			case '?':
				p++
				i++
				continue
			case '[':
				ok, next, valid := matchGlobClass(pattern, p, s[i])
				if valid {
					if ok {
						p = next
						i++
						continue
					}
				} else if s[i] == '[' {
					p++
					i++
					continue
				}
			case '\\':
				if p+1 < len(pattern) {
					if pattern[p+1] == s[i] {
						p += 2
						i++
						continue
					}
				} else if s[i] == '\\' {
					p++
					i++
					continue
				}
			default:
				if pattern[p] == s[i] {
					p++
					i++
					continue
				}
			}
		}

		if starP >= 0 {
			starI++
			i = starI
			p = starP + 1
			continue
		}
		return false
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}
	return p == len(pattern)
}

// matchGlobClass evaluates a [...] character class starting at start.
// It reports whether the class matched, the pattern index just past it, and
// whether the class was terminated at all.
func matchGlobClass(pattern string, start int, ch byte) (matched bool, next int, valid bool) {
	end := strings.IndexByte(pattern[start:], ']')
	if end < 0 {
		return false, 0, false
	}
	end += start

	class := pattern[start+1 : end]
	negate := strings.HasPrefix(class, "^")
	if negate {
		class = class[1:]
	}

	for k := 0; k < len(class); k++ {
		if k+2 < len(class) && class[k+1] == '-' {
			if ch >= class[k] && ch <= class[k+2] {
				matched = true
			}
			k += 2
			continue
		}
		if class[k] == ch {
			matched = true
		}
	}
	if negate {
		matched = !matched
	}
	return matched, end + 1, true
}
