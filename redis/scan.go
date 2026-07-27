package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// Keys returns all keys matching pattern.
//
// This is a full scan of the key namespace and stays O(n) on the cluster; it is
// meant for debugging, tests and migrations, not for request paths. Unlike a
// plain value scan it now sees keys of every type, because the kv table holds a
// registry row for hashes, sets and lists as well.
func (c *Client) Keys(ctx context.Context, pattern string) *StringSliceCmd {
	cmd := &StringSliceCmd{}

	if err := c.ensureReady(ctx); err != nil {
		cmd.err = err
		return cmd
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.kvScan, c.kvScanArgs(),
		iterOptions{pageSize: c.core.scanPageSize})

	var (
		key       string
		kind      string
		out       []string
		truncated bool
	)
	for iter.Scan(&key, &kind) {
		if key == guardKey {
			continue
		}
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
		cmd.err = fmt.Errorf("rediscompat: Keys matched more than MaxKeysScan (%d) keys, use Scan", c.core.maxKeysScan)
		return cmd
	}

	cmd.val = out
	return cmd
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
	if count > math.MaxInt32 {
		count = math.MaxInt32
	}

	var pageState []byte
	if cursor != 0 {
		entry, ok := c.core.cursors.load(cursor)
		if !ok {
			cmd.err = ErrCursorUnknown
			return cmd
		}
		if entry.pattern != match {
			cmd.err = errors.New("rediscompat: Scan MATCH must stay the same for the whole iteration")
			return cmd
		}
		pageState = entry.state
		c.core.cursors.drop(cursor)
	}

	iter := c.core.runner.Iterate(ctx, c.core.schema.kvScan, c.kvScanArgs(), iterOptions{
		pageSize:   int(count),
		pageState:  pageState,
		singlePage: true,
	})

	var (
		key  string
		kind string
		page []string
	)
	for iter.Scan(&key, &kind) {
		if key == guardKey {
			continue
		}
		if globMatch(match, key) {
			page = append(page, key)
		}
	}
	next := iter.PageState()
	if err := iter.Close(); err != nil {
		cmd.err = err
		return cmd
	}

	if len(next) > 0 {
		cmd.cursor = c.core.cursors.save(match, next)
	}
	cmd.val = page
	return cmd
}

// cursorRegistry hands out small integer handles for server paging state so the
// go-redis uint64 cursor API can be preserved. Entries are bounded and expire,
// so an abandoned iteration cannot pin memory.
type cursorRegistry struct {
	mu      sync.Mutex
	next    uint64
	max     int
	ttl     time.Duration
	entries map[uint64]*cursorEntry
}

type cursorEntry struct {
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

func (r *cursorRegistry) save(pattern string, state []byte) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
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

	r.next++
	if r.next == 0 {
		// Zero means "iteration finished" in the Redis protocol.
		r.next = 1
	}
	id := r.next
	r.entries[id] = &cursorEntry{pattern: pattern, state: state, created: now}
	return id
}

func (r *cursorRegistry) load(cursor uint64) (*cursorEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[cursor]
	if !ok {
		return nil, false
	}
	if time.Since(entry.created) > r.ttl {
		delete(r.entries, cursor)
		return nil, false
	}
	return entry, true
}

func (r *cursorRegistry) drop(cursor uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.entries, cursor)
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
