package redis

import (
	"context"
	"sync"
	"time"
)

// waiterRegistry is the local half of the blocking pop notification path.
//
// A producer in this process wakes a waiter directly, with no query at all,
// which covers the common case of one service both filling and draining a
// queue. Anything cross process needs the shared wakeup channel below, and
// beyond that the poll in blockingListPop remains the correctness floor: a
// wakeup that never arrives costs latency, never a lost element.
type waiterRegistry struct {
	mu      sync.Mutex
	waiters map[waiterKey]map[*waiter]struct{}
	tailer  bool
	count   int
}

type waiterKey struct {
	bucket string
	key    string
}

type waiter struct {
	ch   chan struct{}
	keys []waiterKey
}

func newWaiterRegistry() *waiterRegistry {
	return &waiterRegistry{waiters: make(map[waiterKey]map[*waiter]struct{})}
}

func (r *waiterRegistry) register(bucket string, keys []string) *waiter {
	w := &waiter{ch: make(chan struct{}, 1), keys: make([]waiterKey, 0, len(keys))}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		wk := waiterKey{bucket, key}
		if r.waiters[wk] == nil {
			r.waiters[wk] = map[*waiter]struct{}{}
		}
		r.waiters[wk][w] = struct{}{}
		w.keys = append(w.keys, wk)
	}
	r.count++
	return w
}

func (r *waiterRegistry) release(w *waiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, wk := range w.keys {
		set := r.waiters[wk]
		delete(set, w)
		if len(set) == 0 {
			delete(r.waiters, wk)
		}
	}
	r.count--
}

// notify wakes every waiter on a key. The channel has room for one pending
// wakeup, so a burst of pushes costs one wake-up rather than one per push: the
// woken consumer drains until the list is empty anyway.
func (r *waiterRegistry) notify(bucket, key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for w := range r.waiters[waiterKey{bucket, key}] {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

func (r *waiterRegistry) idle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count == 0
}

// claimTailer reports whether the caller should run the wakeup tailer, so at
// most one goroutine per client tails the shared channel however many waiters
// there are. That is the property that makes a tight poll interval affordable.
func (r *waiterRegistry) claimTailer() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.tailer {
		return false
	}
	r.tailer = true
	return true
}

func (r *waiterRegistry) releaseTailer() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tailer = false
}

func wakeupSlot(t time.Time) time.Time {
	return t.UTC().Truncate(wakeupSlotSeconds * time.Second)
}

// notifyPush announces that a key received elements. Local waiters are woken
// synchronously; the shared channel write is best effort, because the poll
// covers a lost one and a producer must not fail because a notification did.
func (c *Client) notifyPush(ctx context.Context, key string) {
	c.core.waiters.notify(c.bucket, key)
	if !c.core.wakeupEnabled {
		return
	}
	_ = c.core.runner.Exec(ctx, c.core.schema.wakeupWrite,
		wakeupSlot(c.now()), c.bucket, key, wakeupSlotSeconds*2)
}

// tailWakeups polls the shared wakeup partition while this client has waiters.
//
// It reads the current and previous slot, so a notification written just before
// a slot boundary is still seen, and it dispatches to the local registry. The
// cost is two small reads per interval for the whole client, independent of how
// many keys or waiters there are, which is the entire reason the interval can be
// short.
func (c *Client) tailWakeups() {
	defer c.core.waiters.releaseTailer()

	ticker := time.NewTicker(c.core.wakeupPoll)
	defer ticker.Stop()

	for {
		select {
		case <-c.core.done:
			return
		case <-ticker.C:
		}
		if c.core.waiters.idle() {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), c.core.wakeupPoll+time.Second)
		now := c.now()
		for _, slot := range []time.Time{wakeupSlot(now), wakeupSlot(now.Add(-wakeupSlotSeconds * time.Second))} {
			iter := c.core.runner.Iterate(ctx, c.core.schema.wakeupScan, []any{slot}, iterOptions{})
			var bucket, key string
			for iter.Scan(&bucket, &key) {
				c.core.waiters.notify(bucket, key)
			}
			// A failed tail is not worth reporting: the poll below it is what
			// guarantees delivery.
			_ = iter.Close()
		}
		cancel()
	}
}

func (c *Client) ensureTailer() {
	if !c.core.wakeupEnabled {
		return
	}
	if c.core.waiters.claimTailer() {
		go c.tailWakeups()
	}
}
