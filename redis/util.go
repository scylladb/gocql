package redis

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// runConcurrent applies fn to indexes [0,count) using at most limit workers.
//
// Bulk commands fan out one request per key instead of issuing a multi
// partition IN query: with the token aware policy each request is routed
// straight to the owning shard, which keeps load balanced across the cluster
// instead of concentrating coordinator work on a single node. The worker limit
// bounds the amplification a single large argument list can cause.
func runConcurrent(ctx context.Context, count, limit int, fn func(ctx context.Context, i int) error) error {
	if count <= 0 {
		return nil
	}
	if limit <= 0 {
		limit = 1
	}
	if count == 1 || limit == 1 {
		for i := 0; i < count; i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := fn(ctx, i); err != nil {
				return err
			}
		}
		return nil
	}
	if limit > count {
		limit = count
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		next     atomic.Int64
	)

	record := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		mu.Unlock()
	}

	wg.Add(limit)
	for w := 0; w < limit; w++ {
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= count {
					return
				}
				if err := ctx.Err(); err != nil {
					record(err)
					return
				}
				if err := fn(ctx, i); err != nil {
					record(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	return firstErr
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// backoffFor returns an exponentially growing delay with full jitter applied to
// the lower half of the interval. Jitter matters here because every waiter in a
// BLPop fan-in or every writer retrying a CAS would otherwise wake up in
// lockstep and hammer the same partition.
func backoffFor(base time.Duration, attempt int, max time.Duration) time.Duration {
	if base <= 0 {
		base = 5 * time.Millisecond
	}
	if max <= 0 {
		max = base
	}
	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= max {
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
