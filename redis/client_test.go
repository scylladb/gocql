package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSetGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	value, err := client.Get(ctx, "greeting").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if value != "hello" {
		t.Fatalf("Get = %q, want %q", value, "hello")
	}
	if !db.indexed("", "greeting") {
		t.Fatal("Set did not record the key in the enumeration index")
	}

	if err := client.Get(ctx, "missing").Err(); err != Nil {
		t.Fatalf("Get(missing) error = %v, want Nil", err)
	}
}

func TestSetKeepTTLPreservesExpiry(t *testing.T) {
	ctx := context.Background()
	client, db, clock := newTestClientWithClock(t, nil)

	if err := client.Set(ctx, "session", "a", 90*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	want, _ := db.metaOf("", "session")

	if err := client.Set(ctx, "session", "b", KeepTTL).Err(); err != nil {
		t.Fatalf("Set KeepTTL: %v", err)
	}
	row, ok := db.metaOf("", "session")
	if !ok {
		t.Fatal("key vanished")
	}
	if !row.expires.Equal(want.expires) {
		t.Fatalf("expires = %v, want %v", row.expires, want.expires)
	}
	mustEqualBytes(t, row.value, "b")

	ttl, err := client.TTL(ctx, "session").Result()
	if err != nil || ttl != 90*time.Second {
		t.Fatalf("TTL = (%v, %v), want 90s", ttl, err)
	}

	// The cell TTL outlives the logical expiry, so expires_at stays the only
	// thing that decides whether the key is gone.
	clock.advance(91 * time.Second)
	if err := client.Get(ctx, "session").Err(); err != Nil {
		t.Fatalf("Get after expiry = %v, want Nil", err)
	}
	if _, ok := db.metaOf("", "session"); ok {
		t.Fatal("reading an expired key did not reclaim it")
	}
}

func TestSetExRejectsNonPositiveExpiration(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.SetEx(ctx, "k", "v", 0).Err(); !errors.Is(err, ErrInvalidExpire) {
		t.Fatalf("SetEx(0) error = %v, want ErrInvalidExpire", err)
	}
	if err := client.SetEx(ctx, "k", "v", -time.Second).Err(); !errors.Is(err, ErrInvalidExpire) {
		t.Fatalf("SetEx(-1s) error = %v, want ErrInvalidExpire", err)
	}
}

func TestSetNXIsAtomicAndNeedsNoRead(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	ok, err := client.SetNX(ctx, "lock", "owner-a", time.Minute).Result()
	if err != nil || !ok {
		t.Fatalf("first SetNX = %v, %v; want true, nil", ok, err)
	}
	ok, err = client.SetNX(ctx, "lock", "owner-b", time.Minute).Result()
	if err != nil || ok {
		t.Fatalf("second SetNX = %v, %v; want false, nil", ok, err)
	}
	row, _ := db.metaOf("", "lock")
	mustEqualBytes(t, row.value, "owner-a")
	if n := db.count(db.sch.metaRead); n != 0 {
		t.Fatalf("SetNX issued %d reads, want 0 (conditional insert only)", n)
	}
}

func TestIncrPreservesExpiryAndDetectsOverflow(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "counter", "1", 30*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before, _ := db.metaOf("", "counter")

	got, err := client.Incr(ctx, "counter").Result()
	if err != nil || got != 2 {
		t.Fatalf("Incr = %d, %v; want 2, nil", got, err)
	}
	after, _ := db.metaOf("", "counter")
	if !after.expires.Equal(before.expires) {
		t.Fatalf("expires = %v, want %v (Incr must not drop the expiry)", after.expires, before.expires)
	}

	if err := client.Set(ctx, "big", strconv.FormatInt(1<<62, 10), 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.IncrBy(ctx, "big", 1<<62).Err(); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("IncrBy overflow error = %v, want ErrIncrOverflow", err)
	}

	if err := client.Set(ctx, "word", "abc", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Incr(ctx, "word").Err(); !errors.Is(err, ErrValueNotInteger) {
		t.Fatalf("Incr(non numeric) error = %v, want ErrValueNotInteger", err)
	}
}

func TestDecrByMinInt64DoesNotWrap(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.DecrBy(ctx, "k", -9223372036854775808).Err(); !errors.Is(err, ErrIncrOverflow) {
		t.Fatalf("DecrBy(MinInt64) error = %v, want ErrIncrOverflow", err)
	}
}

func TestAppendPreservesExpiryAndRespectsTheValueCeiling(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.MaxValueSize = 8 })

	if err := client.Set(ctx, "log", "a", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	before, _ := db.metaOf("", "log")

	length, err := client.Append(ctx, "log", "bc").Result()
	if err != nil || length != 3 {
		t.Fatalf("Append = %d, %v; want 3, nil", length, err)
	}
	after, _ := db.metaOf("", "log")
	if !after.expires.Equal(before.expires) {
		t.Fatal("Append dropped the expiry")
	}

	// The ceiling applies to the result, not only to the argument.
	if err := client.Append(ctx, "log", "defghij").Err(); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("Append over the ceiling error = %v, want ErrValueTooLarge", err)
	}
}

func TestGetSetAndGetDel(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.GetSet(ctx, "k", "first").Err(); err != Nil {
		t.Fatalf("GetSet on a missing key error = %v, want Nil", err)
	}
	previous, err := client.GetSet(ctx, "k", "second").Result()
	if err != nil || previous != "first" {
		t.Fatalf("GetSet = %q, %v; want first, nil", previous, err)
	}

	value, err := client.GetDel(ctx, "k").Result()
	if err != nil || value != "second" {
		t.Fatalf("GetDel = %q, %v; want second, nil", value, err)
	}
	if err := client.Get(ctx, "k").Err(); err != Nil {
		t.Fatalf("Get after GetDel error = %v, want Nil", err)
	}
	if db.indexed("", "k") {
		t.Fatal("GetDel left the key in the enumeration index")
	}
}

func TestWrongTypeIsReportedFromOneRead(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.HSet(ctx, "profile", "name", "ada").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Get(ctx, "profile").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Get on a hash error = %v, want WRONGTYPE", err)
	}
	if err := client.LPush(ctx, "profile", "x").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("LPush on a hash error = %v, want WRONGTYPE", err)
	}

	// The type travels with the data, so a fresh client sees it with no
	// warm-up and no extra lookup.
	fresh := &Client{core: client.core, bucket: client.bucket, root: false}
	if err := fresh.SMembers(ctx, "profile").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("SMembers on a cold client error = %v, want WRONGTYPE", err)
	}
	if _, ok := db.metaOf("", "profile"); !ok {
		t.Fatal("the hash lost its meta row")
	}
}

func TestSetReplacesACollectionAtomically(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.HSet(ctx, "k", "f", "v", "g", "w").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Set(ctx, "k", "plain", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got := db.elementCount("", "k", kindField); got != 0 {
		t.Fatalf("%d hash fields survived the replacement, want 0", got)
	}
	value, err := client.Get(ctx, "k").Result()
	if err != nil || value != "plain" {
		t.Fatalf("Get = %q, %v; want plain, nil", value, err)
	}
	if err := client.HGetAll(ctx, "k").Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("HGetAll after replacement error = %v, want WRONGTYPE", err)
	}
}

func TestDelCountsExactlyAndTakesElementsWithIt(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "s", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "h", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.SAdd(ctx, "m", "a", "b").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.RPush(ctx, "l", "x").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	removed, err := client.Del(ctx, "s", "h", "m", "l", "absent").Result()
	if err != nil || removed != 4 {
		t.Fatalf("Del = %d, %v; want 4, nil", removed, err)
	}
	for _, key := range []string{"s", "h", "m", "l"} {
		if _, ok := db.metaOf("", key); ok {
			t.Fatalf("%s still has a meta row", key)
		}
		for _, kind := range []int8{kindField, kindMember, kindPos} {
			if got := db.elementCount("", key, kind); got != 0 {
				t.Fatalf("%s kept %d elements of kind %d", key, got, kind)
			}
		}
		if db.indexed("", key) {
			t.Fatalf("%s is still in the enumeration index", key)
		}
	}

	// A second Del of the same keys counts nothing, which is what makes the
	// count usable as "how many did I remove".
	removed, err = client.Del(ctx, "s", "h").Result()
	if err != nil || removed != 0 {
		t.Fatalf("second Del = %d, %v; want 0, nil", removed, err)
	}
}

func TestExistsSeesEveryKeyType(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "s", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "h", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.SAdd(ctx, "m", "a").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.LPush(ctx, "l", "x").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}

	found, err := client.Exists(ctx, "s", "h", "m", "l", "absent").Result()
	if err != nil || found != 4 {
		t.Fatalf("Exists = %d, %v; want 4, nil", found, err)
	}
}

func TestDrainedCollectionsStopExisting(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	cases := []struct {
		name  string
		fill  func() error
		drain func() error
	}{
		{
			name:  "hash",
			fill:  func() error { return client.HSet(ctx, "k", "f", "v").Err() },
			drain: func() error { return client.HDel(ctx, "k", "f").Err() },
		},
		{
			name:  "set",
			fill:  func() error { return client.SAdd(ctx, "k", "m").Err() },
			drain: func() error { return client.SRem(ctx, "k", "m").Err() },
		},
		{
			name:  "list",
			fill:  func() error { return client.RPush(ctx, "k", "v").Err() },
			drain: func() error { return client.LPop(ctx, "k").Err() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fill(); err != nil {
				t.Fatalf("fill: %v", err)
			}
			if err := tc.drain(); err != nil {
				t.Fatalf("drain: %v", err)
			}

			if got, err := client.Exists(ctx, "k").Result(); err != nil || got != 0 {
				t.Fatalf("Exists on a drained %s = %d, %v; want 0, nil", tc.name, got, err)
			}
			if _, ok := db.metaOf("", "k"); ok {
				t.Fatalf("a drained %s kept its meta row", tc.name)
			}
			if keys, err := client.Keys(ctx, "*").Result(); err != nil || len(keys) != 0 {
				t.Fatalf("Keys after draining a %s = %v, %v; want empty", tc.name, keys, err)
			}

			// The key is gone, so it may become any other type.
			if err := client.Set(ctx, "k", "now-a-string", 0).Err(); err != nil {
				t.Fatalf("Set after draining a %s: %v", tc.name, err)
			}
			if got, err := client.Get(ctx, "k").Result(); err != nil || got != "now-a-string" {
				t.Fatalf("Get = %q, %v", got, err)
			}
			if _, err := client.Del(ctx, "k").Result(); err != nil {
				t.Fatalf("cleanup Del: %v", err)
			}
		})
	}
}

func TestExpireTTLAndPersistCoverEveryType(t *testing.T) {
	ctx := context.Background()
	client, _, clock := newTestClientWithClock(t, nil)

	if err := client.Set(ctx, "s", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "h", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	if ttl, err := client.TTL(ctx, "s").Result(); err != nil || ttl != -1*time.Second {
		t.Fatalf("TTL of a persistent key = (%v, %v), want -1s", ttl, err)
	}
	if ttl, err := client.TTL(ctx, "absent").Result(); err != nil || ttl != -2*time.Second {
		t.Fatalf("TTL of a missing key = (%v, %v), want -2s", ttl, err)
	}

	for _, key := range []string{"s", "h"} {
		ok, err := client.Expire(ctx, key, time.Minute).Result()
		if err != nil || !ok {
			t.Fatalf("Expire(%s) = %v, %v; want true, nil", key, ok, err)
		}
		if ttl, err := client.TTL(ctx, key).Result(); err != nil || ttl != time.Minute {
			t.Fatalf("TTL(%s) = (%v, %v), want 1m", key, ttl, err)
		}
	}

	ok, err := client.Persist(ctx, "h").Result()
	if err != nil || !ok {
		t.Fatalf("Persist = %v, %v; want true, nil", ok, err)
	}
	if ttl, err := client.TTL(ctx, "h").Result(); err != nil || ttl != -1*time.Second {
		t.Fatalf("TTL after Persist = (%v, %v), want -1s", ttl, err)
	}
	if ok, err := client.Persist(ctx, "h").Result(); err != nil || ok {
		t.Fatalf("second Persist = %v, %v; want false, nil", ok, err)
	}

	// An expired collection reads as absent, whatever is still on disk.
	clock.advance(2 * time.Minute)
	if got, err := client.Exists(ctx, "s").Result(); err != nil || got != 0 {
		t.Fatalf("Exists on an expired string = %d, %v; want 0, nil", got, err)
	}
	if err := client.Expire(ctx, "absent", time.Minute).Err(); err != nil {
		t.Fatalf("Expire on a missing key: %v", err)
	}
	if ok, _ := client.Expire(ctx, "absent", time.Minute).Result(); ok {
		t.Fatal("Expire on a missing key reported true")
	}
}

func TestExpireWithNonPositiveDurationDeletes(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.SAdd(ctx, "k", "a").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	ok, err := client.Expire(ctx, "k", -time.Second).Result()
	if err != nil || !ok {
		t.Fatalf("Expire(-1s) = %v, %v; want true, nil", ok, err)
	}
	if got, err := client.Exists(ctx, "k").Result(); err != nil || got != 0 {
		t.Fatalf("Exists = %d, %v; want 0, nil", got, err)
	}
}

func TestSweepReclaimsExpiredKeys(t *testing.T) {
	ctx := context.Background()
	client, db, clock := newTestClientWithClock(t, nil)

	if err := client.Set(ctx, "temp", "v", 30*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "hash", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Expire(ctx, "hash", 30*time.Second).Err(); err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if len(db.expiry) != 2 {
		t.Fatalf("%d expiry index entries, want 2", len(db.expiry))
	}

	clock.advance(time.Minute)
	reclaimed, err := client.Sweep(ctx).Result()
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if reclaimed != 2 {
		t.Fatalf("Sweep reclaimed %d, want 2", reclaimed)
	}
	if _, ok := db.metaOf("", "hash"); ok {
		t.Fatal("Sweep left the expired hash behind")
	}
	if db.elementCount("", "hash", kindField) != 0 {
		t.Fatal("Sweep left the expired hash elements behind")
	}
	if len(db.expiry) != 0 {
		t.Fatalf("%d expiry index entries survived the sweep, want 0", len(db.expiry))
	}
}

func TestSweepKeepsAKeyWhoseExpiryMovedOut(t *testing.T) {
	ctx := context.Background()
	client, db, clock := newTestClientWithClock(t, nil)

	if err := client.Set(ctx, "k", "v", 30*time.Second).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Expire(ctx, "k", 2*time.Hour).Err(); err != nil {
		t.Fatalf("Expire: %v", err)
	}

	clock.advance(time.Minute)
	if _, err := client.Sweep(ctx).Result(); err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if _, ok := db.metaOf("", "k"); !ok {
		t.Fatal("Sweep removed a key whose expiry had been extended")
	}
}

func TestRenameSameKeyIsNoOp(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Rename(ctx, "missing", "missing").Err(); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("Rename(missing) error = %v, want ErrNoSuchKey", err)
	}
	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Rename(ctx, "k", "k").Err(); err != nil {
		t.Fatalf("Rename onto itself: %v", err)
	}
	if got, err := client.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Fatalf("Get after self rename = %q, %v; want v, nil", got, err)
	}
}

func TestRenamePreservesExpiryAndRemovesTheSource(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "old", "v", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	source, _ := db.metaOf("", "old")

	if err := client.Rename(ctx, "old", "new").Err(); err != nil {
		t.Fatalf("Rename: %v", err)
	}
	if err := client.Get(ctx, "old").Err(); err != Nil {
		t.Fatalf("source survived the rename: %v", err)
	}
	row, ok := db.metaOf("", "new")
	if !ok {
		t.Fatal("destination missing")
	}
	if !row.expires.Equal(source.expires) {
		t.Fatalf("expires = %v, want %v", row.expires, source.expires)
	}
	if !db.indexed("", "new") || db.indexed("", "old") {
		t.Fatal("the enumeration index did not follow the rename")
	}
}

func TestRenameRollsBackItsOwnWrite(t *testing.T) {
	ctx := context.Background()

	t.Run("empty destination is cleaned up", func(t *testing.T) {
		client, db := newTestClient(t, func(o *Options) { o.CASMaxRetries = 1 })
		if err := client.Set(ctx, "src", "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
		// Every attempt to give up the source loses, as if another writer kept
		// changing it.
		db.interfere = func(db *fakeDB, stmt string, args []any) {
			if stmt != db.sch.metaDeleteCAS {
				return
			}
			if row, ok := db.row(db.meta("", "src")); ok {
				row.version = nextVersion()
			}
		}

		if err := client.Rename(ctx, "src", "dst").Err(); !errors.Is(err, ErrCASExhausted) {
			t.Fatalf("Rename error = %v, want ErrCASExhausted", err)
		}
		db.interfere = nil
		if _, ok := db.metaOf("", "dst"); ok {
			t.Fatal("a failed rename left the value under the destination as well")
		}
		if got, err := client.Get(ctx, "src").Result(); err != nil || got != "v" {
			t.Fatalf("source = %q, %v; want v, nil", got, err)
		}
	})

	t.Run("occupied destination is left in place", func(t *testing.T) {
		client, db := newTestClient(t, func(o *Options) { o.CASMaxRetries = 1 })
		if err := client.Set(ctx, "src", "v", 0).Err(); err != nil {
			t.Fatalf("Set src: %v", err)
		}
		if err := client.Set(ctx, "dst", "existing", 0).Err(); err != nil {
			t.Fatalf("Set dst: %v", err)
		}
		db.interfere = func(db *fakeDB, stmt string, args []any) {
			if stmt != db.sch.metaDeleteCAS {
				return
			}
			if row, ok := db.row(db.meta("", "src")); ok {
				row.version = nextVersion()
			}
		}

		if err := client.Rename(ctx, "src", "dst").Err(); !errors.Is(err, ErrCASExhausted) {
			t.Fatalf("Rename error = %v, want ErrCASExhausted", err)
		}
		db.interfere = nil
		// A destination that existed beforehand is never deleted by a failed
		// rename, because that would destroy data the caller never named.
		if _, ok := db.metaOf("", "dst"); !ok {
			t.Fatal("a failed rename deleted a destination that existed beforehand")
		}
	})
}

func TestRenameIsAtomicWithinABucket(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.Set(ctx, "old", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "new", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.Rename(ctx, "old", "new").Err(); err != nil {
		t.Fatalf("Rename: %v", err)
	}

	if got, err := client.Get(ctx, "new").Result(); err != nil || got != "v" {
		t.Fatalf("Get = %q, %v; want v, nil", got, err)
	}
	if db.elementCount("default", "new", kindField) != 0 {
		t.Fatal("the destination kept the hash fields it held before the rename")
	}
	if _, ok := db.metaOf("default", "old"); ok {
		t.Fatal("the source survived an atomic rename")
	}
}

func TestCopySemantics(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if got, err := client.Copy(ctx, "missing", "dst", 0, false).Result(); err != nil || got != 0 {
		t.Fatalf("Copy(missing) = %d, %v; want 0, nil", got, err)
	}
	if err := client.Set(ctx, "src", "v", time.Minute).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if got, err := client.Copy(ctx, "src", "dst", 0, false).Result(); err != nil || got != 1 {
		t.Fatalf("Copy = %d, %v; want 1, nil", got, err)
	}
	source, _ := db.metaOf("", "src")
	dest, _ := db.metaOf("", "dst")
	if !dest.expires.Equal(source.expires) {
		t.Fatal("Copy did not carry the expiry")
	}

	if got, err := client.Copy(ctx, "src", "dst", 0, false).Result(); err != nil || got != 0 {
		t.Fatalf("Copy onto an occupied destination = %d, %v; want 0, nil", got, err)
	}
	if got, err := client.Copy(ctx, "src", "dst", 0, true).Result(); err != nil || got != 1 {
		t.Fatalf("Copy with replace = %d, %v; want 1, nil", got, err)
	}

	// There is one namespace per client, so a non-zero DB cannot be honoured
	// and must not be silently ignored.
	if err := client.Copy(ctx, "src", "dst", 3, true).Err(); err == nil {
		t.Fatal("Copy into another DB was accepted")
	}
}

func TestCopyWithoutReplaceLeavesTheDestinationIntact(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "src", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "dst", "f", "keep").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	if got, err := client.Copy(ctx, "src", "dst", 0, false).Result(); err != nil || got != 0 {
		t.Fatalf("Copy = %d, %v; want 0, nil", got, err)
	}
	if got := db.elementCount("", "dst", kindField); got != 1 {
		t.Fatalf("the destination hash lost %d fields to a refused Copy", 1-got)
	}
	if value, err := client.HGet(ctx, "dst", "f").Result(); err != nil || value != "keep" {
		t.Fatalf("HGet = %q, %v; want keep, nil", value, err)
	}
}

func TestTypeReportsTheStoredType(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if got, _ := client.Type(ctx, "absent").Result(); got != "none" {
		t.Fatalf("Type(absent) = %q, want none", got)
	}
	if err := client.SAdd(ctx, "k", "a").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if got, err := client.Type(ctx, "k").Result(); err != nil || got != "set" {
		t.Fatalf("Type = %q, %v; want set, nil", got, err)
	}
}

func TestHashCountsAndRoundTrip(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	created, err := client.HSet(ctx, "h", "a", "1", "b", "2").Result()
	if err != nil || created != 2 {
		t.Fatalf("HSet = %d, %v; want 2, nil", created, err)
	}
	// Overwriting a field is not a creation, and a repeated field is one field.
	created, err = client.HSet(ctx, "h", "a", "9", "c", "3", "c", "4").Result()
	if err != nil || created != 1 {
		t.Fatalf("HSet = %d, %v; want 1, nil", created, err)
	}

	if got, err := client.HLen(ctx, "h").Result(); err != nil || got != 3 {
		t.Fatalf("HLen = %d, %v; want 3, nil", got, err)
	}
	all, err := client.HGetAll(ctx, "h").Result()
	if err != nil {
		t.Fatalf("HGetAll: %v", err)
	}
	want := map[string]string{"a": "9", "b": "2", "c": "4"}
	if fmt.Sprint(all) != fmt.Sprint(want) {
		t.Fatalf("HGetAll = %v, want %v", all, want)
	}

	if ok, err := client.HExists(ctx, "h", "b").Result(); err != nil || !ok {
		t.Fatalf("HExists = %v, %v; want true, nil", ok, err)
	}
	if ok, err := client.HExists(ctx, "h", "zz").Result(); err != nil || ok {
		t.Fatalf("HExists(absent) = %v, %v; want false, nil", ok, err)
	}
	if err := client.HGet(ctx, "h", "zz").Err(); err != Nil {
		t.Fatalf("HGet(absent) error = %v, want Nil", err)
	}

	removed, err := client.HDel(ctx, "h", "a", "zz", "b").Result()
	if err != nil || removed != 2 {
		t.Fatalf("HDel = %d, %v; want 2, nil", removed, err)
	}
	if got, err := client.HLen(ctx, "h").Result(); err != nil || got != 1 {
		t.Fatalf("HLen after HDel = %d, %v; want 1, nil", got, err)
	}
}

func TestHashWriteCostsOneReadAndOneBatch(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.HSet(ctx, "h", "a", "1", "b", "2", "c", "3").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	// One multi-column read for the meta row and the three fields, one batch.
	// The count must not grow with the number of fields.
	if got := db.count("(kind, sub) IN"); got != 1 {
		t.Fatalf("HSet issued %d element reads, want 1", got)
	}
	if got := db.count("BATCH_CAS"); got != 1 {
		t.Fatalf("HSet issued %d conditional batches, want 1", got)
	}
}

func TestSetCommandsAndExactCardinality(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	added, err := client.SAdd(ctx, "s", "a", "b", "a").Result()
	if err != nil || added != 2 {
		t.Fatalf("SAdd = %d, %v; want 2, nil", added, err)
	}
	added, err = client.SAdd(ctx, "s", "b", "c").Result()
	if err != nil || added != 1 {
		t.Fatalf("SAdd = %d, %v; want 1, nil", added, err)
	}

	if got, err := client.SCard(ctx, "s").Result(); err != nil || got != 3 {
		t.Fatalf("SCard = %d, %v; want 3, nil", got, err)
	}
	// The count comes from the key's meta row, so it does not read the members.
	if got := db.count(db.sch.kindRead); got != 0 {
		t.Fatalf("SCard read the members %d times, want 0", got)
	}

	members, err := client.SMembers(ctx, "s").Result()
	if err != nil || strings.Join(members, ",") != "a,b,c" {
		t.Fatalf("SMembers = %v, %v; want [a b c], nil", members, err)
	}
	if ok, err := client.SIsMember(ctx, "s", "b").Result(); err != nil || !ok {
		t.Fatalf("SIsMember = %v, %v; want true, nil", ok, err)
	}
	if ok, err := client.SIsMember(ctx, "s", "zz").Result(); err != nil || ok {
		t.Fatalf("SIsMember(absent) = %v, %v; want false, nil", ok, err)
	}

	removed, err := client.SRem(ctx, "s", "a", "zz").Result()
	if err != nil || removed != 1 {
		t.Fatalf("SRem = %d, %v; want 1, nil", removed, err)
	}
	if got, err := client.SCard(ctx, "s").Result(); err != nil || got != 2 {
		t.Fatalf("SCard after SRem = %d, %v; want 2, nil", got, err)
	}
}

func TestListPushPopOrderingAndLength(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	length, err := client.RPush(ctx, "q", "b", "c").Result()
	if err != nil || length != 2 {
		t.Fatalf("RPush = %d, %v; want 2, nil", length, err)
	}
	length, err = client.LPush(ctx, "q", "a").Result()
	if err != nil || length != 3 {
		t.Fatalf("LPush = %d, %v; want 3, nil", length, err)
	}

	if got, err := client.LLen(ctx, "q").Result(); err != nil || got != 3 {
		t.Fatalf("LLen = %d, %v; want 3, nil", got, err)
	}
	if got := db.count(db.sch.kindRead); got != 0 {
		t.Fatalf("LLen read the elements %d times, want 0", got)
	}
	values, err := client.LRange(ctx, "q", 0, -1).Result()
	if err != nil || strings.Join(values, ",") != "a,b,c" {
		t.Fatalf("LRange = %v, %v; want [a b c], nil", values, err)
	}

	if got, err := client.LPop(ctx, "q").Result(); err != nil || got != "a" {
		t.Fatalf("LPop = %q, %v; want a, nil", got, err)
	}
	if got, err := client.RPop(ctx, "q").Result(); err != nil || got != "c" {
		t.Fatalf("RPop = %q, %v; want c, nil", got, err)
	}
	if got, err := client.LPop(ctx, "q").Result(); err != nil || got != "b" {
		t.Fatalf("LPop = %q, %v; want b, nil", got, err)
	}
	if err := client.LPop(ctx, "q").Err(); err != Nil {
		t.Fatalf("LPop on an empty list error = %v, want Nil", err)
	}
	if got, err := client.LLen(ctx, "q").Result(); err != nil || got != 0 {
		t.Fatalf("LLen on a drained list = %d, %v; want 0, nil", got, err)
	}
}

func TestLPushOrderMatchesRedis(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	// LPUSH k a b c leaves the list as c, b, a.
	if err := client.LPush(ctx, "k", "a", "b", "c").Err(); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	values, err := client.LRange(ctx, "k", 0, -1).Result()
	if err != nil || strings.Join(values, ",") != "c,b,a" {
		t.Fatalf("LRange = %v, %v; want [c b a], nil", values, err)
	}
}

func TestLRangeHandlesNegativeIndexes(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.RPush(ctx, "k", "a", "b", "c", "d").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	cases := []struct {
		start, stop int64
		want        string
	}{
		{0, -1, "a,b,c,d"},
		{1, 2, "b,c"},
		{-2, -1, "c,d"},
		{2, 99, "c,d"},
		{3, 1, ""},
		{-99, 0, "a"},
	}
	for _, tc := range cases {
		got, err := client.LRange(ctx, "k", tc.start, tc.stop).Result()
		if err != nil {
			t.Fatalf("LRange(%d,%d): %v", tc.start, tc.stop, err)
		}
		if strings.Join(got, ",") != tc.want {
			t.Fatalf("LRange(%d,%d) = %v, want %q", tc.start, tc.stop, got, tc.want)
		}
	}
}

func TestPushRejectsExhaustedPositionSpace(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.RPush(ctx, "k", "v").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	db.mu.Lock()
	row := db.rows[db.meta("", "k")]
	row.head = 9223372036854775807
	row.tail = 9223372036854775807
	db.mu.Unlock()

	if err := client.RPush(ctx, "k", "overflow").Err(); !errors.Is(err, ErrListPositionExhausted) {
		t.Fatalf("RPush at the top of the range error = %v, want ErrListPositionExhausted", err)
	}

	db.mu.Lock()
	row.head = -9223372036854775808
	row.tail = -9223372036854775808
	db.mu.Unlock()
	if err := client.LPush(ctx, "k", "overflow").Err(); !errors.Is(err, ErrListPositionExhausted) {
		t.Fatalf("LPush at the bottom of the range error = %v, want ErrListPositionExhausted", err)
	}
}

func TestPopDeliversAnElementExactlyOnce(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.CASMaxRetries = 64 })

	const jobs = 40
	values := make([]interface{}, jobs)
	for i := range values {
		values[i] = fmt.Sprintf("job-%d", i)
	}
	if err := client.RPush(ctx, "q", values...).Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	var (
		mu    sync.Mutex
		seen  = map[string]int{}
		wg    sync.WaitGroup
		total int
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				value, err := client.LPop(ctx, "q").Result()
				if err == Nil {
					return
				}
				if errors.Is(err, ErrCASExhausted) {
					continue
				}
				if err != nil {
					t.Errorf("LPop: %v", err)
					return
				}
				mu.Lock()
				seen[value]++
				total++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if total != jobs {
		t.Fatalf("popped %d elements, want %d", total, jobs)
	}
	for value, n := range seen {
		if n != 1 {
			t.Fatalf("%q was delivered %d times", value, n)
		}
	}
}

func TestPopReportsContentionRatherThanEmpty(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.CASMaxRetries = 2 })

	if err := client.RPush(ctx, "q", "a", "b").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	// Every guard loses, as if another consumer always got there first.
	db.interfere = func(db *fakeDB, stmt string, args []any) {
		if stmt != db.sch.collCAS && stmt != db.sch.metaDeleteCAS {
			return
		}
		if row, ok := db.row(db.meta("", "q")); ok {
			row.version = nextVersion()
		}
	}

	err := client.LPop(ctx, "q").Err()
	if !errors.Is(err, ErrCASExhausted) {
		t.Fatalf("LPop under permanent contention error = %v, want ErrCASExhausted", err)
	}
	if err == Nil {
		t.Fatal("a contended pop reported an empty list, which would stop a consumer draining it")
	}
}

func TestBlockingPopTimesOutWithBoundedPolling(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) {
		o.BlockingPollInterval = time.Millisecond
		o.BlockingPollMaxInterval = 2 * time.Millisecond
	})

	start := time.Now()
	err := client.BLPop(ctx, 30*time.Millisecond, "q").Err()
	elapsed := time.Since(start)
	if err != Nil {
		t.Fatalf("BLPop error = %v, want Nil", err)
	}
	if elapsed < 30*time.Millisecond {
		t.Fatalf("BLPop returned after %s, want at least the timeout", elapsed)
	}
	if polls := db.count(db.sch.edgeRead); polls == 0 {
		t.Fatal("BLPop never polled")
	}
}

func TestBlockingPopWakesOnALocalPush(t *testing.T) {
	ctx := context.Background()
	// The poll interval is longer than the test would tolerate, so a prompt
	// return can only come from the wakeup.
	client, _ := newTestClient(t, func(o *Options) {
		o.BlockingPollInterval = 30 * time.Second
		o.BlockingPollMaxInterval = 30 * time.Second
	})

	type result struct {
		values []string
		err    error
	}
	done := make(chan result, 1)
	go func() {
		values, err := client.BLPop(ctx, 10*time.Second, "q").Result()
		done <- result{values, err}
	}()

	// Give the waiter time to register before the push.
	time.Sleep(20 * time.Millisecond)
	if err := client.RPush(ctx, "q", "payload").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("BLPop: %v", got.err)
		}
		if strings.Join(got.values, ",") != "q,payload" {
			t.Fatalf("BLPop = %v, want [q payload]", got.values)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BLPop did not wake on a push from the same process")
	}
}

func TestBlockingPopKeepsWaitingWhenContended(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) {
		o.CASMaxRetries = 1
		o.BlockingPollInterval = time.Millisecond
		o.BlockingPollMaxInterval = time.Millisecond
	})

	if err := client.RPush(ctx, "q", "a").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	db.interfere = func(db *fakeDB, stmt string, args []any) {
		if stmt != db.sch.collCAS && stmt != db.sch.metaDeleteCAS {
			return
		}
		if row, ok := db.row(db.meta("", "q")); ok {
			row.version = nextVersion()
		}
	}

	// Losing the race is not a failure for a blocking pop: it means someone
	// else got the element, so it keeps waiting and then times out.
	if err := client.BLPop(ctx, 20*time.Millisecond, "q").Err(); err != Nil {
		t.Fatalf("BLPop error = %v, want Nil", err)
	}
}

func TestBlockingPopRespectsContextCancellation(t *testing.T) {
	client, _ := newTestClient(t, func(o *Options) {
		o.BlockingPollInterval = 10 * time.Millisecond
	})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if err := client.BLPop(ctx, 0, "q").Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("BLPop error = %v, want context.Canceled", err)
	}
}

func TestKeysAndScanEnumerateEveryType(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "s:1", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "h:1", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.SAdd(ctx, "m:1", "a").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.RPush(ctx, "l:1", "x").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}

	keys, err := client.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	if len(keys) != 4 {
		t.Fatalf("Keys = %v, want four keys", keys)
	}
	matched, err := client.Keys(ctx, "h:*").Result()
	if err != nil || strings.Join(matched, ",") != "h:1" {
		t.Fatalf("Keys(h:*) = %v, %v; want [h:1], nil", matched, err)
	}
}

func TestEnumerationRepairsAStaleIndexEntry(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.Set(ctx, "real", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	// An index entry can outlive its key: it is written first, so a failed
	// write or a crash leaves one behind.
	db.mu.Lock()
	db.index[indexKey{"", "ghost"}] = struct{}{}
	db.mu.Unlock()

	keys, err := client.Keys(ctx, "*").Result()
	if err != nil || strings.Join(keys, ",") != "real" {
		t.Fatalf("Keys = %v, %v; want [real], nil", keys, err)
	}
	if db.indexed("", "ghost") {
		t.Fatal("enumeration did not repair the stale index entry")
	}
}

func TestScanPagesWithAServerCursor(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	for i := 0; i < 7; i++ {
		if err := client.Set(ctx, fmt.Sprintf("k%02d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	var (
		seen   []string
		cursor uint64
	)
	for pages := 0; ; pages++ {
		if pages > 10 {
			t.Fatal("Scan did not finish")
		}
		page, next, err := client.Scan(ctx, cursor, "k*", 3).Result()
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		seen = append(seen, page...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	if len(seen) != 7 {
		t.Fatalf("Scan returned %d keys, want 7", len(seen))
	}
	if err := client.Scan(ctx, 12345, "k*", 3).Err(); !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("Scan with an unknown cursor error = %v, want ErrCursorUnknown", err)
	}
}

func TestScanCursorIsBoundToItsBucket(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.PartitionByBucket = true })

	tenantA := client.Bucketed("a")
	tenantB := client.Bucketed("b")
	for i := 0; i < 4; i++ {
		if err := tenantA.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}

	_, cursor, err := tenantA.Scan(ctx, 0, "*", 2).Result()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if cursor == 0 {
		t.Fatal("expected more pages")
	}
	if err := tenantB.Scan(ctx, cursor, "*", 2).Err(); !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("another bucket accepted the cursor: %v", err)
	}
	// The rejected cursor is still usable by its owner.
	if _, _, err := tenantA.Scan(ctx, cursor, "*", 2).Result(); err != nil {
		t.Fatalf("owner could not continue: %v", err)
	}
}

func TestScanRequiresAStablePattern(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	for i := 0; i < 4; i++ {
		if err := client.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	_, cursor, err := client.Scan(ctx, 0, "*", 2).Result()
	if err != nil || cursor == 0 {
		t.Fatalf("Scan = %v, %v", cursor, err)
	}
	if err := client.Scan(ctx, cursor, "k*", 2).Err(); err == nil {
		t.Fatal("Scan accepted a different MATCH mid-iteration")
	}
}

func TestScanCountIsCapped(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.MaxScanPageSize = 2 })

	for i := 0; i < 5; i++ {
		if err := client.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err(); err != nil {
			t.Fatalf("Set: %v", err)
		}
	}
	page, cursor, err := client.Scan(ctx, 0, "*", 1000).Result()
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(page) > 2 {
		t.Fatalf("Scan returned %d keys for a capped COUNT, want at most 2", len(page))
	}
	if cursor == 0 {
		t.Fatal("a capped page should leave a cursor behind")
	}
}

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern string
		key     string
		want    bool
	}{
		{"*", "anything", true},
		{"user:*", "user:1", true},
		{"user:*", "order:1", false},
		{"user:?", "user:1", true},
		{"user:?", "user:12", false},
		{"[ab]c", "ac", true},
		{"[ab]c", "cc", false},
		{"[^a]c", "bc", true},
		{"[a-c]x", "bx", true},
		{`\*`, "*", true},
		{"*a*a*a*b", strings.Repeat("a", 40), false},
	}
	for _, tc := range cases {
		if got := globMatch(tc.pattern, tc.key); got != tc.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", tc.pattern, tc.key, got, tc.want)
		}
	}
}

func TestSortDefaultsToNoLimit(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.RPush(ctx, "l", "3", "1", "2").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	values, err := client.Sort(ctx, "l", &Sort{}).Result()
	if err != nil || strings.Join(values, ",") != "1,2,3" {
		t.Fatalf("Sort = %v, %v; want [1 2 3], nil", values, err)
	}
	values, err = client.Sort(ctx, "l", &Sort{Order: "DESC"}).Result()
	if err != nil || strings.Join(values, ",") != "3,2,1" {
		t.Fatalf("Sort DESC = %v, %v", values, err)
	}
	values, err = client.Sort(ctx, "l", &Sort{Offset: 1, Count: 1}).Result()
	if err != nil || strings.Join(values, ",") != "2" {
		t.Fatalf("Sort limited = %v, %v", values, err)
	}
	if err := client.Sort(ctx, "l", &Sort{Get: []string{"x"}}).Err(); err == nil {
		t.Fatal("Sort accepted an unsupported GET pattern")
	}
}

func TestSortUsesTheStoredKeyType(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.SAdd(ctx, "k", "2", "1").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	values, err := client.Sort(ctx, "k", &Sort{}).Result()
	if err != nil || strings.Join(values, ",") != "1,2" {
		t.Fatalf("Sort on a set = %v, %v", values, err)
	}
	if err := client.Set(ctx, "str", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Sort(ctx, "str", &Sort{}).Err(); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Sort on a string error = %v, want WRONGTYPE", err)
	}
}

func TestMSetIsOneBatchAndReplacesCollections(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.HSet(ctx, "b", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}
	if err := client.MSet(ctx, "a", "1", "b", "2").Err(); err != nil {
		t.Fatalf("MSet: %v", err)
	}
	if got := db.count("BATCH"); got == 0 {
		t.Fatal("MSet did not use a batch")
	}
	if db.elementCount("", "b", kindField) != 0 {
		t.Fatal("MSet left the hash fields of a replaced key behind")
	}
	for key, want := range map[string]string{"a": "1", "b": "2"} {
		if got, err := client.Get(ctx, key).Result(); err != nil || got != want {
			t.Fatalf("Get(%s) = %q, %v; want %q", key, got, err, want)
		}
	}
}

func TestMSetRefusesAnOversizedBatch(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.MaxBatchStatements = 4 })

	pairs := make([]interface{}, 0, 12)
	for i := 0; i < 6; i++ {
		pairs = append(pairs, fmt.Sprintf("k%d", i), "v")
	}
	err := client.MSet(ctx, pairs...).Err()
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("MSet error = %v, want ErrBatchTooLarge", err)
	}
	// Refused, not partially applied: atomicity must not depend on how many
	// arguments the caller passed.
	if got, err := client.Exists(ctx, "k0").Result(); err != nil || got != 0 {
		t.Fatalf("Exists = %d, %v; want 0, nil", got, err)
	}
}

func TestMGetSkipsWrongTypeKeys(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "a", "1", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.HSet(ctx, "b", "f", "v").Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	values, err := client.MGet(ctx, "a", "b", "missing").Result()
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if values[0] != "1" || values[1] != nil || values[2] != nil {
		t.Fatalf("MGet = %v, want [1 <nil> <nil>]", values)
	}
}

func TestTransactionAppliesEveryQueuedCommand(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.Set(ctx, "from", "100", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Set(ctx, "to", "1", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.SAdd(ctx, "audit", "x").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}

	tx := client.Watch(ctx, "from", "to")
	tx.IncrBy("from", -10).IncrBy("to", 10).Del("audit").Set("note", "moved")
	if err := tx.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}

	if got, err := client.Get(ctx, "from").Result(); err != nil || got != "90" {
		t.Fatalf("from = %q, %v; want 90", got, err)
	}
	if got, err := client.Get(ctx, "to").Result(); err != nil || got != "11" {
		t.Fatalf("to = %q, %v; want 11", got, err)
	}
	if got, err := client.Exists(ctx, "audit").Result(); err != nil || got != 0 {
		t.Fatalf("audit = %d, %v; want 0", got, err)
	}
	if got, err := client.Get(ctx, "note").Result(); err != nil || got != "moved" {
		t.Fatalf("note = %q, %v; want moved", got, err)
	}
}

// The version is pinned when the key is watched, so a change made while the
// caller was still deciding what to queue has to abort the transaction. This is
// the WATCH contract, and it is what makes read-then-write safe.
func TestTransactionAbortsWhenAWatchedKeyChanged(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.Set(ctx, "balance", "100", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Set(ctx, "audit", "0", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	tx := client.Watch(ctx, "balance").Set("audit", "1")

	// Another writer moves the watched key after it was watched. Nothing about
	// this write is concurrent with Exec: the transaction is refused because it
	// was prepared against a state that no longer holds.
	if err := client.Set(ctx, "balance", "50", 0).Err(); err != nil {
		t.Fatalf("interfering Set: %v", err)
	}

	if err := tx.Exec(ctx); !errors.Is(err, ErrTxAborted) {
		t.Fatalf("Exec error = %v, want ErrTxAborted", err)
	}
	if got, err := client.Get(ctx, "audit").Result(); err != nil || got != "0" {
		t.Fatalf("audit = %q, %v; want 0 (nothing may be applied)", got, err)
	}
}

// A watched key that did not exist has to be guarded too, or WATCH would miss
// exactly the race a lock or a claim depends on.
func TestTransactionAbortsWhenAWatchedKeyAppeared(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	tx := client.Watch(ctx, "claim").Set("winner", "me")
	if err := client.Set(ctx, "claim", "someone-else", 0).Err(); err != nil {
		t.Fatalf("interfering Set: %v", err)
	}

	if err := tx.Exec(ctx); !errors.Is(err, ErrTxAborted) {
		t.Fatalf("Exec error = %v, want ErrTxAborted", err)
	}
	if got, err := client.Exists(ctx, "winner").Result(); err != nil || got != 0 {
		t.Fatalf("winner exists = %d, %v; want 0", got, err)
	}

	// The guard on absence must not have created the key it was guarding.
	if got, err := client.Get(ctx, "claim").Result(); err != nil || got != "someone-else" {
		t.Fatalf("claim = %q, %v; want someone-else", got, err)
	}
}

// Watching a key that stays untouched must not stop the transaction, and the
// guard must leave no trace of a key that never existed.
func TestTransactionAppliesWithAnUntouchedWatch(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.Watch(ctx, "absent", "also-absent").Set("k", "v").Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if got, err := client.Get(ctx, "k").Result(); err != nil || got != "v" {
		t.Fatalf("k = %q, %v; want v", got, err)
	}
	if got, err := client.Exists(ctx, "absent").Result(); err != nil || got != 0 {
		t.Fatalf("watched key was created by its own guard: %d, %v", got, err)
	}
	if _, ok := db.metaOf("default", "absent"); ok {
		t.Fatal("the absence guard left a row behind")
	}
}

func TestTransactionNeedsTheBucketLayout(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	tx := client.Watch(ctx, "a")
	tx.Set("a", "1")
	if err := tx.Exec(ctx); !errors.Is(err, ErrTxUnsupported) {
		t.Fatalf("Exec error = %v, want ErrTxUnsupported", err)
	}

	bucketed, _ := newTestClient(t, func(o *Options) { o.PartitionByBucket = true })
	if err := bucketed.Multi().Set("a", "1").Exec(ctx); !errors.Is(err, ErrTxUnsupported) {
		t.Fatalf("Exec in one-key-per-partition mode error = %v, want ErrTxUnsupported", err)
	}
}

func TestEmptyTransactionIsRejected(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.Watch(ctx, "a").Exec(ctx); !errors.Is(err, ErrTxEmpty) {
		t.Fatalf("Exec error = %v, want ErrTxEmpty", err)
	}
}

func TestTransactionRefusesAWrongTypeIncrement(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.TransactionsByBucket = true })

	if err := client.SAdd(ctx, "k", "a").Err(); err != nil {
		t.Fatalf("SAdd: %v", err)
	}
	if err := client.Multi().IncrBy("k", 1).Exec(ctx); !errors.Is(err, ErrWrongType) {
		t.Fatalf("Exec error = %v, want WRONGTYPE", err)
	}
}

func TestBucketedViewsShareInitAndIsolateKeys(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, func(o *Options) { o.PartitionByBucket = true })

	a := client.Bucketed("tenant-a")
	b := client.Bucketed("tenant-b")

	if err := a.Set(ctx, "k", "a", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := b.Set(ctx, "k", "b", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}

	if got, err := a.Get(ctx, "k").Result(); err != nil || got != "a" {
		t.Fatalf("tenant-a Get = %q, %v", got, err)
	}
	if got, err := b.Get(ctx, "k").Result(); err != nil || got != "b" {
		t.Fatalf("tenant-b Get = %q, %v", got, err)
	}
	if keys, err := a.Keys(ctx, "*").Result(); err != nil || len(keys) != 1 {
		t.Fatalf("tenant-a Keys = %v, %v; want one key", keys, err)
	}
	if got := db.count("CREATE TABLE"); got != len(db.sch.ddl) {
		t.Fatalf("schema created %d times, want once (%d statements)", got, len(db.sch.ddl))
	}
}

func TestBucketedRequiresBucketMode(t *testing.T) {
	client, _ := newTestClient(t, nil)
	if err := client.Bucketed("x").Get(context.Background(), "k").Err(); err == nil {
		t.Fatal("Bucketed was accepted without a bucket mode")
	}
}

func TestInitFailureIsRetriedAfterACooldown(t *testing.T) {
	ctx := context.Background()
	client, db, clock := newTestClientWithClock(t, func(o *Options) { o.InitRetryCooldown = time.Second })
	db.fail["CREATE TABLE"] = errors.New("boom")

	if err := client.Set(ctx, "k", "v", 0).Err(); err == nil {
		t.Fatal("Set succeeded while the schema could not be created")
	}
	// Within the cooldown the client does not hammer the cluster with DDL.
	before := db.count("CREATE TABLE")
	if err := client.Set(ctx, "k", "v", 0).Err(); err == nil {
		t.Fatal("Set succeeded during the cooldown")
	}
	if after := db.count("CREATE TABLE"); after != before {
		t.Fatalf("DDL was retried during the cooldown: %d then %d", before, after)
	}

	delete(db.fail, "CREATE TABLE")
	clock.advance(2 * time.Second)
	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set after the cooldown: %v", err)
	}
}

func TestInitIgnoresCallerCancellation(t *testing.T) {
	client, _ := newTestClient(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Schema creation runs detached, so one cancelled request cannot leave the
	// client unusable for everyone else.
	if err := client.ensureReady(ctx); err != nil {
		t.Fatalf("ensureReady with a cancelled context: %v", err)
	}
	if !client.core.initialized.Load() {
		t.Fatal("schema was not initialized")
	}
}

func TestIdentifiersAreValidated(t *testing.T) {
	cases := []*Options{
		{Keyspace: "ks", Table: "kv; DROP TABLE x"},
		{Keyspace: "ks; --", Table: "kv"},
		{Keyspace: "ks", Table: "a.b.c"},
		{Table: "kv"},
	}
	for _, opt := range cases {
		if client := newConfiguredClient(opt); client.configErr == nil {
			t.Fatalf("accepted %+v", opt)
		}
	}
	if client := newConfiguredClient(&Options{Table: "ks.kv"}); client.configErr != nil {
		t.Fatalf("rejected a fully qualified table: %v", client.configErr)
	}
}

func TestPlaintextCredentialsAreRefused(t *testing.T) {
	client := newConfiguredClient(&Options{Keyspace: "ks", Username: "u", Password: "p"})
	if client.configErr == nil {
		t.Fatal("credentials without TLS were accepted")
	}
	allowed := newConfiguredClient(&Options{
		Keyspace: "ks", Username: "u", Password: "p", AllowPlaintextCredentials: true,
	})
	if allowed.configErr != nil {
		t.Fatalf("explicit opt-in was refused: %v", allowed.configErr)
	}
}

func TestResourceCeilings(t *testing.T) {
	ctx := context.Background()

	t.Run("value size", func(t *testing.T) {
		client, _ := newTestClient(t, func(o *Options) { o.MaxValueSize = 4 })
		if err := client.Set(ctx, "k", "12345", 0).Err(); !errors.Is(err, ErrValueTooLarge) {
			t.Fatalf("Set error = %v, want ErrValueTooLarge", err)
		}
	})

	t.Run("collection scan", func(t *testing.T) {
		client, _ := newTestClient(t, func(o *Options) { o.MaxCollectionScan = 2 })
		if err := client.SAdd(ctx, "s", "a", "b", "c").Err(); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		if err := client.SMembers(ctx, "s").Err(); !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("SMembers error = %v, want ErrResultTooLarge", err)
		}
	})

	t.Run("keys scan", func(t *testing.T) {
		client, _ := newTestClient(t, func(o *Options) { o.MaxKeysScan = 2 })
		for i := 0; i < 3; i++ {
			if err := client.Set(ctx, fmt.Sprintf("k%d", i), "v", 0).Err(); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}
		if err := client.Keys(ctx, "*").Err(); !errors.Is(err, ErrResultTooLarge) {
			t.Fatalf("Keys error = %v, want ErrResultTooLarge", err)
		}
	})

	t.Run("hash batch", func(t *testing.T) {
		client, _ := newTestClient(t, func(o *Options) { o.MaxBatchStatements = 3 })
		values := make([]interface{}, 0, 10)
		for i := 0; i < 5; i++ {
			values = append(values, fmt.Sprintf("f%d", i), "v")
		}
		if err := client.HSet(ctx, "h", values...).Err(); !errors.Is(err, ErrBatchTooLarge) {
			t.Fatalf("HSet error = %v, want ErrBatchTooLarge", err)
		}
	})
}

func TestKeysAndElementsAreValidated(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "", "v", 0).Err(); err == nil {
		t.Fatal("an empty key was accepted")
	}
	if err := client.Set(ctx, "bad\xff", "v", 0).Err(); err == nil {
		t.Fatal("a key that is not valid UTF-8 was accepted")
	}
	// Hash fields and set members are blobs, so they are binary safe.
	if err := client.HSet(ctx, "h", "field\xff", "v").Err(); err != nil {
		t.Fatalf("HSet with a binary field: %v", err)
	}
	if value, err := client.HGet(ctx, "h", "field\xff").Result(); err != nil || value != "v" {
		t.Fatalf("HGet = %q, %v; want v, nil", value, err)
	}
	if err := client.SAdd(ctx, "s", "member\xff").Err(); err != nil {
		t.Fatalf("SAdd with a binary member: %v", err)
	}
	if ok, err := client.SIsMember(ctx, "s", "member\xff").Result(); err != nil || !ok {
		t.Fatalf("SIsMember = %v, %v; want true, nil", ok, err)
	}
}

func TestConcurrentIncrementsDoNotLoseUpdates(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.CASMaxRetries = 200 })

	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				if err := client.Incr(ctx, "counter").Err(); err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	got, err := client.Get(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != strconv.Itoa(workers*perWorker) {
		t.Fatalf("counter = %s, want %d", got, workers*perWorker)
	}
}

func TestConcurrentCollectionWritesKeepTheCountExact(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, func(o *Options) { o.CASMaxRetries = 200 })

	const workers = 8
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if err := client.SAdd(ctx, "s", fmt.Sprintf("m-%d-%d", w, i)).Err(); err != nil {
					t.Errorf("SAdd: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	card, err := client.SCard(ctx, "s").Result()
	if err != nil {
		t.Fatalf("SCard: %v", err)
	}
	if card != workers*10 {
		t.Fatalf("SCard = %d, want %d", card, workers*10)
	}
	members, err := client.SMembers(ctx, "s").Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if int64(len(members)) != card {
		t.Fatalf("SCard reports %d but there are %d members", card, len(members))
	}
}

func TestStatementPlaceholdersMatchArguments(t *testing.T) {
	value := []byte("v")
	expires := time.Now()

	for _, mode := range []string{"flat", "bucketed", "grouped"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			client, _ := newTestClient(t, func(o *Options) {
				o.PartitionByBucket = mode == "bucketed"
				o.TransactionsByBucket = mode == "grouped"
			})
			s := client.core.schema
			meta := keyMeta{version: 1, size: 2, head: 3, tail: 4}

			cases := []struct {
				name string
				stmt string
				args []any
			}{
				{"metaRead", s.metaRead, client.keyArgs("k")},
				{"kindRead", s.kindRead, client.kindReadArgs("k", kindField)},
				{"edgeRead", s.edgeRead, client.keyArgs("k")},
				{"edgeLast", s.edgeLast, client.keyArgs("k")},
				{"pick(0)", s.pick(0), client.pickArgs("k", kindField, nil)},
				{"pick(2)", s.pick(2), client.pickArgs("k", kindField, [][]byte{value, value})},
				{"strWrite", s.strWrite, client.strWriteArgs("k", value, 1, expires, 0)},
				{"strWriteTTL", s.strWriteTTL, client.strWriteArgs("k", value, 1, expires, 5)},
				{"strWriteNX", s.strWriteNX, client.strWriteArgs("k", value, 1, expires, 0)},
				{"strWriteNXTTL", s.strWriteNXTTL, client.strWriteArgs("k", value, 1, expires, 5)},
				{"strCAS", s.strCAS, client.strCASArgs("k", value, 1, expires, 2, 0)},
				{"strCASTTL", s.strCASTTL, client.strCASArgs("k", value, 1, expires, 2, 5)},
				{"collCreate", s.collCreate, client.collCreateArgs("k", typeHash, meta)},
				{"collCAS", s.collCAS, client.collCASArgs("k", typeHash, meta, 7)},
				{"expireCAS", s.expireCAS, client.expireCASArgs("k", 1, expires, 2)},
				{"absentCAS", s.absentCAS, client.keyArgs("k")},
				{"metaDeleteIf", s.metaDeleteIf, client.keyArgs("k")},
				{"metaDeleteCAS", s.metaDeleteCAS, client.metaDeleteCASArgs("k", 1)},
				{"elemWrite", s.elemWrite, client.elemWriteArgs("k", kindField, value, value)},
				{"elemDelete", s.elemDelete, client.elemDeleteArgs("k", kindField, value)},
				{"elemsDelete", s.elemsDelete, client.keyArgs("k")},
				{"keyDelete", s.keyDelete, client.keyArgs("k")},
				{"indexWrite", s.indexWrite, client.keyArgs("k")},
				{"indexDelete", s.indexDelete, client.keyArgs("k")},
				{"indexScan", s.indexScan, client.scanArgs()},
				{"expiryWrite", s.expiryWrite, []any{expires, "b", "k", 1}},
				{"expiryScan", s.expiryScan, []any{expires}},
				{"expiryDrop", s.expiryDrop, []any{expires, "b", "k"}},
				{"wakeupWrite", s.wakeupWrite, []any{expires, "b", "k", 1}},
				{"wakeupScan", s.wakeupScan, []any{expires}},
			}

			for _, tc := range cases {
				if got, want := strings.Count(tc.stmt, "?"), len(tc.args); got != want {
					t.Errorf("%s: %d placeholders but %d arguments\n  %s", tc.name, got, want, tc.stmt)
				}
			}

			// A multi-column relation may not skip a clustering column, and the
			// server is the only thing that enforces it. Which columns are
			// clustered depends on the layout: with the bucket as the whole
			// partition key the key is clustered and belongs in the tuple, and
			// otherwise it is part of the partition and must stay out of it.
			wantTuple := "(kind, sub) IN"
			if mode == "grouped" {
				wantTuple = "(key, kind, sub) IN"
			}
			if !strings.Contains(s.pick(1), wantTuple) {
				t.Errorf("pick does not restrict the whole clustering prefix in %s mode\n  %s", mode, s.pick(1))
			}

			// An INSERT must bind or inline one value per named column.
			for _, tc := range cases {
				if !strings.HasPrefix(tc.stmt, "INSERT INTO ") {
					continue
				}
				columns := strings.Count(betweenParens(tc.stmt, "INSERT INTO"), ",") + 1
				values := strings.Count(betweenParens(tc.stmt, "VALUES"), ",") + 1
				if columns != values {
					t.Errorf("%s: %d columns but %d values\n  %s", tc.name, columns, values, tc.stmt)
				}
			}
		})
	}
}

// betweenParens returns the contents of the first parenthesised group that
// follows marker.
func betweenParens(stmt, marker string) string {
	rest := stmt[strings.Index(stmt, marker)+len(marker):]
	open := strings.Index(rest, "(")
	end := strings.Index(rest, ")")
	if open < 0 || end < open {
		return ""
	}
	return rest[open+1 : end]
}

func TestSchemaCreatesEveryTable(t *testing.T) {
	ctx := context.Background()
	client, db := newTestClient(t, nil)

	if err := client.ensureReady(ctx); err != nil {
		t.Fatalf("ensureReady: %v", err)
	}
	for _, want := range []string{db.sch.table, db.sch.index, db.sch.expiry, db.sch.wakeup} {
		if got := db.count("CREATE TABLE IF NOT EXISTS " + want + " "); got != 1 {
			t.Fatalf("table %s was created %d times, want once", want, got)
		}
	}
}

func TestListPositionEncodingSortsNumerically(t *testing.T) {
	positions := []int64{-9223372036854775808, -2, -1, 0, 1, 2, 9223372036854775807}
	for i := 1; i < len(positions); i++ {
		lower := string(encodePos(positions[i-1]))
		higher := string(encodePos(positions[i]))
		if !(lower < higher) {
			t.Fatalf("encodePos(%d) does not sort before encodePos(%d)", positions[i-1], positions[i])
		}
	}
	for _, pos := range positions {
		got, ok := decodePos(encodePos(pos))
		if !ok || got != pos {
			t.Fatalf("decodePos(encodePos(%d)) = %d, %v", pos, got, ok)
		}
	}
}

func TestClosedClientRefusesWork(t *testing.T) {
	ctx := context.Background()
	client, _ := newTestClient(t, nil)

	if err := client.Set(ctx, "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Get(ctx, "k").Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close error = %v, want ErrClosed", err)
	}
}
