# Adaptive per-statement compression history

Status: **Draft — under discussion, not yet implemented**
Relates to: [#775](https://github.com/scylladb/gocql/pull/775)
Discussion: [r3037216523](https://github.com/scylladb/gocql/pull/775#discussion_r3037216523), [r3037230509](https://github.com/scylladb/gocql/pull/775#discussion_r3037230509), [r3037252588](https://github.com/scylladb/gocql/pull/775#discussion_r3037252588), [r3037328116](https://github.com/scylladb/gocql/pull/775#discussion_r3037328116), [r3063401474](https://github.com/scylladb/gocql/pull/775#discussion_r3063401474)
Prerequisite (already shipped, same PR): `Query.NoCompression()` / `Batch.NoCompression()`, the manual escape hatch for payloads known up front to be incompressible (e.g. vectors). See [r3063545884](https://github.com/scylladb/gocql/pull/775#discussion_r3063545884) and `doc.go`'s Compression section.

## Problem

`CompressionPolicy` gates compression purely on frame size and topology (`MinCompressLocalSize`/`MinCompressRemoteSize`/`Scope`). It cannot tell "large and compressible" from "large and incompressible" — that distinction only exists in the actual bytes. `NoCompression()` lets a user *tell* the driver a given statement is incompressible, but that requires the user to already know and to remember to set it. This design makes that judgment automatic: the driver observes whether recent compression attempts for a given prepared statement actually paid off, and stops trying (for a while) when they don't — without any user action, and without the size/topology mechanism disappearing (this is a third gate layered after it, not a replacement).

## Goals

- No configuration required to benefit; default is fully off / bit-for-bit identical to today.
- Bounded, cheap, statement-local memory. No global coordination.
- Never override an explicit `NoCompression()` — a manual call always wins and is never recorded as a data point.
- Reuse the existing kept/discarded signal from `MinSavingsPercent` — no new "compressibility" metric.

## Non-goals (v1)

- Cross-host learning / cluster-wide convergence (see Decision 1).
- BATCH frame support beyond "don't break it" (see Decision 2).
- Any change to how the *size* threshold itself is computed.

## The state machine

Per statement (see Decision 1 for exactly what "per statement" means), track two numbers:

```go
type compressionHistory struct {
    mu       sync.Mutex
    window   uint64 // ring of the last ≤64 real attempt outcomes: 1 = kept, 0 = discarded
    cooldown int32  // qualifying frames left to skip before the next forced probe
}
```

### Definitions

**Qualifying frame**: a frame whose serialized body already met the connection's existing
`MinCompressLocalSize`/`MinCompressRemoteSize` threshold — i.e. a frame that would be a
compression *candidate* under today's logic, before this mechanism is even considered.
Frames below that threshold, control frames that never carry `FlagCompress`, frames on a
connection with no compressor, and frames explicitly sent via `NoCompression(true)` are
**not qualifying**: they never read or write `window`/`cooldown`. This matters because a
skipped-for-unrelated-reasons frame is not a data point, and must not be counted against
either the window or the cooldown budget.

**Cooldown** is a countdown measured strictly in units of qualifying frames (not wall time,
not "any frame"). Its behavior is fully specified by three rules, and there is no other way
for it to change:

1. `cooldown == 0` → **probe**. The next qualifying frame attempts compression for real
   (`Encode` + `MinSavingsPercent`, exactly as today). Its outcome is shifted into `window`:
   `window = (window << 1) | (1 if kept else 0)`. The 64-bit left shift naturally drops the
   oldest recorded outcome once more than 64 real attempts have occurred — no masking needed.
2. `cooldown > 0` → **resting**. The next qualifying frame skips the attempt outright
   (`FlagCompress` cleared before `finish()` ever inspects it — the same mechanism
   `NoCompression` already uses), and `cooldown` is decremented by exactly 1. `window` is
   untouched.
3. `cooldown` is assigned a positive value in exactly one place: immediately after a probe
   (rule 1) whose *resulting* `window` is entirely `0` — i.e. every one of the last ≤64 real
   attempts on this statement failed the savings check. At that point `cooldown = Y`, where
   `Y` is `CompressionPolicy.HistoryCooldownFrames`. Nowhere else does cooldown increase.

`Y == 0` makes rule 3 a no-op forever (cooldown can never become positive), so every
qualifying frame is always a probe: **identical to today's behavior.** This is the default —
the feature is opt-in via `Y > 0`.

Note the decision test is `cooldown > 0`, not `window == 0`, which is what makes the very
first-ever frame for a statement behave correctly with no special-casing: `cooldown` starts
at its zero value, so the first qualifying frame is unconditionally a probe, regardless of
`window` also starting at its (indistinguishable-from-"all failed") zero value.

A consequence worth calling out explicitly: a *single* failed probe already zeroes `window`
and arms the cooldown (nothing requires a minimum sample count before trusting the signal).
This is deliberately the simple version originally sketched; it means the mechanism pauses
fast and only needs one success to stay in probing mode indefinitely. Tune `Y` down if this
is too trigger-happy for a given workload; this is exactly the kind of thing a benchmark
against real workloads should inform, not a guess baked into the default.

### Concurrency

One `sync.Mutex` per `compressionHistory`, guarding both fields together. This is not a
hot-hot path — it's only reached by frames that already cleared the size threshold, which is
already a minority of traffic, and a mutex lock/unlock is negligible next to the `Encode`
call it's protecting. Not worth lock-free atomics unless a benchmark says otherwise.

## Decision 1 — where does the history live?

**Resolved: piggyback on the existing per-`(hostID, keyspace, statement)` `preparedStatment`
cache entry** (`conn.go`, `session.stmtsLRU`). Add `compressionHistory` as a plain embedded
field (not a pointer — the mutex + two integers are a few bytes, and `preparedLRU` already
bounds the cache via `MaxPreparedStmts`, so there's no reason to pay for lazy-alloc
nil-checks). No new cache, no new eviction policy, no new key derivation.

Consequence accepted on purpose: history resets whenever a fresh `preparedStatment` is
created — LRU eviction, an `UNPREPARED` response forcing a re-prepare, or simply a new
connection/host that hasn't seen this statement yet. Cross-host learning does not happen;
each host that serves a given statement converges independently.

This was a real choice, not a forced one (we could preserve history across invalidation with
more bookkeeping), and the case for accepting the reset is: (a) it's simpler, and (b) most
events that actually invalidate a prepared statement (schema change causing `UNPREPARED`,
in particular) are exactly the events after which the previous compressibility signal is
*least* trustworthy anyway. Starting fresh isn't just simpler, it's arguably more correct.

## Decision 2 — tiering / bucket count

**Resolved: no explicit bucketing needed.** Because history lives on the per-host
`preparedStatment` entry (Decision 1), and a given host's tier relative to this client
(same rack / same DC other rack / remote DC) never changes, per-host storage is already at
least as fine-grained as any tier split we could pick — every host in tier N gets its own
entry, so "3 buckets" (same-rack / same-DC-other-rack / remote-DC — which matters because
cloud providers price these three differently) falls out for free, with no tier-lookup code
and no interaction with `CompressionScope`/`HostTierer` at all. The mechanism doesn't need
to know the raw tier; it only ever needs the one `*preparedStatment` it already has.

The tradeoff is that this is actually *finer* than 3 buckets — it's one bucket per host, and
multiple hosts in the same tier do not share learning. That's the same tradeoff already
accepted in Decision 1 (independent convergence per host) and is consistent with it.

## Decision 3 — BATCH frames

**Resolved for v1: BATCH does not participate.** A BATCH frame bundles N statements (each
with its own, possibly-absent, history entry) into one physical frame with one compress
decision, and there isn't a clean way to combine N independent signals into one without
either being wrong some of the time or adding real complexity. `writeBatchFrame` keeps
today's threshold + savings-ratio behavior unconditionally; `noCompress` (the v1
`Batch.NoCompression()` manual override) is unaffected and keeps working as already shipped.

Sketched, not built, as a future extension if it turns out to matter: an "all agree" rule —
skip the whole batch only if *every* member statement's own entry currently has
`cooldown > 0` (an entry-less/unprepared batch statement counts as "wants to probe", forcing
an attempt) — and when a real attempt does happen, feed its one outcome into *every*
member's window. This is close to free for the common "same statement repeated N times"
batch (e.g. bulk vector inserts), degrades to "always attempt" (today's behavior, harmless)
for mixed-statement batches, and needs `executeBatch` to keep a parallel slice of each
member's `*compressionHistory` alongside `req.statements` (available for free — `info` is
already in hand there before it gets flattened into `batchStatment{preparedID: info.id}`).
Not doing this now; flagging it so it doesn't need re-deriving later.

## Threading it through `finish()`

`finish()` already computes `bodyLen >= threshold` and already knows the true kept/discarded
outcome after `Encode` + the ratio check. Rather than re-deriving "was this a real attempt"
from the caller side by inspecting `f.buf[1]` after the call — which is ambiguous, since a
clear `FlagCompress` bit means either "never a candidate" or "attempted and discarded", and
those must be told apart to update `window` correctly — `finish()` gains one new optional
parameter, invoked at most once, exactly where it already knows the answer:

```go
// onAttempt, if non-nil, is invoked exactly once, iff a real compression attempt was made
// (i.e. the frame was a qualifying candidate and was not skipped by threshold/noCompress),
// with the true kept/discarded outcome. nil for every frame type that doesn't participate.
onAttempt func(kept bool)
```

`writeExecuteFrame` supplies a non-nil callback only when `noCompress` is false and the
statement's `cooldown == 0` (this frame is a probe candidate); the callback locks the
statement's `compressionHistory` and applies state-machine rules 1/3 above. Every other call
site (`writeQueryFrame`, `writePrepareFrame`, `writeBatchFrame` in v1, STARTUP/OPTIONS/etc.)
passes `nil` — one extra nil-check, zero-cost, matching how `compOpts` already behaves when
unused.

The `cooldown > 0` **skip** decision itself is made the same way `NoCompression` already
works today: compute a local flags byte with `FlagCompress` cleared before `writeHeader`,
never mutate the pooled framer's `compOpts`. No changes needed there.

## Configuration

New field on `CompressionPolicy`:

```go
// HistoryCooldownFrames enables adaptive per-statement compression skipping. When a
// statement's last (up to 64) real compression attempts all failed the MinSavingsPercent
// check, the driver skips attempting compression for this many subsequent qualifying
// frames before probing again. 0 (default) disables this entirely — behavior is identical
// to not having this field.
HistoryCooldownFrames int
```

`ClusterConfig.Validate()` rejects `HistoryCooldownFrames < 0`, consistent with the existing
threshold/percent range checks.

## Testing plan

- Deterministic state-machine tests driving a `compressionHistory` through forced sequences
  of kept/discarded outcomes: single failure arms cooldown; a success anywhere in the window
  keeps probing; cooldown expiry forces exactly one re-probe; `Y == 0` never arms.
- Concurrency test: many goroutines hammering the same `compressionHistory` concurrently,
  run under `-race`.
- A benchmark (this PR's existing culture benchmarks every path) comparing "always attempt on
  synthetic incompressible data" (today) against "history-gated" over a long run, to put a
  real number on the CPU claim rather than asserting it.
- Regression: existing `TestFramerFinishNoCompressOverride`/`TestFramerFinishCompressionThreshold`/`TestFramerFinishMinSavingsPercent` must keep passing unmodified — this is a strictly additive gate in front of, not a replacement for, the existing logic.

## Backward compatibility

`HistoryCooldownFrames == 0` (default) means rule 3 above can never fire, so `cooldown` can
never leave `0`, so every qualifying frame is always a probe. This is byte-for-byte the
current behavior. The feature is purely additive and opt-in.

## Open questions

- Exact field/knob name (`HistoryCooldownFrames` is a placeholder).
- Whether `Y`'s default, once opted in, should be workload-derived (e.g. proportional to
  `MinSavingsPercent`) or just a fixed suggested constant in docs — deferred until there's a
  benchmark to look at.
