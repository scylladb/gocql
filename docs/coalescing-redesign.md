# Write-coalescing redesign (after PR #857)

## Context

PR #857 tried to fix coalescing latency/throughput and failed. Its opcode-based bypass (skip `OpExecute`) was the wrong signal — prepared SELECTs and prepared INSERTs share `OpExecute` — but that is not the whole story. The facts below show the current coalescer is mis-modelled at a more basic level (its 200µs timer effectively fires at ~1ms at low load, it collects <1 extra frame per window at realistic per-connection rates, and it is disabled entirely for TLS — i.e. for most cross-DC links). This plan replaces guesswork with a short measurement phase whose outputs gate the policy, then a small, coherent design.

Prior work to salvage: dangling tag `coalesce-redesign-wip-2026-09-09` (statement-keyword classifier `stmtSkipsCoalesce`, `flushImmediately` plumbing, `startupRTT`, `conn_coalesce_bench_test.go` with `delayedWriter`), and branch `perf/coalesce-redesign` (`cfb225ed`, allocation reuse in `flush`).

## 1. Established facts (evidence, not assumptions)

F1. **Nagle is off.** Go sets `TCP_NODELAY` on every TCP conn (`$GOROOT/src/net/tcpsock.go` `newTCPConn` → `setNoDelay(fd, true)`); gocql never overrides (driver_config.go:576). Every `write`/`writev` becomes segment(s) immediately (cwnd permitting).

F2. **The kernel does not batch for us.** gocql uses no `TCP_CORK` (Go has no API; would need `SyscallConn`+`SetsockoptInt`). Linux `tcp_autocorking=1` (default, on here) only corks a small write while a previous skb is *still queued in the qdisc/NIC* — i.e. under transmit backpressure. On loopback / an idle 10-25GbE link the previous packet is gone in ~1µs, so autocorking never engages in the latency-sensitive low/medium-rate regime. Under saturation it does engage, which is why "no user-space coalescing" does not collapse at very high load.

F3. **The 200µs timer is a ~1ms timer when the process is idle.** `$GOROOT/src/runtime/netpoll_epoll.go` `netpoll()`: `delay < 1e6 → waitms = 1` — any sub-millisecond timer deadline becomes `epoll_wait(…, 1ms)` when all Ps park (Go 1.26 still). Timers are only honoured at ~µs resolution while some P is busy running goroutines. In-repo evidence: WIP benchmark, 8 goroutines, zero simulated latency, no threshold → **7,300 ops/s = 8/7300 = 1.10ms per timer cycle**, not 200µs (40k/s). So at low load a lone request pays ~1ms, not 200µs; and the "100µs average" is only the high-load asymptote (see §2). Must be confirmed by E1.

F4. **Per-connection rate is low because of shard fan-out.** Scylla → one connection per shard per host (scylla.go:622-624). λ_conn = cluster rps / (hosts × shards). 100k rps on 3×16 shards ≈ 2.1k frames/s/conn → a 200µs window collects **λW ≈ 0.4 extra frames**; at 1M rps ≈ 4. Coalescing only pays when λ_conn·W ≳ 1-2.

F5. **v5 segments are per frame, before the coalescer.** `framer.prepareModernLayout` (frame.go:2140-2190) wraps each frame in its own self-contained segment (uncompressed 6B header, compressed 8B, +4B CRC32; `internal/segment/segment.go:48-61`) and the coalescer only ever sees already-segmented bytes. The spec allows many frames inside one self-contained segment (`MaxPayloadSize` = 128KB−1). Batching *before* segmentation would save 10-12B/frame of framing **and** compress the whole batch as one LZ4 block instead of ~120B blocks — small blocks compress poorly (LZ4 needs a window to find matches, and repeated CQL/prepared-id/param bytes across frames are exactly the redundancy it could exploit), so this is where compressed cross-DC links stand to gain most. Deliberately out of v1 scope (it moves the batching boundary from `writeCoalescer` up into `execInternal`/`framer`, i.e. a different change); recorded as the top follow-up with its own measurement.

F6. **TLS has no coalescing at all today.** `WrapTLS` sets `DisableCoalesce = true` (dial.go:118) because `net.Buffers.WriteTo` (`$GOROOT/src/net/net.go`) falls back to one `Write` per buffer when the writer lacks `writev` → one TLS record + one packet per frame. Each record costs 22B (TLS 1.3: 5 header + 1 type + 16 AEAD tag) or 29B (TLS 1.2 GCM) on the wire *and* a per-record AEAD/nonce setup + syscall on both ends, so a ~120B frame carries ~18-24% TLS overhead on top of the TCP/IP 30% in F7. Cross-DC links are usually TLS, so the coalescer is absent exactly where packets and CPU cost most. Fix: one contiguous `Write` per batch → one record (≤16KB) → one packet train; cost = one memcpy.

F7. **Per-packet overhead vs frame size.** IPv4 20 + TCP 20 + timestamps 12 (`tcp_timestamps=1`) = 52B; +Ethernet/FCS = 70B on wire. A prepared 3-column INSERT ≈ 110B frame → ~120B v5 segment. Uncoalesced overhead ≈ 52/172 = **30%** of bytes (37% on wire); 8 frames/packet → 5%. Cross-AZ egress ($0.01/GB/direction on AWS) at 50k rps cross-AZ ≈ $220/month/direction uncoalesced vs ≈ $160 coalesced ×8 — real but modest; the larger remote effect is **pps**: cloud NICs have per-instance packet-rate caps, and each request packet also costs a server-side softirq and (delayed-)ACK.

F8. **MSS, not "1400".** Linux MSS = MTU − 40 − 12(ts) = **1448** for MTU 1500 (IPv6: 1428). Cloud: AWS 9001 inside a VPC incl. cross-AZ (MSS 8949), 1500 to internet/inter-region; GCP default MTU **1460** (MSS 1408), configurable; Azure 1500. The negotiated value is available per socket via `getsockopt(TCP_MAXSEG)` through `(*net.TCPConn).SyscallConn()`. Rationale for MSS as the threshold: once ≥1 MSS is buffered the kernel emits a full segment anyway; waiting longer only adds segments. A larger k×MSS threshold saves *syscalls* (TSO/GSO handle multi-MSS cheaply) — a CPU objective, not a packet objective.

## 2. Model: added latency vs packets saved

Timer-only window W, Poisson arrivals λ per connection: batch = 1 + λW frames; first frame waits W, the rest average W/2 →
**A(λ) = W·(1 + λW/2)/(1 + λW)**, packets saved = λW/(1+λW).

| λ_conn | W=200µs: extra frames | packets saved | added latency A |
|---|---|---|---|
| 200/s | 0.04 | 4% | 198µs |
| 2k/s (≈100k rps cluster) | 0.4 | 29% | 171µs |
| 20k/s (≈1M rps cluster) | 4 | 80% | 120µs |
| same, but W→1.1ms (F3, idle) | 2.2 @2k/s | 69% | ≈0.8ms |

Take-aways: (a) at low λ the penalty is the *full* W, and coalescing saves almost nothing; (b) at high λ, A → W/2 and coalescing is worth it; (c) the regime where Go's timer is accurate (busy Ps) is exactly the high-λ regime. So the wait must be **conditional on the connection being in a burst**, not always-on and not always-off. Two zero-wait alternatives to quantify in E3: *opportunistic* (flush now; drain whatever raced in during the previous flush's ~5-20µs syscall → ≈1+λ·T_flush frames, e.g. 1.1-1.4 at 20k/s), and *Nagle-style* (arm the window only if the previous flush was < W ago; else flush now). Nagle-style gives A≈0 at low λ and A≈W/2 with full batching at high λ, for one `time.Now()` (~25ns) per write.

Where the latency budget sits: local RTT 0.1-0.3ms + service ≈ 0.3-1ms end-to-end → +170µs (or +1ms per F3) is +20-100%+. Cross-AZ RTT 0.5-2ms; cross-DC 10-100ms → a **budget-based window W = f·RTT** (f ≈ 5%: 10µs local, 50-100µs cross-AZ, 2-5ms cross-DC) is defensible; the WIP's RTT/2 adds 50% latency and is not.

## 3. Phase 0 — measure before freezing policy (gates everything below)

Each experiment has a hypothesis and the decision it gates. Harness code goes under `tests/bench` (manual, flag-gated, not in `make check`); the existing `tests/bench/bench_single_conn_test.go` is replay-based with no network and is *not* suitable for latency work.

- **E1 Timer fire latency**: histogram of `time.Timer` fire delay for W ∈ {50,100,200,500µs}, process idle vs one busy goroutine. H: idle ≈ 1.0-1.1ms, busy ≈ W+5-30µs. Gates: whether any sub-ms always-on wait is acceptable (expected: no → Nagle-style rule required), and the floor for W_local.
- **E2 Syscall/packet cost**: `writev` 1×120B vs 8×120B vs 12×120B on loopback and on a veth pair / two hosts; µs/call and packets emitted (`/proc/net/snmp Tcp:OutSegs` delta, `ss -ti`). H: with NODELAY, uncoalesced ⇒ 1 segment per frame (F1/F2 confirmed); per-frame send cost ≈ 2-5µs uncoalesced vs <1µs coalesced. Gates: the CPU side of the local trade-off the user asked to see.
- **E3 Policy sweep on the WIP microbench** (`conn_coalesce_bench_test.go`, `delayedWriter`, fixed iterations): policies {always-wait (today), opportunistic, Nagle-style} × W ∈ {50,100,200µs} × λ (via concurrency & pacing) → table of A and frames/flush. Also measures the flusher-goroutine handoff cost and evaluates a **direct-write fast path** (caller writes under a try-lock when no batch is open; no channel hop, no goroutine wake) as an option. Gates: choice among the three policies and W_local.
- **E4 End-to-end vs Scylla** on a scylla-ccm docker topology (`ScyllaDockerCluster`, tests/bench/coalesce_cluster.py): a four-node cluster (`dc1:rack1:1,rack2:1;dc2:rack1:2`), `--inter-rack-delay 0.25..0.5` (netem is per-direction egress, so RTT = 2×delay → 0.5-1ms cross-AZ), `--inter-dc-delay 20` (40ms RTT). The tc rules only apply inside container netns, so the gocql load generator must run in the ccm client container's namespace (`nsenter --net` into `ccm-client`, or as a container on the rack1 network) — from the host it would see no delay. Use `RackAwareRoundRobinPolicy(dc1, rack1)` so tiers 0/1/2 are all exercised on one cluster. Workload: prepared INSERT / SELECT / 50-50 mix at fixed offered load {5k, 50k, 200k rps} and fixed concurrency; measure p50/p99 per tier, client CPU, `Tcp:OutSegs`/s per policy. Gates: f (RTT fraction), whether statement-type priority (§4.5) moves p99 at all.
- **E5 TLS** (its own objective, not a footnote to E3/E4): today's `DisableCoalesce` path vs the contiguous-buffer path, measuring **records/s and client+server CPU per request**, not just latency — one record per frame costs 22B (TLS 1.3) or 29B (TLS 1.2 GCM) of wire plus a per-record AEAD setup/finalisation, so batching n frames into one record should show a roughly n-fold drop in record count and a measurable CPU/req drop at fixed rps. H: TLS gains *more* from coalescing than plaintext does (it saves record overhead + crypto calls on top of packets), which would make W for TLS conns deserve to be ≥ the plaintext W at the same tier. Gates: whether the TLS path needs its own (larger) window or can share the tier's.
- **E6 v5 compression block size** (informs the follow-up in F5, not v1 code): offline, compress N≈120B CQL frames individually vs concatenated, with the repo's LZ4 and Snappy compressors, on real recorded traffic (`dialer/recorder` captures under `testdata/`) → compressed bytes per frame for block sizes 1, 2, 4, 8, 16 frames. Gates: whether multi-frame self-contained segments are worth a follow-up PR and at what batch size the ratio stops improving.

Deliverable: one table per experiment committed to the PR description (raw numbers + machine/kernel/Go version), not prose.

## 4. Design (default recommendations; each marked with the experiment that can overturn it)

4.1 **Locality tier per Conn**, computed once in `Conn.init` (conn.go:511-546) from `c.session.policy` and `c.host`: `HostTierer.HostTier` if implemented (policies.go:440; precedent policies.go:1046-1070), else `IsLocal` → {0, 2}, else (default `roundRobinHostPolicy`, always "local") → tier 0. Unknown ⇒ local, because the failure mode of guessing "remote" is imposing a wait on everyone using the default policy.

4.2 **RTT per Conn**: take the **minimum** of the round trips already performed during startup (OPTIONS, STARTUP, AUTH/REGISTER — `startupCoordinator`), not a single sample; no EMA. Min-of-3 is robust to one stalled sample and costs nothing extra.

4.3 **Window**: `W = clamp(f·RTT, 0, WriteCoalesceWaitTime)`, f an internal constant seeded from E3/E4 (starting point 0.05); the cap is the existing user-configurable field, default unchanged at 200µs (user decision: 2ms is too much for cross-AZ; 200µs is fine and stays tunable). Resulting defaults: local RTT 0.2ms → 10µs (≈ zero-wait, below timer granularity per E1, so tier 0 collapses to the policy chosen in E3); cross-AZ RTT 1ms → 50µs; cross-DC → capped at 200µs, raise the field for more cross-DC batching. No separate local knob.

4.4 **Arming rule (Nagle-style, all tiers)**: on a write arriving to an idle flusher, arm the window only if `now − lastFlush < W`; otherwise flush immediately (draining anything that raced in). First request of a burst pays 0; steady bursts batch fully. Falls back to pure opportunistic when W → 0. E3 decides.

4.5 **Query type**: the keyword classifier from the WIP (`stmtSkipsCoalesce`, session.go near `stmtKeyword` at :2245) stays as the *signal* (opcode is wrong, F: `OpExecute` ambiguity), but its *use* changes: **remote tiers ignore it** (at 40ms RTT a 2ms wait is 5% and every packet costs; the user's point stands — SELECTs should ride the batch remotely). Locally it is at most a "priority flush" hint that closes the open batch early (bounded benefit ≤ W_local/2 ≈ tens of µs). Ship it only if E4 shows a measurable p99 gain; otherwise drop it (YAGNI). Cache the classification on the prepared-statement cache entry rather than re-scanning per execution. BATCH stays eligible.

> **Deferred, not implemented in v1.** E4 did not justify it (YAGNI per this section's own gate); `stmtSkipsCoalesce` was not shipped.

4.6 **Threshold = MSS-aware "flush before crossing"**: read `TCP_MAXSEG` via `SyscallConn` at init (fallback 1448 when the conn is not a `*net.TCPConn`); when `buffered + len(frame) > MSS`, flush the open batch first, then start a new batch with the frame — one full segment per flush instead of full+runt. Same rule for all tiers (the WIP's "disable threshold remotely" is dropped: a full segment gains nothing from waiting). On TLS, the budget is `MSS − record overhead` (22B TLS 1.3 / 29B TLS 1.2-GCM) so the record still fits one segment. Optional k×MSS (syscall-saving) only if E2/E4 show client CPU matters more than pps locally.

> **Deferred, not implemented in v1.** `coalesceFlushThreshold` (conn.go) is a fixed 1448-byte default, not MSS-probed per connection or TLS-overhead-adjusted; see the `ponytail:` note at its definition.

4.7 **TLS path** (E5): drop `DisableCoalesce = true` in `WrapTLS` (dial.go:118); when the underlying writer lacks `writev` (not a `*net.TCPConn`), `flush` concatenates into a reusable contiguous buffer and issues one `Write` → **one TLS record per batch instead of one per frame**, saving both the 22-29B/frame record overhead and a per-frame AEAD encrypt+seal on the client and the matching open on the server. This is a CPU win as much as a packet win, and it is why TLS conns may warrant a window at least as large as plaintext at the same tier (E5 decides). Keep `DialedHost.DisableCoalesce` as an explicit opt-out for custom dialers.

4.8 **Config surface** (decided): keep `ClusterConfig.WriteCoalesceWaitTime` (cluster.go:157/466/677), default 200µs, `0` still disables; non-zero = **cap** on the RTT-scaled window (doc comment updated: "maximum time to wait…; the effective wait per connection scales with its RTT and is ≈0 for same-rack hosts"). No public knobs for f, MSS or the classifier. Drop #857's `WriteCoalescePolicy`/`WriteCoalesceBypassOps` entirely. CHANGELOG notes the behaviour change (local traffic no longer waits; TLS connections now coalesce).

4.9 **Kept from prior work**: `cfb225ed` allocation reuse; the drain-on-flush loop; the `flushImmediately`/`writeContext(ctx, p, hint)` plumbing (renamed to a small `writeHint` struct if 4.5 survives, else dropped); the benchmark scaffold; `TestWriteCoalescing`'s clock-injection pattern (conn_test.go:1457).

## 5. Commit sequence (one branch `perf/coalesce-locality-aware` off master, sequential commits, no review pause between phases; `make check` before every push; review fixes amended into the causing commit)

1. Cherry-pick `cfb225ed` (allocation reuse) — no behaviour change.
2. Phase 0 harness + results: `tests/bench` coalesce policy bench (E1/E3), scylla-ccm podman runbook + load generator under `tests/bench` (E4, run via `nsenter` into the client netns), TLS variant (E5). Raw results tables go into the PR description; §4 parameters (policy choice, f, W_local, whether 4.5 ships) are filled in from them before commits 3-7 are written.
3. ~~MSS-aware threshold + `TCP_MAXSEG` probe~~ (4.6) — deferred, not implemented in v1; shipped with the fixed `coalesceFlushThreshold` default instead.
4. Locality tier + min-RTT + `W = f·RTT` + Nagle-style arming (4.1-4.4); table-driven tests: tier × λ-pattern → arms/doesn't; fake policies for HostTierer / IsLocal-only / round-robin.
5. TLS contiguous-buffer flush path (4.7) + test over `tls.Conn` on `tcpConnPair`.
6. ~~Statement-type priority flush~~ (4.5) — deferred, not implemented in v1; step 2/E4 did not justify it (YAGNI).
7. Config semantics + validation + CHANGELOG (4.8).

Follow-ups (separate PRs, not v1): **multi-frame self-contained v5 segments** (F5/E6) — batch frames *before* segmentation so one segment header+CRC covers many frames and, on compressed connections, one LZ4/Snappy block covers the whole batch instead of ~120B blocks that barely compress; this moves the batching boundary from `writeCoalescer` into `execInternal`/`framer`, so it needs its own design once E6 quantifies the ratio gain. Also: direct-write fast path if E3 shows the goroutine hop dominates low-load latency.

## 6. Verification

- Unit (in `make check`): injected-timer tests for arm/no-arm decisions, TLS concat path equivalence (bytes on the wire identical to the writev path), config validation, #857 regression. (MSS boundary flush and the statement-type classifier table are not applicable: 4.5/4.6 were deferred, not implemented in v1.)
- Benchmarks (`make test-bench`, label-gated in CI per `.github/workflows/bench-tests.yml`): extended `conn_coalesce_bench_test.go` matrix (policy × W × λ × tier) reporting ops/s, frames/flush, and added-latency p50/p99 — fixed iteration counts, not `b.N`.
- Real cluster (manual/nightly, scylla-ccm podman topology): E4 + E5 rerun on the final code; acceptance = local p50/p99 within noise of coalescing-disabled, cross-AZ/DC `Tcp:OutSegs`/s reduced with p99 within f of RTT, throughput at 200k rps not below today's on any tier, client CPU/frame not above today's, and on TLS both records/s and CPU/req strictly below today's (where the coalescer is off entirely).
