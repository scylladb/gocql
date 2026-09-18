#!/usr/bin/env python3
"""Aggregate write-coalescing benchmark results, one table per archetype.

Each archetype is reported against its own objective: ingest by throughput and
client CPU per op, OLTP/read by latency percentiles. Mixing them -- e.g.
quoting p99 for a saturating ingest run -- is what made earlier passes
meaningless, so the objective is chosen from the experiment metadata rather
than printed uniformly.

Usage: summarize_coalesce_bench.py [results_dir ...]
"""

import os
import re
import statistics
import sys

CONFIG_ORDER = [
    "master",
    "master-plus-alloc-reduction",
    "branch-disabled",
    "branch-new-design",
]
TIER_ORDER = ["same-rack", "same-dc-other-rack", "cross-dc"]
BASELINE = "master"
RESULTS_ROOT = "tests/bench/results"

DUR = re.compile(r"([0-9.]+)\s*(ns|µs|us|ms|s)\b")
UNIT_US = {"ns": 1e-3, "µs": 1.0, "us": 1.0, "ms": 1e3, "s": 1e6}


def to_us(text):
    m = DUR.search(text)
    return float(m.group(1)) * UNIT_US[m.group(2)] if m else None


def read_meta(d):
    meta = {}
    path = os.path.join(d, "experiment.txt")
    if not os.path.exists(path):
        return None
    for line in open(path):
        if ":" in line:
            k, v = line.split(":", 1)
            meta[k.strip()] = v.strip()
    meta["label"] = os.path.basename(d).split("-", 2)[-1]
    return meta


def parse_cpu(path):
    """/usr/bin/time '%U %S %e'; tolerate a leading error line."""
    if not os.path.exists(path):
        return None
    lines = [l.strip() for l in open(path) if l.strip()]
    for line in reversed(lines):
        parts = line.split()
        if len(parts) == 3:
            try:
                u, s, e = (float(p) for p in parts)
                return {"cpu_s": u + s, "wall_s": e}
            except ValueError:
                continue
    return None


def parse_run(path):
    body = open(path).read()
    if "WARNING: ran" in body:
        return None  # truncated run
    if re.search(r"^FAILED$", body, re.M):
        return None  # scylla-bench exited nonzero

    out = {"under_target": "WARNING: achieved" in body}
    for key, pat in (("ops", r"^Operations/s:\s*(\d+)"), ("total", r"^Total ops:\s*(\d+)")):
        m = re.search(pat, body, re.M)
        if not m:
            return None
        out[key] = int(m.group(1))

    if "raw latency" not in body:
        return None
    rest = body.split("raw latency", 1)[1]
    co_block = (rest.split("c-o fixed latency", 1) + [""])[1]
    for key, label in (("median", "median"), ("p99", "99th"),
                       ("p999", "99.9th"), ("mean", "mean")):
        m = re.search(rf"^\s+{re.escape(label)}:\s*(.+)$", co_block, re.M)
        out["co_" + key] = to_us(m.group(1)) if m else None

    stem = path[: -len(".txt")]
    cpu = parse_cpu(stem + ".cpu")
    if cpu and out["total"]:
        out["cpu_us_per_op"] = cpu["cpu_s"] * 1e6 / out["total"]
    segs_path = stem + ".outsegs"
    if os.path.exists(segs_path):
        try:
            out["segs"] = int(open(segs_path).read().strip())
        except ValueError:
            pass
    return out


REP = re.compile(r"^rep\d+$")


def collect(d):
    runs = {}
    seen_reps = {}
    for name in sorted(os.listdir(d)):
        if not name.endswith(".txt") or ".provenance" in name:
            continue
        if name in ("experiment.txt", "original-head.txt", "mode.txt"):
            continue
        stem = name[: -len(".txt")]
        parts = stem.split(".")
        if len(parts) != 3 or not REP.match(parts[2]):
            continue  # not a "cfg.tier.repN" run file (e.g. stray/duplicate)
        cfg, tier, rep = parts
        key = (cfg, tier)
        reps = seen_reps.setdefault(key, set())
        if rep in reps:
            sys.exit(f"{d}: duplicate repetition {rep} for {cfg}/{tier} (file {name})")
        reps.add(rep)
        parsed = parse_run(os.path.join(d, name))
        if parsed:
            runs.setdefault(key, []).append(parsed)
    return runs


def stat(values):
    vals = [v for v in values if v is not None]
    if not vals:
        return None, None
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.fmean(vals), statistics.stdev(vals)


def pct(new, base):
    if not base or new is None:
        return "-"
    return f"{(new - base) / base * 100:+.1f}%"


def fmt_us(v):
    if v is None:
        return "-"
    return f"{v / 1000:.2f}ms" if v >= 1000 else f"{v:.0f}µs"


def sig(new, new_sd, base, base_sd):
    """Flag only differences larger than the combined spread of both cells."""
    if None in (new, base):
        return ""
    noise = (new_sd or 0) + (base_sd or 0)
    if noise == 0:
        return ""  # single repeat: spread unknown, so claim nothing
    return "" if abs(new - base) <= noise else " *"


def report(d):
    meta = read_meta(d)
    if not meta:
        return
    runs = collect(d)
    if not runs:
        return

    want_repeats = int(meta.get("repeats", 0) or 0)
    missing = [f"{cfg}/{tier} ({len(runs.get((cfg, tier), []))}/{want_repeats})"
               for tier in TIER_ORDER for cfg in CONFIG_ORDER
               if len(runs.get((cfg, tier), [])) < want_repeats]
    if missing:
        sys.exit(f"{d}: incomplete experiment matrix, missing repeats for: "
                  + ", ".join(missing))

    label = meta["label"]
    # Objective is per-tier: rates are configured per tier, and a tier with
    # rate=0 (unthrottled) is a throughput run while others may be latency
    # runs at a fixed offered rate. Picking one objective globally would
    # mislabel whichever tiers don't match it.
    rate_by_tier = {}
    for entry in meta.get("rates", "").split(","):
        if "=" in entry:
            t, r = entry.split("=", 1)
            rate_by_tier[t.strip()] = r.strip()

    print(f"\n## {label}")
    print(f"\n`opmode={meta.get('opmode')} rates={meta.get('rates')} "
          f"duration={meta.get('duration')} repeats={meta.get('repeats')} "
          f"conns={meta.get('connections')} client_cpus={meta.get('client_cpus')} "
          f"tls={meta.get('tls')}`\n")

    last_objective = None
    for tier in TIER_ORDER:
        base = runs.get((BASELINE, tier))
        if not base:
            continue

        objective = "throughput" if rate_by_tier.get(tier) == "0" else "latency"
        if objective != last_objective:
            if objective == "throughput":
                print("\nObjective: throughput and client CPU per op. Latency "
                      "is not reported: these runs saturate, so latency is "
                      "queueing delay.\n")
                print("| Tier | Config | n | ops/s | Δ | CPU µs/op | Δ | Skbs/op |")
                print("|" + "---|" * 8)
            else:
                print("\nObjective: latency at a fixed offered rate "
                      "(coordinated-omission corrected).\n")
                print("| Tier | Config | n | achieved ops/s | p50 | Δ | p99 | Δ | "
                      "p99.9 | CPU µs/op |")
                print("|" + "---|" * 10)
            last_objective = objective
        base_ok = [r for r in base if not r.get("under_target")] or base
        b_ops, b_ops_sd = stat([r["ops"] for r in base])
        b_cpu, b_cpu_sd = stat([r.get("cpu_us_per_op") for r in base])
        b_p50, b_p50_sd = stat([r["co_median"] for r in base_ok])
        b_p99, b_p99_sd = stat([r["co_p99"] for r in base_ok])

        for cfg in CONFIG_ORDER:
            rs = runs.get((cfg, tier))
            if not rs:
                continue
            ops, ops_sd = stat([r["ops"] for r in rs])
            cpu, cpu_sd = stat([r.get("cpu_us_per_op") for r in rs])
            total, _ = stat([r["total"] for r in rs])
            segs, _ = stat([r.get("segs") for r in rs])
            tier_cell = tier if cfg == CONFIG_ORDER[0] else ""
            flag = " (under target)" if any(r.get("under_target") for r in rs) else ""
            rs_ok = [r for r in rs if not r.get("under_target")] or rs

            if objective == "throughput":
                spo = f"{segs / total:.3f}" if segs and total else "-"
                print(f"| {tier_cell} | {cfg} | {len(rs)} | "
                      f"{ops:,.0f} ±{ops_sd:,.0f} | {pct(ops, b_ops)}"
                      f"{sig(ops, ops_sd, b_ops, b_ops_sd)} | "
                      f"{cpu:.1f} ±{cpu_sd:.1f} | {pct(cpu, b_cpu)}"
                      f"{sig(cpu, cpu_sd, b_cpu, b_cpu_sd)} | {spo} |")
            else:
                p50, p50_sd = stat([r["co_median"] for r in rs_ok])
                p99, p99_sd = stat([r["co_p99"] for r in rs_ok])
                p999, _ = stat([r["co_p999"] for r in rs_ok])
                print(f"| {tier_cell} | {cfg} | {len(rs)} | "
                      f"{ops:,.0f}{flag} | "
                      f"{fmt_us(p50)} | {pct(p50, b_p50)}"
                      f"{sig(p50, p50_sd, b_p50, b_p50_sd)} | "
                      f"{fmt_us(p99)} | {pct(p99, b_p99)}"
                      f"{sig(p99, p99_sd, b_p99, b_p99_sd)} | "
                      f"{fmt_us(p999)} | {cpu:.1f} |")


def main():
    dirs = sys.argv[1:]
    if not dirs:
        dirs = [os.path.join(RESULTS_ROOT, d) for d in sorted(os.listdir(RESULTS_ROOT))]
    dirs = [d for d in dirs if os.path.isdir(d) and os.path.exists(os.path.join(d, "experiment.txt"))]
    if not dirs:
        sys.exit("no result directories with experiment.txt found")

    print("Values are mean ±stddev across repeats. `*` marks a difference "
          "larger than the combined spread of the two cells; everything "
          "unmarked is within noise.")
    for d in dirs:
        report(d)
    print("\nSkbs/op are Tcp:OutSegs deltas from the client netns. Client and "
          "nodes are joined by veth/bridge with no physical NIC, so GSO "
          "buffers are never split: this is write batching, NOT wire packets.")


if __name__ == "__main__":
    main()
