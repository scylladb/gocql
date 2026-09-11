#!/usr/bin/env bash
# Full write-coalescing evaluation, organised by workload archetype.
#
# Each archetype has ONE objective. Reporting p99 for an ingest job or ops/s
# for an OLTP job is meaningless, so they are measured -- and later read --
# separately.
#
#   ingest      bulk load / ETL / backfill. Deep queues, high per-connection
#               rate. Objective: ops/s and client CPU per op. Latency ignored.
#   ingest-cpu  same, client squeezed onto one core, so that any client-side
#               saving has somewhere to show up. Objective: ops/s.
#   oltp        request-response serving, a few ops per user request.
#               Objective: p99 at a fixed offered rate.
#   read/mixed  same shape as oltp with SELECTs. Feeds the section 4.5
#               decision (skip coalescing for reads), which is NOT implemented
#               on this branch.
#
# Rate points come from lambda_conn*W (docs/coalescing-redesign.md section 2):
# with W=200us and 4 connections, lambda_conn*W = rate*200e-6/4.
#   rate  2,000 -> 0.10  window collects ~nothing; pure latency tax
#   rate 20,000 -> 1.00  break-even, ~1 extra frame per window
#   unthrottled -> >1    batching regime
# Cross-DC tops out near 12.4k ops/s, so it cannot reach lambda*W=1; its rates
# are scaled to the same fraction of its own ceiling instead.
#
# Usage: tests/bench/run_coalesce_plan.sh [archetype ...]   (default: all)

set -euo pipefail

cd "$(dirname "$0")/.."/..
RUN=tests/bench/run_coalesce_bench.sh

export DURATION="${DURATION:-30s}"
export REPEATS="${REPEATS:-3}"

run_ingest() {
	LABEL=ingest OPMODE=write CLIENT_CPUS=12-15 \
		RATES="same-rack=0,same-dc-other-rack=0,cross-dc=0" \
		"$RUN"
}

run_ingest_cpu() {
	# One core for the client: if coalescing saves client work, this is where
	# it converts into throughput.
	LABEL=ingest-cpubound OPMODE=write CLIENT_CPUS=12 \
		RATES="same-rack=0,same-dc-other-rack=0,cross-dc=0" \
		"$RUN"
}

run_oltp_low() {
	LABEL=oltp-lam0.1 OPMODE=write CLIENT_CPUS=12-15 \
		RATES="same-rack=2000,same-dc-other-rack=2000,cross-dc=500" \
		"$RUN"
}

run_oltp_break() {
	LABEL=oltp-lam1.0 OPMODE=write CLIENT_CPUS=12-15 \
		RATES="same-rack=20000,same-dc-other-rack=20000,cross-dc=3000" \
		"$RUN"
}

run_read() {
	LABEL=read-lam0.1 OPMODE=read WORKLOAD=uniform CLIENT_CPUS=12-15 \
		RATES="same-rack=2000,same-dc-other-rack=2000,cross-dc=500" \
		"$RUN"
}

run_mixed() {
	LABEL=mixed-lam1.0 OPMODE=mixed WORKLOAD=uniform CLIENT_CPUS=12-15 \
		RATES="same-rack=20000,same-dc-other-rack=20000,cross-dc=3000" \
		"$RUN"
}

ALL=(ingest ingest_cpu oltp_low oltp_break read mixed)
SELECTED=("${@:-${ALL[@]}}")

for name in "${SELECTED[@]}"; do
	echo "############ archetype: $name ############"
	"run_$name"
	echo
done
