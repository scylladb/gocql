#!/usr/bin/env bash
# Write-coalescing benchmark runner (docs/coalescing-redesign.md E4).
#
# Runs ONE experiment -- a fixed (statement mode, offered rate, TLS, client CPU
# budget) -- across every tier x driver config, repeated. run_coalesce_plan.sh
# composes these into the workload archetypes.
#
# Design notes that matter for the validity of the output:
#
#  * Binaries are built once up front, not per repeat, so build order cannot
#    correlate with result order.
#  * The loop is tier -> repeat -> config, i.e. the four driver configs run
#    back-to-back within a repeat. Machine drift over the pass then hits all
#    four roughly equally instead of being confounded with config.
#  * The client is pinned with taskset and its CPU time recorded, because
#    client CPU per op -- not throughput -- is what coalescing actually
#    changes. Throughput only moves when the client is the bottleneck.
#  * scylla-bench links gocql through a `replace` pointed at a detached
#    worktree of this repo, rebuilt per ref, and every run records the commit
#    it linked.
#
# Prereqs:
#   tests/bench/coalesce_cluster.py create
#   scylla-bench with scylla-bench-coalesce-flag.patch applied
#
# Key env: OPMODE RATES DURATION REPEATS CLIENT_CPUS TLS CONNECTIONS LABEL

set -euo pipefail

GOCQL_DIR="${GOCQL_DIR:-$HOME/github/gocql}"
SB_DIR="${SB_DIR:-$HOME/github/scylla-bench}"
CLUSTER="${CLUSTER:-coalesce-bench}"

# write = prepared INSERT, read = prepared SELECT, mixed = both.
OPMODE="${OPMODE:-write}"
WORKLOAD="${WORKLOAD:-sequential}"

# Offered load per tier, "tier=rps,..."; 0 means unthrottled. Rates are chosen
# from lambda_conn*W (see docs/coalescing-redesign.md section 2): with W=200us
# and C connections, lambda_conn*W = rate*200e-6/C. That product, not the
# absolute rps, decides whether a window collects anything.
RATES="${RATES:-same-rack=0,same-dc-other-rack=0,cross-dc=0}"

DURATION="${DURATION:-60s}"
REPEATS="${REPEATS:-5}"
CONCURRENCY="${CONCURRENCY:-256}"
CONNECTIONS="${CONNECTIONS:-4}"
# Nodes run at smp=1 (podman ccm default), so keep the client off their cores.
CLIENT_CPUS="${CLIENT_CPUS:-12-15}"
TLS="${TLS:-false}"
LABEL="${LABEL:-$OPMODE}"

RF="${RF:-2}"
# Sequential workload stops at the end of the key space; keep it far larger
# than rate*duration so no run is cut short.
PARTITIONS="${PARTITIONS:-2000000}"
COMPRESSION="${COMPRESSION:-false}"
BASE_REF="${BASE_REF:-master}"

OUTDIR="${OUTDIR:-$GOCQL_DIR/tests/bench/results/$(date +%Y%m%d-%H%M%S)-$LABEL}"

TIERS=(
	"same-rack:dc1:rack1"
	"same-dc-other-rack:dc1:rack2"
	"cross-dc:dc2:rack1"
)

mkdir -p "$OUTDIR"

grep -q "write-coalesce-wait-time" "$SB_DIR/main.go" || {
	echo "apply scylla-bench-coalesce-flag.patch in $SB_DIR first" >&2
	exit 1
}

if ! git -C "$GOCQL_DIR" diff --quiet || ! git -C "$GOCQL_DIR" diff --cached --quiet; then
	echo "refusing to run: $GOCQL_DIR has uncommitted tracked changes" >&2
	exit 1
fi

# A busy machine silently invalidates every number below, so the check belongs
# in the harness, not in the operator's memory. Load average is useless here:
# an idle Scylla reactor busy-polls, so 4 idle nodes already show ~1.0 load.
# Measure actual CPU utilisation from /proc/stat instead, which an idle
# (polling) cluster still leaves low. MAX_CPU_PCT=0 skips the check.
MAX_CPU_PCT="${MAX_CPU_PCT:-25}"
cpu_busy_pct() {
	read -r _ a b c d e f g h _ < /proc/stat
	local i1=$d t1=$((a+b+c+d+e+f+g+h))
	sleep "${1:-3}"
	read -r _ a b c d e f g h _ < /proc/stat
	local i2=$d t2=$((a+b+c+d+e+f+g+h))
	awk -v i="$((i2-i1))" -v t="$((t2-t1))" 'BEGIN{if(t<=0){print 100}else{printf "%.0f", 100*(1-i/t)}}'
}
if [ "$MAX_CPU_PCT" != "0" ]; then
	for probe in 1 2 3; do
		busy=$(cpu_busy_pct 3)
		if [ "$busy" -gt "$MAX_CPU_PCT" ]; then
			echo "refusing to run: CPU ${busy}% busy, above MAX_CPU_PCT=${MAX_CPU_PCT}%" >&2
			echo "top consumers:" >&2
			ps -eo pcpu,pid,comm --sort=-pcpu 2>/dev/null | head -6 >&2
			echo "wait for the machine to go quiet, or set MAX_CPU_PCT=0 to override" >&2
			exit 1
		fi
	done
	echo "quiet check passed (CPU ${busy}% busy, limit ${MAX_CPU_PCT}%)"
fi

CLIENT=$(python3 "$GOCQL_DIR/tests/bench/coalesce_cluster.py" info --name "$CLUSTER" | awk '/^client:/{print $2}')
CLIENT_PID=$(podman inspect --format '{{.State.Pid}}' "$CLIENT")
[ -n "$CLIENT_PID" ] && [ "$CLIENT_PID" != "0" ] || { echo "client container not running" >&2; exit 1; }

in_netns() { nsenter -t "$CLIENT_PID" --user --net "$@"; }

out_segs() {
	in_netns cat /proc/net/snmp |
		awk '/^Tcp:/{if(c){print $c; exit} for(i=1;i<=NF;i++) if($i=="OutSegs") c=i}'
}

# Saturating archetypes leave a compaction backlog that steals server CPU from
# the next run; without this the same binary measures 65k-81k ops/s run to run.
COMPACTION_DRAIN_TIMEOUT="${COMPACTION_DRAIN_TIMEOUT:-180}"
drain_compaction() {
	local deadline=$((SECONDS + COMPACTION_DRAIN_TIMEOUT)) pending
	while [ "$SECONDS" -lt "$deadline" ]; do
		pending=0
		for node in $(podman ps --format '{{.Names}}' | grep "^ccm-$CLUSTER-node"); do
			pending=$((pending + $(podman exec "$node" nodetool compactionstats 2>/dev/null |
				awk '/^pending tasks:/{print $3; exit}' || echo 0)))
		done
		[ "$pending" -eq 0 ] && return 0
		sleep 5
	done
	echo "ERROR: compaction still pending after ${COMPACTION_DRAIN_TIMEOUT}s" >&2
	return 1
}

node_ip() {
	python3 "$GOCQL_DIR/tests/bench/coalesce_cluster.py" info --name "$CLUSTER" |
		awk -v dc="$1" -v rack="$2" '/^node:/ && $3==dc"/"rack {print $4; exit}'
}

rate_for() {
	echo "$RATES" | tr ',' '\n' | awk -F= -v t="$1" '$1==t{print $2; exit}'
}

ORIGINAL_REF=$(git -C "$GOCQL_DIR" rev-parse HEAD)
{
	echo "opmode:      $OPMODE"
	echo "workload:    $WORKLOAD"
	echo "rates:       $RATES"
	echo "duration:    $DURATION"
	echo "repeats:     $REPEATS"
	echo "concurrency: $CONCURRENCY"
	echo "connections: $CONNECTIONS"
	echo "client_cpus: $CLIENT_CPUS"
	echo "tls:         $TLS"
	echo "compression: $COMPRESSION"
	echo "rf:          $RF"
	echo "partitions:  $PARTITIONS"
	echo "head:        $ORIGINAL_REF"
} > "$OUTDIR/experiment.txt"

WORKTREE="${WORKTREE:-$(mktemp -d -t gocql-coalesce-wt-XXXXXX)}"
rmdir "$WORKTREE"
SB_ORIGINAL_REPLACE=$(cd "$SB_DIR" && go list -m -f '{{with .Replace}}{{.Path}}{{if .Version}}@{{.Version}}{{end}}{{end}}' github.com/gocql/gocql 2>/dev/null || true)
cleanup() {
	git -C "$GOCQL_DIR" worktree remove --force "$WORKTREE" 2>/dev/null || true
	if [ -n "$SB_ORIGINAL_REPLACE" ]; then
		(cd "$SB_DIR" && go mod edit -replace "github.com/gocql/gocql=$SB_ORIGINAL_REPLACE")
	else
		(cd "$SB_DIR" && go mod edit -dropreplace github.com/gocql/gocql)
	fi
}
trap cleanup EXIT
git -C "$GOCQL_DIR" worktree add --detach -q "$WORKTREE" "$ORIGINAL_REF"
(cd "$SB_DIR" && go mod edit -replace "github.com/gocql/gocql=$WORKTREE")

ALLOC_REF=$(git -C "$GOCQL_DIR" log --format=%H --grep="reduce writeCoalescer per-flush allocations" \
	"$BASE_REF..$ORIGINAL_REF" | tail -1)
[ -n "$ALLOC_REF" ] || { echo "could not locate the allocation-reduction commit" >&2; exit 1; }

CONFIGS=(
	"master:$BASE_REF"
	"master-plus-alloc-reduction:$ALLOC_REF"
	"branch-disabled:$ORIGINAL_REF"
	"branch-new-design:$ORIGINAL_REF"
)

# Build every config once, before any measurement.
echo "building:"
for cfg in "${CONFIGS[@]}"; do
	label="${cfg%%:*}"; ref="${cfg#*:}"
	git -C "$WORKTREE" checkout -q --detach "$ref"
	( cd "$SB_DIR" && go build -o "$OUTDIR/scylla-bench-$label" . )
	{
		go version -m "$OUTDIR/scylla-bench-$label" | grep -E 'gocql|=>'
		echo "ref:     $ref"
		echo "commit:  $(git -C "$WORKTREE" rev-parse HEAD)"
		echo "subject: $(git -C "$WORKTREE" log -1 --format=%s)"
	} > "$OUTDIR/$label.provenance.txt"
	echo "  $label -> $(git -C "$WORKTREE" rev-parse --short HEAD)"
done

echo
echo "cluster: $CLUSTER   client: $CLIENT (pid $CLIENT_PID, cpus $CLIENT_CPUS)"
echo "experiment: $LABEL  opmode=$OPMODE tls=$TLS rates=$RATES"
echo "outdir: $OUTDIR"
echo

for tier in "${TIERS[@]}"; do
	tlabel="${tier%%:*}"; rest="${tier#*:}"; dc="${rest%%:*}"; rack="${rest#*:}"
	ip=$(node_ip "$dc" "$rack")
	rate=$(rate_for "$tlabel")
	[ -n "$rate" ] || { echo "RATES has no entry for tier '$tlabel'" >&2; exit 1; }

	for rep in $(seq 1 "$REPEATS"); do
		# Configs run adjacent in time so drift is shared, not confounded.
		for cfg in "${CONFIGS[@]}"; do
			label="${cfg%%:*}"
			out="$OUTDIR/$label.$tlabel.rep$rep"

			extra=()
			[ "$label" = "branch-disabled" ] && extra+=(-write-coalesce-wait-time 0)
			# With no native_transport_port_ssl set, 9042 itself is TLS.
			[ "$TLS" = "true" ] && extra+=(-tls)

			drain_compaction || exit 1
			printf '%-28s %-20s rep%-2s rate=%-7s ' "$label" "$tlabel" "$rep" "$rate"
			before=$(out_segs)
			/usr/bin/time -f "%U %S %e" -o "$out.cpu" \
				taskset -c "$CLIENT_CPUS" \
				nsenter -t "$CLIENT_PID" --user --net \
				"$OUTDIR/scylla-bench-$label" \
				-workload "$WORKLOAD" -mode "$OPMODE" \
				-nodes "$ip" \
				-host-selection-policy token-aware -datacenter "$dc" -rack "$rack" \
				-replication-factor "$RF" -partition-count "$PARTITIONS" \
				-client-compression="$COMPRESSION" \
				-concurrency "$CONCURRENCY" -connection-count "$CONNECTIONS" \
				-max-rate "$rate" -duration "$DURATION" \
				-consistency-level one \
				"${extra[@]}" \
				> "$out.txt" 2>&1 || echo -n "FAILED "
			after=$(out_segs)
			echo "$((after - before))" > "$out.outsegs"

			secs=$(awk '/^Time \(avg\):/{gsub(/[^0-9.]/,"",$3); print int($3)}' "$out.txt")
			want=${DURATION%s}
			if [ -n "$secs" ] && [ "$secs" -lt $((want * 9 / 10)) ]; then
				echo "WARNING: ran ${secs}s of ${want}s -- key space exhausted" |
					tee -a "$out.txt"
			else
				achieved=$(awk -F'\t' '/^Operations\/s:/{print $2}' "$out.txt")
				printf '%s ops/s' "$achieved"
				awk '{printf "  cpu=%.2fs", $1+$2}' "$out.cpu"
				# A fixed-rate run that could not reach its target is really a
				# saturation run, and its latency must not be read as
				# "latency at rate X".
				if [ "$rate" != "0" ] && [ -n "$achieved" ] &&
					awk -v a="$achieved" -v r="$rate" 'BEGIN{exit !(a < 0.9*r)}'; then
					printf '  UNDER TARGET (%s of %s)' "$achieved" "$rate"
					echo "WARNING: achieved $achieved of target $rate ops/s" >> "$out.txt"
				fi
				echo
			fi
		done
	done
done

echo
echo "raw results in $OUTDIR"
