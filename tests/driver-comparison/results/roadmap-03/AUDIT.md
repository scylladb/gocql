# Candidate 3: fused built-in scalar row decoding

Prototype at isolated baseline worktree `.codex-cache/driver-comparison/roadmap-03-source`.
Full patch, changed sources and complete source hashes are archived here. No commit
or primary checkout driver changes were made by this agent.

Primary workload: canonical paged buffered reads. Target: at least 3% CPU/op
reduction, subject to paired confidence interval and all acceptance controls.
The decode-only 32-row microbenchmark is screening evidence, not an accepted
workload improvement. CPU10-15 keeps compilation/testing off the measurement CPUs.

The private `readColumnInto` hook recognizes built-in scalar codecs. Integer
codecs validate the full cell, then consume the scalar from the shared row cursor.
Blob codecs create only the final borrowed view. Custom codecs and `ReadColumn`
retain the generic immutable `ValueReader` API. Other scalar codecs use the
same existing scalar decode after the hook dispatch. Compilation still captures
immutable scalar configuration. No public API changes.

Framing length and full body availability are validated before cursor advancement
becomes permanent. Malformed frames roll back both cell position and column
index. Null, type mismatch, and wrong width consume the same cell/index as the
generic decoder and leave destinations unchanged. Integer decoding retains frame,
present-value, and cursor lease guards. Blob views retain the original page lease.
Retained page access survives one release and fails after final release. Custom
callbacks receive an independent snapshot that remains at the first cell after
subsequent fused reads. Optional missing columns keep their null semantics.

This differs from rejected fixedCursor/lease-check experiments: fixedCursor is
unchanged, lease guards are preserved, and value wrapper construction is bypassed
only for known built-in cell decoders using the shared row cursor.

Memory budget: no extra per-row/page/request objects or response retention;
scalar codec descriptors gain one 8-byte function pointer. The benchmark has
identical 160B/2 allocations per 32-row page in both paths. That allocation is
row reader/cursor scope, not a new fused allocation.

Focused codec semantic tests compare exact errors, column index, remaining
cursor bytes, and values against the generic fallback for byte-fragmented and
contiguous cells; all signed widths, counters, null/unset/invalid lengths,
truncation, width mismatch, metadata type mismatch, expired leases, missing
columns, nil destinations, and custom decoder snapshot lifetime are covered.

Acceptance is pending parent-orchestrated full workload screens, paired
confirmation, real Scylla validation and regression controls.

Final exact-source default tests, full unit-tag race tests and full vet all
passed; exit codes/commands are retained in `final-checks.json`. Initial unit
race failure from absent ignored PKI fixtures is preserved separately in
`unit-race-missing-fixtures.log`; fixture provenance is hashed. The final
microbenchmark uses a frozen test binary identified in `microbench-summary.json`.
