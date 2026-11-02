Candidate 2: native streaming read-ahead

Hypothesis declared before workload timing: reduce socket/deadline work on the
canonical paged streaming workload (four workers; 128 rows of 4 KiB; page size32;
four populated pages and terminal empty page). CPU/op is the primary metric.
Synthetic source-read counting is mechanism evidence, not acceptance evidence.

The exclusive reservation owns lazily allocated native-v3/v4 scratch capped by
min(ReadAheadBytes, 4 KiB, observed body size). The fill target starts at256 bytes
and doubles only after full progress, up to that bound. Small metadata/scalar
reads use scratch. Large destination reads bypass it after draining any prefix.
Only checked body bytes are prefetched. The next header remains on the raw
reader, and v5 continues through its existing CRC-verified unit reader.

The underlying readAvailable method arms the same socket deadline but performs
one Read, so speculative capacity cannot wait to fill. It starts no goroutine.
Reservation close clears scratch before restoring the shared reader. Working
storage accounting includes retained scratch. The existing ReadAheadBytes=0
normalization (32KiB default budget) is unchanged. Actual default retention
increases; default-change validation is required for retention as a default.

Three existing tests required every raw blob byte to land directly in caller
storage. Their assertions now allow at most a4KiB prefix, still require bulk
reads in caller storage, and retain the original total-working-storage ceilings.
The new tests cover a one-byte net.Pipe progress return, configured caps, exact
body boundaries, delayed partial-read errors, deadline-arm failures, invalid
headers before body reads, scratch reuse, and release before shared admission.

Default tests and focused transport/frame/stream race checks passed. Full race
checks then exposed a cancellation-entry scheduling race (6 failures/30 candidate
repetitions versus0/30 baseline repetitions). The failure evidence is preserved.
Both InternalWithIO closing entry gates now preserve the recorded winning
cancellation cause, matching their existing postcall behavior; manual close
continues to return ErrClosed. Deterministic gate tests,100 repetitions of the
original page-cancel race test, the full default suite, full race suite and vet
all pass on the final candidate. No commit or canonical workload measurements
were made by this worker; parent owns timing and final acceptance.
