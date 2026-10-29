# Go v2 hot-path design review ledger

Completed ten sequential review-and-fix cycles: **19 findings fixed**. Stopped
at the requested ten-cycle limit. The tenth cycle found two reconciliation
issues and fixed them; no additional independent clean cycle is claimed.
No identified finding remains open after the recorded fixes.

All cycles were performed by the primary agent `/root` as self-reviews. These
are additional performance-design reviews, separate from the historical
[33-finding architecture review](../go-v2-design/). Each report records the
inspected contracts, finding/fix rationale and chained before/after SHA-256
identities for all three documents. Historical reports were preserved.

The scope was [plan.md](../../../plan.md), the
[API contract](../../gocql-v2-api.md), and the
[implementation/acceptance contract](../../gocql-v2-implementation.md).
Implementation remains paused. No driver code or existing runtime edits were
changed, and no speedup is asserted from documentation alone.

| Cycle | Review focus | Findings fixed |
| --- | --- | --- |
| [01](cycle-01.md) | Fragmented ranges and physical row access | 2 |
| [02](cycle-02.md) | Compiled codecs and result-plan reuse | 2 |
| [03](cycle-03.md) | Lazy container work and dynamic access | 2 |
| [04](cycle-04.md) | First-target routing and latency selection | 2 |
| [05](cycle-05.md) | Span count and transport work | 2 |
| [06](cycle-06.md) | Paging history and prepared-cache scope | 2 |
| [07](cycle-07.md) | Admission, observation and scheduler overhead | 2 |
| [08](cycle-08.md) | Feedback publication and ownership atomics | 2 |
| [09](cycle-09.md) | Enforce performance contracts during implementation | 1 |
| [10](cycle-10.md) | Whole-design cost and lifetime reconciliation | 2 |

The revised contracts require shared fragmented ranges and indexed nested skips,
streaming typed rows, compiled nested codecs and stable result plans, lazy
routing with published latency decisions, amortized transport descriptors,
hash-indexed paging history, direct warm prepared-state access, disabled-feature
fast paths, structurally shared feedback updates and page-level ownership.
Focused performance checks now gate each relevant implementation handoff;
final-tree performance and release checks remain mandatory.

The final nested-range bound distinguishes O(N + S) nonoverlapping streaming
from O(J log S) additional nested/random jumps over one shared O(S) span index.
Temporary row-reader scratch and escaping DynamicRow indices have distinct
ownership. See cycle 10 for both final reconciliations.

## Validation

Documentation-only validation checks the ten-report count, 19-finding total,
continuous SHA-256 chain from the starting HEAD documents to the final files,
local link targets, balanced code fences, whitespace and the historical design
review verifier. Runtime benchmarks, compilation and database tests were not
run: this task changes design, and the new cost bounds remain implementation
acceptance gates.
