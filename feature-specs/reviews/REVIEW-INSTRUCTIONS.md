# Instructions for independent reference-spec review rounds

The assigned section, round number, and unique agent identity are supplied in the
task. Own only that section's Markdown file and its report. Other agents are
working in the repository; preserve their work. Do not spawn child agents.

Read `feature-specs/README.md`, the complete assigned specification, and the
relevant implementation in `/extra/scylladb/gocql-rust-reference-f8b0ee9` pinned
to `f8b0ee928dc0dcb475247dc82cce4e42804b0621`. Independently trace behavior from
source; prior reviews are context, not proof. This is a review AND fix assignment:
edit the spec to correct verified mistakes and omissions, but do not change Go
or Rust implementation. Capture the spec's SHA-256 before and after the review.

Every round checks the whole section, including language-independent behavior,
Rust evidence, subfeatures, invariants, and test claims. The additional focus
below is a way to find different defects, not permission to ignore other parts.

| Round | Additional focus |
| --- | --- |
| 01 | Trace claims to implementation; correct factual mismatches. |
| 02 | Defaults, exact conditions, limits, ordering, and fallback boundaries. |
| 03 | Errors, partial success, failover, retry, and recovery. |
| 04 | Concurrency, cancellation, snapshot/lifetime boundaries, and races. |
| 05 | Test expectations and whether referenced tests actually cover claims. |
| 06 | Omitted public features, optional features, and less common paths. |
| 07 | Link/anchor accuracy and precise implementation versus contract language. |
| 08 | Cross-section consistency; follow callers and callees across boundaries. |
| 09 | Adversarial edge cases and source quirks that defeat broad statements. |
| 10 | Final comprehensive fidelity and completeness audit. |

Write `feature-specs/reviews/<section>-round-<NN>.md` with:

- Section, round, agent identity, source commit, before/after SHA-256.
- Files and key symbols actually inspected, with line references where useful.
- Verified findings and fixes, each with the source evidence and what changed.
- Explicit checked behaviors even when there are no findings.
- Test coverage claims checked, and any uncertainty or remaining gap.

Do not manufacture findings or edits. A zero-finding round is valid if supported
by actual independent checking. Do not imply execution of tests that you only
read. Flag cross-section problems to the parent instead of editing another
agent's file. Finish with the report path and concise findings summary.
