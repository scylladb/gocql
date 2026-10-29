# Sequential implementation status

User authorization: active goal resumed on 2026-10-29. Execute the complete
plan using a fresh agent for each of its 17 steps, in order. An unmet handoff
gate keeps that step active. Historical evidence is a checkpoint, not proof of
completion on the final tree.

Preserved starting point: commit `661f169` on `v2-execution-checkpoint`.
The full workspace, including ignored test fixture files, is also archived at
`/extra/scylladb/gocql-v2-execution-checkpoints/workspace-before-execution-20261029.tar.gz`.
Archive SHA-256: `54f68b3d4c34e1c1b77a6dec28eff531a84720b7da0040c4fbc4d79209cba2e2`.

Preparation: streaming signatures/task reconciliation settled; unit race
baseline and specification traceability verifier passed. The focused API
preflight budget clarification has been applied.
Preparation accepted at commit `f2fb206`. Steps 01 and 02 are accepted at `6cfb37a` and `1fc00bf`; step 03 is active.

| Step | Branch | State | Agent / committed evidence |
| --- | --- | --- | --- |
| 1 | `v2-01-wire-protocol` | Accepted | `/root/step01_wire_protocol`; `6cfb37a`; [handoff](01-wire-protocol.md) |
| 2 | `v2-02-codecs` | Accepted | `/root/step02_codecs`; `1fc00bf`; [handoff](02-codecs.md) |
| 3 | `v2-03-prepared-paging` | Active | `/root/step03_prepared_paging`; predecessor `1fc00bf` |
| 4 | `v2-04-transport-pools` | Pending | — |
| 5 | `v2-05-metadata-control` | Pending | — |
| 6 | `v2-06-tablet-routing` | Pending | — |
| 7 | `v2-07-execution-config` | Pending | — |
| 8 | `v2-08-fixture-harness` | Pending | — |
| 9 | `v2-09-integration-migration` | Pending | — |
| 10 | `v2-10-remove-legacy` | Pending | — |
| 11 | `v2-11-extract-foundations` | Pending | — |
| 12 | `v2-12-extract-runtime` | Pending | — |
| 13 | `v2-13-public-module-docs` | Pending | — |
| 14 | `v2-14-full-matrix` | Pending | — |
| 15 | `v2-15-performance` | Pending | — |
| 16 | `v2-16-independent-review` | Pending | — |
| 17 | `v2-17-release-verification` | Pending | — |

Each accepted handoff must record the branch and commit; scope and changed
behavior; exact validation commands, exit statuses and evidence paths;
performance/copy checks for that seam; and an explicit gate assessment.

Final verification must separately prove the API/ownership requirements,
streaming receive acceptance criteria, package dependency rules, required
configuration/routing functionality, and a real successful applicable lane for
each of the 174 baseline tests. Skips, compile-only checks and old-tree passes
do not discharge those requirements.

Known later follow-ups from step 02: full-root GOARCH=386 compilation fails
in unchanged legacy serialization constants (step 10 removal audit); benchmark
submodule needs manifest consolidation (step 13). Neither diagnostic is counted
as a successful required lane. See step 02 evidence for exact retained results.
