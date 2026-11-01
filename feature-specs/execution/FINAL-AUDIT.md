# Final root acceptance

All 17 sequential plan steps are accepted. Step 17's candidate at
`51d53986b295fa19bedc69526d7602589b7a27f9` passed root review. This record
closes its pending-root-audit gate; the immutable candidate manifest and handoff
retain their original pre-acceptance status and hashes.

Runtime: 663 inputs, source SHA-256
`74a54ef2ae1485fb2f7065efee1ffb10218efed431f331b0e579f70ba4285180`.
Candidate manifest SHA-256:
`79e60e06a802d3ef8699368f9475e807c051c8a38c9b19fd4fa8bb34895b2854`.

Root ran `python3 feature-specs/tools/step17_verify.py` successfully against
the committed candidate. It checked all 1,831 retained external artifact hashes
and the bound reports, methods and handoff. Root additionally checked 721 Git
blob bindings, documentation hashes, the complete ten-document inventory and
all 2,790 individual dispositions. All 560 complete assertion sources and 395
implementation declarations match the current source; their recorded passes
were checked against actual successful packages. The requirement review
retains explicit distinctions between Go obligations, historical descriptions,
structural examples and reference-contract differences. Broad suite success
alone does not establish a paragraph's claims.

Independent terminal-event parsing of 74 successful package logs confirmed
all 174 original identities and 1,063 expected nested cases. All 17 required
native modes, actual development tablet/leader behavior and CCM passed. The
final evidence also covers 16 streaming criteria and 40 storage shapes, 106
configuration fields, 23 performance rows, race/386/arm64/tag/API/module/docs
checks and 13 fresh fuzz campaigns with 20,817,426 executions. All 17 verifier
negative controls rejected their falsified inputs. Root independently found
all 272 recorded processes absent and all 63 owned endpoints closed.

The source fixes and scoped limits are described in the
[step 17 handoff](17-release-verification.md). One same-source Cassandra v5
raw/TLS materialized-view cleanup timed out after successful DROP; its cause
remains unknown. It supplies no pass credit. One strict fresh unmodified full
package passed at the same assertions and bounds; logging diagnostics supply
no final credit. Performance evidence retains observed regressions, cache and
memory scopes, TLS costs, due-publication allocations and forced-expiry
contention costs. Synthetic large-length checks do not claim physical
multiGiB transfers. No universal speedup or flake-free claim is made.

The root audit script and result are retained outside the candidate artifact
set at `/extra/scylladb/gocql-api-test-cluster/step17/root-final-audit.py` and
`root-final-audit.json`. Their SHA-256 hashes are respectively
`a0aec76e46b21275fb2d699da9257987c7010147b09c02539dbb0ce86ce61f3c` and
`39a655e9484854a8336ef50c80320b6673ddfc9da399b12da88dbdda6ef3a972`.

This acceptance changes execution records only. No push, deployment or
publication was performed.
