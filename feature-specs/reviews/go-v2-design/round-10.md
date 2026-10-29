# Go v2 design review round 10: Final whole-design consistency

Reviewer: `/root/design_reconciliation/round10_consistency`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

The current complete plan, detailed public API and implementation contract,
including all newly reconciled ownership, signatures, limits/defaults, internal
query dependencies, routing/configuration semantics and release evidence gates.
Root independently flagged the same current-page lifetime issue.

## Findings and dispositions

1 finding; 1 fixed; 0 unresolved.

1. **Session.Close recycles a valid row (API:618–626 versus 962–967).** Reviewer: row views are valid until Next/row Close, but session shutdown releases “current unretained pages.” Preserved the delivered current page's independent output owner until application Next/Close. Shutdown releases only undelivered/prefetched pages; current pages and explicit leases do not delay session/input drains. Next after shutdown releases that owner and reports ErrClosed, preserving earlier terminal errors.

## Additional behaviors checked

Generic signatures and target package boundaries; typed internal queries without
cycles; canonical spans and options drains; detached Clone/Collect; prepared
routing and exact revision/epoch CAS; bounded paging history; tri-state LWT safety;
strict targets and mandatory tablet leaders; pool sizes and capped orphan budgets;
queued cancellations and endpoint generations; context-aware TLS/auth hooks and
nil inheritance; Cassandra-safe timeout defaults; expected UDT identity and tuple
Empty; all 17 sequential branches, fixture lanes and 174 baseline identities.
No additional verified contradiction was found in those checks.

## Final document reconciliation

Marked the ten design cycles complete in plan status/M0, added the results summary
and ledger link, preserved implementation-paused status, and reflowed ordinary
prose without changing code blocks/tables/lists. These final changes precede the
After hashes below. The reports and ledger do not claim implementation completion.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `ee59260dbcd057c2cb1264a7a61705c7bbb9678b0ae7be6757d43403a029787e` | `17a21017eb92c2837cfbcb1e6ca52bb650b464d2eaf803d5b2c75c2429a64d2d` |
| `feature-specs/gocql-v2-api.md` | `0dac706a10a69f476510bb70327142b75f3459b8005bf83693516afa1919436b` | `154a2201c22e8a8ba744e4dfc903998a5addae32dd70ec559d31d88d6c4289f2` |
| `feature-specs/gocql-v2-implementation.md` | `3b51f7c3e420818a537550504326a76beffd643b912d27908182ece7c8f63d74` | `167f40e2d980934bf82351f5de01780910ca3d136f4eb000b6c4872397598524` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
