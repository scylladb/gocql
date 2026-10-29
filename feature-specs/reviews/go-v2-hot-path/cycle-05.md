# Hot-path design review cycle 05: Span count and transport work

Reviewer and fix owner: primary agent `/root`; sequential self-review.
Scope: design documents only. No implementation, benchmark or live-fixture claim.

2 findings; 2 fixed; 0 unresolved findings from this cycle.

## Findings and fixes

1. **Zero-copy scalar output can still generate one iovec/TLS write per field.** Slab sharing was optional and descriptor scratch lifetime unspecified. Required merging adjacent unpublished slab ranges and reusing private write scratch across batches/segments, with reference clearing and original ownership/order preserved. Added explicit span/syscall/TLS amplification evidence; unavoidable tiny borrowed-span costs remain visible.

2. **Span-aware compression/checksums can scan the chain for every byte.** Required forward cursors for sequential work and a once-built descriptor-only offset index or bounded position-aware state for nonmonotonic compression matches. This prevents fragmentation-multiplied descriptor work without copying a source dictionary.

## Re-review

Re-reviewed builder rollback/patch/freeze, writev mutation and OS limits, queue cancellation, partial writes, v5 CRC/segment framing, compressed raw fallback and TLS copy exclusions. Coalescing applies only to physically adjacent same-owner bytes; scratch stays owned through write completion.

Verification: reread the changed contracts against the other two documents;
checked that ownership, validation and existing zero-copy requirements remain intact.
Runtime performance remains an implementation acceptance gate.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` | `3c027c2c5f21cc709e9032df200282737118b0c5179f7336f9072ef230b83e7e` |
| `feature-specs/gocql-v2-api.md` | `a0b3aafc30e1a340e75817fa9e2a9d5a42a2242d05a38bf52e41d1c74d6ca743` | `a0b3aafc30e1a340e75817fa9e2a9d5a42a2242d05a38bf52e41d1c74d6ca743` |
| `feature-specs/gocql-v2-implementation.md` | `48da006d33c19304fecb590332599f7fbadf5e3dc0bddcfdb243de785ad9f4a7` | `687f0384990006249d661f4f251cf8ba9037df42fa75b6c60463e0314fc38fa4` |
