# Go v2 design review round 07: Codecs, types and framing

Reviewer: `/root/design_reconciliation/round07_codecs`. Fix owner: `/root/design_reconciliation`.
Branch: `v2-design-reconciliation`. Scope: design documents only.

## Inspected contracts

API DataType, ValueWriter/Reader, EncodedValue, scalar/empty wrappers, UDT/tuple
composition, lazy containers and vectors; implementation framing/rollback and
limits; local serialization-protocol reference.

## Findings and dispositions

3 findings; 3 fixed; 0 unresolved.

1. **UDT expected identity absent (API:337–339,364–365).** Added required expected Keyspace/Name to UDTOptions, exact identity/type validation at CheckType/Compile and validation errors for missing identity. Made directional evolution explicit: extra database fields encode null/decode checked skips; missing application fields encode ignored/decode explicitly allowed defaults.
2. **Logical Empty mapping missing (API:136–138,147–154,219–228; implementation:124–127).** Added ValueWriter.Empty, supported-type and zero-body checks, unknown-state rejection, and explicit three native states. Empty is logical Present zero bytes, not a new negative sentinel; raw readers normalize to Present, ordinary decoders/EmptyCodec select interpretation, text/blob remain Present.
3. **Tuple empty/suffix ambiguity (API:281–284,362–364; implementation:137–141).** Reviewer proposed removing tuple Empty. That recommendation was not adopted because tuple Empty is already promised. Instead documented codec-selected interpretation: ordinary TupleDecoder applies nullable omitted suffixes; EmptyCodec intercepts zero body as Empty=true. TupleEncoder emits all null field frames, preserving Empty=false for nonzero arity; zero-arity tuple wire equivalence is explicit. No support was removed.

## Additional behaviors checked

Immutable descriptors/frozen annotations; recursive CheckType on null/unset;
scalar widths/text; collection null distinctions; nested unset rejection;
independent Clone/detached Collect; lazy framing versus scalar validation;
terminal iterator errors; checked vector dimensions/width; frame/decompression/depth
limits; rollback before executable binding publication.

## Document identity

| Document | Before SHA-256 | After SHA-256 |
| --- | --- | --- |
| `plan.md` | `d215bced05949448600172082b8095a424a60ddf0294b5fade04a89d35c54814` | `d215bced05949448600172082b8095a424a60ddf0294b5fade04a89d35c54814` |
| `feature-specs/gocql-v2-api.md` | `cfe5d88bb5b26f38b0aced78f006fee355123afd1ab4bca6479bc2795a564903` | `81041900ed119b70e4afdd14a5f7e707dad67837b15bd65b59cb89c37950a38f` |
| `feature-specs/gocql-v2-implementation.md` | `4a39a982901c4b400ac81d1041eb4658bb8405687948fcf2f1e7c2310d181ebd` | `2982fd008a1027f2fcb691d1b18f1bafb8b263c44ef6d6e623cea2f7525f4985` |

Verification: reviewed the corrected passages and their cross-document references; no implementation, fixture, or driver test was run. These results do not establish driver correctness.
