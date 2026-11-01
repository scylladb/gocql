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
Preparation accepted at commit `f2fb206`. All 17 steps are accepted. Final runtime
and release evidence: `51d53986b295fa19bedc69526d7602589b7a27f9`.
The [final root audit](FINAL-AUDIT.md) closes the candidate's pending audit gate.

| Step | Branch | State | Agent / committed evidence |
| --- | --- | --- | --- |
| 1 | `v2-01-wire-protocol` | Accepted | `/root/step01_wire_protocol`; `6cfb37a`; [handoff](01-wire-protocol.md) |
| 2 | `v2-02-codecs` | Accepted | `/root/step02_codecs`; `1fc00bf`; [handoff](02-codecs.md) |
| 3 | `v2-03-prepared-paging` | Accepted | `/root/step03_prepared_paging`; `3071430`; [handoff](03-prepared-paging.md) |
| 4 | `v2-04-transport-pools` | Accepted | `/root/step04_transport_pools`; `0a05d36`; [handoff](04-transport-pools.md) |
| 5 | `v2-05-metadata-control` | Accepted | `/root/step05_metadata_control`; `0ca4fd2`; [handoff](05-metadata-control.md) |
| 6 | `v2-06-tablet-routing` | Accepted | `/root/step06_tablet_routing`; `07a3e0e`; [handoff](06-tablet-routing.md) |
| 7 | `v2-07-execution-config` | Accepted | `/root/step07_execution_config`; `f4a5b43`; [handoff](07-execution-config.md) |
| 8 | `v2-08-fixture-harness` | Accepted | `/root/step08_fixture_harness`; `beef9d9`; [handoff](08-fixture-harness.md) |
| 9 | `v2-09-integration-migration` | Accepted | `/root/step09_integration_migration`; `f542da9`; [handoff](09-integration-migration.md) |
| 10 | `v2-10-remove-legacy` | Accepted | `/root/step10_remove_legacy`; `61ae08b`; [handoff](10-remove-legacy.md) |
| 11 | `v2-11-extract-foundations` | Accepted | `/root/step11_extract_foundations`; `c3505d5`; [handoff](11-extract-foundations.md) |
| 12 | `v2-12-extract-runtime` | Accepted | `/root/step12_extract_runtime`; `cdba62a`; [handoff](12-extract-runtime.md) |
| 13 | `v2-13-public-module-docs` | Accepted | `/root/step13_public_module_docs`; `75f015a`; [handoff](13-public-module-docs.md) |
| 14 | `v2-14-full-matrix` | Accepted | `/root/step14_full_matrix`; `5edeb77`; [handoff](14-full-matrix.md) |
| 15 | `v2-15-performance` | Accepted | `/root/step15_performance`; `7a358b2`; [handoff](15-performance.md) |
| 16 | `v2-16-independent-review` | Accepted | `/root/step16_independent_review`; `bc3d9fe`; [handoff](16-independent-review.md) |
| 17 | `v2-17-release-verification` | Accepted | `/root/step17_release_verification`; `51d5398`; [handoff](17-release-verification.md), [root acceptance](FINAL-AUDIT.md) |

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


Step 04 accepted:42 committed source/document hashes and13 successful final
command/lane logs verified. Later metadata/session lifecycle and public
RunStream obligations remain explicit in its handoff.

Execution efficiency: future agents start with no inherited conversation and
read bounded plan/spec/handoff context; coordinator batches gate review, limits
log output, and repeats checks only after relevant changes/failures.

Step 05 accepted: committed source hashes and nine successful final check/lane
log hashes verified. Native task-owned Scylla/Cassandra clusters stopped/joined;
ports are closed. Reuse/provision via the retained step05 fixture manifest and
runner; do not assume the old container fixtures are running.

Step 08 accepted:580 committed source hashes and10 final/affected check log hashes verified. Ten native package passes/no skips and20 test/cleanup log hashes verified; all nodes joined and endpoints refused. Full174 migration09 and final-wide matrices14/17 remain.

Step 09 accepted:582 committed source hashes,7 check log hashes,12 native packages and all174 effective behavioral passes verified. Original29 file hashes and174 original/translated test body hashes match; all-skipped parents excluded. All owned nodes joined/endpoints refused. Final-wide matrices14/17 remain.

Step10/13 boundary: benchmark callers must migrate/compile in10. Old replay captures encode historical control page5000+timestamp rather than current typed page1000/no timestamp and topology setup. Historical capture integrity is not operational replay evidence. Step13 MUST regenerate functioning typed-control captures and restore equivalent replay workload/assertions during benchmark module consolidation; record current mismatches/failures, never silently drop coverage.

Step10 accepted:15 artifact hashes/412 committed files/16 check digests verified (15pass, historical replay failure explicitly13).24 reconstructed header-only changes preserve exact package bodies/build constraints;21 match native→final delta. Six native packages/cleanup/all6 node joins verified,158 current effective baseline passes;16 development identities explicitly14/17. All174 test bodies preserved,177 deleted-source dispositions verified; streaming native scalar/skip adapters and unused legacy serializers reviewed.

Step11 accepted:18 artifact hashes/455 committed source hashes/16 check digests verified (15success, initial vet diagnostic resolved). Independently reconstructed39 exact go/types-bound renames,2 gofmt transforms and original doc-only restoration. Actual lower import graph, nongeneric model/direct response destination and4 baseline rewrites reviewed. All174 bodies and957 declaration relocations verified;4 native packages+cleanup/all6 nodes joined/refused. Full174 final matrix14/17 remains.

Step12 candidate657debe remains UNACCEPTED pending transitive settings dependency fix: transport/pool→settings→routing→metadata violates lower-contract boundary despite direct-edge audit0. Same12 worker resumed. Other batched evidence verified:26artifact hashes,618 committed inputs,11successful check logs,exact cosmetic delta,174 body sources/hashes,49collision sources/hashes,successful7native packages+cleanup/6node joins,158 effective baseline passes.16development identities remain14/17. No repeat unchanged proof absent delta.

Step12 accepted atcdba62a: lower connectivity projection removes transitive transport/pool→fullsettings→routing→metadata edges; actual closure independently verified0.31artifact digests/622committed sources/9freshcheckdigests verified,174+49proof artifacts unchanged,2TLS/Unix bodies+7contracts preserved, stale inventory hash corrected against actual unchanged source. Fresh7native packages+cleanup pass,6nodesjoined/42endpoints independently refused,158effective baseline passes/missing16development explicitly14/17.13 must complete singlemodule including old standaloneLZ4 dependency and operational replay capture/workload repair.

Step13 candidate583b274 remains UNACCEPTED pending new-workload namespace/cleanup ownership and ConvictionPolicy interface nameability fixes. Same13 agent resumed. Other batched candidate proof verified:18artifacts/621inputs/14check logs,174bodies/135helpers/94methods unchanged,3compressor files package-rename-only,9normalconsumerexamples,8capture hashes/60replay case passes20unique,one actual module/no oldedges/lowerclosure0,6native packages+cleanup. Namespace fix focused native passes reported; external policy signature fix in progress.

Step13 accepted75f015a: one module/no old compressor edge,507canonicalexports/90compat removed,31external interface implementations compile. Ownership/ConvictionPolicy blockers fixed. Root verified25artifacts/622committed inputs/eight delta inputs/eightfreshchecklogs,normal external31interface+statefulconsumer rerun,174bodies/135helpers/94methods unchanged,6nativepackages+cleanup,6nodesjoined/42ports refused. Freshownedcorpus has20exactcases/60racepasses,success/error/cancel/killed-child namespaceabsence proven. Original13candidate583b274 remains historical;14mustprove all174 and full streaming matrix on current tree.

Step14 accepted5edeb77:623committed source hashes/README+handoff binding verified.11successfulreceipts/62commandlogs,35JSONnative+cleanup packages,174effectiveidentities+ALLexpectednestedsubcases,403source/helper/methodbodies,22checks,12freshfuzz campaigns/496mutationhashes,9artifact+7toolhashes verified.63ports independentlyrefused. Bytecapacity/lifetime/codefix scope reviewed. Fullmatrix17modes+actualdevv2/global/leader/CCM pass. Steps15–17 remain; finaltreeproof required againafterfixes.

Step15 accepted7a358b2:654committedinputs,720rawartifact hashes,10reporthashes/methodsource,37checks,23requirements(164actualtestrefs+123measurementrefs),60native/baseline/memory/affected/cleanupJSONpackages/12matchedactualmodes,403body/helper/methodsources verified. Root independentlyreproducedv5scopedproof653archive/exact5delta/63ASTbodies/ownerbranch; correctedadapterpaths/per-modepins verified.42portsrefused.8finalScylla sourcef49b+4Cassv5source8e560 scoped—notfullhashsame. Numericalregressions/retention/mixedchurn andsafetylimits explicit.16freshreview/17FULLfinalmatrix remainmandatoryafterproductionoptimizations.

Step16 acceptedbc3d9fe:660committedsourcehashes/12retainedartifact+method+contract/report hashes/12checklogs/258unchangedpreexistingtestfiles/2failingreprologhashes verified. Focused3native modes+cleanup6JSONpackages/sourceboundpass,PIDgone/21portsrefused. Configurable256MiBdefaultlimits nowhonoracceptedint32range;typedlargecounts64bit/386;concurrentlatencyprobeCAS/Observe/expiry/fallback fixed.22independentcoveragegroups/0unresolved reviewed.17MUSTfullfinalmatrix/174nested/stream/fuzz/race/perf/publicdocs/requirementsaudit beforegoalcomplete.

Step 17 accepted at `51d5398`: the independent verifier passed on the final
663-input source pin. Root checked 721 committed source/report/method/handoff
bindings, all 2,790 inventory records, 560 complete assertion sources and 395
implementation guards. Independent parsing of 74 successful package logs
confirmed all 174 original identities and 1,063 expected nested cases. All 17
native modes, 16 streaming groups, 106 configuration fields, 23 performance
rows and 13 fresh fuzz campaigns have final-source evidence. Root independently
confirmed 272 recorded processes absent and 63 owned endpoints closed.
The unknown Cassandra post-DDL timeout, successful strict fresh verification
and measured performance limitations remain explicit in the final audit.
All 17 sequential steps and the required final audit are complete. No push or
publication was performed.
