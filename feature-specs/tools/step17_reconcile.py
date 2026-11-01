# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Retain explicit scope decisions and primary witnesses for every contract item.

Range assignments are manual review boundaries in the normative documents, not
keyword rules. API code leaves additionally name their actual declarations.
Reference items retain their own text and the explicit Go disposition; Rust
quirks, API syntax, async scheduling and feature flags are never auto-required.
An item with a missing dossier/pass/declaration remains open.
"""
import collections,json,re
from pathlib import Path
import step17_inventory as inventory
import step17_matrix as matrix
import step17_proofs as proofs

ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
# Each range was reconciled against its complete contract section. Repeated
# requirements share the same assertion dossier, with each item identity kept.
RANGES={
 'plan.md':[(1,17,['public-examples','native-behavior']), (18,61,['public-examples','scalar-domains','stream-execution','routing-plan']), (68,101,['profiles-options','setup-hooks','tls-cloud','authentication']), (106,137,['prepared-fences','input-admission','standalone-owned']), (142,219,['paging-destinations','stream-execution','stream-types']), (224,283,['records-composition','scalar-domains','collections','dynamic','standalone-owned']), (289,325,['public-examples','records-composition','prepared-fences','input-admission']), (329,365,['profiles-options','batches','metadata-snapshots','metadata-events','agreement-ddl','observers-optional','tablets-leaders','latency-probes']), (369,410,['input-admission','leases','standalone-owned','transport-lifetime','compression','paging-destinations']), (414,490,['public-examples','prepared-fences','transport-lifetime','metadata-snapshots']), (495,509,['pool-capacity','shards-socket','transport-lifetime']), (513,525,['metadata-snapshots','metadata-events','agreement-ddl']), (529,542,['prepared-fences','tablets-leaders']), (546,773,['native-behavior','public-examples','performance-counters','stream-independent','stream-execution'])],
 'feature-specs/gocql-v2-api.md':[(1,15,['public-examples']), (19,171,['type-descriptors','records-composition','scalar-domains','wire-builder','wire-ranges']), (175,210,['standalone-owned','scalar-domains','text-views']), (214,257,['records-composition','standalone-owned']), (262,338,['scalar-domains','standalone-owned']), (343,447,['text-views','collections','records-composition','leases']), (451,530,['dynamic','type-descriptors']), (535,660,['prepared-fences','input-admission','standalone-owned','routing-tokens']), (665,811,['paging-destinations','leases','records-composition']), (815,1084,['stream-types','stream-execution','stream-reservation','stream-independent','protocol-limits']), (1089,1196,['profiles-options','batches','input-admission','observers-optional']), (1200,1267,['routing-plan','profiles-options','observers-optional']), (1268,1294,['routing-plan','latency-probes','routing-tokens']), (1297,1321,['tablets-leaders','routing-plan']), (1323,1328,['routing-tokens']), (1330,1370,['profiles-options','observers-optional','tablets-leaders']), (1373,1420,['metadata-snapshots','metadata-events','routing-plan']), (1422,1434,['agreement-ddl']), (1436,1448,['input-admission','leases','metadata-events']), (1450,1479,['profiles-options','setup-hooks','tls-cloud','authentication','routes','protocol-limits']), (1484,1520,['setup-hooks','tls-cloud','shards-socket','pool-capacity','stream-reservation'])],
 'feature-specs/gocql-v2-implementation.md':[(1,7,['public-examples']), (11,122,['wire-ranges','wire-builder','leases']), (126,203,['records-composition','standalone-owned','dynamic']), (207,313,['records-composition','paging-destinations','prepared-fences']), (317,359,['metadata-snapshots','metadata-events','agreement-ddl','routes']), (363,387,['profiles-options','observers-optional','input-admission']), (391,419,['prepared-fences']), (423,440,['tablets-leaders']), (444,468,['routing-plan','latency-probes']), (472,493,['collections','dynamic']), (497,660,['transport-lifetime','protocol-limits','compression','leases']), (664,771,['stream-types','stream-execution','stream-reservation','stream-independent']), (775,1014,['profiles-options','setup-hooks','tls-cloud','authentication','shards-socket','pool-capacity','routes','metadata-snapshots','metadata-events','observers-optional','protocol-limits']), (1019,1058,['routing-plan','routing-tokens','tablets-leaders','latency-probes','pool-capacity','transport-lifetime','metadata-snapshots','metadata-events','prepared-fences','profiles-options','batches','collections','scalar-domains','authentication','tls-cloud','routes','observers-optional']), (1062,1119,['public-examples','native-behavior']), (1123,1228,['performance-counters','native-behavior','stream-independent','stream-execution','public-examples']), (1233,1291,['setup-hooks','tls-cloud','shards-socket','pool-capacity','stream-reservation','metadata-events'])],
 'feature-specs/gocql-v2-streaming-receive.md':[(1,40,['stream-execution','stream-independent']), (44,117,['stream-types','stream-execution']), (121,153,['stream-types','protocol-limits','stream-independent']), (157,178,['stream-independent','stream-reservation','compression']), (182,226,['stream-execution','stream-reservation','input-admission']), (230,244,['stream-independent','stream-types']), (248,256,['stream-types','stream-execution','stream-reservation','stream-independent']), (262,282,['stream-types','stream-execution','stream-reservation','stream-independent']), (286,292,['native-behavior','stream-execution']), (296,302,['performance-counters','stream-independent']), (306,359,['stream-types','stream-execution','stream-reservation','stream-independent','native-behavior','public-examples','performance-counters'])],
}
REFERENCE={
 'routing.md':(['routing-plan','routing-tokens','tablets-leaders','latency-probes','input-admission'], 'Go uses canonical encoded bindings, explicit policy Plan/Next and tri-state conditional safety; unknown partitioners disable token routing; no Rust caching-wrapper or all-host preparation API is required. Leader/tablet/latency behavior remains required and has actual current assertions.'),
 'connections.md':(['pool-capacity','shards-socket','transport-lifetime','setup-hooks','tls-cloud','stream-reservation'], 'Go retains pool/shard/reconnect/keepalive capabilities with explicit checked controls. Stable request identity covers queued cancellation; effective orphan capacity is enforced. Concurrent session USE propagation is deliberately replaced by initial/qualified/v5 per-request keyspace. Rust unbounded queues, ignored cancellation races and identity-only diagnostics are not copied as defects.'),
 'metadata-control-events.md':(['metadata-snapshots','metadata-events','agreement-ddl','routes','tablets-leaders'], 'Go exposes immutable deep snapshots and typed internal queries, supports peers_v2 where appropriate, rejects malformed tokens/duplicates and protects complete snapshots. Structured partial errors, independent drop tombstones and DDL-coordinator agreement intentionally strengthen Rust limitations. LifecycleObserver/subscription replace Rust listener/feature syntax.'),
 'execution.md':(['profiles-options','prepared-fences','paging-destinations','batches','input-admission','observers-optional','routing-plan'], 'Go has immutable Statement/Prepared/Bound and explicit QUERY metadata, lazy host prepare and exact local revision/epoch fences. It does not reproduce all-host Rust prepare fanout, mutable statements, timeout gaps, or stale ID chronology. All-member batch admission, terminal callback accounting, conditional safety and bounded exact history are stricter Go requirements.'),
 'serialization-protocol.md':(['type-descriptors','scalar-domains','standalone-owned','records-composition','collections','dynamic','text-views','protocol-limits','compression','stream-types'], 'Go supports native v3/v4/v5, explicit typed/generic accessors and tagged dynamic values without reflection. Strict widths/UTF8/count/depth/state/trailing validation, explicit null/empty/unset and borrowed span ownership replace permissive Rust quirks. Macros/serde/foreign-runtime adapters are language-specific; typed composition/custom interfaces provide the Go feature.'),
 'configuration-security-observability.md':(['profiles-options','setup-hooks','tls-cloud','authentication','routes','observers-optional','pool-capacity','shards-socket'], 'Go snapshots configuration, uses crypto/tls with logical verified identity and per-connection auth exchanges including Close. Source-port, socket, route, reporting, tracing and observer features remain real requirements. Cargo/TLS-backend alternatives, Rust history syntax, unchecked casts and fresh-per-phase deadline gaps are not required Go defects.'),
}

def build():
    catalog=proofs.build();declarations=matrix.read(OUT/'current-declarations.json')
    manual_path=E/'step17-manual-audit.json'
    manual=matrix.read(manual_path)if manual_path.exists()else None
    confirmations={r['item_id']:r for r in manual['records']}if manual and manual['runtime_source_sha256']==catalog['runtime_source_sha256']else {}
    production=[d for d in declarations if not d['File'].endswith('_test.go')]
    docs=[inventory.extract(name) for name in inventory.DOCS]
    decisions={};open_items=[]
    for doc in docs:
        for item in doc['items']:
            line=item['start_line'];name=item['document'];scope=item['scope']
            groups=[];explanation=''
            if name in RANGES:
                assignment=next((g for first,last,g in RANGES[name] if first<=line<=last),None)
                if assignment:groups=assignment
                if (name=='feature-specs/gocql-v2-implementation.md'and 775<=line<=1014)or(name=='feature-specs/gocql-v2-api.md'and 1089<=line<=1520):groups=list(groups)+['release-config-oracles']
                explanation='Reviewed Go contract scope. The attached dossiers state the exact tested invariants and retain full assertion sources/current effective package receipts. Repeated statements share proof, not pass counts.'
            else:
                groups,explanation=REFERENCE[Path(name).name]
                if scope=='reference-source':
                    explanation='Pinned Rust source/test/provenance description, not an additional Go API or defect-reproduction obligation. '+explanation
            symbols=[]
            artifact_witnesses=[]
            for target in re.findall(r'\]\(([^)#]+)(?:#[^)]*)?\)',item['text']):
                if '://' in target:continue
                path=(ROOT/Path(name).parent/target).resolve()
                if path.is_file():artifact_witnesses.append(dict(path=str(path.relative_to(ROOT)),sha256=matrix.sha(path)))
            historical=(name=='plan.md' and (line<18 or 635<=line<=672 or line>=744)) or (name=='feature-specs/gocql-v2-implementation.md' and 1062<=line<=1119)
            if historical:
                explanation='Historical design/staging/provenance statement; not an assertion of final runtime behavior. Required carried-forward features are discharged by the linked current dossiers and the 174/streaming release ledgers. Retained artifact bytes are separately bound. '+explanation
            if item['kind']=='code':
                # Exact declarations are additional primary shape evidence;
                # illustrative local variables/internal pseudocode are scoped
                # below, never mistaken for missing public functionality.
                names=set(re.findall(r'\b[A-Z][A-Za-z0-9_]*\b',item['text']))
                if item.get('code_owner'):names.add(item['code_owner'])
                for d in production:
                    if d['Name'] in names:
                        symbols.append(dict(file=d['File'],line=d['Line'],name=d['Name'],receiver=d['Receiver'],sha256=d['SHA']))
                explanation+=' Code is checked against the current declaration witnesses; conceptual private seams and application example locals express behavior rather than mandatory old file/function names.'
            missing=[g for g in groups if catalog['groups'][g]['status']!='current-assertions-passed']
            # This is a source-navigation index inside the manually reviewed
            # dossier scope. Exact name occurrence never establishes a pass.
            named=set()
            for fragment in re.findall(r'`([^`]+)`',item['text']):
                named.update(re.findall(r'\b[A-Z][A-Za-z0-9_]*\b',fragment))
            if item['kind']=='code':named.update(re.findall(r'\b[A-Z][A-Za-z0-9_]*\b',item['text']))
            indexed=[]
            for identity in sorted({identity for group in groups for identity in catalog['groups'][group]['tests']}):
                witness=catalog['assertions'][identity]
                matches=sorted(n for n in named if re.search(r'\b'+re.escape(n)+r'\b',witness['complete_assertion_source']))
                if matches:indexed.append(dict(assertion_identity=identity,named_symbols=matches))
            indexed=sorted(indexed,key=lambda r:(-len(r['named_symbols']),r['assertion_identity']))[:8]
            status='open' if not groups or missing else 'primary-evidence-mapped'
            if scope=='structural-delimiter':status='structural-no-obligation'
            # A current assertion dossier is necessary, but separate integration,
            # fuzz, performance, API and artifact gates must all pass too.
            extra=[]
            if name=='plan.md' and 414<=line<=477:extra=['step17-architecture.json']
            if name=='plan.md' and 583<=line<=631:extra=['step17-checks.json','step17-lanes.json','step17-fuzz.json','step17-cleanup.json']
            if name=='feature-specs/gocql-v2-implementation.md' and 775<=line<=1014:extra=['step17-config-audit.json']
            if name=='feature-specs/gocql-v2-implementation.md' and 1123<=line<=1228:extra=['step17-performance.json','step17-verification.json']
            if name=='feature-specs/gocql-v2-streaming-receive.md':extra=['step17-streaming-acceptance.json','step17-performance.json']
            decision=dict(text_sha256=item['text_sha256'],status=status,proof_groups=groups,proof=['step17-proof-catalog.json#/groups/'+g for g in groups]+extra,declaration_witnesses=symbols,assertion_navigation_index=indexed,navigation_index_scope='Exact named-symbol locations within manually selected primary assertion dossiers; not keyword-based achievement.',artifact_witnesses=artifact_witnesses,historical_or_staging_scope=historical,disposition=explanation,missing_current_dossiers=missing)
            decisions[item['id']]=decision
            confirmation=confirmations.get(item['id'])
            if confirmation:
                assert confirmation['text_sha256']==item['text_sha256']
                decision['individual_scope_confirmation']=dict(artifact='step17-manual-audit.json',item_id=item['id'],classification=confirmation['status'],confirmed_claims=confirmation.get('confirmed_claims',[]),implementation_guards=confirmation.get('implementation_guards',[]),exact_assertions=confirmation.get('assertions',[]),go_disposition=confirmation.get('go_disposition'),review=confirmation['review'],missing_current_passes=confirmation['missing_current_passes'],missing_artifacts=confirmation['missing_artifacts'])
            else:decision['individual_scope_confirmation']=dict(classification='open',review='Navigation mapping alone does not establish individual requirement achievement.')
            if status=='open':open_items.append(item['id'])
    (OUT/'inventory-dispositions.json').write_text(json.dumps(decisions,sort_keys=True,indent=2)+'\n')
    inventory.main()
    print(json.dumps(dict(items=len(decisions),open=len(open_items),open_items=open_items[:40])))
if __name__=='__main__':build()
