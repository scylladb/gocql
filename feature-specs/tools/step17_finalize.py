# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Build a complete release candidate from retained final-pin primary evidence."""
import json,shutil,subprocess
from pathlib import Path
import step17_acceptance,step17_architecture,step17_cleanup,step17_config_audit
import step17_fuzz_evidence,step17_performance_evidence,step17_preservation,step17_reconcile
import step17_manual_audit
import step17_profiles
import step17_matrix as matrix
import step17_verify
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E

def build():
    source=matrix.read(OUT/'source-frozen.json');binding=matrix.sha(OUT/'source-frozen.json')
    assert matrix.read(OUT/'checks/source.json')==source==matrix.read(OUT/'performance/source.json')
    shutil.copy2(OUT/'checks/declarations.log',OUT/'current-declarations.json')
    mapping,streaming=step17_acceptance.build()
    performance=step17_performance_evidence.build()
    step17_profiles.build()
    architecture=step17_architecture.build();config=step17_config_audit.build()
    assert config['gate']=='current-source-witnesses-complete'
    step17_reconcile.build()
    inventory=matrix.read(E/'step17-requirement-inventory.json');assert not inventory['counts'].get('open',0)
    fuzz=step17_fuzz_evidence.build();preservation=step17_preservation.build();cleanup=step17_cleanup.build()
    modes=[]
    for name,receipt in mapping['native'].items():
        lane=receipt['lanes'][0]
        for mode in receipt['configuration']['lanes'][0]['modes']:
            command=next(c for c in receipt['commands']if c['name']==lane['name']+'-'+mode['name'])
            cleaned=next(c for c in receipt['commands']if c['name']==command['name']+'-cleanup')
            events=matrix.events(command['log']);skips=[]
            for event in events:
                if event.get('Action')=='skip'and event.get('Test'):
                    skips.append(dict(test=event['Test'],package=event['Package'],output=''.join(e.get('Output','')for e in events if e.get('Test')==event['Test'])))
            modes.append(dict(receipt=name,mode=mode['name'],arguments=mode['args'],selected_tests=mode['tests'],command=command,cleanup=cleaned,source_sha256=binding,server=receipt['configuration']['lanes'][0],joined=lane['joined'],skips=skips,passed_events=sum(e.get('Action')=='pass'and bool(e.get('Test'))for e in events)))
    matrix.write(E/'step17-lanes.json',dict(source_sha256=binding,modes=modes))
    checks=dict(source_sha256=binding,checks=matrix.read(OUT/'checks/checks.json'),performance=matrix.read(OUT/'performance/checks.json'),supplemental=matrix.read(OUT/'supplemental-checks.json'),complete_tag_union=matrix.read(OUT/'complete-tag-checks.json'))
    matrix.write(E/'step17-checks.json',checks)
    # Bind documentation/build/release inputs separately from native inputs.
    names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0')
    docs={name:matrix.sha(ROOT/name)for name in sorted(set(names))if name and(ROOT/name).is_file()and(name.startswith('docs/')or name in ['README.md','CONTRIBUTING.md','RELEASING.md','Makefile','plan.md','tests/fixtures/README.md'])}
    matrix.write(E/'step17-documentation.json',dict(runtime_source_sha256=binding,inputs=docs,executed_checks=[c for c in checks['checks']if c['name']in ['spec-verifier','docs-version-tests','docs-build','public-contracts']],scope='All nine cookbook examples compile as normal public consumers; four output-bearing examples execute in normal Go suites. The standalone seeded nested streaming CLI executes against every applicable native mode, with explicit unsupported-compression behavior and cleanup.'))
    manual=step17_manual_audit.build()
    assert manual['gate']=='Go-contract-paragraphs-reviewed'and not manual['problems']
    step17_reconcile.build()
    candidate=dict(branch='v2-17-release-verification',accepted_predecessor='bc3d9fe132c4c1f5dc16050870cfd4991d322a73',gate='passed-candidate-pending-root-audit',runtime_source_sha256=binding,runtime_inputs=len(source),original_identities=174,mandatory_native_modes=17,streaming_groups=16,requirement_items=2790,config_option_fields=106,fuzz_campaigns=13,performance_rows=23,normal_public_exports=507,thin_forwarders=130,goal_complete=False,root_audit_required=True,source_scope='One complete final663-input pin after all changes. Earlier660/662-source receipts remain diagnostics with their own sources, no final credit. Docs, evidence and verifier tooling are separately hashed.',findings=[dict(id='S17-1',scope='test oracle',status='fixed',description='One mutable tablet snapshot was treated as permanent authority. The original failure correlates with migration of its exact token range; sole cause was not established. Current oracle brackets every routed query with agreeing non-transitioning server reads on every node, requires eight consecutive exact current cache/coordinator matches in96total attempts, and rejects duplicates.'),dict(id='S17-2',scope='production latency policy',status='fixed',description='Valid positive MaxDuration RetryPeriod/UpdateInterval wrapped absolute UnixNano deadlines. Before probes32/32 and premature publication reproduced. Full Time probe deadline uses existing per-host sample mutex; immutable full Time publication deadline uses CAS. No duration clamp/rejection, policy-wide lock or new timer.')],limitations=performance['limits']+['Saved fuzz mutation corpus is not every in-memory execution.','A physical multiGiB native transfer is not claimed; signed-int32 framing/large completion checks use synthetic logical backing and actual386execution.','The final candidate requires the root agent audit before goal completion; no push, remote publication, workflow or external message is authorized.'])
    candidate['observed_same_pin_failure']=dict(receipt=str(OUT/'cassandra-v5-no-compression-tls.json'),sha256=matrix.sha(OUT/'cassandra-v5-no-compression-tls.json'),scope='Protected materialized-view recreation final DROP succeeded; automatic agreement/refresh deadline failed. Cause unknown; no causal source defect established. Original failed package supplies no credit. Focused/full logging diagnostics perturb scheduling and supply no final credit. One explicit fresh unmodified full package is required at identical pin/assertions/bounds; no retry-until-green loop or successful-DDL replay.',selected_verification=matrix.read(OUT/'native-selected.json')['selected']['cassandra-v5-no-compression-tls'])
    candidate['repository_artifacts']={str(p.relative_to(ROOT)):matrix.sha(p)for p in E.glob('step17-*.json')if p.name not in ['step17-verification.json','step17-negative-controls.json']}
    methods=list((ROOT/'feature-specs/tools').glob('step17_*.py'))+[ROOT/'feature-specs/tools'/name for name in ['step15_native.py','step15_public.py','step13_public_audit.go','step12_declarations.go','step12_dependency_audit.py','step15_text_identity.py','step15_vector_overlay.py','step15_envelope_overlay.py','step15_segment_overlay.py','step15_heap_overlay.py','step15_evidence.py']]
    candidate['method_sources']={str(p.relative_to(ROOT)):matrix.sha(p)for p in methods}
    candidate['external_artifacts']={str(p):matrix.sha(p)for p in OUT.rglob('*')if p.is_file()and '__pycache__'not in p.parts and p.name not in ['finalize.log','verify.log','commit-binding.json']}
    for receipt in mapping['native'].values():
        candidate['external_artifacts'][receipt['source_snapshot']]=matrix.sha(receipt['source_snapshot'])
        for lane in receipt['lanes']:
            for path,digest in lane['configs'].items():
                assert matrix.sha(path)==digest
                candidate['external_artifacts'][path]=digest
        configuration=receipt['configuration']['lanes'][0]
        binary=Path(configuration['install'])/configuration['binary']
        assert matrix.sha(binary)==configuration['sha256']
        candidate['external_artifacts'][str(binary)]=configuration['sha256']
        local_install=Path(receipt['fixture_root'])/(configuration['name']+'-install')
        if configuration['kind']=='cassandra':
            for path in [local_install/'lib/gocql-fixture.jar']+list((local_install/'classes').rglob('*.class')):
                candidate['external_artifacts'][str(path)]=matrix.sha(path)
    candidate['handoff_sha256']=matrix.sha(ROOT/'feature-specs/execution/17-release-verification.md')
    result=step17_verify.verify(candidate)
    negative=step17_verify.negative_controls(candidate)
    candidate['negative_verifier_controls']=dict(count=len(negative),path='feature-specs/evidence/step17-negative-controls.json',sha256=matrix.sha(E/'step17-negative-controls.json'))
    matrix.write(E/'step17-verification.json',candidate)
    print(json.dumps(dict(**result,raw_artifacts=len(candidate['external_artifacts']),reports=len(candidate['repository_artifacts']),negative_controls=len(negative))))
    return candidate
if __name__=='__main__':build()
