# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Independently validate retained release evidence; falsified inputs fail closed."""
import argparse,copy,hashlib,json,re
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E

def verify(candidate,reader=matrix.read,hasher=matrix.sha):
    source=reader(OUT/'source-frozen.json');binding=hasher(OUT/'source-frozen.json')
    assert binding==candidate['runtime_source_sha256']and len(source)==candidate['runtime_inputs']
    for name,digest in source.items():assert hasher(ROOT/name)==digest,'runtime source drift '+name
    for name,digest in candidate['repository_artifacts'].items():assert hasher(ROOT/name)==digest,'repository artifact drift '+name
    for name,digest in candidate['external_artifacts'].items():assert hasher(name)==digest,'external artifact drift '+name
    for name,digest in candidate['method_sources'].items():assert hasher(ROOT/name)==digest,'method source drift '+name
    assert hasher(ROOT/'feature-specs/execution/17-release-verification.md')==candidate['handoff_sha256'],'handoff drift'
    mapping=reader(E/'step17-test-mapping.json')
    assert mapping['source_sha256']==binding and mapping['gate']=='passed'and not mapping['problems']
    assert mapping['identities']==mapping['current_effective_native_passes']==174
    expected={row['file']+'::'+name for row in reader(ROOT/'feature-specs/integration-baseline.json')['files']for name in row['tests']}
    assert {row['identity']for row in mapping['tests']}==expected
    for row in mapping['tests']:
        assert row['complete_source_unchanged']and row['current_effective_passes']and not row['missing_subcase_passes']
        assert set(row['expected_translated_subcases'])<=set(row['effective_subcase_passes'])
    for rows in mapping['preservation'].values():assert all(row['same_complete_source']for row in rows)
    lanes=reader(E/'step17-lanes.json')['modes'];assert len(lanes)==17
    expected_modes={'stable':{'v4-'+c+suffix for c in ['no-compression','snappy','lz4']for suffix in ['','-tls']}|{'v3-maintenance-stream'},'development':{'v4-development'},'stable-v3tls':{'v3-tls-stream'}}
    expected_modes.update({'cassandra-'+mode:{mode}for mode in ['v4-no-compression','v4-lz4','v4-no-compression-tls','v4-lz4-tls','v5-no-compression','v5-lz4','v5-no-compression-tls','v5-lz4-tls']})
    assert {(r['receipt'],r['mode'])for r in lanes}=={(lane,mode)for lane,modes in expected_modes.items()for mode in modes}
    for row in lanes:
        assert row['source_sha256']==binding and row['command']['exit_code']==row['cleanup']['exit_code']==0
        assert row['joined']and all(row['joined'].values())
        assert hasher(row['command']['log'])==row['command']['sha256']and hasher(row['cleanup']['log'])==row['cleanup']['sha256']
    checks=reader(E/'step17-checks.json')
    assert checks['source_sha256']==binding and len(checks['checks'])==38 and len(checks['performance'])==19
    for row in checks['checks']+checks['performance']+checks['supplemental']:
        assert row['exit_code']==0 and row['source_sha256']==binding and hasher(row['log'])==row['sha256']
    union=reader(OUT/'complete-tag-checks.json');assert len(union)==3 and all(not r['exit_code']and r['source_sha256']==binding for r in union)
    streaming=reader(E/'step17-streaming-acceptance.json')
    assert streaming['source_sha256']==binding and streaming['gate']=='passed'and not streaming['problems']
    assert len(streaming['criteria'])==16 and len(streaming['storage_measurements'])==40 and len(streaming['live_modes'])==8
    assert all(r['complete_interruption_metadata_event_recovery']for r in streaming['live_modes'])
    fuzz=reader(E/'step17-fuzz.json');assert fuzz['runtime_source_sha256']==binding and len(fuzz['campaigns'])==13 and fuzz['gate']=='passed'
    for row in fuzz['campaigns']:
        assert row['exit_code']==0 and row['seed_provenance']['initial_cache_empty']and row['executions']>row['seed_provenance']['initial_builtin_seed_count']*10
        assert row['budget_seconds']==(60 if 'CancellationLifetime'in row['name']else 30)
        for path,digest in row['seed_provenance']['retained_mutation_corpus'].items():assert hasher(Path(row['seed_provenance']['selected_cache'])/path)==digest
    architecture=reader(E/'step17-architecture.json');assert architecture['gate']=='passed'and not architecture['findings']and len(architecture['normal_and_all_tag_closures'])==3
    assert architecture['public_exports']==507 and architecture['thin_forwarders']==130 and architecture['session_methods']==9 and architecture['normal_external_consumer']['exit_code']==0
    config=reader(E/'step17-config-audit.json');assert len(config['fields'])==106 and config['gate']=='current-source-witnesses-complete'
    assert all(r['implementation_symbols']and r['current_behavioral_test_witnesses']and r['status']=='evidence-attached'for r in config['fields'])
    inventory=reader(E/'step17-requirement-inventory.json');assert len(inventory['documents'])==10 and sum(len(d['items'])for d in inventory['documents'])==2790
    assert not inventory['counts'].get('open',0)
    for doc in inventory['documents']:
        assert hasher(ROOT/doc['document'])==doc['sha256']
        for item in doc['items']:
            assert item['status']in ['primary-evidence-mapped','structural-no-obligation']
            if item['status']!='structural-no-obligation':assert item['proof_groups']and item['proof']
            confirmation=item['individual_scope_confirmation']
            assert confirmation['classification']not in ['open','awaiting-native-evidence','awaiting-final-artifact-evidence']
            assert not confirmation.get('missing_current_passes')and not confirmation.get('missing_artifacts')
    manual=reader(E/'step17-manual-audit.json')
    assert manual['runtime_source_sha256']==binding and len(manual['records'])==2790 and manual['gate']=='Go-contract-paragraphs-reviewed'and not manual['problems']
    for row in manual['records']:assert not row['missing_current_passes']and not row['missing_artifacts']
    proofs=reader(E/'step17-proof-catalog.json');assert proofs['runtime_source_sha256']==binding and not proofs['missing']and len(proofs['groups'])==40
    assert all(g['status']=='current-assertions-passed'and not g['missing_effective_passes']for g in proofs['groups'].values())
    performance=reader(E/'step17-performance.json');assert performance['runtime_source_sha256']==binding and performance['gate']=='passed'
    assert len(performance['requirements'])==23 and len(performance['modes'])==16
    assert all(row['status']=='current-evidence-passed'and row['tests']and row['measurements']for row in performance['requirements'])
    expected_shapes={'small','page1','page32','rows1','rows128','cell256K','cell8M','concurrent','slow'}
    normalize=lambda x:sorted((n['host_id'],n['host_connections'],n['missing_connections'],n['shards'],n['shard'],n['protocol'],n['compression'],n['metadata_id'],n.get('tls_version'),n.get('tls_cipher'),n['authentication_fixture'])for n in x['Negotiated'])
    for mode in performance['modes']:
        values=[x for x in performance['results']if x['mode']==mode['name']]
        apis={'row','into','legacy-scan'}|({'stream'}if '-v4-snappy'not in mode['name']and'-v4-lz4'not in mode['name']else set())
        assert len(values)==9*len(apis)and{x['API']for x in values}==apis
        for api in apis:assert{x['Shape']['Name']for x in values if x['API']==api}==expected_shapes
        expected=normalize(next(x for x in values if x['API']=='legacy-scan'));assert expected
        assert all(normalize(x)==expected for x in values)
    latency=reader(OUT/'latency-costs/verification.json');assert len(latency['checks'])==6
    assert hasher(ROOT/'internal/routing/v2_policy.go')==latency['current_production_sha256']
    preservation=reader(E/'step17-preservation.json');assert preservation['gate']=='passed'and preservation['original_identities']==174
    assert len(preservation['replay']['cases'])==60 and len(set(preservation['replay']['cases']))==20 and len(preservation['replay']['benchmark_cases'])==20
    cleanup=reader(E/'step17-cleanup.json');assert cleanup['gate']=='passed'and len(cleanup['lanes'])==11 and len(cleanup['endpoints'])==63
    assert not cleanup['matching_process_survivors']and all(r['gone']for r in cleanup['processes'])and all(r['refused']for r in cleanup['endpoints'])
    assert len(cleanup['cancellation_probe']['signals_sent'])==5
    return dict(gate='passed',runtime_source_sha256=binding,original_identities=174,native_modes=17,streaming_groups=16,requirements=2790,goal_complete=False,root_audit_required=True)

def negative_controls(candidate):
    rows=[]
    def probe(name,read_mutation=None,hash_mutation=None,candidate_mutation=None):
        value=copy.deepcopy(candidate)
        if candidate_mutation:candidate_mutation(value)
        def reader(path):
            data=copy.deepcopy(matrix.read(path))
            return read_mutation(str(path),data)if read_mutation else data
        def hasher(path):
            digest=matrix.sha(path)
            return hash_mutation(str(path),digest)if hash_mutation else digest
        try:verify(value,reader,hasher)
        except (AssertionError,KeyError,IndexError)as error:rows.append(dict(name=name,rejected=True,error=str(error)))
        else:raise AssertionError('false passing negative control '+name)
    probe('runtime-drift',hash_mutation=lambda p,h:'0'*64 if p==str(ROOT/'internal/routing/v2_policy.go')else h)
    probe('artifact-hash-drift',hash_mutation=lambda p,h:'0'*64 if p==str(E/'step17-fuzz.json')else h)
    probe('missing-original-test',read_mutation=lambda p,v:dict(v,tests=v['tests'][1:])if p.endswith('step17-test-mapping.json')else v)
    def nested(path,value):
        if path.endswith('step17-test-mapping.json'):
            row=next(r for r in value['tests']if r['expected_translated_subcases']);row['effective_subcase_passes'].pop(row['expected_translated_subcases'][0])
        return value
    probe('missing-translated-nested-pass',read_mutation=nested)
    probe('missing-native-mode',read_mutation=lambda p,v:dict(v,modes=v['modes'][:-1])if p.endswith('step17-lanes.json')else v)
    probe('missing-complete-tag-link',read_mutation=lambda p,v:v[:-1]if p.endswith('complete-tag-checks.json')else v)
    probe('missing-streaming-mode',read_mutation=lambda p,v:dict(v,live_modes=v['live_modes'][:-1])if p.endswith('step17-streaming-acceptance.json')else v)
    probe('missing-config-field',read_mutation=lambda p,v:dict(v,fields=v['fields'][:-1])if p.endswith('step17-config-audit.json')else v)
    probe('open-requirement',read_mutation=lambda p,v:dict(v,counts=dict(v['counts'],open=1))if p.endswith('step17-requirement-inventory.json')else v)
    probe('missing-performance-table-row',read_mutation=lambda p,v:dict(v,requirements=v['requirements'][:-1])if p.endswith('step17-performance.json')else v)
    probe('missing-performance-result-shape',read_mutation=lambda p,v:dict(v,results=v['results'][:-1])if p.endswith('step17-performance.json')else v)
    def negotiation(path,value):
        if path.endswith('step17-performance.json'):value['results'][0]['Negotiated'][0]['host_connections']+=1
        return value
    probe('actual-negotiated-capacity-mismatch',read_mutation=negotiation)
    probe('missing-fuzz-campaign',read_mutation=lambda p,v:dict(v,campaigns=v['campaigns'][:-1])if p.endswith('step17-fuzz.json')else v)
    probe('missing-process-join',read_mutation=lambda p,v:dict(v,processes=[dict(v['processes'][0],gone=False)]+v['processes'][1:])if p.endswith('step17-cleanup.json')else v)
    assert not matrix.effective({'T':'pass','T/a':'pass','T/a/only':'skip'},'T')
    rows.append(dict(name='hollow-parent-all-terminal-children-skip',rejected=True))
    import tempfile
    with tempfile.NamedTemporaryFile('w')as log:
        events=[dict(Package='p',Test='T',Action='pass'),dict(Package='p',Action='fail')]
        log.write(''.join(json.dumps(e)+'\n'for e in events));log.flush()
        assert not matrix.package_outcomes(dict(exit_code=0,log=log.name))
        assert not matrix.package_outcomes(dict(exit_code=1,log=log.name))
    rows.extend([dict(name='failed-whole-package-despite-zero-command-exit',rejected=True),dict(name='nonzero-command-exit',rejected=True)])
    matrix.write(E/'step17-negative-controls.json',dict(probes=rows,all_rejected=True,source_logs_receipts_unchanged=True))
    return rows

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--negative-controls',action='store_true');args=parser.parse_args()
    candidate=matrix.read(E/'step17-verification.json');print(json.dumps(verify(candidate)))
    if args.negative_controls:print(json.dumps(dict(negative_controls=len(negative_controls(candidate)))))
