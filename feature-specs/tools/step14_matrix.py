# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Verify same-source native/package receipts and original behavioral identities.

Rejects failed package credit, missing identities, hash drift, and hollow parent
passes. Historical evidence supplies assertion lineage and expected subcases;
only current successful package events can satisfy behavioral gates.
"""
import argparse
import hashlib
from functools import cache
import subprocess
import json
from pathlib import Path
import re

ROOT=Path(__file__).resolve().parents[2]
TASK=Path('/extra/scylladb/gocql-api-test-cluster/step14/verified')
E=ROOT/'feature-specs/evidence'
BASE='75f015a390167da694f99b5d326dbc41fce39111'

def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def read(path):return json.loads(Path(path).read_text())
def write(path,value):Path(path).write_text(json.dumps(value,sort_keys=True,indent=2)+'\n')
@cache
def events(path):
    out=[]
    for line in Path(path).read_text().splitlines():
        try:out.append(json.loads(line))
        except ValueError:pass
    return out

def effective(tests,name):
    if tests.get(name)!='pass':return False
    children=[key for key in tests if key.startswith(name+'/')]
    if not children:return True
    # Internal parents whose only terminal descendants skipped carry no credit.
    leaves=[key for key in children if not any(x.startswith(key+'/') for x in children)]
    return any(tests[key]=='pass' for key in leaves)

def package_outcomes(command):
    if command['exit_code']!=0:return {}
    parsed=events(command['log'])
    packages={e['Package']:e['Action'] for e in parsed if 'Package' in e and 'Test' not in e and e.get('Action') in ('pass','fail','skip')}
    tests={}
    for e in parsed:
        if e.get('Test') and e.get('Action') in ('pass','fail','skip'):
            if packages.get(e['Package'])=='pass':tests[(e['Package'],e['Test'])]=e['Action']
    return tests

def audit(allow_incomplete=False):
    problems=[];native={};commands={};source=None
    cassandra_modes=['v5-no-compression','v5-lz4','v5-no-compression-tls','v5-lz4-tls','v4-no-compression','v4-no-compression-tls','v4-lz4','v4-lz4-tls']
    lane_names=['stable','development','stable-v3tls']+['cassandra-'+name for name in cassandra_modes]
    for lane in lane_names:
        receipt_path=TASK/(lane+'.json')
        if not receipt_path.exists():
            problems.append('mandatory fixture receipt missing '+lane);continue
        receipt=read(receipt_path);native[lane]=receipt
        source=source or receipt['source_sha256']
        if receipt['source_sha256']!=source:problems.append('source mismatch '+lane)
        if receipt.get('status')!='passed':problems.append('native lane not passed '+lane)
        if sha(receipt['source_snapshot'])!=source:problems.append('source snapshot hash mismatch '+lane)
        if sha(TASK/(lane+'-config.json'))!=receipt['config_sha256']:problems.append('config hash mismatch '+lane)
        configuration=receipt['configuration']['lanes'][0]
        if sha(Path(configuration['install'])/configuration['binary'])!=configuration['sha256']:problems.append('server binary drift '+lane)
        required_modes={'stable':{'v4-'+c+suffix for c in ('no-compression','snappy','lz4') for suffix in ('','-tls')}|{'v3-maintenance-stream'},'cassandra':{'v'+str(v)+'-'+c+suffix for v in (4,5) for c in ('no-compression','lz4') for suffix in ('','-tls')},'development':{'v4-development'},'stable-v3tls':{'v3-tls-stream'}}
        if lane.startswith('cassandra-'):required_modes[lane]={lane.removeprefix('cassandra-')}
        if {m['name'] for m in configuration['modes']}!=required_modes[lane]:problems.append('mandatory configured mode identities differ '+lane)
        for mode in configuration['modes']:
            protocol=3 if mode['name'].startswith('v3-') else int(mode['name'][1])
            if '-proto='+str(protocol) not in mode['args']:problems.append('protocol argument differs '+lane)
            if mode['name'].endswith('-tls') or mode['name']=='v3-tls-stream':
                if '-runssl' not in mode['args']:problems.append('TLS argument missing '+lane)
            if configuration['kind']=='cassandra' and mode['tests']!='^Test':problems.append('Cassandra package selection narrowed '+lane)
        if lane=='stable' and not any(c['name'].endswith('-ccm') and c['exit_code']==0 for c in receipt.get('commands',[])):problems.append('actual CCM missing')
        for row in receipt['lanes']:
            if len(row.get('health',[]))!=3:problems.append('native health missing '+lane)
            for path,digest in row.get('configs',{}).items():
                if sha(path)!=digest:problems.append('server config drift '+path)
            if not all(row.get('joined',{}).values()) or not row.get('joined') or not row.get('endpoints_refused'):problems.append('missing cleanup/join '+lane)
            for mode in receipt['configuration']['lanes'][0]['modes']:
                name=row['name']+'-'+mode['name']
                command=next((c for c in receipt.get('commands',[]) if c['name']==name),None)
                if not command or command['exit_code']!=0:problems.append('mandatory mode not passed '+name)
                if command:
                    argv=command['command']
                    if '-run' not in argv or argv[argv.index('-run')+1]!=mode['tests']:problems.append('actual selected test scope differs '+name)
                    if not all(arg in argv for arg in mode['args']):problems.append('actual mode arguments differ '+name)
                    package_events=events(command['log'])
                    if not any(e.get('Action')=='pass' and e.get('Package')=='github.com/scylladb/gocql/v2' and 'Test' not in e for e in package_events):problems.append('whole native package pass event missing '+name)
                    if mode['tests']=='^Test':
                        root_tests={r['test'] for r in read(E/'step13-assertion-source.json')['tests'] if Path(r['translated']['file']).parent==Path('.')}
                        terminals={e['Test'] for e in package_events if e.get('Test') and e.get('Action') in ('pass','skip','fail') and e.get('Package')=='github.com/scylladb/gocql/v2'}
                        if root_tests-terminals:problems.append('full native package omitted original identities '+name+': '+repr(sorted(root_tests-terminals)))
        for command in receipt.get('commands',[]):
            if sha(command['log'])!=command['sha256']:problems.append('log hash drift '+command['name'])
            if command['exit_code']!=0:problems.append('native command failed '+command['name'])
            commands[lane+'/'+command['name']]=command
    paths=read(native['stable']['source_snapshot'])
    current_names=set()
    for name in subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=ROOT).decode().split('\0'):
        path=ROOT/name
        if path.is_file() and not name.startswith('feature-specs/') and '__pycache__' not in name and path.suffix in ('.go','.mod','.sum','.py','.java','.cql','.crt','.key','.p12','.cnf','.sh'):current_names.add(name)
    if current_names!=set(paths):problems.append('current source file set differs from pin')
    for path,digest in paths.items():
        if sha(ROOT/path)!=digest:problems.append('source drift '+path)
    checks=read(TASK/'checks/checks.json')
    required_checks={'spec-verifier','fixture-python','unit-race','vet','all-tags','386','arm64-build','module-verify','cost-fragmented','cost-comparison'}|{'fuzz-'+name for name in ('FuzzStreamCells','FuzzStreamEnvelope','FuzzValidatedUnits','FuzzV2StreamingValues','FuzzV2ScalarUTF8','FuzzV2ExclusiveIncrementalFraming','FuzzStep07StreamExecutionBoundary','FuzzV2PagingHistoryBudgets','FuzzV2PageDestinationFraming','FuzzStep06TabletPayload','FuzzSpanCompression','FuzzStep14StreamingCancellationLifetime')}
    if {c['name'] for c in checks}!=required_checks:problems.append('mandatory check/fuzz commands incomplete')
    for command in checks:
        if command['source_sha256']!=source:problems.append('check source mismatch '+command['name'])
        if command['exit_code']!=0:problems.append('check failed '+command['name'])
        if sha(command['log'])!=command['sha256']:problems.append('check log hash drift '+command['name'])
        commands['checks/'+command['name']]=command
    baseline=read(ROOT/'feature-specs/integration-baseline.json')
    expected={row['file']+'::'+name for row in baseline['files'] for name in row['tests']}
    ledger=read(E/'step13-assertion-source.json');lineage=read(E/'step09-test-mapping.json')
    if expected!={row['identity'] for row in ledger['tests']} or len(expected)!=174:problems.append('original identity ledger mismatch')
    decl=read(TASK/'current-declarations.json')
    declarations={(d['File'],d['Receiver'],d['Name']):d for d in decl}
    previous={row['identity']:row for row in lineage['tests']}
    parsed={name:package_outcomes(c) for name,c in commands.items()}
    mapping=[]
    for row in ledger['tests']:
        identity=row['identity'];name=row['test'];body=row['translated']
        current=declarations.get((body['file'],'',name))
        if not current or current['Source']!=body['source']:problems.append('translated assertions changed '+identity)
        package='github.com/scylladb/gocql/v2'+('/'+str(Path(body['file']).parent) if Path(body['file']).parent!=Path('.') else '')
        outcomes=[];passes=[];allchildren={}
        for command_name,result in parsed.items():
            tests={test:action for (pkg,test),action in result.items() if pkg==package}
            if name not in tests:continue
            children={test:action for test,action in tests.items() if test.startswith(name+'/')}
            passed=effective(tests,name)
            # Only native commands supply original integration behavioral credit.
            if passed and not command_name.startswith('checks/'):passes.append(command_name)
            for test,action in children.items():
                if effective(tests,test) and not command_name.startswith('checks/'):allchildren.setdefault(test,[]).append(command_name)
            log_output=[e.get('Output','').strip() for e in events(commands[command_name]['log']) if e.get('Test')==name and e.get('Output') and ('skip' in e.get('Output','').lower() or '.go:' in e.get('Output',''))]
            outcomes.append(dict(command_id=command_name,outcome=tests[name],effective_behavior_pass=passed,subtests=children,skip_or_assertion_output=log_output))
        # Preserve every historical translated subcase that actually passed.
        # This is an identity requirement, never reuse of historical pass credit.
        expectedchildren={c['test'] for o in previous[identity]['outcomes'] for c in o['subtests'] if c['outcome']=='pass'}
        missingchildren=sorted(expectedchildren-allchildren.keys())
        if not passes:problems.append('no effective native pass '+identity)
        if missingchildren:problems.append('missing translated subcase passes '+identity+': '+repr(missingchildren))
        mapping.append(dict(identity=identity,original_sha256=row['original']['sha256'],translated_sha256=body['sha256'],current_sha256=current['SHA'] if current else None,complete_source_unchanged=bool(current and current['Source']==body['source']),translated_assertions=previous[identity]['translated_assertions'],assertions=body['assertions'],fixture_requirements=previous[identity]['fixture_requirements'],direct_skip_gates=previous[identity]['direct_skip_gates'],helper_skip_gates=previous[identity]['helper_skip_gates'],expected_translated_subcases=sorted(expectedchildren),effective_subcase_passes=allchildren,missing_subcase_passes=missingchildren,current_effective_passes=passes,outcomes=outcomes))
    # Helpers and supporting methods preserve the accepted complete sources too.
    preserved={}
    for group in ('helpers','supporting_methods'):
        rows=[]
        for identity,body in ledger[group]['translated'].items():
            member=identity.split('::')[1];receiver=''
            if '.' in member:receiver,member=member.split('.',1);receiver=re.sub(r'\[.*\]','',receiver)
            current=declarations.get((body['file'],receiver,member));same=bool(current and current['Source']==body['source'])
            if not same:problems.append('preserved assertion helper changed '+identity)
            rows.append(dict(identity=identity,sha256=body['sha256'],same_complete_source=same))
        preserved[group]=rows
    report=dict(branch='v2-14-full-matrix',accepted_predecessor=BASE,source_sha256=source,source_snapshot='step14-source.json',native=native,checks=checks,baseline_sha256=sha(ROOT/'feature-specs/integration-baseline.json'),assertion_lineage_sha256=sha(E/'step13-assertion-source.json'),identities=len(mapping),current_effective_native_passes=sum(bool(x['current_effective_passes']) for x in mapping),tests=mapping,preservation=preserved,problems=problems,gate='pending' if problems else 'passed')
    write(E/'step14-source.json',paths);write(E/'step14-test-mapping.json',report)
    print(json.dumps(dict(source_sha256=source,identities=len(mapping),effective_passes=report['current_effective_native_passes'],problems=problems),indent=2))
    if problems and not allow_incomplete:raise SystemExit(1)
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--allow-incomplete',action='store_true');audit(parser.parse_args().allow_incomplete)
