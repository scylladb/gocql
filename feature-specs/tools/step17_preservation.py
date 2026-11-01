# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Reconcile original assertions/helpers, test-only changes and exact replay cases."""
import json,re,subprocess
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
BASE='bc3d9fe132c4c1f5dc16050870cfd4991d322a73'
def build():
    current=matrix.read(OUT/'checks/source.json')
    old=matrix.read(ROOT/'feature-specs/evidence/step16-verification.json')['preservation']
    files=[];changes=[]
    for row in old['files']:
        path=row['file'];digest=matrix.sha(ROOT/path);same=digest==row['sha256']
        files.append(dict(file=path,previous_sha256=row['sha256'],current_sha256=digest,unchanged=same))
        if not same:changes.append(path)
    assert changes==['v2_tablet_routing_integration_test.go'],changes
    delta=subprocess.check_output(['git','diff',BASE,'--name-only','--','*.go'],cwd=ROOT,text=True).splitlines()
    assert set(delta)==set(changes+['internal/routing/v2_policy.go']),delta
    tracked=set(subprocess.check_output(['git','ls-tree','-r','--name-only',BASE],cwd=ROOT,text=True).splitlines())
    added=sorted(p for p in current if p.endswith('.go')and p not in tracked)
    assert added==['internal/routing/step17_deadlines_test.go','step17_config_behavior_test.go','step17_tablet_oracle_test.go'],added
    # The matrix independently matches all complete original declarations and
    # translated helper/method sources, and all expected current nested cases.
    mapping=matrix.read(E/'step17-test-mapping.json')
    assert mapping['identities']==mapping['current_effective_native_passes']==174
    assert not mapping['problems']
    assert all(t['complete_source_unchanged']and not t['missing_subcase_passes']for t in mapping['tests'])
    assert all(row['same_complete_source']for rows in mapping['preservation'].values()for row in rows)
    replay=next(c for c in matrix.read(OUT/'checks/checks.json')if c['name']=='replay-operations')
    events=matrix.events(replay['log'])
    cases=[e['Test']for e in events if e.get('Action')=='pass'and re.fullmatch(r'TestSingleConnectionReplay/(select|insert)/Case[0-9]',e.get('Test',''))]
    expected={'TestSingleConnectionReplay/'+op+'/Case'+str(i)for op in ['select','insert']for i in range(10)}
    assert len(cases)==60 and set(cases)==expected
    bench=next(c for c in matrix.read(OUT/'checks/checks.json')if c['name']=='replay-benchmark')
    bench_cases=re.findall(r'(?m)^(BenchmarkSingleConnection(?:Select|Insert)/Case[0-9])-\d+\s',Path(bench['log']).read_text())
    assert len(bench_cases)==20 and len(set(bench_cases))==20
    corpus=matrix.read(ROOT/'tests/bench/corpus.json');assert corpus['namespace'].startswith('g')and corpus['protocol']==4
    result=dict(runtime_source_sha256=matrix.sha(OUT/'checks/source.json'),accepted_predecessor=BASE,preexisting_test_files=files,added_test_files={p:matrix.sha(ROOT/p)for p in added},changed_preexisting_test_files=changes,production_changes=['internal/routing/v2_policy.go'],original_identities=174,preserved_helpers=len(mapping['preservation']['helpers']),preserved_supporting_methods=len(mapping['preservation']['supporting_methods']),expected_subcase_identities=sum(len(t['expected_translated_subcases'])for t in mapping['tests']),final_effective_subcase_passes=sum(len(t['effective_subcase_passes'])for t in mapping['tests']),test_only_change='Mutable tablet authority is now bracketed on every host, rejects transitions/disagreement, preserves three cold host pins, and requires eight consecutive exact cache/coordinator matches within96total attempts. Set equality rejects duplicate replicas and wrong shards. All original174bodies/helpers/methods remain unchanged.',production_fix='Positive RetryPeriod/UpdateInterval deadlines no longer narrow to UnixNano. Existing per-host sample mutex guards the full Time probe deadline; immutable publication deadline pointers preserve CAS without a policy-wide mutex.',replay=dict(operational_command=replay,cases=cases,case_identities=sorted(expected),benchmark_command=bench,benchmark_cases=bench_cases,corpus_config=corpus,recordings={str(p.relative_to(ROOT)):matrix.sha(p)for p in sorted((ROOT/'tests/bench').rglob('*'))if p.is_file()and p.suffix in ['.json','.txt','.bin','.golden']}),gate='passed')
    matrix.write(E/'step17-preservation.json',result)
    print(json.dumps(dict(original=174,helpers=result['preserved_helpers'],methods=result['preserved_supporting_methods'],changed_test_files=len(changes),new_test_files=len(added),replay_passes=len(cases),replay_benchmarks=len(bench_cases))))
    return result
if __name__=='__main__':build()
