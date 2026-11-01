# Copyright 2026 The Gocql Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license.
"""Verify fresh per-target caches, seed/oracle sources, budgets and mutations."""
import json,re,subprocess
from pathlib import Path
import step17_matrix as matrix
ROOT=matrix.ROOT;OUT=matrix.TASK;E=matrix.E
SEEDS=dict(FuzzStreamCells=2,FuzzStreamEnvelope=2,FuzzValidatedUnits=2,FuzzV2StreamingValues=4,FuzzV2ScalarUTF8=6,FuzzV2ExclusiveIncrementalFraming=3,FuzzStep07StreamExecutionBoundary=3,FuzzV2PagingHistoryBudgets=2,FuzzV2PageDestinationFraming=2,FuzzStep06TabletPayload=1,FuzzSpanCompression=3,FuzzStep14StreamingCancellationLifetime=8,FuzzCursorTake=3)
def build():
    declarations=matrix.read(OUT/'current-declarations.json');byname={d['Name']:d for d in declarations if d['Name']in SEEDS}
    campaigns=[];source=matrix.sha(OUT/'checks/source.json')
    for command in matrix.read(OUT/'checks/checks.json'):
        if not command['name'].startswith('fuzz-'):continue
        name=command['name'][5:];d=byname[name];raw=Path(command['log']).read_text()
        baseline=re.search(r'gathering baseline coverage: 0/(\d+) completed',raw)
        assert baseline and int(baseline[1])==SEEDS[name],(name,'unexpected initial corpus')
        assert command['exit_code']==0 and command['source_sha256']==source and matrix.sha(command['log'])==command['sha256']
        cache=next(Path(arg.split('=',1)[1])for arg in command['command']if arg.startswith('-test.fuzzcachedir='))
        assert cache==OUT/'fuzz-corpus'/name
        summary=[line for line in raw.splitlines()if 'execs:' in line][-1]
        executions=int(re.search(r'execs: (\d+)',summary)[1]);assert executions>SEEDS[name]*10
        budget=60 if name=='FuzzStep14StreamingCancellationLifetime'else 30
        assert '-fuzztime='+str(budget)+'s' in command['command'] and '-parallel=2'in command['command'] and command['seconds']>=budget
        package=command['command'][command['command'].index('-timeout=3m')+1]
        testdata=ROOT/package/'testdata/fuzz'/name
        seeds={str(p.relative_to(ROOT)):matrix.sha(p)for p in testdata.rglob('*')if p.is_file()}if testdata.exists()else {}
        assert not seeds,'initial builtins-only claim must include source testdata corpus'
        campaigns.append(dict(**command,budget_seconds=budget,executions=executions,summary=summary,seed_provenance=dict(initial_cache_empty=True,enforcement='The executed step17_checks.py asserts the target cache path absent, creates it, then invokes this command. Its observed initial seed count equals the explicit builtins. Actual interesting mutations were saved beneath this unique override.',selected_cache=str(cache),initial_builtin_seed_count=SEEDS[name],source_testdata_corpus=seeds,source_file=d['File'],complete_oracle_source=d['Source'],oracle_source_sha256=d['SHA'],retained_mutation_corpus={str(p.relative_to(cache)):matrix.sha(p)for p in cache.rglob('*')if p.is_file()})))
    assert {r['name'][5:]for r in campaigns}==set(SEEDS)
    goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip())
    provenance=dict(go_version=subprocess.check_output(['go','version'],text=True).strip(),implementation_sources={str(p):matrix.sha(p)for p in [goroot/'src/testing/fuzz.go',goroot/'src/cmd/go/internal/test/test.go',goroot/'src/flag/flag.go']},selection='cmd/go prepends its default fuzzcachedir. testing registers a scalar string flag; flag.Parse applies occurrences in argument order, so the appended user override wins. Builtins-only initial coverage and actual saved files beneath every requested fresh override independently establish selected caches.')
    result=dict(runtime_source_sha256=source,campaigns=campaigns,flag_selection_provenance=provenance,persistence_scope='Saved interesting mutation corpus is retained and hashed; it is not every in-memory execution.',gate='passed')
    matrix.write(E/'step17-fuzz.json',result)
    print(json.dumps(dict(campaigns=len(campaigns),executions=sum(r['executions']for r in campaigns),saved_mutations=sum(len(r['seed_provenance']['retained_mutation_corpus'])for r in campaigns))))
    return result
if __name__=='__main__':build()
